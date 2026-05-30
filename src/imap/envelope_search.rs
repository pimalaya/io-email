//! IMAP envelope-search coroutine.
//!
//! Composes `SELECT <mailbox>` with `UID SORT <criteria> UTF-8 <keys>`
//! (RFC 5256) and `UID FETCH <uids> (UID FLAGS ENVELOPE RFC822.SIZE
//! [BODYSTRUCTURE])` (RFC 3501 §6.4.5).
//!
//! The shared filter AST is translated to RFC 9051 §6.4.4 `SearchKey`
//! clauses (date filters target the `Date:` header through `SENTON` /
//! `SENTSINCE`; the sent-at rule must stay consistent across every
//! backend). An absent filter defaults to `[ALL]`; an absent sort
//! defaults to `REVERSE DATE` (date descending).
//!
//! UID SORT requires the server to advertise the `SORT` capability
//! (RFC 5256). When unsupported, the server returns `BAD` and the
//! error surfaces as [`ImapEnvelopeSearchError::Sort`]; callers can
//! decide to fall back to plain `UID SEARCH` + client-side sort.

use alloc::{
    boxed::Box,
    collections::BTreeMap,
    string::{String, ToString},
    vec,
    vec::Vec,
};
use core::{mem, num::NonZeroU32};

use io_imap::{
    codec::fragmentizer::Fragmentizer,
    coroutine::{ImapCoroutine, ImapCoroutineState, ImapYield},
    rfc3501::{
        fetch::{ImapMessageFetch, ImapMessageFetchError},
        select::{ImapMailboxSelect, ImapMailboxSelectError},
    },
    rfc5256::sort::{ImapMailboxSort, ImapMailboxSortError},
    types::{
        core::{AString, Atom, Vec1},
        datetime::NaiveDate as ImapNaiveDate,
        extensions::sort::{SortCriterion, SortKey},
        fetch::{MacroOrMessageDataItemNames, MessageDataItem},
        search::SearchKey,
        sequence::SequenceSet,
    },
};
use log::trace;
use thiserror::Error;

use crate::{
    envelope::Envelope,
    flag::IanaFlag,
    imap::{
        convert::{InvalidMailboxName, parse_mailbox},
        envelope_list::{build_item_names, envelope_from},
    },
    search::{
        filter::query::SearchEmailsFilterQuery,
        query::SearchEmailsQuery,
        sort::query::{SearchEmailsSorter, SearchEmailsSorterKind, SearchEmailsSorterOrder},
    },
};

/// Errors produced by [`ImapEnvelopeSearch`].
#[derive(Debug, Error)]
pub enum ImapEnvelopeSearchError {
    #[error(transparent)]
    Select(#[from] ImapMailboxSelectError),
    #[error(transparent)]
    Sort(#[from] ImapMailboxSortError),
    #[error(transparent)]
    Fetch(#[from] ImapMessageFetchError),
    #[error("invalid IMAP mailbox `{0}`")]
    InvalidMailbox(String),
    #[error("invalid IMAP search pattern `{0}`")]
    InvalidPattern(String),
    #[error("invalid IMAP keyword `{0}`")]
    InvalidKeyword(String),
    #[error("invalid IMAP date `{0}`")]
    InvalidDate(String),
    #[error("invalid IMAP UID set `{0}`")]
    InvalidUidSet(String),
    #[error("coroutine was resumed after completion")]
    ResumedAfterDone,
}

impl From<InvalidMailboxName> for ImapEnvelopeSearchError {
    fn from(err: InvalidMailboxName) -> Self {
        Self::InvalidMailbox(err.0)
    }
}

/// I/O-free coroutine listing envelopes matching the shared search
/// query from a mailbox. Pagination is applied to the SORT-ordered UID
/// list before FETCH; `page = 1`-indexed; `page_size = None` keeps the
/// whole result set.
pub struct ImapEnvelopeSearch {
    state: State,
}

impl ImapEnvelopeSearch {
    pub fn new(
        mailbox: &str,
        query: Option<&SearchEmailsQuery>,
        page: Option<u32>,
        page_size: Option<u32>,
        with_attachment: bool,
    ) -> Result<Self, ImapEnvelopeSearchError> {
        trace!("prepare IMAP envelope search");
        let mbox = parse_mailbox(mailbox)?;
        let search_criteria = search_keys(query.and_then(|q| q.filter.as_ref()))?;
        let sort_criteria = sort_criteria(query.and_then(|q| q.sort.as_deref()));
        let item_names = build_item_names(with_attachment);

        Ok(Self {
            state: State::Selecting {
                select: ImapMailboxSelect::new(mbox),
                page,
                page_size,
                item_names,
                search_criteria,
                sort_criteria,
            },
        })
    }
}

enum State {
    Selecting {
        select: ImapMailboxSelect,
        page: Option<u32>,
        page_size: Option<u32>,
        item_names: MacroOrMessageDataItemNames<'static>,
        search_criteria: Vec1<SearchKey<'static>>,
        sort_criteria: Vec1<SortCriterion>,
    },
    Sorting {
        sort: ImapMailboxSort,
        page: Option<u32>,
        page_size: Option<u32>,
        item_names: MacroOrMessageDataItemNames<'static>,
    },
    Fetching {
        fetch: ImapMessageFetch,
        order: Vec<u32>,
    },
    Done,
}

/// Builds the IMAP `SEARCH` key list for the given filter, defaulting
/// to `[ALL]` when no filter is provided.
fn search_keys(
    filter: Option<&SearchEmailsFilterQuery>,
) -> Result<Vec1<SearchKey<'static>>, ImapEnvelopeSearchError> {
    let key = match filter {
        None => SearchKey::All,
        Some(filter) => convert_filter(filter)?,
    };
    Ok(Vec1::from(key))
}

/// Builds the IMAP `SORT` criterion list for the given sort chain,
/// defaulting to `REVERSE DATE` when the chain is empty or absent.
fn sort_criteria(sort: Option<&[SearchEmailsSorter]>) -> Vec1<SortCriterion> {
    let criteria: Vec<SortCriterion> = match sort {
        Some(chain) if !chain.is_empty() => chain.iter().map(convert_sorter).collect(),
        _ => vec![SortCriterion {
            reverse: true,
            key: SortKey::Date,
        }],
    };

    Vec1::try_from(criteria).expect("non-empty by construction")
}

/// Slices `uids` according to `(page, page_size)`, preserving SORT
/// order. `page = None` is treated as page 1; `page_size = None` keeps
/// the whole list.
fn paginate_uids(uids: &[NonZeroU32], page: Option<u32>, page_size: Option<u32>) -> Vec<u32> {
    let total = uids.len();
    let size = page_size.map(|n| n as usize);
    let start = ((page.unwrap_or(1).max(1) - 1) as usize).saturating_mul(size.unwrap_or(0));

    if start >= total {
        return Vec::new();
    }

    let end = match size {
        Some(n) => start.saturating_add(n).min(total),
        None => total,
    };

    uids[start..end].iter().map(|u| u.get()).collect()
}

fn convert_filter(
    filter: &SearchEmailsFilterQuery,
) -> Result<SearchKey<'static>, ImapEnvelopeSearchError> {
    use SearchEmailsFilterQuery as Q;

    Ok(match filter {
        Q::And(left, right) => {
            let keys = vec![convert_filter(left)?, convert_filter(right)?];
            SearchKey::And(Vec1::try_from(keys).expect("non-empty by construction"))
        }
        Q::Or(left, right) => SearchKey::Or(
            Box::new(convert_filter(left)?),
            Box::new(convert_filter(right)?),
        ),
        Q::Not(inner) => SearchKey::Not(Box::new(convert_filter(inner)?)),

        // `Date(D)` is "Date: header on day D", same shape as IMAP
        // `SENTON`.
        Q::Date(date) => SearchKey::SentOn(imap_date(*date)?),

        // `AfterDate(D)` is the strict `Date: header > D`. IMAP
        // `SENTSINCE D'` is `Date: header >= D'`, so bump by one day.
        Q::AfterDate(date) => {
            let bumped = date.succ_opt().unwrap_or(*date);
            SearchKey::SentSince(imap_date(bumped)?)
        }

        Q::From(pattern) => SearchKey::From(astring(pattern)?),
        Q::To(pattern) => SearchKey::To(astring(pattern)?),
        Q::Subject(pattern) => SearchKey::Subject(astring(pattern)?),
        Q::Body(pattern) => SearchKey::Body(astring(pattern)?),

        Q::Flag(flag) => {
            // IMAP exposes dedicated search keys for the four classic
            // system flags plus `\Deleted`; every other IANA keyword
            // and every custom keyword goes through `Keyword(Atom)`.
            match flag.iana() {
                Some(IanaFlag::Seen) => SearchKey::Seen,
                Some(IanaFlag::Answered) => SearchKey::Answered,
                Some(IanaFlag::Flagged) => SearchKey::Flagged,
                Some(IanaFlag::Draft) => SearchKey::Draft,
                Some(IanaFlag::Deleted) => SearchKey::Deleted,
                _ => SearchKey::Keyword(
                    Atom::try_from(String::from(flag.raw()))
                        .map_err(|_| ImapEnvelopeSearchError::InvalidKeyword(flag.raw().into()))?,
                ),
            }
        }
    })
}

fn convert_sorter(sorter: &SearchEmailsSorter) -> SortCriterion {
    let SearchEmailsSorter(kind, order) = sorter;

    let key = match kind {
        SearchEmailsSorterKind::Date => SortKey::Date,
        SearchEmailsSorterKind::From => SortKey::From,
        SearchEmailsSorterKind::To => SortKey::To,
        SearchEmailsSorterKind::Subject => SortKey::Subject,
    };

    SortCriterion {
        reverse: matches!(order, SearchEmailsSorterOrder::Descending),
        key,
    }
}

fn astring(pattern: &str) -> Result<AString<'static>, ImapEnvelopeSearchError> {
    AString::try_from(String::from(pattern))
        .map_err(|_| ImapEnvelopeSearchError::InvalidPattern(pattern.into()))
}

fn imap_date(date: chrono::NaiveDate) -> Result<ImapNaiveDate, ImapEnvelopeSearchError> {
    ImapNaiveDate::try_from(date)
        .map_err(|_| ImapEnvelopeSearchError::InvalidDate(date.to_string()))
}

/// Maps the FETCH response back into the requested UID order, dropping
/// UIDs the server failed to return.
fn reorder_envelopes(
    data: BTreeMap<NonZeroU32, Vec1<MessageDataItem<'static>>>,
    order: &[u32],
) -> Vec<Envelope> {
    let by_uid: BTreeMap<u32, Envelope> = data
        .into_iter()
        .map(|(seq, items)| {
            let items = items.into_inner();
            let uid = items.iter().find_map(|item| match item {
                MessageDataItem::Uid(u) => Some(u.get()),
                _ => None,
            });
            let env = envelope_from(seq.get(), items);
            (uid.unwrap_or(seq.get()), env)
        })
        .collect();

    order
        .iter()
        .filter_map(|u| by_uid.get(u).cloned())
        .collect()
}

impl ImapCoroutine for ImapEnvelopeSearch {
    type Yield = ImapYield;
    type Return = Result<Vec<Envelope>, ImapEnvelopeSearchError>;

    fn resume(
        &mut self,
        fragmentizer: &mut Fragmentizer,
        mut bytes: Option<&[u8]>,
    ) -> ImapCoroutineState<Self::Yield, Self::Return> {
        loop {
            match mem::replace(&mut self.state, State::Done) {
                State::Selecting {
                    mut select,
                    page,
                    page_size,
                    item_names,
                    search_criteria,
                    sort_criteria,
                } => match select.resume(fragmentizer, bytes.take()) {
                    ImapCoroutineState::Yielded(yielded) => {
                        self.state = State::Selecting {
                            select,
                            page,
                            page_size,
                            item_names,
                            search_criteria,
                            sort_criteria,
                        };
                        return ImapCoroutineState::Yielded(yielded);
                    }
                    ImapCoroutineState::Complete(Ok(data)) => {
                        if data.exists.unwrap_or(0) == 0 {
                            return ImapCoroutineState::Complete(Ok(Vec::new()));
                        }
                        let sort = ImapMailboxSort::new(sort_criteria, search_criteria, true);
                        self.state = State::Sorting {
                            sort,
                            page,
                            page_size,
                            item_names,
                        };
                    }
                    ImapCoroutineState::Complete(Err(err)) => {
                        return ImapCoroutineState::Complete(Err(err.into()));
                    }
                },
                State::Sorting {
                    mut sort,
                    page,
                    page_size,
                    item_names,
                } => match sort.resume(fragmentizer, bytes.take()) {
                    ImapCoroutineState::Yielded(yielded) => {
                        self.state = State::Sorting {
                            sort,
                            page,
                            page_size,
                            item_names,
                        };
                        return ImapCoroutineState::Yielded(yielded);
                    }
                    ImapCoroutineState::Complete(Ok(uids)) => {
                        if uids.is_empty() {
                            return ImapCoroutineState::Complete(Ok(Vec::new()));
                        }
                        let page_uids = paginate_uids(&uids, page, page_size);
                        if page_uids.is_empty() {
                            return ImapCoroutineState::Complete(Ok(Vec::new()));
                        }
                        let uid_str = page_uids
                            .iter()
                            .map(u32::to_string)
                            .collect::<Vec<_>>()
                            .join(",");
                        let sequence_set: SequenceSet = match uid_str.as_str().try_into() {
                            Ok(set) => set,
                            Err(_) => {
                                return ImapCoroutineState::Complete(Err(
                                    ImapEnvelopeSearchError::InvalidUidSet(uid_str),
                                ));
                            }
                        };
                        self.state = State::Fetching {
                            fetch: ImapMessageFetch::new(sequence_set, item_names, true),
                            order: page_uids,
                        };
                    }
                    ImapCoroutineState::Complete(Err(err)) => {
                        return ImapCoroutineState::Complete(Err(err.into()));
                    }
                },
                State::Fetching { mut fetch, order } => {
                    match fetch.resume(fragmentizer, bytes.take()) {
                        ImapCoroutineState::Yielded(yielded) => {
                            self.state = State::Fetching { fetch, order };
                            return ImapCoroutineState::Yielded(yielded);
                        }
                        ImapCoroutineState::Complete(Ok(data)) => {
                            return ImapCoroutineState::Complete(Ok(reorder_envelopes(
                                data, &order,
                            )));
                        }
                        ImapCoroutineState::Complete(Err(err)) => {
                            return ImapCoroutineState::Complete(Err(err.into()));
                        }
                    }
                }
                State::Done => {
                    return ImapCoroutineState::Complete(Err(
                        ImapEnvelopeSearchError::ResumedAfterDone,
                    ));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use alloc::boxed::Box;

    use chrono::NaiveDate;

    use super::*;
    use crate::flag::Flag;

    fn naive(y: i32, m: u32, d: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(y, m, d).unwrap()
    }

    #[test]
    fn empty_filter_yields_all() {
        let keys = search_keys(None).unwrap();
        let inner: Vec<_> = keys.into_inner();
        assert!(matches!(inner.as_slice(), [SearchKey::All]));
    }

    #[test]
    fn date_clauses_target_sent_at() {
        let q = SearchEmailsFilterQuery::Date(naive(2026, 1, 15));
        let key = convert_filter(&q).unwrap();
        assert!(matches!(key, SearchKey::SentOn(_)));
    }

    #[test]
    fn after_date_bumps_by_one_day() {
        let q = SearchEmailsFilterQuery::AfterDate(naive(2026, 1, 15));
        let key = convert_filter(&q).unwrap();
        match key {
            SearchKey::SentSince(d) => assert_eq!(*d.as_ref(), naive(2026, 1, 16)),
            other => panic!("expected SentSince, got {other:?}"),
        }
    }

    #[test]
    fn and_or_not_translate_to_searchkey_combinators() {
        let q = SearchEmailsFilterQuery::And(
            Box::new(SearchEmailsFilterQuery::From("alice".into())),
            Box::new(SearchEmailsFilterQuery::Not(Box::new(
                SearchEmailsFilterQuery::Subject("draft".into()),
            ))),
        );
        let key = convert_filter(&q).unwrap();
        assert!(matches!(key, SearchKey::And(_)));

        let q = SearchEmailsFilterQuery::Or(
            Box::new(SearchEmailsFilterQuery::From("a".into())),
            Box::new(SearchEmailsFilterQuery::From("b".into())),
        );
        let key = convert_filter(&q).unwrap();
        assert!(matches!(key, SearchKey::Or(_, _)));
    }

    #[test]
    fn flag_lcd_mapping() {
        for (iana, expected_seen) in [
            (IanaFlag::Seen, true),
            (IanaFlag::Answered, false),
            (IanaFlag::Flagged, false),
        ] {
            let key =
                convert_filter(&SearchEmailsFilterQuery::Flag(Flag::from_iana(iana))).unwrap();
            assert_eq!(matches!(key, SearchKey::Seen), expected_seen);
        }
    }

    #[test]
    fn flag_custom_keyword_becomes_imap_keyword() {
        let key = convert_filter(&SearchEmailsFilterQuery::Flag(Flag::from_raw("Work"))).unwrap();
        assert!(matches!(key, SearchKey::Keyword(_)));
    }

    #[test]
    fn empty_sort_defaults_to_date_descending() {
        let crit = sort_criteria(None).into_inner();
        assert_eq!(crit.len(), 1);
        assert!(crit[0].reverse);
        assert!(matches!(crit[0].key, SortKey::Date));
    }

    #[test]
    fn sort_chain_preserves_order_and_direction() {
        let chain = vec![
            SearchEmailsSorter(
                SearchEmailsSorterKind::Date,
                SearchEmailsSorterOrder::Descending,
            ),
            SearchEmailsSorter(
                SearchEmailsSorterKind::Subject,
                SearchEmailsSorterOrder::Ascending,
            ),
        ];
        let crit = sort_criteria(Some(&chain)).into_inner();
        assert_eq!(crit.len(), 2);
        assert!(crit[0].reverse);
        assert!(matches!(crit[0].key, SortKey::Date));
        assert!(!crit[1].reverse);
        assert!(matches!(crit[1].key, SortKey::Subject));
    }
}
