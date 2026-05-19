//! IMAP envelope search (`SELECT` + `UID SORT` + `UID FETCH UID FLAGS
//! ENVELOPE RFC822.SIZE [BODYSTRUCTURE]`).
//!
//! Translates a shared [`SearchEmailsQuery`] into IMAP primitives:
//!
//! - Filter -> `Vec1<SearchKey<'static>>` (RFC 9051 §6.4.4)
//! - Sort   -> `Vec1<SortCriterion>` (RFC 5256)
//!
//! Date filters target the `Date:` header through `SENTON` /
//! `SENTSINCE`; `INTERNALDATE`-based keys (`ON`, `SINCE`) are not used
//! because the sent-at rule must stay consistent across every backend.
//! An absent filter defaults to `[ALL]`; an absent sort defaults to
//! `REVERSE DATE` (date descending).

use core::mem;

use alloc::{
    boxed::Box,
    string::{String, ToString},
    vec,
    vec::Vec,
};

use io_imap::{
    context::ImapContext,
    rfc3501::{
        fetch::{ImapMessageFetch, ImapMessageFetchError, ImapMessageFetchResult},
        select::{ImapMailboxSelect, ImapMailboxSelectError, ImapMailboxSelectResult},
    },
    rfc5256::sort::{ImapMailboxSort, ImapMailboxSortError, ImapMailboxSortResult},
    types::{
        core::{AString, Vec1},
        datetime::NaiveDate as ImapNaiveDate,
        extensions::sort::{SortCriterion, SortKey},
        fetch::MacroOrMessageDataItemNames,
        mailbox::Mailbox as ImapMailbox,
        search::SearchKey,
        sequence::SequenceSet,
    },
};
use log::trace;
use thiserror::Error;

use crate::{
    client::EmailClientStdError,
    envelope::Envelope,
    flag::Flag,
    imap::envelope_list::{build_item_names, envelope_from},
    search::{
        filter::query::SearchEmailsFilterQuery,
        query::SearchEmailsQuery,
        sort::query::{SearchEmailsSorter, SearchEmailsSorterKind, SearchEmailsSorterOrder},
    },
};

/// Errors produced while orchestrating SELECT + SORT + FETCH for IMAP
/// envelope search.
#[derive(Debug, Error)]
pub enum ImapEnvelopeSearchError {
    #[error(transparent)]
    Select(#[from] ImapMailboxSelectError),
    #[error(transparent)]
    Sort(#[from] ImapMailboxSortError),
    #[error(transparent)]
    Fetch(#[from] ImapMessageFetchError),
    #[error(transparent)]
    Convert(#[from] EmailClientStdError),
    #[error("invalid IMAP UID set `{0}`")]
    InvalidUidSet(String),
    #[error("IMAP envelope search was resumed after completion")]
    AlreadyDone,
}

/// Result returned by [`ImapEnvelopeSearch::resume`].
#[derive(Debug)]
pub enum ImapEnvelopeSearchResult {
    Ok(Vec<Envelope>),
    WantsRead,
    WantsWrite(Vec<u8>),
    Err(ImapEnvelopeSearchError),
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

/// I/O-free coroutine wrapping `SELECT <mailbox>` + `UID SORT
/// <criteria> UTF-8 <keys>` + `UID FETCH <uids> (UID FLAGS ENVELOPE
/// RFC822.SIZE [BODYSTRUCTURE])`. `page` is 1-indexed; the page is
/// sliced from the server-returned UID list, preserving the SORT
/// order.
pub struct ImapEnvelopeSearch {
    state: State,
}

impl ImapEnvelopeSearch {
    /// `page_size = None` returns the full result set; `page = None` is
    /// treated as page 1. `with_attachment = true` additionally fetches
    /// `BODYSTRUCTURE` to populate [`Envelope::has_attachment`].
    pub fn new(
        context: ImapContext,
        mailbox: ImapMailbox<'static>,
        query: Option<&SearchEmailsQuery>,
        page: Option<u32>,
        page_size: Option<u32>,
        with_attachment: bool,
    ) -> Result<Self, EmailClientStdError> {
        trace!("prepare IMAP envelope search");
        Self::with_select(
            ImapMailboxSelect::new(context, mailbox),
            query,
            page,
            page_size,
            with_attachment,
        )
    }

    /// Read-only variant: issues `EXAMINE` instead of `SELECT`.
    pub fn read_only(
        context: ImapContext,
        mailbox: ImapMailbox<'static>,
        query: Option<&SearchEmailsQuery>,
        page: Option<u32>,
        page_size: Option<u32>,
        with_attachment: bool,
    ) -> Result<Self, EmailClientStdError> {
        trace!("prepare IMAP envelope search (read-only)");
        Self::with_select(
            ImapMailboxSelect::read_only(context, mailbox),
            query,
            page,
            page_size,
            with_attachment,
        )
    }

    fn with_select(
        select: ImapMailboxSelect,
        query: Option<&SearchEmailsQuery>,
        page: Option<u32>,
        page_size: Option<u32>,
        with_attachment: bool,
    ) -> Result<Self, EmailClientStdError> {
        let search_criteria = search_keys(query.and_then(|q| q.filter.as_ref()))?;
        let sort_criteria = sort_criteria(query.and_then(|q| q.sort.as_deref()));
        let item_names = build_item_names(with_attachment);

        Ok(Self {
            state: State::Selecting {
                select,
                page,
                page_size,
                item_names,
                search_criteria,
                sort_criteria,
            },
        })
    }

    pub fn resume(&mut self, mut arg: Option<&[u8]>) -> ImapEnvelopeSearchResult {
        loop {
            match mem::replace(&mut self.state, State::Done) {
                State::Selecting {
                    mut select,
                    page,
                    page_size,
                    item_names,
                    search_criteria,
                    sort_criteria,
                } => match select.resume(arg.take()) {
                    ImapMailboxSelectResult::WantsRead => {
                        self.state = State::Selecting {
                            select,
                            page,
                            page_size,
                            item_names,
                            search_criteria,
                            sort_criteria,
                        };
                        return ImapEnvelopeSearchResult::WantsRead;
                    }
                    ImapMailboxSelectResult::WantsWrite(bytes) => {
                        self.state = State::Selecting {
                            select,
                            page,
                            page_size,
                            item_names,
                            search_criteria,
                            sort_criteria,
                        };
                        return ImapEnvelopeSearchResult::WantsWrite(bytes);
                    }
                    ImapMailboxSelectResult::Err { err, .. } => {
                        return ImapEnvelopeSearchResult::Err(err.into());
                    }
                    ImapMailboxSelectResult::Ok { context, data } => {
                        let exists = data.exists.unwrap_or(0);
                        if exists == 0 {
                            return ImapEnvelopeSearchResult::Ok(Vec::new());
                        }

                        let sort =
                            ImapMailboxSort::new(context, sort_criteria, search_criteria, true);
                        self.state = State::Sorting {
                            sort,
                            page,
                            page_size,
                            item_names,
                        };
                    }
                },
                State::Sorting {
                    mut sort,
                    page,
                    page_size,
                    item_names,
                } => match sort.resume(arg.take()) {
                    ImapMailboxSortResult::WantsRead => {
                        self.state = State::Sorting {
                            sort,
                            page,
                            page_size,
                            item_names,
                        };
                        return ImapEnvelopeSearchResult::WantsRead;
                    }
                    ImapMailboxSortResult::WantsWrite(bytes) => {
                        self.state = State::Sorting {
                            sort,
                            page,
                            page_size,
                            item_names,
                        };
                        return ImapEnvelopeSearchResult::WantsWrite(bytes);
                    }
                    ImapMailboxSortResult::Err { err, .. } => {
                        return ImapEnvelopeSearchResult::Err(err.into());
                    }
                    ImapMailboxSortResult::Ok { context, ids } => {
                        if ids.is_empty() {
                            return ImapEnvelopeSearchResult::Ok(Vec::new());
                        }

                        let page_uids = paginate_uids(&ids, page, page_size);
                        if page_uids.is_empty() {
                            return ImapEnvelopeSearchResult::Ok(Vec::new());
                        }

                        let uid_str = page_uids
                            .iter()
                            .map(u32::to_string)
                            .collect::<Vec<_>>()
                            .join(",");

                        let sequence_set: SequenceSet = match uid_str.as_str().try_into() {
                            Ok(set) => set,
                            Err(_) => {
                                return ImapEnvelopeSearchResult::Err(
                                    ImapEnvelopeSearchError::InvalidUidSet(uid_str),
                                );
                            }
                        };

                        let fetch = ImapMessageFetch::new(context, sequence_set, item_names, true);
                        self.state = State::Fetching {
                            fetch,
                            order: page_uids,
                        };
                    }
                },
                State::Fetching { mut fetch, order } => match fetch.resume(arg.take()) {
                    ImapMessageFetchResult::WantsRead => {
                        self.state = State::Fetching { fetch, order };
                        return ImapEnvelopeSearchResult::WantsRead;
                    }
                    ImapMessageFetchResult::WantsWrite(bytes) => {
                        self.state = State::Fetching { fetch, order };
                        return ImapEnvelopeSearchResult::WantsWrite(bytes);
                    }
                    ImapMessageFetchResult::Err { err, .. } => {
                        return ImapEnvelopeSearchResult::Err(err.into());
                    }
                    ImapMessageFetchResult::Ok { data, .. } => {
                        let envelopes = reorder_envelopes(data, &order);
                        return ImapEnvelopeSearchResult::Ok(envelopes);
                    }
                },
                State::Done => {
                    return ImapEnvelopeSearchResult::Err(ImapEnvelopeSearchError::AlreadyDone);
                }
            }
        }
    }
}

/// Builds the IMAP `SEARCH` key list for the given filter, defaulting
/// to `[ALL]` when no filter is provided.
pub fn search_keys(
    filter: Option<&SearchEmailsFilterQuery>,
) -> Result<Vec1<SearchKey<'static>>, EmailClientStdError> {
    let key = match filter {
        None => SearchKey::All,
        Some(filter) => convert_filter(filter)?,
    };
    Ok(Vec1::from(key))
}

/// Builds the IMAP `SORT` criterion list for the given sort chain,
/// defaulting to `REVERSE DATE` when the chain is empty or absent.
pub fn sort_criteria(sort: Option<&[SearchEmailsSorter]>) -> Vec1<SortCriterion> {
    let criteria: Vec<SortCriterion> = match sort {
        Some(chain) if !chain.is_empty() => chain.iter().map(convert_sorter).collect(),
        _ => vec![SortCriterion {
            reverse: true,
            key: SortKey::Date,
        }],
    };

    Vec1::try_from(criteria).expect("non-empty by construction")
}

/// Slices `uids` according to `(page, page_size)`, preserving order.
/// `page = None` is treated as page 1; `page_size = None` keeps the
/// full list.
pub fn paginate_uids(
    uids: &[core::num::NonZeroU32],
    page: Option<u32>,
    page_size: Option<u32>,
) -> Vec<u32> {
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
) -> Result<SearchKey<'static>, EmailClientStdError> {
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

        // Our `Date(D)` is "Date: header on day D", same shape as
        // IMAP `SENTON`.
        Q::Date(date) => SearchKey::SentOn(imap_date(*date)?),

        // Our `AfterDate(D)` is the strict `Date: header > D`. IMAP
        // `SENTSINCE D'` is `Date: header >= D'`, so bump by one day.
        Q::AfterDate(date) => {
            let bumped = date.succ_opt().unwrap_or(*date);
            SearchKey::SentSince(imap_date(bumped)?)
        }

        Q::From(pattern) => SearchKey::From(astring(pattern)?),
        Q::To(pattern) => SearchKey::To(astring(pattern)?),
        Q::Subject(pattern) => SearchKey::Subject(astring(pattern)?),
        Q::Body(pattern) => SearchKey::Body(astring(pattern)?),

        Q::Flag(flag) => match flag {
            Flag::Seen => SearchKey::Seen,
            Flag::Answered => SearchKey::Answered,
            Flag::Flagged => SearchKey::Flagged,
            Flag::Draft => SearchKey::Draft,
        },
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

    let reverse = matches!(order, SearchEmailsSorterOrder::Descending);

    SortCriterion { reverse, key }
}

fn astring(pattern: &str) -> Result<AString<'static>, EmailClientStdError> {
    AString::try_from(String::from(pattern))
        .map_err(|_| EmailClientStdError::OperationFailed("invalid IMAP search pattern"))
}

fn imap_date(date: chrono::NaiveDate) -> Result<ImapNaiveDate, EmailClientStdError> {
    ImapNaiveDate::try_from(date)
        .map_err(|_| EmailClientStdError::OperationFailed("invalid IMAP date"))
}

/// Maps the FETCH response back into the requested UID order, dropping
/// UIDs the server failed to return.
fn reorder_envelopes(
    data: alloc::collections::BTreeMap<
        core::num::NonZeroU32,
        Vec1<io_imap::types::fetch::MessageDataItem<'static>>,
    >,
    order: &[u32],
) -> Vec<Envelope> {
    use alloc::collections::BTreeMap;
    use io_imap::types::fetch::MessageDataItem;

    let by_uid: BTreeMap<u32, Envelope> = data
        .into_iter()
        .map(|(_, items)| {
            let items = items.into_inner();
            let uid = items.iter().find_map(|item| match item {
                MessageDataItem::Uid(u) => Some(u.get()),
                _ => None,
            });
            let env = envelope_from(0, items);
            (uid.unwrap_or(0), env)
        })
        .collect();

    order
        .iter()
        .filter_map(|u| by_uid.get(u).cloned())
        .collect()
}

#[cfg(test)]
mod tests {
    use alloc::{boxed::Box, vec};

    use chrono::NaiveDate;

    use super::*;

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
        match key {
            SearchKey::SentOn(d) => assert_eq!(*d.as_ref(), naive(2026, 1, 15)),
            other => panic!("expected SentOn, got {other:?}"),
        }
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
    fn and_collects_two_keys() {
        let q = SearchEmailsFilterQuery::And(
            Box::new(SearchEmailsFilterQuery::From("alice".into())),
            Box::new(SearchEmailsFilterQuery::Subject("release".into())),
        );
        let key = convert_filter(&q).unwrap();
        match key {
            SearchKey::And(keys) => {
                let inner: Vec<_> = keys.into_inner();
                assert_eq!(inner.len(), 2);
                assert!(matches!(inner[0], SearchKey::From(_)));
                assert!(matches!(inner[1], SearchKey::Subject(_)));
            }
            other => panic!("expected And, got {other:?}"),
        }
    }

    #[test]
    fn or_and_not_box_the_subtree() {
        let q = SearchEmailsFilterQuery::Or(
            Box::new(SearchEmailsFilterQuery::From("alice".into())),
            Box::new(SearchEmailsFilterQuery::Not(Box::new(
                SearchEmailsFilterQuery::Subject("draft".into()),
            ))),
        );
        let key = convert_filter(&q).unwrap();
        match key {
            SearchKey::Or(l, r) => {
                assert!(matches!(*l, SearchKey::From(_)));
                assert!(matches!(*r, SearchKey::Not(_)));
            }
            other => panic!("expected Or, got {other:?}"),
        }
    }

    #[test]
    fn flag_lcd_mapping() {
        for (variant, expected_seen) in [
            (Flag::Seen, true),
            (Flag::Answered, false),
            (Flag::Flagged, false),
            (Flag::Draft, false),
        ] {
            let key = convert_filter(&SearchEmailsFilterQuery::Flag(variant)).unwrap();
            assert_eq!(matches!(key, SearchKey::Seen), expected_seen);
        }
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
