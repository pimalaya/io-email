//! Maildir envelope search, wrapping
//! [`io_maildir::coroutines::message_list::MaildirMessagesList`] with a
//! client-side filter/sort/paginate pass.
//!
//! Maildir has no server-side filter or sort; every message in the
//! Maildir is parsed first, then the shared [`SearchEmailsQuery`] is
//! evaluated against the materialised envelope list. `body` filter
//! clauses are not honored (they would require fetching and parsing
//! every candidate message file); use [`filter_references_body`] to
//! detect the case before invoking [`MaildirEnvelopeSearch`].

use alloc::{
    collections::{BTreeMap, BTreeSet},
    string::String,
    vec::Vec,
};

use chrono::{DateTime, FixedOffset, NaiveDate};
use io_maildir::{
    coroutines::message_list::{
        MaildirMessagesList as InnerMaildirMessagesList, MaildirMessagesListArg,
        MaildirMessagesListError, MaildirMessagesListResult,
    },
    maildir::Maildir,
    path::MaildirPath,
};
use log::trace;
use thiserror::Error;

use crate::{
    address::Address,
    client::EmailClientStdError,
    envelope::Envelope,
    search::{
        filter::query::SearchEmailsFilterQuery,
        query::SearchEmailsQuery,
        sort::query::{SearchEmailsSorter, SearchEmailsSorterKind, SearchEmailsSorterOrder},
    },
};

/// Argument fed back to [`MaildirEnvelopeSearch::resume`].
#[derive(Debug)]
pub enum MaildirEnvelopeSearchArg {
    DirRead(BTreeMap<MaildirPath, BTreeSet<MaildirPath>>),
    FileExists(BTreeMap<MaildirPath, bool>),
    FileRead(BTreeMap<MaildirPath, Vec<u8>>),
}

/// Errors produced while running Maildir envelope search.
#[derive(Debug, Error)]
pub enum MaildirEnvelopeSearchError {
    #[error(transparent)]
    List(#[from] MaildirMessagesListError),
    #[error("envelopes search `body` filter is not yet supported on Maildir")]
    BodyFilterUnsupported,
}

/// Result returned by [`MaildirEnvelopeSearch::resume`].
#[derive(Debug)]
pub enum MaildirEnvelopeSearchResult {
    Ok(Vec<Envelope>),
    WantsDirRead(BTreeSet<MaildirPath>),
    WantsFileExists(BTreeSet<MaildirPath>),
    WantsFileRead(BTreeSet<MaildirPath>),
    Err(MaildirEnvelopeSearchError),
}

/// I/O-free coroutine listing every message inside a single Maildir,
/// then applying the shared query (filter + sort) client-side before
/// pagination. `page = None` is treated as page 1; `page_size = None`
/// keeps the full match.
pub struct MaildirEnvelopeSearch {
    inner: Option<InnerMaildirMessagesList>,
    filter: Option<SearchEmailsFilterQuery>,
    sort: Option<Vec<SearchEmailsSorter>>,
    page: Option<u32>,
    page_size: Option<u32>,
}

impl MaildirEnvelopeSearch {
    pub fn new(
        maildir: Maildir,
        query: Option<&SearchEmailsQuery>,
        page: Option<u32>,
        page_size: Option<u32>,
    ) -> Result<Self, MaildirEnvelopeSearchError> {
        trace!("prepare Maildir envelope search");

        let filter = query.and_then(|q| q.filter.clone());
        let sort = query.and_then(|q| q.sort.clone());

        if let Some(ref f) = filter
            && filter_references_body(f)
        {
            return Err(MaildirEnvelopeSearchError::BodyFilterUnsupported);
        }

        Ok(Self {
            inner: Some(InnerMaildirMessagesList::new(maildir)),
            filter,
            sort,
            page,
            page_size,
        })
    }

    pub fn resume(&mut self, arg: Option<MaildirEnvelopeSearchArg>) -> MaildirEnvelopeSearchResult {
        let Some(inner) = self.inner.as_mut() else {
            return MaildirEnvelopeSearchResult::Ok(Vec::new());
        };

        let inner_arg = arg.map(|arg| match arg {
            MaildirEnvelopeSearchArg::DirRead(entries) => MaildirMessagesListArg::DirRead(entries),
            MaildirEnvelopeSearchArg::FileExists(probes) => {
                MaildirMessagesListArg::FileExists(probes)
            }
            MaildirEnvelopeSearchArg::FileRead(contents) => {
                MaildirMessagesListArg::FileRead(contents)
            }
        });

        match inner.resume(inner_arg) {
            MaildirMessagesListResult::WantsDirRead(paths) => {
                MaildirEnvelopeSearchResult::WantsDirRead(paths)
            }
            MaildirMessagesListResult::WantsFileExists(paths) => {
                MaildirEnvelopeSearchResult::WantsFileExists(paths)
            }
            MaildirMessagesListResult::WantsFileRead(paths) => {
                MaildirEnvelopeSearchResult::WantsFileRead(paths)
            }
            MaildirMessagesListResult::Err(err) => MaildirEnvelopeSearchResult::Err(err.into()),
            MaildirMessagesListResult::Ok(messages) => {
                self.inner = None;

                let mut envelopes: Vec<Envelope> =
                    messages.into_iter().map(Envelope::from).collect();

                if let Some(ref f) = self.filter {
                    envelopes.retain(|env| matches(env, f));
                }

                match self.sort.as_deref() {
                    Some(sort) if !sort.is_empty() => {
                        envelopes.sort_by(|a, b| compare(a, b, sort));
                    }
                    _ => envelopes.sort_by(|a, b| b.date.cmp(&a.date)),
                }

                MaildirEnvelopeSearchResult::Ok(paginate(envelopes, self.page, self.page_size))
            }
        }
    }
}

/// Tests whether `envelope` matches `filter`.
///
/// Date clauses target the `Date:` header (sent-at) via
/// [`Envelope::date`]; text clauses use case-insensitive substring
/// matching. `Body` returns `false` unconditionally; callers must
/// filter those out beforehand (see [`filter_references_body`]).
pub fn matches(envelope: &Envelope, filter: &SearchEmailsFilterQuery) -> bool {
    use SearchEmailsFilterQuery::*;

    match filter {
        And(left, right) => matches(envelope, left) && matches(envelope, right),
        Or(left, right) => matches(envelope, left) || matches(envelope, right),
        Not(inner) => !matches(envelope, inner),
        Date(target) => same_day(envelope.date, *target),
        AfterDate(target) => after_day(envelope.date, *target),
        From(pattern) => addresses_contain(&envelope.from, pattern),
        To(pattern) => addresses_contain(&envelope.to, pattern),
        Subject(pattern) => contains_ci(&envelope.subject, pattern),
        Body(_) => false,
        Flag(flag) => envelope.flags.contains(flag),
    }
}

/// Returns `true` when the filter tree carries at least one `Body`
/// clause. Maildir orchestration uses this to bail with a clear error
/// instead of silently dropping every envelope.
pub fn filter_references_body(filter: &SearchEmailsFilterQuery) -> bool {
    use SearchEmailsFilterQuery::*;

    match filter {
        Body(_) => true,
        And(left, right) | Or(left, right) => {
            filter_references_body(left) || filter_references_body(right)
        }
        Not(inner) => filter_references_body(inner),
        _ => false,
    }
}

/// Compares two envelopes through the sort chain `sort`, applying each
/// sorter as a tiebreaker on the previous one. An empty `sort` returns
/// [`core::cmp::Ordering::Equal`]; the caller is responsible for the
/// default ordering in that case.
pub fn compare(
    left: &Envelope,
    right: &Envelope,
    sort: &[SearchEmailsSorter],
) -> core::cmp::Ordering {
    use core::cmp::Ordering;

    for SearchEmailsSorter(kind, order) in sort {
        let cmp = match kind {
            SearchEmailsSorterKind::Date => left.date.cmp(&right.date),
            SearchEmailsSorterKind::From => {
                first_addr_key(&left.from).cmp(&first_addr_key(&right.from))
            }
            SearchEmailsSorterKind::To => first_addr_key(&left.to).cmp(&first_addr_key(&right.to)),
            SearchEmailsSorterKind::Subject => left.subject.cmp(&right.subject),
        };

        let cmp = match order {
            SearchEmailsSorterOrder::Ascending => cmp,
            SearchEmailsSorterOrder::Descending => cmp.reverse(),
        };

        if cmp != Ordering::Equal {
            return cmp;
        }
    }

    Ordering::Equal
}

/// Slices `envelopes` according to `(page, page_size)`. `page = None`
/// is treated as page 1; `page_size = None` keeps the full list.
pub fn paginate(
    envelopes: Vec<Envelope>,
    page: Option<u32>,
    page_size: Option<u32>,
) -> Vec<Envelope> {
    let Some(size) = page_size else {
        return envelopes;
    };

    if size == 0 {
        return Vec::new();
    }

    let page = page.unwrap_or(1).max(1);
    let skip = ((page - 1) as usize).saturating_mul(size as usize);

    if skip >= envelopes.len() {
        return Vec::new();
    }

    envelopes
        .into_iter()
        .skip(skip)
        .take(size as usize)
        .collect()
}

impl From<MaildirEnvelopeSearchError> for EmailClientStdError {
    fn from(err: MaildirEnvelopeSearchError) -> Self {
        match err {
            MaildirEnvelopeSearchError::List(err) => {
                io_maildir::client::MaildirClientError::from(err).into()
            }
            MaildirEnvelopeSearchError::BodyFilterUnsupported => {
                EmailClientStdError::OperationFailed(
                    "envelopes search `body` filter is not yet supported on Maildir",
                )
            }
        }
    }
}

fn same_day(date: Option<DateTime<FixedOffset>>, target: NaiveDate) -> bool {
    date.map(|d| d.date_naive() == target).unwrap_or(false)
}

fn after_day(date: Option<DateTime<FixedOffset>>, target: NaiveDate) -> bool {
    date.map(|d| d.date_naive() > target).unwrap_or(false)
}

fn addresses_contain(addrs: &[Address], pattern: &str) -> bool {
    let needle = pattern.to_lowercase();
    addrs.iter().any(|addr| {
        let email_hit = addr.email.to_lowercase().contains(&needle);
        let name_hit = addr
            .name
            .as_deref()
            .map(|n| n.to_lowercase().contains(&needle))
            .unwrap_or(false);
        email_hit || name_hit
    })
}

fn contains_ci(haystack: &str, needle: &str) -> bool {
    haystack.to_lowercase().contains(&needle.to_lowercase())
}

/// Sort key for a `From`/`To` comparison: pick the first address,
/// prefer the display name when present, fall back to the email
/// local-part+domain. Empty lists sort first.
fn first_addr_key(addrs: &[Address]) -> Option<String> {
    addrs.first().map(|a| {
        a.name
            .as_deref()
            .map(str::to_lowercase)
            .unwrap_or_else(|| a.email.to_lowercase())
    })
}

#[cfg(test)]
mod tests {
    use alloc::{boxed::Box, string::String, vec};
    use core::cmp::Ordering;

    use chrono::DateTime;

    use super::*;
    use crate::flag::Flag;

    fn envelope() -> Envelope {
        Envelope {
            id: String::from("1"),
            message_id: None,
            flags: Default::default(),
            subject: String::from("Release notes"),
            from: vec![Address {
                name: Some(String::from("Alice")),
                email: String::from("alice@example.org"),
            }],
            to: vec![Address {
                name: None,
                email: String::from("team@example.org"),
            }],
            date: DateTime::parse_from_rfc3339("2026-05-15T10:00:00+00:00").ok(),
            size: 1024,
            has_attachment: None,
        }
    }

    fn naive(y: i32, m: u32, d: u32) -> chrono::NaiveDate {
        chrono::NaiveDate::from_ymd_opt(y, m, d).unwrap()
    }

    #[test]
    fn from_match_is_case_insensitive() {
        let env = envelope();
        assert!(matches(
            &env,
            &SearchEmailsFilterQuery::From("alice".into())
        ));
        assert!(matches(
            &env,
            &SearchEmailsFilterQuery::From("ALICE".into())
        ));
        assert!(matches(
            &env,
            &SearchEmailsFilterQuery::From("example.org".into())
        ));
        assert!(!matches(&env, &SearchEmailsFilterQuery::From("bob".into())));
    }

    #[test]
    fn subject_match_is_case_insensitive_substring() {
        let env = envelope();
        assert!(matches(
            &env,
            &SearchEmailsFilterQuery::Subject("release".into())
        ));
        assert!(matches(
            &env,
            &SearchEmailsFilterQuery::Subject("NOTES".into())
        ));
        assert!(!matches(
            &env,
            &SearchEmailsFilterQuery::Subject("draft".into())
        ));
    }

    #[test]
    fn date_clauses_target_sent_at_header() {
        let env = envelope();
        assert!(matches(
            &env,
            &SearchEmailsFilterQuery::Date(naive(2026, 5, 15))
        ));
        assert!(!matches(
            &env,
            &SearchEmailsFilterQuery::Date(naive(2026, 5, 14))
        ));
        assert!(matches(
            &env,
            &SearchEmailsFilterQuery::AfterDate(naive(2026, 5, 14))
        ));
        assert!(!matches(
            &env,
            &SearchEmailsFilterQuery::AfterDate(naive(2026, 5, 15))
        ));
    }

    #[test]
    fn boolean_combinators() {
        let env = envelope();
        let q = SearchEmailsFilterQuery::And(
            Box::new(SearchEmailsFilterQuery::From("alice".into())),
            Box::new(SearchEmailsFilterQuery::Subject("release".into())),
        );
        assert!(matches(&env, &q));

        let q = SearchEmailsFilterQuery::Or(
            Box::new(SearchEmailsFilterQuery::From("bob".into())),
            Box::new(SearchEmailsFilterQuery::Subject("release".into())),
        );
        assert!(matches(&env, &q));

        let q = SearchEmailsFilterQuery::Not(Box::new(SearchEmailsFilterQuery::From("bob".into())));
        assert!(matches(&env, &q));
    }

    #[test]
    fn flag_match() {
        use crate::flag::IanaFlag;

        let mut env = envelope();
        env.flags.insert(Flag::from_iana(IanaFlag::Seen));
        assert!(matches(
            &env,
            &SearchEmailsFilterQuery::Flag(Flag::from_iana(IanaFlag::Seen))
        ));
        assert!(!matches(
            &env,
            &SearchEmailsFilterQuery::Flag(Flag::from_iana(IanaFlag::Flagged))
        ));
    }

    #[test]
    fn body_clause_is_detected_but_never_matches() {
        let env = envelope();
        let q = SearchEmailsFilterQuery::Body("anything".into());
        assert!(!matches(&env, &q));
        assert!(filter_references_body(&q));

        let q = SearchEmailsFilterQuery::And(
            Box::new(SearchEmailsFilterQuery::From("alice".into())),
            Box::new(SearchEmailsFilterQuery::Body("anything".into())),
        );
        assert!(filter_references_body(&q));

        let q = SearchEmailsFilterQuery::From("alice".into());
        assert!(!filter_references_body(&q));
    }

    #[test]
    fn sort_by_date_descending_returns_newer_first() {
        let mut older = envelope();
        older.date = DateTime::parse_from_rfc3339("2026-01-01T00:00:00+00:00").ok();
        let newer = envelope();
        let sort = vec![SearchEmailsSorter(
            SearchEmailsSorterKind::Date,
            SearchEmailsSorterOrder::Descending,
        )];
        assert_eq!(compare(&newer, &older, &sort), Ordering::Less);
        assert_eq!(compare(&older, &newer, &sort), Ordering::Greater);
    }

    #[test]
    fn sort_chain_uses_secondary_key_on_tie() {
        let mut a = envelope();
        let mut b = envelope();
        a.subject = String::from("aaa");
        b.subject = String::from("bbb");
        let sort = vec![
            SearchEmailsSorter(
                SearchEmailsSorterKind::Date,
                SearchEmailsSorterOrder::Ascending,
            ),
            SearchEmailsSorter(
                SearchEmailsSorterKind::Subject,
                SearchEmailsSorterOrder::Ascending,
            ),
        ];
        assert_eq!(compare(&a, &b, &sort), Ordering::Less);
    }

    #[test]
    fn empty_sort_returns_equal() {
        let env = envelope();
        assert_eq!(compare(&env, &env, &[]), Ordering::Equal);
    }
}
