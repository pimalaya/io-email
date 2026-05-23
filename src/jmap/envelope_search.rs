//! JMAP envelope search (batched `Email/query` + `Email/get`).
//!
//! Translates a shared [`SearchEmailsQuery`] into JMAP primitives with
//! two intentional limitations versus IMAP / Maildir:
//!
//! 1. JMAP's `FilterCondition` (RFC 8621 §4.4.1) has no recursive
//!    operator object. Multiple non-`None` fields are an implicit AND,
//!    but `OR` and `NOT` are not expressible without a wrapping
//!    `FilterOperator`, which `io-jmap` does not model yet. Queries
//!    containing those nodes are rejected at conversion time with
//!    [`EmailClientStdError::OperationFailed`].
//!
//! 2. JMAP's `before` / `after` filter primitives are anchored to
//!    `receivedAt`, while our DSL targets `sentAt`. The conversion
//!    over-approximates by anchoring on `receivedAt`, then re-applies
//!    the exact `sentAt` predicate client-side via [`PostFilter`].
//!
//! When post-filters are present, server-side pagination would slice
//! the over-approximated result before we trim it; the coroutine
//! fetches without `position`/`limit` and paginates after the
//! client-side pass.

use alloc::{string::String, vec, vec::Vec};

use chrono::{Datelike, NaiveDate};
use io_jmap::{
    rfc8620::session::JmapSession,
    rfc8621::{
        email::{EmailComparator, EmailFilter, EmailSortProperty},
        email_query::{JmapEmailQuery, JmapEmailQueryError, JmapEmailQueryResult},
    },
};
use log::trace;
use secrecy::SecretString;

use crate::{
    client::EmailClientStdError,
    envelope::Envelope,
    jmap::{convert::compute_position_limit, envelope_list::envelope_properties},
    search::{
        filter::query::SearchEmailsFilterQuery,
        query::SearchEmailsQuery,
        sort::query::{SearchEmailsSorter, SearchEmailsSorterKind, SearchEmailsSorterOrder},
    },
};

/// Errors produced while running JMAP envelope search.
#[derive(Debug, thiserror::Error)]
pub enum JmapEnvelopeSearchError {
    #[error(transparent)]
    Query(#[from] JmapEmailQueryError),
    #[error(transparent)]
    Convert(#[from] EmailClientStdError),
}

/// Result returned by [`JmapEnvelopeSearch::resume`].
#[derive(Debug)]
pub enum JmapEnvelopeSearchResult {
    Ok(Vec<Envelope>),
    WantsRead,
    WantsWrite(Vec<u8>),
    Err(JmapEnvelopeSearchError),
}

/// Client-side residual predicate left over after the server filter
/// was applied. Each variant re-checks one AST leaf against the
/// envelope's `sentAt` (carried in [`Envelope::date`]).
#[derive(Clone, Debug)]
pub enum PostFilter {
    Date(NaiveDate),
    AfterDate(NaiveDate),
}

/// Output of [`build`]: the JMAP-side filter to send to `Email/query`,
/// the JMAP-side comparator list, and the residual client-side
/// predicates the caller must re-apply on the returned envelopes.
#[derive(Debug)]
pub struct Converted {
    pub filter: EmailFilter,
    pub sort: Vec<EmailComparator>,
    pub post_filters: Vec<PostFilter>,
}

/// I/O-free coroutine wrapping a batched `Email/query` + `Email/get`
/// scoped to a single mailbox. Applies any residual `sentAt` predicate
/// client-side before paginating.
pub struct JmapEnvelopeSearch {
    inner: JmapEmailQuery,
    post_filters: Vec<PostFilter>,
    page: Option<u32>,
    page_size: Option<u32>,
    paginate_client_side: bool,
}

impl JmapEnvelopeSearch {
    /// `page` is 1-indexed; `page_size = None` lets the server pick.
    /// `mailbox_filter` is the optional base filter (typically the
    /// `inMailbox` constraint from
    /// [`crate::jmap::convert::mailbox_filter`]). `query` carries the
    /// shared filter+sort AST; `None` defaults to "all envelopes,
    /// `sentAt` descending".
    pub fn new(
        session: &JmapSession,
        http_auth: &SecretString,
        mailbox_filter: Option<EmailFilter>,
        query: Option<&SearchEmailsQuery>,
        page: Option<u32>,
        page_size: Option<u32>,
    ) -> Result<Self, JmapEnvelopeSearchError> {
        trace!("prepare JMAP envelope search");

        let base = mailbox_filter.unwrap_or_default();
        let converted = build(query, base)?;

        let paginate_client_side = !converted.post_filters.is_empty();
        let (position, limit) = if paginate_client_side {
            (None, None)
        } else {
            compute_position_limit(page, page_size)
        };

        let inner = JmapEmailQuery::new(
            session,
            http_auth,
            Some(converted.filter),
            Some(converted.sort),
            position,
            limit,
            Some(envelope_properties()),
        )?;

        Ok(Self {
            inner,
            post_filters: converted.post_filters,
            page,
            page_size,
            paginate_client_side,
        })
    }

    pub fn resume(&mut self, arg: Option<&[u8]>) -> JmapEnvelopeSearchResult {
        match self.inner.resume(arg) {
            JmapEmailQueryResult::WantsRead => JmapEnvelopeSearchResult::WantsRead,
            JmapEmailQueryResult::WantsWrite(bytes) => JmapEnvelopeSearchResult::WantsWrite(bytes),
            JmapEmailQueryResult::Err(err) => JmapEnvelopeSearchResult::Err(err.into()),
            JmapEmailQueryResult::Ok { emails, .. } => {
                let mut envelopes: Vec<Envelope> = emails.into_iter().map(Envelope::from).collect();

                if self.paginate_client_side {
                    envelopes.retain(|env| post_filter(env, &self.post_filters));
                    envelopes = paginate(envelopes, self.page, self.page_size);
                }

                JmapEnvelopeSearchResult::Ok(envelopes)
            }
        }
    }
}

/// Converts the shared query into JMAP primitives plus residual
/// predicates. The `base` filter is merged with the query's leaves;
/// callers typically pass [`crate::jmap::convert::mailbox_filter`]'s
/// result so the result stays scoped to the active mailbox.
pub fn build(
    query: Option<&SearchEmailsQuery>,
    base: EmailFilter,
) -> Result<Converted, EmailClientStdError> {
    let mut filter = base;
    let mut post_filters = Vec::new();

    if let Some(query) = query
        && let Some(ref f) = query.filter
    {
        let mut leaves = Vec::new();
        collect_conjunction(f, &mut leaves)?;

        for leaf in leaves {
            apply_leaf(&mut filter, &mut post_filters, leaf)?;
        }
    }

    let sort = query
        .and_then(|q| q.sort.as_deref())
        .filter(|chain| !chain.is_empty())
        .map(|chain| chain.iter().map(convert_sorter).collect())
        .unwrap_or_else(|| vec![sent_at_desc()]);

    Ok(Converted {
        filter,
        sort,
        post_filters,
    })
}

/// Returns `true` when `envelope` matches every residual predicate.
/// Apply this after the JMAP server returns its (over-approximating)
/// result set to drop the false positives.
pub fn post_filter(envelope: &Envelope, post_filters: &[PostFilter]) -> bool {
    post_filters.iter().all(|pf| match pf {
        PostFilter::Date(target) => envelope
            .date
            .map(|d| d.date_naive() == *target)
            .unwrap_or(false),
        PostFilter::AfterDate(target) => envelope
            .date
            .map(|d| d.date_naive() > *target)
            .unwrap_or(false),
    })
}

/// Default comparator: `sentAt` descending. Matches the sent-at rule
/// the shared DSL applies on every backend.
pub fn sent_at_desc() -> EmailComparator {
    EmailComparator {
        property: EmailSortProperty::SentAt,
        is_ascending: Some(false),
        collation: None,
        keyword: None,
    }
}

fn paginate(envelopes: Vec<Envelope>, page: Option<u32>, page_size: Option<u32>) -> Vec<Envelope> {
    let total = envelopes.len();
    let size = page_size.map(|n| n as usize);
    let start = ((page.unwrap_or(1).max(1) - 1) as usize).saturating_mul(size.unwrap_or(0));

    if start >= total {
        return Vec::new();
    }

    let end = match size {
        Some(n) => start.saturating_add(n).min(total),
        None => total,
    };

    envelopes[start..end].to_vec()
}

fn collect_conjunction<'a>(
    node: &'a SearchEmailsFilterQuery,
    out: &mut Vec<&'a SearchEmailsFilterQuery>,
) -> Result<(), EmailClientStdError> {
    use SearchEmailsFilterQuery as Q;

    match node {
        Q::And(left, right) => {
            collect_conjunction(left, out)?;
            collect_conjunction(right, out)
        }
        Q::Or(_, _) => Err(EmailClientStdError::OperationFailed(
            "envelopes search `or` is not yet supported on JMAP",
        )),
        Q::Not(_) => Err(EmailClientStdError::OperationFailed(
            "envelopes search `not` is not yet supported on JMAP",
        )),
        leaf => {
            out.push(leaf);
            Ok(())
        }
    }
}

fn apply_leaf(
    filter: &mut EmailFilter,
    post_filters: &mut Vec<PostFilter>,
    leaf: &SearchEmailsFilterQuery,
) -> Result<(), EmailClientStdError> {
    use SearchEmailsFilterQuery as Q;

    match leaf {
        Q::From(pattern) => set_once(&mut filter.from, pattern.clone(), "from"),
        Q::To(pattern) => set_once(&mut filter.to, pattern.clone(), "to"),
        Q::Subject(pattern) => set_once(&mut filter.subject, pattern.clone(), "subject"),
        Q::Body(pattern) => set_once(&mut filter.body, pattern.clone(), "body"),
        Q::Flag(flag) => {
            let keyword = crate::jmap::convert::keyword_from(flag);
            set_once(&mut filter.has_keyword, keyword, "flag")
        }
        // Over-approximate via `after = start-of-day(D)` (the lowest
        // `receivedAt` consistent with "sentAt-day == D"); the exact
        // sent-at constraint is re-checked client-side.
        Q::Date(target) => {
            tighten_after(&mut filter.after, *target)?;
            post_filters.push(PostFilter::Date(*target));
            Ok(())
        }
        // Over-approximate via `after = start-of-day(D+1)` (the lowest
        // `receivedAt` consistent with "sentAt-day > D"); the strict
        // sent-at constraint is re-checked client-side.
        Q::AfterDate(target) => {
            let bumped = target.succ_opt().unwrap_or(*target);
            tighten_after(&mut filter.after, bumped)?;
            post_filters.push(PostFilter::AfterDate(*target));
            Ok(())
        }
        Q::And(_, _) | Q::Or(_, _) | Q::Not(_) => {
            // collect_conjunction already filtered these out
            unreachable!()
        }
    }
}

fn set_once(
    slot: &mut Option<String>,
    value: String,
    field: &'static str,
) -> Result<(), EmailClientStdError> {
    if slot.is_some() {
        return Err(EmailClientStdError::OperationFailed(match field {
            "from" => "JMAP filter accepts at most one `from` clause",
            "to" => "JMAP filter accepts at most one `to` clause",
            "subject" => "JMAP filter accepts at most one `subject` clause",
            "body" => "JMAP filter accepts at most one `body` clause",
            "flag" => "JMAP filter accepts at most one `flag` clause",
            _ => "JMAP filter accepts at most one clause per field",
        }));
    }
    *slot = Some(value);
    Ok(())
}

/// Replaces `slot` with the tighter of the existing and the new lower
/// bound. Both bounds are expressed as JMAP UTCDate strings anchored
/// to midnight UTC on the given day.
fn tighten_after(
    slot: &mut Option<String>,
    candidate: NaiveDate,
) -> Result<(), EmailClientStdError> {
    let candidate_str = utc_midnight(candidate);
    match slot {
        Some(existing) if existing.as_str() >= candidate_str.as_str() => Ok(()),
        _ => {
            *slot = Some(candidate_str);
            Ok(())
        }
    }
}

fn utc_midnight(date: NaiveDate) -> String {
    alloc::format!(
        "{:04}-{:02}-{:02}T00:00:00Z",
        date.year(),
        date.month(),
        date.day()
    )
}

fn convert_sorter(sorter: &SearchEmailsSorter) -> EmailComparator {
    let SearchEmailsSorter(kind, order) = sorter;

    let property = match kind {
        SearchEmailsSorterKind::Date => EmailSortProperty::SentAt,
        SearchEmailsSorterKind::From => EmailSortProperty::From,
        SearchEmailsSorterKind::To => EmailSortProperty::To,
        SearchEmailsSorterKind::Subject => EmailSortProperty::Subject,
    };

    let is_ascending = match order {
        SearchEmailsSorterOrder::Ascending => Some(true),
        SearchEmailsSorterOrder::Descending => Some(false),
    };

    EmailComparator {
        property,
        is_ascending,
        collation: None,
        keyword: None,
    }
}

#[cfg(test)]
mod tests {
    use alloc::boxed::Box;

    use chrono::{DateTime, NaiveDate};

    use super::*;
    use crate::{address::Address, flag::Flag};

    fn naive(y: i32, m: u32, d: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(y, m, d).unwrap()
    }

    fn envelope_at(date: &str) -> Envelope {
        Envelope {
            id: "1".into(),
            message_id: None,
            flags: Default::default(),
            subject: String::new(),
            from: vec![Address {
                name: None,
                email: String::new(),
            }],
            to: vec![],
            date: DateTime::parse_from_rfc3339(date).ok(),
            size: 0,
            has_attachment: None,
        }
    }

    #[test]
    fn or_and_not_are_rejected() {
        let query = SearchEmailsQuery {
            filter: Some(SearchEmailsFilterQuery::Or(
                Box::new(SearchEmailsFilterQuery::From("a".into())),
                Box::new(SearchEmailsFilterQuery::From("b".into())),
            )),
            sort: None,
        };
        assert!(build(Some(&query), EmailFilter::default()).is_err());

        let query = SearchEmailsQuery {
            filter: Some(SearchEmailsFilterQuery::Not(Box::new(
                SearchEmailsFilterQuery::From("a".into()),
            ))),
            sort: None,
        };
        assert!(build(Some(&query), EmailFilter::default()).is_err());
    }

    #[test]
    fn conjunction_folds_into_a_single_filter() {
        let query = SearchEmailsQuery {
            filter: Some(SearchEmailsFilterQuery::And(
                Box::new(SearchEmailsFilterQuery::From("alice".into())),
                Box::new(SearchEmailsFilterQuery::Subject("release".into())),
            )),
            sort: None,
        };
        let converted = build(Some(&query), EmailFilter::default()).unwrap();
        assert_eq!(converted.filter.from.as_deref(), Some("alice"));
        assert_eq!(converted.filter.subject.as_deref(), Some("release"));
        assert!(converted.post_filters.is_empty());
    }

    #[test]
    fn duplicate_clause_is_rejected() {
        let query = SearchEmailsQuery {
            filter: Some(SearchEmailsFilterQuery::And(
                Box::new(SearchEmailsFilterQuery::From("alice".into())),
                Box::new(SearchEmailsFilterQuery::From("bob".into())),
            )),
            sort: None,
        };
        assert!(build(Some(&query), EmailFilter::default()).is_err());
    }

    #[test]
    fn date_clause_records_post_filter_and_tightens_after() {
        let query = SearchEmailsQuery {
            filter: Some(SearchEmailsFilterQuery::Date(naive(2026, 1, 15))),
            sort: None,
        };
        let converted = build(Some(&query), EmailFilter::default()).unwrap();
        assert_eq!(
            converted.filter.after.as_deref(),
            Some("2026-01-15T00:00:00Z")
        );
        assert!(matches!(
            converted.post_filters.as_slice(),
            [PostFilter::Date(d)] if *d == naive(2026, 1, 15)
        ));
    }

    #[test]
    fn after_clause_bumps_lower_bound_by_one_day() {
        let query = SearchEmailsQuery {
            filter: Some(SearchEmailsFilterQuery::AfterDate(naive(2026, 1, 15))),
            sort: None,
        };
        let converted = build(Some(&query), EmailFilter::default()).unwrap();
        assert_eq!(
            converted.filter.after.as_deref(),
            Some("2026-01-16T00:00:00Z")
        );
        assert!(matches!(
            converted.post_filters.as_slice(),
            [PostFilter::AfterDate(d)] if *d == naive(2026, 1, 15)
        ));
    }

    #[test]
    fn post_filter_drops_false_positives_for_date_clause() {
        let post = [PostFilter::Date(naive(2026, 5, 15))];
        let on_day = envelope_at("2026-05-15T10:00:00+00:00");
        let next_day = envelope_at("2026-05-16T00:00:00+00:00");
        assert!(post_filter(&on_day, &post));
        assert!(!post_filter(&next_day, &post));
    }

    #[test]
    fn post_filter_strict_after() {
        let post = [PostFilter::AfterDate(naive(2026, 5, 15))];
        let on_day = envelope_at("2026-05-15T23:59:59+00:00");
        let next_day = envelope_at("2026-05-16T00:00:00+00:00");
        assert!(!post_filter(&on_day, &post));
        assert!(post_filter(&next_day, &post));
    }

    #[test]
    fn empty_sort_defaults_to_sent_at_descending() {
        let converted = build(None, EmailFilter::default()).unwrap();
        assert_eq!(converted.sort.len(), 1);
        assert!(matches!(
            converted.sort[0].property,
            EmailSortProperty::SentAt
        ));
        assert_eq!(converted.sort[0].is_ascending, Some(false));
    }

    #[test]
    fn flag_clause_sets_has_keyword() {
        use crate::flag::IanaFlag;

        let query = SearchEmailsQuery {
            filter: Some(SearchEmailsFilterQuery::Flag(Flag::from_iana(
                IanaFlag::Flagged,
            ))),
            sort: None,
        };
        let converted = build(Some(&query), EmailFilter::default()).unwrap();
        assert_eq!(converted.filter.has_keyword.as_deref(), Some("$flagged"));
    }
}
