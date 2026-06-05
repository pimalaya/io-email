//! JMAP envelope-search coroutine: batched Email/query + Email/get
//! scoped to one mailbox id.
//!
//! AND/OR/NOT become [`JmapFilterOperator`](io_jmap::rfc8620::JmapFilterOperator)s;
//! leaves become flat [`JmapEmailFilter`] conditions. Date filters
//! target the Date: header (sentAt) while JMAP before/after are
//! receivedAt-anchored, so the coroutine over-approximates on the
//! wire and re-applies the strict predicate client-side via
//! [`PostFilter`], paginating after the trim.
//!
//! # Example
//!
//! ```rust,ignore
//! use io_email::envelope::jmap::search::JmapEnvelopeSearch;
//!
//! let envs = client.run(JmapEnvelopeSearch::new(&session, &auth, "mailbox-id", Some(&query), None, None)?)?;
//! ```

use alloc::{string::String, vec, vec::Vec};
use core::mem;

use chrono::{Datelike, NaiveDate};
use io_jmap::{
    coroutine::{JmapCoroutine, JmapCoroutineState, JmapYield},
    rfc8620::{JmapFilter, JmapSession},
    rfc8621::email::{
        JmapEmailComparator, JmapEmailFilter, JmapEmailSortProperty,
        query::{
            JmapEmailQuery as InnerQuery, JmapEmailQueryError as QueryErr, JmapEmailQueryOptions,
        },
    },
};
use log::trace;
use secrecy::SecretString;
use thiserror::Error;

use crate::{
    envelope::types::Envelope,
    jmap::convert::{compute_position_limit, envelope_from, envelope_properties, keyword_from},
    search::{
        filter::query::SearchEmailsFilterQuery,
        query::SearchEmailsQuery,
        sort::query::{SearchEmailsSorter, SearchEmailsSorterKind, SearchEmailsSorterOrder},
    },
};

/// Errors produced by [`JmapEnvelopeSearch`].
#[derive(Debug, Error)]
pub enum JmapEnvelopeSearchError {
    #[error(transparent)]
    EmailQuery(#[from] QueryErr),
    #[error("coroutine was resumed after completion")]
    ResumedAfterDone,
}

/// Residual client-side predicate left after the JMAP filter ran.
#[derive(Clone, Debug)]
pub enum PostFilter {
    Date(NaiveDate),
    AfterDate(NaiveDate),
}

/// I/O-free coroutine running Email/query + Email/get scoped to one
/// JMAP mailbox id, with optional client-side `sentAt` re-check.
pub struct JmapEnvelopeSearch {
    state: State,
    page: Option<u32>,
    page_size: Option<u32>,
}

impl JmapEnvelopeSearch {
    pub fn new(
        session: &JmapSession,
        http_auth: &SecretString,
        mailbox: &str,
        query: Option<&SearchEmailsQuery>,
        page: Option<u32>,
        page_size: Option<u32>,
    ) -> Result<Self, JmapEnvelopeSearchError> {
        trace!("prepare JMAP envelope search");
        let Converted {
            filter,
            sort,
            post_filters,
        } = build(query, mailbox.into());

        let paginate_client_side = !post_filters.is_empty();
        let (position, limit) = if paginate_client_side {
            (None, None)
        } else {
            compute_position_limit(page, page_size)
        };

        let opts = JmapEmailQueryOptions {
            filter: Some(filter),
            sort: Some(sort),
            position,
            limit,
            properties: Some(envelope_properties()),
        };
        let inner = InnerQuery::new(session, http_auth, opts)?;
        Ok(Self {
            state: State::Searching {
                inner,
                post_filters,
            },
            page,
            page_size,
        })
    }
}

enum State {
    Searching {
        inner: InnerQuery,
        post_filters: Vec<PostFilter>,
    },
    Done,
}

/// JMAP-side filter + sort, plus the residual client-side predicates.
struct Converted {
    filter: JmapFilter<JmapEmailFilter>,
    sort: Vec<JmapEmailComparator>,
    post_filters: Vec<PostFilter>,
}

/// Converts `query` into JMAP primitives, AND-scoped to `mailbox_id`.
fn build(query: Option<&SearchEmailsQuery>, mailbox_id: String) -> Converted {
    let mailbox_scope = JmapFilter::Condition(JmapEmailFilter {
        in_mailbox: Some(mailbox_id),
        ..JmapEmailFilter::default()
    });

    let mut post_filters = Vec::new();
    let user_filter = query
        .and_then(|q| q.filter.as_ref())
        .map(|f| convert_filter(f, &mut post_filters));

    let filter = match user_filter {
        Some(uf) => JmapFilter::and(vec![mailbox_scope, uf]),
        None => mailbox_scope,
    };

    let sort = query
        .and_then(|q| q.sort.as_deref())
        .filter(|chain| !chain.is_empty())
        .map(|chain| chain.iter().map(convert_sorter).collect())
        .unwrap_or_else(|| vec![sent_at_desc()]);

    Converted {
        filter,
        sort,
        post_filters,
    }
}

/// Recursively translates `node` into a JMAP filter tree; date leaves
/// push a [`PostFilter`] so the strict sentAt rule can be re-applied.
fn convert_filter(
    node: &SearchEmailsFilterQuery,
    post_filters: &mut Vec<PostFilter>,
) -> JmapFilter<JmapEmailFilter> {
    use SearchEmailsFilterQuery as Q;

    match node {
        Q::And(left, right) => JmapFilter::and(vec![
            convert_filter(left, post_filters),
            convert_filter(right, post_filters),
        ]),
        Q::Or(left, right) => JmapFilter::or(vec![
            convert_filter(left, post_filters),
            convert_filter(right, post_filters),
        ]),
        Q::Not(inner) => JmapFilter::not(vec![convert_filter(inner, post_filters)]),

        Q::From(pattern) => JmapFilter::Condition(JmapEmailFilter {
            from: Some(pattern.clone()),
            ..JmapEmailFilter::default()
        }),
        Q::To(pattern) => JmapFilter::Condition(JmapEmailFilter {
            to: Some(pattern.clone()),
            ..JmapEmailFilter::default()
        }),
        Q::Subject(pattern) => JmapFilter::Condition(JmapEmailFilter {
            subject: Some(pattern.clone()),
            ..JmapEmailFilter::default()
        }),
        Q::Body(pattern) => JmapFilter::Condition(JmapEmailFilter {
            body: Some(pattern.clone()),
            ..JmapEmailFilter::default()
        }),
        Q::Flag(flag) => JmapFilter::Condition(JmapEmailFilter {
            has_keyword: Some(keyword_from(flag)),
            ..JmapEmailFilter::default()
        }),

        // NOTE: over-approximate via after = start-of-day(D); the
        // exact sent-at rule is re-checked client-side.
        Q::Date(target) => {
            post_filters.push(PostFilter::Date(*target));
            JmapFilter::Condition(JmapEmailFilter {
                after: Some(utc_midnight(*target)),
                ..JmapEmailFilter::default()
            })
        }
        // NOTE: over-approximate via after = start-of-day(D+1); the
        // strict sent-at rule is re-checked client-side.
        Q::AfterDate(target) => {
            post_filters.push(PostFilter::AfterDate(*target));
            let bumped = target.succ_opt().unwrap_or(*target);
            JmapFilter::Condition(JmapEmailFilter {
                after: Some(utc_midnight(bumped)),
                ..JmapEmailFilter::default()
            })
        }
    }
}

/// True when `envelope` matches every residual predicate.
fn post_match(envelope: &Envelope, post_filters: &[PostFilter]) -> bool {
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

fn sent_at_desc() -> JmapEmailComparator {
    JmapEmailComparator {
        property: JmapEmailSortProperty::SentAt,
        is_ascending: Some(false),
        collation: None,
        keyword: None,
    }
}

fn convert_sorter(sorter: &SearchEmailsSorter) -> JmapEmailComparator {
    let SearchEmailsSorter(kind, order) = sorter;

    let property = match kind {
        SearchEmailsSorterKind::Date => JmapEmailSortProperty::SentAt,
        SearchEmailsSorterKind::From => JmapEmailSortProperty::From,
        SearchEmailsSorterKind::To => JmapEmailSortProperty::To,
        SearchEmailsSorterKind::Subject => JmapEmailSortProperty::Subject,
    };

    let is_ascending = match order {
        SearchEmailsSorterOrder::Ascending => Some(true),
        SearchEmailsSorterOrder::Descending => Some(false),
    };

    JmapEmailComparator {
        property,
        is_ascending,
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

fn utc_midnight(date: NaiveDate) -> String {
    alloc::format!(
        "{:04}-{:02}-{:02}T00:00:00Z",
        date.year(),
        date.month(),
        date.day()
    )
}

impl JmapCoroutine for JmapEnvelopeSearch {
    type Yield = JmapYield;
    type Return = Result<Vec<Envelope>, JmapEnvelopeSearchError>;

    fn resume(&mut self, bytes: Option<&[u8]>) -> JmapCoroutineState<Self::Yield, Self::Return> {
        match mem::replace(&mut self.state, State::Done) {
            State::Searching {
                mut inner,
                post_filters,
            } => match inner.resume(bytes) {
                JmapCoroutineState::Complete(Ok(ok)) => {
                    let mut envelopes: Vec<Envelope> =
                        ok.emails.into_iter().map(envelope_from).collect();

                    if !post_filters.is_empty() {
                        envelopes.retain(|env| post_match(env, &post_filters));
                        envelopes = paginate(envelopes, self.page, self.page_size);
                    }

                    JmapCoroutineState::Complete(Ok(envelopes))
                }
                JmapCoroutineState::Yielded(y) => {
                    self.state = State::Searching {
                        inner,
                        post_filters,
                    };
                    JmapCoroutineState::Yielded(y)
                }
                JmapCoroutineState::Complete(Err(err)) => {
                    JmapCoroutineState::Complete(Err(err.into()))
                }
            },
            State::Done => {
                JmapCoroutineState::Complete(Err(JmapEnvelopeSearchError::ResumedAfterDone))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use alloc::boxed::Box;

    use chrono::{DateTime, NaiveDate};
    use io_jmap::rfc8620::{JmapFilterOperator, JmapFilterOperatorKind};

    use super::*;
    use crate::{address::Address, flag::types::Flag};

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

    fn pluck_user_filter(filter: JmapFilter<JmapEmailFilter>) -> JmapFilter<JmapEmailFilter> {
        // NOTE: build() wraps the user filter in AND(mailbox_scope,
        // user_filter) when a user filter is present.
        let JmapFilter::Operator(JmapFilterOperator { conditions, .. }) = filter else {
            panic!("expected top-level AND combinator");
        };
        conditions.into_iter().nth(1).expect("expected user filter")
    }

    #[test]
    fn empty_query_yields_just_the_mailbox_scope() {
        let c = build(None, "mbox-1".into());
        match c.filter {
            JmapFilter::Condition(JmapEmailFilter {
                in_mailbox: Some(id),
                ..
            }) => assert_eq!(id, "mbox-1"),
            other => panic!("expected mailbox-scope condition, got {other:?}"),
        }
        assert!(c.post_filters.is_empty());
        assert_eq!(c.sort.len(), 1);
        assert!(matches!(c.sort[0].property, JmapEmailSortProperty::SentAt));
        assert_eq!(c.sort[0].is_ascending, Some(false));
    }

    #[test]
    fn or_translates_to_filter_operator() {
        let q = SearchEmailsQuery {
            filter: Some(SearchEmailsFilterQuery::Or(
                Box::new(SearchEmailsFilterQuery::From("alice".into())),
                Box::new(SearchEmailsFilterQuery::From("bob".into())),
            )),
            sort: None,
        };
        let c = build(Some(&q), "mbox".into());
        let inner = pluck_user_filter(c.filter);
        let JmapFilter::Operator(JmapFilterOperator {
            operator,
            conditions,
        }) = inner
        else {
            panic!("expected OR operator");
        };
        assert_eq!(operator, JmapFilterOperatorKind::Or);
        assert_eq!(conditions.len(), 2);
    }

    #[test]
    fn not_wraps_a_single_subfilter() {
        let q = SearchEmailsQuery {
            filter: Some(SearchEmailsFilterQuery::Not(Box::new(
                SearchEmailsFilterQuery::From("a".into()),
            ))),
            sort: None,
        };
        let c = build(Some(&q), "mbox".into());
        let inner = pluck_user_filter(c.filter);
        let JmapFilter::Operator(JmapFilterOperator {
            operator,
            conditions,
        }) = inner
        else {
            panic!("expected NOT operator");
        };
        assert_eq!(operator, JmapFilterOperatorKind::Not);
        assert_eq!(conditions.len(), 1);
    }

    #[test]
    fn date_clause_records_post_filter_and_bounds_after() {
        let q = SearchEmailsQuery {
            filter: Some(SearchEmailsFilterQuery::Date(naive(2026, 1, 15))),
            sort: None,
        };
        let c = build(Some(&q), "mbox".into());
        assert!(matches!(
            c.post_filters.as_slice(),
            [PostFilter::Date(d)] if *d == naive(2026, 1, 15)
        ));
        let inner = pluck_user_filter(c.filter);
        match inner {
            JmapFilter::Condition(JmapEmailFilter { after, .. }) => {
                assert_eq!(after.as_deref(), Some("2026-01-15T00:00:00Z"));
            }
            other => panic!("expected condition, got {other:?}"),
        }
    }

    #[test]
    fn after_clause_bumps_lower_bound_by_one_day() {
        let q = SearchEmailsQuery {
            filter: Some(SearchEmailsFilterQuery::AfterDate(naive(2026, 1, 15))),
            sort: None,
        };
        let c = build(Some(&q), "mbox".into());
        assert!(matches!(
            c.post_filters.as_slice(),
            [PostFilter::AfterDate(d)] if *d == naive(2026, 1, 15)
        ));
        let inner = pluck_user_filter(c.filter);
        match inner {
            JmapFilter::Condition(JmapEmailFilter { after, .. }) => {
                assert_eq!(after.as_deref(), Some("2026-01-16T00:00:00Z"));
            }
            other => panic!("expected condition, got {other:?}"),
        }
    }

    #[test]
    fn post_filter_drops_false_positives_for_date_clause() {
        let post = [PostFilter::Date(naive(2026, 5, 15))];
        let on_day = envelope_at("2026-05-15T10:00:00+00:00");
        let next_day = envelope_at("2026-05-16T00:00:00+00:00");
        assert!(post_match(&on_day, &post));
        assert!(!post_match(&next_day, &post));
    }

    #[test]
    fn post_filter_strict_after() {
        let post = [PostFilter::AfterDate(naive(2026, 5, 15))];
        let on_day = envelope_at("2026-05-15T23:59:59+00:00");
        let next_day = envelope_at("2026-05-16T00:00:00+00:00");
        assert!(!post_match(&on_day, &post));
        assert!(post_match(&next_day, &post));
    }

    #[test]
    fn flag_clause_sets_has_keyword() {
        use crate::flag::types::IanaFlag;

        let q = SearchEmailsQuery {
            filter: Some(SearchEmailsFilterQuery::Flag(Flag::from_iana(
                IanaFlag::Flagged,
            ))),
            sort: None,
        };
        let c = build(Some(&q), "mbox".into());
        let inner = pluck_user_filter(c.filter);
        match inner {
            JmapFilter::Condition(JmapEmailFilter { has_keyword, .. }) => {
                assert_eq!(has_keyword.as_deref(), Some("$flagged"));
            }
            other => panic!("expected condition, got {other:?}"),
        }
    }
}
