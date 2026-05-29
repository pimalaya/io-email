//! JMAP envelope-search coroutine.
//!
//! Two-stage state machine:
//! 1. `Mailbox/query + Mailbox/get` resolves the shared mailbox name
//!    to a JMAP id.
//! 2. `Email/query + Email/get` (batched in a single HTTP round-trip)
//!    runs the translated filter + sort scoped to that mailbox.
//!
//! The shared filter AST is translated into a JMAP
//! [`Filter<EmailFilter>`] tree: AND/OR/NOT map to
//! [`FilterOperator`]s and leaves map to flat `EmailFilter`
//! conditions. The mailbox scoping is added as a top-level AND.
//!
//! Date semantics: the shared DSL targets the `Date:` header
//! (sent-at) while JMAP's `before` / `after` filter primitives are
//! anchored to `receivedAt`. The conversion over-approximates by
//! anchoring on `receivedAt`, then re-applies the exact `sentAt`
//! predicate client-side via [`PostFilter`]. When post-filters are
//! present, server-side pagination would slice the over-approximated
//! result before we trim it, so the coroutine fetches without
//! `position` / `limit` and paginates after the client-side pass.

use alloc::{string::String, vec, vec::Vec};
use core::mem;

use chrono::{Datelike, NaiveDate};
use io_jmap::{
    coroutine::{JmapCoroutine, JmapCoroutineState, JmapYield},
    rfc8620::{filter::Filter, session::JmapSession},
    rfc8621::{
        email::{EmailComparator, EmailFilter, EmailSortProperty},
        email_query::{JmapEmailQuery as InnerQuery, JmapEmailQueryError as QueryErr},
        mailbox::{MailboxFilter, MailboxProperty},
        mailbox_query::{
            JmapMailboxQuery as InnerMailboxQuery, JmapMailboxQueryError as MailboxQueryErr,
        },
    },
};
use log::trace;
use secrecy::SecretString;
use thiserror::Error;

use crate::{
    coroutine::{EmailBackend, EmailCoroutine, EmailCoroutineArg, EmailCoroutineState, JmapStep},
    envelope::Envelope,
    jmap::convert::{
        compute_position_limit, envelope_from, envelope_properties, find_mailbox_id, keyword_from,
    },
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
    MailboxQuery(#[from] MailboxQueryErr),
    #[error(transparent)]
    EmailQuery(#[from] QueryErr),
    #[error("no JMAP mailbox named `{0}` found")]
    NotFound(String),
    #[error("coroutine was resumed with the wrong EmailCoroutineArg variant")]
    InvalidArg,
    #[error("coroutine was resumed after completion")]
    ResumedAfterDone,
}

/// Residual client-side predicate left over after the server filter
/// was applied. Each variant re-checks one AST leaf against the
/// envelope's `sentAt` (carried in [`Envelope::date`]).
#[derive(Clone, Debug)]
pub enum PostFilter {
    Date(NaiveDate),
    AfterDate(NaiveDate),
}

/// I/O-free coroutine wrapping a mailbox-name lookup + batched
/// `Email/query` + `Email/get`. Applies any residual `sentAt`
/// predicate client-side before paginating.
pub struct JmapEnvelopeSearch {
    state: State,
    name: String,
    session: JmapSession,
    http_auth: SecretString,
    query: Option<SearchEmailsQuery>,
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
        let resolver = InnerMailboxQuery::new(
            session,
            http_auth,
            Some(MailboxFilter {
                name: Some(mailbox.into()),
                ..MailboxFilter::default()
            }),
            None,
            None,
            None,
            Some(vec![MailboxProperty::Id, MailboxProperty::Name]),
        )?;
        Ok(Self {
            state: State::Resolving(resolver),
            name: mailbox.into(),
            session: session.clone(),
            http_auth: http_auth.clone(),
            query: query.cloned(),
            page,
            page_size,
        })
    }
}

impl EmailCoroutine for JmapEnvelopeSearch {
    type Yield = JmapStep;
    type Return = Result<Vec<Envelope>, JmapEnvelopeSearchError>;

    const BACKEND: EmailBackend = EmailBackend::Jmap;

    fn resume(
        &mut self,
        arg: EmailCoroutineArg<'_>,
    ) -> EmailCoroutineState<Self::Yield, Self::Return> {
        #[allow(irrefutable_let_patterns)]
        let EmailCoroutineArg::Jmap { bytes } = arg else {
            return EmailCoroutineState::Complete(Err(JmapEnvelopeSearchError::InvalidArg));
        };

        match mem::replace(&mut self.state, State::Done) {
            State::Resolving(mut resolver) => match resolver.resume(bytes) {
                JmapCoroutineState::Complete(Ok(ok)) => {
                    let Some(id) = find_mailbox_id(&ok.mailboxes, &self.name) else {
                        return EmailCoroutineState::Complete(Err(
                            JmapEnvelopeSearchError::NotFound(self.name.clone()),
                        ));
                    };

                    let Converted {
                        filter,
                        sort,
                        post_filters,
                    } = build(self.query.as_ref(), id);

                    let paginate_client_side = !post_filters.is_empty();
                    let (position, limit) = if paginate_client_side {
                        (None, None)
                    } else {
                        compute_position_limit(self.page, self.page_size)
                    };

                    let inner = match InnerQuery::new(
                        &self.session,
                        &self.http_auth,
                        Some(filter),
                        Some(sort),
                        position,
                        limit,
                        Some(envelope_properties()),
                    ) {
                        Ok(q) => q,
                        Err(err) => return EmailCoroutineState::Complete(Err(err.into())),
                    };
                    self.state = State::Searching {
                        inner,
                        post_filters,
                    };
                    self.resume(EmailCoroutineArg::Jmap { bytes: None })
                }
                JmapCoroutineState::Yielded(JmapYield::WantsRead) => {
                    self.state = State::Resolving(resolver);
                    EmailCoroutineState::Yielded(JmapStep::WantsRead)
                }
                JmapCoroutineState::Yielded(JmapYield::WantsWrite(out)) => {
                    self.state = State::Resolving(resolver);
                    EmailCoroutineState::Yielded(JmapStep::WantsWrite(out))
                }
                JmapCoroutineState::Complete(Err(err)) => {
                    EmailCoroutineState::Complete(Err(err.into()))
                }
            },
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

                    EmailCoroutineState::Complete(Ok(envelopes))
                }
                JmapCoroutineState::Yielded(JmapYield::WantsRead) => {
                    self.state = State::Searching {
                        inner,
                        post_filters,
                    };
                    EmailCoroutineState::Yielded(JmapStep::WantsRead)
                }
                JmapCoroutineState::Yielded(JmapYield::WantsWrite(out)) => {
                    self.state = State::Searching {
                        inner,
                        post_filters,
                    };
                    EmailCoroutineState::Yielded(JmapStep::WantsWrite(out))
                }
                JmapCoroutineState::Complete(Err(err)) => {
                    EmailCoroutineState::Complete(Err(err.into()))
                }
            },
            State::Done => {
                EmailCoroutineState::Complete(Err(JmapEnvelopeSearchError::ResumedAfterDone))
            }
        }
    }
}

enum State {
    Resolving(InnerMailboxQuery),
    Searching {
        inner: InnerQuery,
        post_filters: Vec<PostFilter>,
    },
    Done,
}

/// Output of [`build`]: the JMAP-side filter (mailbox-scoped, AND/OR/NOT
/// supported), the JMAP-side comparator list, and the residual
/// client-side predicates the coroutine must re-apply on the returned
/// envelopes.
struct Converted {
    filter: Filter<EmailFilter>,
    sort: Vec<EmailComparator>,
    post_filters: Vec<PostFilter>,
}

/// Converts `query` into JMAP primitives. The result is always
/// AND-scoped to `mailbox_id`; an empty user filter yields just the
/// mailbox scope.
fn build(query: Option<&SearchEmailsQuery>, mailbox_id: String) -> Converted {
    let mailbox_scope = Filter::Condition(EmailFilter {
        in_mailbox: Some(mailbox_id),
        ..EmailFilter::default()
    });

    let mut post_filters = Vec::new();
    let user_filter = query
        .and_then(|q| q.filter.as_ref())
        .map(|f| convert_filter(f, &mut post_filters));

    let filter = match user_filter {
        Some(uf) => Filter::and(vec![mailbox_scope, uf]),
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

/// Recursively translates `node` into a JMAP filter tree. Date-leaf
/// conversions push a [`PostFilter`] entry so the strict sent-at
/// semantics can be re-applied client-side.
fn convert_filter(
    node: &SearchEmailsFilterQuery,
    post_filters: &mut Vec<PostFilter>,
) -> Filter<EmailFilter> {
    use SearchEmailsFilterQuery as Q;

    match node {
        Q::And(left, right) => Filter::and(vec![
            convert_filter(left, post_filters),
            convert_filter(right, post_filters),
        ]),
        Q::Or(left, right) => Filter::or(vec![
            convert_filter(left, post_filters),
            convert_filter(right, post_filters),
        ]),
        Q::Not(inner) => Filter::not(vec![convert_filter(inner, post_filters)]),

        Q::From(pattern) => Filter::Condition(EmailFilter {
            from: Some(pattern.clone()),
            ..EmailFilter::default()
        }),
        Q::To(pattern) => Filter::Condition(EmailFilter {
            to: Some(pattern.clone()),
            ..EmailFilter::default()
        }),
        Q::Subject(pattern) => Filter::Condition(EmailFilter {
            subject: Some(pattern.clone()),
            ..EmailFilter::default()
        }),
        Q::Body(pattern) => Filter::Condition(EmailFilter {
            body: Some(pattern.clone()),
            ..EmailFilter::default()
        }),
        Q::Flag(flag) => Filter::Condition(EmailFilter {
            has_keyword: Some(keyword_from(flag)),
            ..EmailFilter::default()
        }),

        // Over-approximate via `after = start-of-day(D)` (the lowest
        // `receivedAt` consistent with "sentAt-day == D"); the exact
        // sent-at constraint is re-checked client-side.
        Q::Date(target) => {
            post_filters.push(PostFilter::Date(*target));
            Filter::Condition(EmailFilter {
                after: Some(utc_midnight(*target)),
                ..EmailFilter::default()
            })
        }
        // Over-approximate via `after = start-of-day(D+1)` (the
        // lowest `receivedAt` consistent with "sentAt-day > D"); the
        // strict sent-at constraint is re-checked client-side.
        Q::AfterDate(target) => {
            post_filters.push(PostFilter::AfterDate(*target));
            let bumped = target.succ_opt().unwrap_or(*target);
            Filter::Condition(EmailFilter {
                after: Some(utc_midnight(bumped)),
                ..EmailFilter::default()
            })
        }
    }
}

/// Returns `true` when `envelope` matches every residual predicate.
/// Apply after the JMAP server returns its (over-approximating)
/// result set to drop the false positives.
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

fn sent_at_desc() -> EmailComparator {
    EmailComparator {
        property: EmailSortProperty::SentAt,
        is_ascending: Some(false),
        collation: None,
        keyword: None,
    }
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

#[cfg(test)]
mod tests {
    use alloc::boxed::Box;

    use chrono::{DateTime, NaiveDate};
    use io_jmap::rfc8620::filter::{FilterOperator, FilterOperatorKind};

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

    fn pluck_user_filter(filter: Filter<EmailFilter>) -> Filter<EmailFilter> {
        // build() always wraps the user filter in `AND(mailbox_scope,
        // user_filter)` when a user filter is present.
        let Filter::Operator(FilterOperator { conditions, .. }) = filter else {
            panic!("expected top-level AND combinator");
        };
        conditions.into_iter().nth(1).expect("expected user filter")
    }

    #[test]
    fn empty_query_yields_just_the_mailbox_scope() {
        let c = build(None, "mbox-1".into());
        match c.filter {
            Filter::Condition(EmailFilter {
                in_mailbox: Some(id),
                ..
            }) => assert_eq!(id, "mbox-1"),
            other => panic!("expected mailbox-scope condition, got {other:?}"),
        }
        assert!(c.post_filters.is_empty());
        assert_eq!(c.sort.len(), 1);
        assert!(matches!(c.sort[0].property, EmailSortProperty::SentAt));
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
        let Filter::Operator(FilterOperator {
            operator,
            conditions,
        }) = inner
        else {
            panic!("expected OR operator");
        };
        assert_eq!(operator, FilterOperatorKind::Or);
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
        let Filter::Operator(FilterOperator {
            operator,
            conditions,
        }) = inner
        else {
            panic!("expected NOT operator");
        };
        assert_eq!(operator, FilterOperatorKind::Not);
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
            Filter::Condition(EmailFilter { after, .. }) => {
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
            Filter::Condition(EmailFilter { after, .. }) => {
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
        use crate::flag::IanaFlag;

        let q = SearchEmailsQuery {
            filter: Some(SearchEmailsFilterQuery::Flag(Flag::from_iana(
                IanaFlag::Flagged,
            ))),
            sort: None,
        };
        let c = build(Some(&q), "mbox".into());
        let inner = pluck_user_filter(c.filter);
        match inner {
            Filter::Condition(EmailFilter { has_keyword, .. }) => {
                assert_eq!(has_keyword.as_deref(), Some("$flagged"));
            }
            other => panic!("expected condition, got {other:?}"),
        }
    }
}
