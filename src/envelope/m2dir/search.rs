//! m2dir envelope-search coroutine: same shape as
//! [`crate::envelope::m2dir::list::M2dirEnvelopeList`], with shared
//! filter + sort + paginate applied client-side.
//!
//! Body matching reuses the already-loaded message bytes.
//!
//! # Example
//!
//! ```rust,ignore
//! use io_email::envelope::m2dir::search::M2dirEnvelopeSearch;
//!
//! let envs = client.run(M2dirEnvelopeSearch::new(&client.root, "INBOX", Some(&query), None, None, false)?)?;
//! ```

use alloc::{collections::BTreeSet, string::String, vec::Vec};
use core::{cmp::Ordering, mem};
use std::path::PathBuf;

use chrono::{DateTime, FixedOffset, NaiveDate};
use io_m2dir::{
    coroutine::*,
    entry::{
        list::{
            M2dirEntryList as InnerList, M2dirEntryListError as InnerErr,
            M2dirEntryListOptions as InnerOpts,
        },
        types::M2dirEntry,
    },
    flag::types::M2dirFlags,
    m2dir::types::M2dir,
    path::M2dirPath,
};
use log::trace;
use mail_parser::MessageParser;
use thiserror::Error;

use crate::{
    address::Address,
    envelope::types::Envelope,
    m2dir::convert::{InvalidMailboxName, envelope_from, paginate, resolve_mailbox},
    search::{
        filter::query::SearchEmailsFilterQuery,
        query::SearchEmailsQuery,
        sort::query::{SearchEmailsSorter, SearchEmailsSorterKind, SearchEmailsSorterOrder},
    },
};

/// Errors produced by [`M2dirEnvelopeSearch`].
#[derive(Debug, Error)]
pub enum M2dirEnvelopeSearchError {
    #[error(transparent)]
    List(#[from] InnerErr),
    #[error(transparent)]
    InvalidMailbox(#[from] InvalidMailboxName),
    #[error("coroutine was resumed with an M2dirArg variant it did not request")]
    UnexpectedArg,
    #[error("coroutine was resumed after completion")]
    ResumedAfterDone,
    #[error("failed to parse m2dir message at {0:?}")]
    Parse(M2dirPath),
}

/// I/O-free coroutine listing then client-side filtering + sorting +
/// paginating an m2dir's messages.
pub struct M2dirEnvelopeSearch {
    state: State,
    m2dir: M2dir,
    filter: Option<SearchEmailsFilterQuery>,
    sort: Option<Vec<SearchEmailsSorter>>,
    page: Option<u32>,
    page_size: Option<u32>,
    with_attachment: bool,
}

impl M2dirEnvelopeSearch {
    pub fn new(
        root: impl Into<PathBuf>,
        mailbox: &str,
        query: Option<&SearchEmailsQuery>,
        page: Option<u32>,
        page_size: Option<u32>,
        with_attachment: bool,
    ) -> Result<Self, M2dirEnvelopeSearchError> {
        trace!("prepare m2dir envelope search");
        let m2dir = resolve_mailbox(root, mailbox)?;
        let inner = InnerList::new(m2dir.clone(), InnerOpts::default());
        Ok(Self {
            state: State::Listing(inner),
            m2dir,
            filter: query.and_then(|q| q.filter.clone()),
            sort: query.and_then(|q| q.sort.clone()),
            page,
            page_size,
            with_attachment,
        })
    }
}

enum State {
    Listing(InnerList),
    Reading(Vec<M2dirEntry>),
    Done,
}

/// Reads a .meta/<id>.flags file (one flag per non-empty trimmed line).
fn parse_meta_flags(bytes: &[u8]) -> M2dirFlags {
    let Ok(text) = core::str::from_utf8(bytes) else {
        return M2dirFlags::default();
    };
    text.lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect()
}

/// Evaluates `filter` against `envelope`; `body` scans `raw`.
fn matches_filter(envelope: &Envelope, raw: &[u8], filter: &SearchEmailsFilterQuery) -> bool {
    use SearchEmailsFilterQuery as Q;

    match filter {
        Q::And(left, right) => {
            matches_filter(envelope, raw, left) && matches_filter(envelope, raw, right)
        }
        Q::Or(left, right) => {
            matches_filter(envelope, raw, left) || matches_filter(envelope, raw, right)
        }
        Q::Not(inner) => !matches_filter(envelope, raw, inner),
        Q::Date(target) => same_day(envelope.date, *target),
        Q::AfterDate(target) => after_day(envelope.date, *target),
        Q::From(pattern) => addresses_contain(&envelope.from, pattern),
        Q::To(pattern) => addresses_contain(&envelope.to, pattern),
        Q::Subject(pattern) => contains_ci(&envelope.subject, pattern),
        Q::Body(pattern) => body_contains(raw, pattern),
        Q::Flag(flag) => envelope.flags.contains(flag),
    }
}

fn body_contains(raw: &[u8], pattern: &str) -> bool {
    let Some(msg) = MessageParser::new().parse(raw) else {
        return false;
    };
    let needle = pattern.as_bytes();
    for part in msg.text_bodies() {
        if contains_ignore_ascii_case(part.contents(), needle) {
            return true;
        }
    }
    for part in msg.html_bodies() {
        if contains_ignore_ascii_case(part.contents(), needle) {
            return true;
        }
    }
    false
}

fn contains_ignore_ascii_case(haystack: &[u8], needle: &[u8]) -> bool {
    if needle.is_empty() {
        return true;
    }
    if needle.len() > haystack.len() {
        return false;
    }
    haystack
        .windows(needle.len())
        .any(|w| w.eq_ignore_ascii_case(needle))
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

fn compare_with(left: &Envelope, right: &Envelope, sort: &[SearchEmailsSorter]) -> Ordering {
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

fn first_addr_key(addrs: &[Address]) -> Option<String> {
    addrs.first().map(|a| {
        a.name
            .as_deref()
            .map(str::to_lowercase)
            .unwrap_or_else(|| a.email.to_lowercase())
    })
}

impl M2dirCoroutine for M2dirEnvelopeSearch {
    type Yield = M2dirYield;
    type Return = Result<Vec<Envelope>, M2dirEnvelopeSearchError>;

    fn resume(&mut self, arg: Option<M2dirArg>) -> M2dirCoroutineState<Self::Yield, Self::Return> {
        match mem::replace(&mut self.state, State::Done) {
            State::Listing(mut inner) => match inner.resume(arg) {
                M2dirCoroutineState::Yielded(y) => {
                    self.state = State::Listing(inner);
                    M2dirCoroutineState::Yielded(y)
                }
                M2dirCoroutineState::Complete(Ok(entries)) => {
                    if entries.is_empty() {
                        return M2dirCoroutineState::Complete(Ok(Vec::new()));
                    }
                    let mut paths: BTreeSet<M2dirPath> = BTreeSet::new();
                    for entry in &entries {
                        paths.insert(entry.path().clone());
                        paths.insert(self.m2dir.flags_path(entry.id()));
                    }
                    self.state = State::Reading(entries);
                    M2dirCoroutineState::Yielded(M2dirYield::WantsFileRead(paths))
                }
                M2dirCoroutineState::Complete(Err(err)) => {
                    M2dirCoroutineState::Complete(Err(err.into()))
                }
            },
            State::Reading(entries) => {
                let Some(M2dirArg::FileRead(mut contents)) = arg else {
                    self.state = State::Reading(entries);
                    return M2dirCoroutineState::Complete(Err(
                        M2dirEnvelopeSearchError::UnexpectedArg,
                    ));
                };
                let parser = MessageParser::default();
                let mut hits: Vec<Envelope> = Vec::with_capacity(entries.len());

                for entry in entries {
                    let Some(body) = contents.remove(entry.path()) else {
                        continue;
                    };
                    let flags_bytes = contents
                        .remove(&self.m2dir.flags_path(entry.id()))
                        .unwrap_or_default();
                    let flags = parse_meta_flags(&flags_bytes);

                    let parsed = if self.with_attachment {
                        parser.parse(&body)
                    } else {
                        parser.parse_headers(&body)
                    };
                    let Some(parsed) = parsed else {
                        return M2dirCoroutineState::Complete(Err(
                            M2dirEnvelopeSearchError::Parse(entry.path().clone()),
                        ));
                    };

                    let mut envelope = envelope_from(&entry, &flags, &parsed);
                    if self.with_attachment {
                        envelope.has_attachment = Some(parsed.attachment_count() > 0);
                    }

                    let keep = match self.filter.as_ref() {
                        Some(f) => matches_filter(&envelope, &body, f),
                        None => true,
                    };
                    if keep {
                        hits.push(envelope);
                    }
                }

                match self.sort.as_deref() {
                    Some(sort) if !sort.is_empty() => {
                        hits.sort_by(|a, b| compare_with(a, b, sort));
                    }
                    _ => hits.sort_by(|a, b| b.date.cmp(&a.date)),
                }

                M2dirCoroutineState::Complete(Ok(paginate(hits, self.page, self.page_size)))
            }
            State::Done => {
                M2dirCoroutineState::Complete(Err(M2dirEnvelopeSearchError::ResumedAfterDone))
            }
        }
    }
}
