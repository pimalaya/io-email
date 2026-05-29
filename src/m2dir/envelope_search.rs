//! m2dir envelope-search coroutine.
//!
//! Same shape as [`crate::m2dir::envelope_list::M2dirEnvelopeList`]:
//!
//! 1. [`M2dirMessageList`] walks the entry directory and emits one
//!    [`M2dirEntry`] per file.
//! 2. A `WantsFileRead` batch fetches the message bytes and each
//!    `.meta/<id>.flags` sidecar in one round.
//! 3. The coroutine parses RFC 5322 headers via [`mail_parser`],
//!    folds in the sidecar flags, evaluates the shared filter (body
//!    matching reuses the in-memory bytes), sorts, paginates.
//!
//! [`M2dirMessageList`]: io_m2dir::coroutines::message_list::M2dirMessageList

use alloc::{
    collections::{BTreeMap, BTreeSet},
    string::String,
    vec::Vec,
};
use core::{cmp::Ordering, mem};
use std::path::PathBuf;

use chrono::{DateTime, FixedOffset, NaiveDate};
use io_m2dir::{
    coroutine::{M2dirArg, M2dirCoroutine, M2dirCoroutineState, M2dirYield},
    coroutines::message_list::{M2dirMessageList as InnerList, M2dirMessageListError as InnerErr},
    entry::M2dirEntry,
    flag::M2dirFlags,
    m2dir::M2dir,
    path::M2dirPath,
};
use log::trace;
use mail_parser::MessageParser;
use thiserror::Error;

use crate::{
    address::Address,
    coroutine::{
        EmailBackend, EmailCoroutine, EmailCoroutineArg, EmailCoroutineState, FsBatch, FsStep,
    },
    envelope::Envelope,
    m2dir::convert::{
        InvalidMailboxName, dirread_in, envelope_from, paginate, paths_out, probes_in,
        resolve_mailbox,
    },
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
    #[error("coroutine was resumed with the wrong EmailCoroutineArg variant")]
    InvalidArg,
    #[error("coroutine was resumed with an FsBatch variant it did not request")]
    UnexpectedBatch,
    #[error("coroutine was resumed after completion")]
    ResumedAfterDone,
    #[error("failed to parse m2dir message at {0:?}")]
    Parse(M2dirPath),
}

/// I/O-free coroutine listing every message inside a single m2dir,
/// then applying the shared filter + sort + paginate client-side.
/// `page = None` is treated as page 1; `page_size = None` keeps the
/// whole match.
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
        let inner = InnerList::new(m2dir.clone());
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

impl EmailCoroutine for M2dirEnvelopeSearch {
    type Yield = FsStep;
    type Return = Result<Vec<Envelope>, M2dirEnvelopeSearchError>;

    const BACKEND: EmailBackend = EmailBackend::M2dir;

    fn resume(
        &mut self,
        arg: EmailCoroutineArg<'_>,
    ) -> EmailCoroutineState<Self::Yield, Self::Return> {
        #[allow(irrefutable_let_patterns)]
        let EmailCoroutineArg::Fs { batch } = arg else {
            return EmailCoroutineState::Complete(Err(M2dirEnvelopeSearchError::InvalidArg));
        };

        match mem::replace(&mut self.state, State::Done) {
            State::Listing(mut inner) => {
                let inner_arg = match batch {
                    None => None,
                    Some(FsBatch::DirRead(entries)) => Some(M2dirArg::DirRead(dirread_in(entries))),
                    Some(FsBatch::FileExists(probes)) => {
                        Some(M2dirArg::FileExists(probes_in(probes)))
                    }
                    Some(_) => {
                        return EmailCoroutineState::Complete(Err(
                            M2dirEnvelopeSearchError::UnexpectedBatch,
                        ));
                    }
                };
                match inner.resume(inner_arg) {
                    M2dirCoroutineState::Yielded(M2dirYield::WantsDirRead(p)) => {
                        self.state = State::Listing(inner);
                        EmailCoroutineState::Yielded(FsStep::WantsDirRead(paths_out(p)))
                    }
                    M2dirCoroutineState::Yielded(M2dirYield::WantsFileExists(p)) => {
                        self.state = State::Listing(inner);
                        EmailCoroutineState::Yielded(FsStep::WantsFileExists(paths_out(p)))
                    }
                    M2dirCoroutineState::Complete(Ok(entries)) => {
                        if entries.is_empty() {
                            return EmailCoroutineState::Complete(Ok(Vec::new()));
                        }
                        let mut paths: BTreeSet<PathBuf> = BTreeSet::new();
                        for entry in &entries {
                            paths.insert(PathBuf::from(entry.path().clone()));
                            paths.insert(PathBuf::from(self.m2dir.flags_path(entry.id())));
                        }
                        self.state = State::Reading(entries);
                        EmailCoroutineState::Yielded(FsStep::WantsFileRead(paths))
                    }
                    M2dirCoroutineState::Complete(Err(err)) => {
                        EmailCoroutineState::Complete(Err(err.into()))
                    }
                    other => {
                        let _ = other;
                        unreachable!("M2dirMessageList never yields this state");
                    }
                }
            }
            State::Reading(entries) => {
                let Some(FsBatch::FileRead(contents)) = batch else {
                    self.state = State::Reading(entries);
                    return EmailCoroutineState::Complete(Err(
                        M2dirEnvelopeSearchError::UnexpectedBatch,
                    ));
                };
                let mut contents: BTreeMap<M2dirPath, Vec<u8>> =
                    contents.into_iter().map(|(k, v)| (k.into(), v)).collect();
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
                        return EmailCoroutineState::Complete(Err(
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

                EmailCoroutineState::Complete(Ok(paginate(hits, self.page, self.page_size)))
            }
            State::Done => {
                EmailCoroutineState::Complete(Err(M2dirEnvelopeSearchError::ResumedAfterDone))
            }
        }
    }
}

enum State {
    Listing(InnerList),
    Reading(Vec<M2dirEntry>),
    Done,
}

/// Parses `.meta/<id>.flags` lines into [`M2dirFlags`]. Matches the
/// implementation in [`crate::m2dir::envelope_list`].
fn parse_meta_flags(bytes: &[u8]) -> M2dirFlags {
    let Ok(text) = core::str::from_utf8(bytes) else {
        return M2dirFlags::default();
    };
    text.lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect()
}

/// Evaluates `filter` against `envelope`. `body` clauses scan the
/// message bytes directly.
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
