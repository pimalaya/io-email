//! Maildir envelope-search coroutine.
//!
//! Same two-phase shape as
//! [`crate::maildir::envelope_list::MaildirEnvelopeList`]:
//!
//! 1. [`MaildirMessagesList`] walks `cur/` + `new/` and returns one
//!    [`MaildirEntry`] per file.
//! 2. A second pass batches the entry paths through
//!    [`FsStep::WantsFileRead`]; the driver reads each
//!    file and feeds the bytes back so the coroutine can build
//!    envelopes (subject, from, to, date, flags) via
//!    [`mail_parser::Message`], evaluate the shared filter against
//!    those envelopes (with `body` filters falling back to a scan of
//!    the same in-memory bytes), apply the sort chain, and paginate.
//!
//! Body-filter handling: the listing pass already loads every
//! candidate file's bytes for header parsing, so `body <pattern>`
//! reuses that buffer to scan plain-text and HTML body parts via
//! [`mail_parser::MessageParser`]. No extra round-trip is needed.
//!
//! [`MaildirMessagesList`]: io_maildir::coroutines::message_list::MaildirMessagesList

use alloc::{
    collections::{BTreeMap, BTreeSet},
    string::{String, ToString},
    vec::Vec,
};
use core::{cmp::Ordering, mem};
use std::path::PathBuf;

use chrono::{DateTime, FixedOffset, NaiveDate};
use io_maildir::{
    coroutine::{MaildirCoroutine, MaildirCoroutineState, MaildirReply, MaildirYield},
    coroutines::message_list::{
        MaildirMessagesList as InnerList, MaildirMessagesListError as InnerErr,
    },
    entry::MaildirEntry,
    maildir::Maildir,
    message::MaildirMessage,
    path::MaildirPath,
};
use log::trace;
use mail_parser::{Address as MailParserAddress, MessageParser};
use thiserror::Error;

use crate::{
    address::Address,
    coroutine::{
        EmailBackend, EmailCoroutine, EmailCoroutineArg, EmailCoroutineState, FsBatch, FsStep,
    },
    envelope::{Envelope, normalize_message_id},
    flag::Flag,
    maildir::convert::{
        InvalidMailboxName, dirread_in, flag_from_char, paginate, paths_out, probes_in,
        resolve_mailbox,
    },
    search::{
        filter::query::SearchEmailsFilterQuery,
        query::SearchEmailsQuery,
        sort::query::{SearchEmailsSorter, SearchEmailsSorterKind, SearchEmailsSorterOrder},
    },
};

/// Errors produced by [`MaildirEnvelopeSearch`].
#[derive(Debug, Error)]
pub enum MaildirEnvelopeSearchError {
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
}

/// I/O-free coroutine listing every message inside a single Maildir,
/// then applying the shared filter + sort + paginate client-side.
/// `page = None` is treated as page 1; `page_size = None` keeps the
/// whole match.
pub struct MaildirEnvelopeSearch {
    state: State,
    filter: Option<SearchEmailsFilterQuery>,
    sort: Option<Vec<SearchEmailsSorter>>,
    page: Option<u32>,
    page_size: Option<u32>,
}

impl MaildirEnvelopeSearch {
    pub fn new(
        root: impl Into<PathBuf>,
        maildir_plus: bool,
        mailbox: &str,
        query: Option<&SearchEmailsQuery>,
        page: Option<u32>,
        page_size: Option<u32>,
    ) -> Result<Self, MaildirEnvelopeSearchError> {
        trace!("prepare Maildir envelope search");
        let path = resolve_mailbox(&root.into(), maildir_plus, mailbox)?;
        let maildir = Maildir::from_path(path);
        Ok(Self {
            state: State::Listing(InnerList::new(maildir)),
            filter: query.and_then(|q| q.filter.clone()),
            sort: query.and_then(|q| q.sort.clone()),
            page,
            page_size,
        })
    }
}

impl EmailCoroutine for MaildirEnvelopeSearch {
    type Yield = FsStep;
    type Return = Result<Vec<Envelope>, MaildirEnvelopeSearchError>;

    const BACKEND: EmailBackend = EmailBackend::Maildir;

    fn resume(
        &mut self,
        arg: EmailCoroutineArg<'_>,
    ) -> EmailCoroutineState<Self::Yield, Self::Return> {
        #[allow(irrefutable_let_patterns)]
        let EmailCoroutineArg::Fs { batch } = arg else {
            return EmailCoroutineState::Complete(Err(MaildirEnvelopeSearchError::InvalidArg));
        };

        match mem::replace(&mut self.state, State::Done) {
            State::Listing(mut inner) => {
                let inner_arg = match batch {
                    None => None,
                    Some(FsBatch::DirRead(entries)) => {
                        Some(MaildirReply::DirRead(dirread_in(entries)))
                    }
                    Some(FsBatch::FileExists(probes)) => {
                        Some(MaildirReply::FileExists(probes_in(probes)))
                    }
                    Some(_) => {
                        return EmailCoroutineState::Complete(Err(
                            MaildirEnvelopeSearchError::UnexpectedBatch,
                        ));
                    }
                };
                match inner.resume(inner_arg) {
                    MaildirCoroutineState::Yielded(MaildirYield::WantsDirRead(paths)) => {
                        self.state = State::Listing(inner);
                        EmailCoroutineState::Yielded(FsStep::WantsDirRead(paths_out(paths)))
                    }
                    MaildirCoroutineState::Yielded(MaildirYield::WantsFileExists(paths)) => {
                        self.state = State::Listing(inner);
                        EmailCoroutineState::Yielded(FsStep::WantsFileExists(paths_out(paths)))
                    }
                    MaildirCoroutineState::Complete(Ok(entries)) => {
                        if entries.is_empty() {
                            return EmailCoroutineState::Complete(Ok(Vec::new()));
                        }
                        let paths: BTreeSet<PathBuf> = entries
                            .iter()
                            .map(|e| PathBuf::from(e.path().clone()))
                            .collect();
                        self.state = State::Reading(entries);
                        EmailCoroutineState::Yielded(FsStep::WantsFileRead(paths))
                    }
                    MaildirCoroutineState::Complete(Err(err)) => {
                        EmailCoroutineState::Complete(Err(err.into()))
                    }
                    other => {
                        let _ = other;
                        unreachable!("MaildirMessagesList never yields this state");
                    }
                }
            }
            State::Reading(entries) => {
                let Some(FsBatch::FileRead(contents)) = batch else {
                    self.state = State::Reading(entries);
                    return EmailCoroutineState::Complete(Err(
                        MaildirEnvelopeSearchError::UnexpectedBatch,
                    ));
                };
                let mut contents: BTreeMap<MaildirPath, Vec<u8>> =
                    contents.into_iter().map(|(k, v)| (k.into(), v)).collect();

                // For each entry, materialise (Envelope, raw bytes) so
                // that `body` filters can scan the same buffer used for
                // header parsing.
                let mut hits: Vec<Envelope> = Vec::with_capacity(entries.len());
                for entry in entries {
                    let Some(bytes) = contents.remove(entry.path()) else {
                        continue;
                    };
                    let envelope = envelope_from_bytes(entry.path(), &bytes);
                    let keep = match self.filter.as_ref() {
                        Some(f) => matches_filter(&envelope, &bytes, f),
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
                EmailCoroutineState::Complete(Err(MaildirEnvelopeSearchError::ResumedAfterDone))
            }
        }
    }
}

enum State {
    Listing(InnerList),
    Reading(BTreeSet<MaildirEntry>),
    Done,
}

/// Builds an [`Envelope`] from a Maildir file: filename letters for
/// flags, RFC 5322 headers via mail-parser.
fn envelope_from_bytes(path: &MaildirPath, bytes: &[u8]) -> Envelope {
    let message = MaildirMessage::from((path.clone(), bytes.to_vec()));
    let id = message.id().unwrap_or_default().to_string();
    let flags = parse_filename_flags(message.path());
    let size = message.contents().len() as u64;
    let parsed = message.parsed();

    let subject = parsed
        .as_ref()
        .and_then(|m| m.subject())
        .unwrap_or_default()
        .to_string();

    let from = parsed
        .as_ref()
        .and_then(|m| m.from())
        .map(addresses_from)
        .unwrap_or_default();

    let to = parsed
        .as_ref()
        .and_then(|m| m.to())
        .map(addresses_from)
        .unwrap_or_default();

    let date = parsed
        .as_ref()
        .and_then(|m| m.date())
        .and_then(|d| DateTime::parse_from_rfc3339(&d.to_rfc3339()).ok());

    let has_attachment = parsed.as_ref().map(|m| m.attachment_count() > 0);

    let message_id = parsed
        .as_ref()
        .and_then(|m| m.message_id())
        .and_then(normalize_message_id);

    Envelope {
        id,
        message_id,
        flags,
        subject,
        from,
        to,
        date,
        size,
        has_attachment,
    }
}

fn parse_filename_flags(path: &MaildirPath) -> BTreeSet<Flag> {
    let Some(name) = path.file_name() else {
        return BTreeSet::new();
    };
    let Some((_, letters)) = name.rsplit_once(',') else {
        return BTreeSet::new();
    };
    letters.chars().filter_map(flag_from_char).collect()
}

fn addresses_from(addrs: &MailParserAddress<'_>) -> Vec<Address> {
    addrs
        .clone()
        .into_list()
        .into_iter()
        .filter_map(|a| {
            let email = a.address?.into_owned();
            if email.is_empty() {
                return None;
            }
            let name = a.name.map(|s| s.into_owned());
            Some(Address { name, email })
        })
        .collect()
}

/// Evaluates `filter` against `envelope`. `body` clauses re-parse
/// `raw` to scan the message's text and HTML body parts.
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

/// Sliding-window case-insensitive substring search on raw bytes.
/// Used for body matching where we want to avoid the UTF-8 decode
/// cost on a multi-megabyte payload.
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

#[cfg(test)]
mod tests {
    use alloc::{boxed::Box, string::String, vec};

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

    fn naive(y: i32, m: u32, d: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(y, m, d).unwrap()
    }

    fn raw_with_body(body: &str) -> Vec<u8> {
        alloc::format!(
            "Subject: x\r\nFrom: a@b\r\nDate: Thu, 15 May 2026 10:00:00 +0000\r\n\r\n{body}"
        )
        .into_bytes()
    }

    #[test]
    fn from_match_is_case_insensitive() {
        let env = envelope();
        let raw = Vec::new();
        assert!(matches_filter(
            &env,
            &raw,
            &SearchEmailsFilterQuery::From("alice".into())
        ));
        assert!(matches_filter(
            &env,
            &raw,
            &SearchEmailsFilterQuery::From("ALICE".into())
        ));
        assert!(!matches_filter(
            &env,
            &raw,
            &SearchEmailsFilterQuery::From("bob".into())
        ));
    }

    #[test]
    fn subject_match_is_case_insensitive_substring() {
        let env = envelope();
        let raw = Vec::new();
        assert!(matches_filter(
            &env,
            &raw,
            &SearchEmailsFilterQuery::Subject("release".into())
        ));
        assert!(matches_filter(
            &env,
            &raw,
            &SearchEmailsFilterQuery::Subject("NOTES".into())
        ));
    }

    #[test]
    fn body_filter_scans_message_bytes() {
        let env = envelope();
        let raw = raw_with_body("Hello, this is a quarterly review.");
        assert!(matches_filter(
            &env,
            &raw,
            &SearchEmailsFilterQuery::Body("quarterly".into())
        ));
        assert!(matches_filter(
            &env,
            &raw,
            &SearchEmailsFilterQuery::Body("QUARTERLY".into())
        ));
        assert!(!matches_filter(
            &env,
            &raw,
            &SearchEmailsFilterQuery::Body("absent".into())
        ));
    }

    #[test]
    fn date_clauses_target_sent_at_header() {
        let env = envelope();
        let raw = Vec::new();
        assert!(matches_filter(
            &env,
            &raw,
            &SearchEmailsFilterQuery::Date(naive(2026, 5, 15))
        ));
        assert!(!matches_filter(
            &env,
            &raw,
            &SearchEmailsFilterQuery::Date(naive(2026, 5, 14))
        ));
        assert!(matches_filter(
            &env,
            &raw,
            &SearchEmailsFilterQuery::AfterDate(naive(2026, 5, 14))
        ));
        assert!(!matches_filter(
            &env,
            &raw,
            &SearchEmailsFilterQuery::AfterDate(naive(2026, 5, 15))
        ));
    }

    #[test]
    fn boolean_combinators_evaluate_both_sides() {
        let env = envelope();
        let raw = Vec::new();
        let q = SearchEmailsFilterQuery::And(
            Box::new(SearchEmailsFilterQuery::From("alice".into())),
            Box::new(SearchEmailsFilterQuery::Subject("release".into())),
        );
        assert!(matches_filter(&env, &raw, &q));

        let q = SearchEmailsFilterQuery::Or(
            Box::new(SearchEmailsFilterQuery::From("bob".into())),
            Box::new(SearchEmailsFilterQuery::Subject("release".into())),
        );
        assert!(matches_filter(&env, &raw, &q));

        let q = SearchEmailsFilterQuery::Not(Box::new(SearchEmailsFilterQuery::From("bob".into())));
        assert!(matches_filter(&env, &raw, &q));
    }

    #[test]
    fn flag_match_uses_envelope_flags() {
        use crate::flag::IanaFlag;

        let mut env = envelope();
        let raw = Vec::new();
        env.flags.insert(Flag::from_iana(IanaFlag::Seen));
        assert!(matches_filter(
            &env,
            &raw,
            &SearchEmailsFilterQuery::Flag(Flag::from_iana(IanaFlag::Seen))
        ));
        assert!(!matches_filter(
            &env,
            &raw,
            &SearchEmailsFilterQuery::Flag(Flag::from_iana(IanaFlag::Flagged))
        ));
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
        assert_eq!(compare_with(&newer, &older, &sort), Ordering::Less);
        assert_eq!(compare_with(&older, &newer, &sort), Ordering::Greater);
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
        assert_eq!(compare_with(&a, &b, &sort), Ordering::Less);
    }
}
