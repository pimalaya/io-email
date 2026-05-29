//! Maildir envelope-listing coroutine.
//!
//! Composes two io-maildir state machines:
//! 1. [`MaildirMessagesList`] walks `cur/` + `new/` and returns one
//!    [`MaildirEntry`] per file.
//! 2. A second pass batches the entry paths through
//!    [`FsStep::WantsFileRead`]; the driver reads each
//!    file and feeds the bytes back so the coroutine can parse RFC
//!    5322 headers (subject, from, to, date, message-id) via
//!    [`mail_parser::Message`].
//!
//! Sorting is by `Date:` header descending; pagination is 1-indexed
//! on the in-memory result.
//!
//! [`MaildirMessagesList`]: io_maildir::coroutines::message_list::MaildirMessagesList

use alloc::{
    collections::{BTreeMap, BTreeSet},
    string::ToString,
    vec::Vec,
};
use core::mem;
use std::path::PathBuf;

use chrono::DateTime;
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
use mail_parser::Address as MailParserAddress;
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
};

/// Errors produced by [`MaildirEnvelopeList`].
#[derive(Debug, Error)]
pub enum MaildirEnvelopeListError {
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
/// sorted by date descending then paginated.
pub struct MaildirEnvelopeList {
    state: State,
    page: Option<u32>,
    page_size: Option<u32>,
}

impl MaildirEnvelopeList {
    pub fn new(
        root: impl Into<PathBuf>,
        maildir_plus: bool,
        mailbox: &str,
        page: Option<u32>,
        page_size: Option<u32>,
    ) -> Result<Self, MaildirEnvelopeListError> {
        trace!("prepare Maildir envelope listing");
        let path = resolve_mailbox(&root.into(), maildir_plus, mailbox)?;
        let maildir = Maildir::from_path(path);
        Ok(Self {
            state: State::Listing(InnerList::new(maildir)),
            page,
            page_size,
        })
    }
}

impl EmailCoroutine for MaildirEnvelopeList {
    type Yield = FsStep;
    type Return = Result<Vec<Envelope>, MaildirEnvelopeListError>;

    const BACKEND: EmailBackend = EmailBackend::Maildir;

    fn resume(
        &mut self,
        arg: EmailCoroutineArg<'_>,
    ) -> EmailCoroutineState<Self::Yield, Self::Return> {
        #[allow(irrefutable_let_patterns)]
        let EmailCoroutineArg::Fs { batch } = arg else {
            return EmailCoroutineState::Complete(Err(MaildirEnvelopeListError::InvalidArg));
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
                            MaildirEnvelopeListError::UnexpectedBatch,
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
                        MaildirEnvelopeListError::UnexpectedBatch,
                    ));
                };
                let mut contents: BTreeMap<MaildirPath, Vec<u8>> =
                    contents.into_iter().map(|(k, v)| (k.into(), v)).collect();
                let mut envelopes: Vec<Envelope> = entries
                    .into_iter()
                    .filter_map(|entry| {
                        let bytes = contents.remove(entry.path())?;
                        Some(envelope_from_message(&MaildirMessage::from((
                            entry.path().clone(),
                            bytes,
                        ))))
                    })
                    .collect();
                envelopes.sort_by(|a, b| b.date.cmp(&a.date));
                EmailCoroutineState::Complete(Ok(paginate(envelopes, self.page, self.page_size)))
            }
            State::Done => {
                EmailCoroutineState::Complete(Err(MaildirEnvelopeListError::ResumedAfterDone))
            }
        }
    }
}

/// Two-phase state: list entries, then read their bytes for header
/// parsing.
enum State {
    Listing(InnerList),
    Reading(BTreeSet<MaildirEntry>),
    Done,
}

/// Builds an [`Envelope`] from a Maildir message: filename letters
/// for flags, RFC 5322 headers via mail-parser.
fn envelope_from_message(message: &MaildirMessage) -> Envelope {
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

/// Extracts the IANA flag set from a Maildir filename's info section.
/// Letters outside the standard six are silently dropped.
fn parse_filename_flags(path: &MaildirPath) -> BTreeSet<Flag> {
    let Some(name) = path.file_name() else {
        return BTreeSet::new();
    };
    let Some((_, letters)) = name.rsplit_once(',') else {
        return BTreeSet::new();
    };
    letters.chars().filter_map(flag_from_char).collect()
}

/// Converts mail-parser's address group into the shared LCD shape.
/// Empty `email` addresses are dropped.
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
