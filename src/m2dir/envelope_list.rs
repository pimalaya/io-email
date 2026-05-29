//! m2dir envelope-listing coroutine.
//!
//! Composes:
//! 1. [`M2dirMessageList`] walks the m2dir entry directory and emits
//!    one [`M2dirEntry`] per file (yields DirRead / FileExists).
//! 2. For each entry, a `WantsFileRead` batch fetches the message
//!    bytes plus the `.meta/<id>.flags` sidecar in a single round.
//! 3. The coroutine parses RFC 5322 headers via [`mail_parser`] and
//!    folds the flags + headers + size into a shared [`Envelope`].
//!
//! Sorting is by `Date:` header descending; pagination is 1-indexed.
//!
//! [`M2dirMessageList`]: io_m2dir::coroutines::message_list::M2dirMessageList

use alloc::{
    collections::{BTreeMap, BTreeSet},
    vec::Vec,
};
use core::mem;
use std::path::PathBuf;

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
    coroutine::{
        EmailBackend, EmailCoroutine, EmailCoroutineArg, EmailCoroutineState, FsBatch, FsStep,
    },
    envelope::Envelope,
    m2dir::convert::{
        InvalidMailboxName, dirread_in, envelope_from, paginate, paths_out, probes_in,
        resolve_mailbox,
    },
};

/// Errors produced by [`M2dirEnvelopeList`].
#[derive(Debug, Error)]
pub enum M2dirEnvelopeListError {
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
/// sorted by date descending then paginated.
pub struct M2dirEnvelopeList {
    state: State,
    m2dir: M2dir,
    page: Option<u32>,
    page_size: Option<u32>,
    with_attachment: bool,
}

impl M2dirEnvelopeList {
    pub fn new(
        root: impl Into<PathBuf>,
        mailbox: &str,
        page: Option<u32>,
        page_size: Option<u32>,
        with_attachment: bool,
    ) -> Result<Self, M2dirEnvelopeListError> {
        trace!("prepare m2dir envelope listing");
        let m2dir = resolve_mailbox(root, mailbox)?;
        let inner = InnerList::new(m2dir.clone());
        Ok(Self {
            state: State::Listing(inner),
            m2dir,
            page,
            page_size,
            with_attachment,
        })
    }
}

impl EmailCoroutine for M2dirEnvelopeList {
    type Yield = FsStep;
    type Return = Result<Vec<Envelope>, M2dirEnvelopeListError>;

    const BACKEND: EmailBackend = EmailBackend::M2dir;

    fn resume(
        &mut self,
        arg: EmailCoroutineArg<'_>,
    ) -> EmailCoroutineState<Self::Yield, Self::Return> {
        #[allow(irrefutable_let_patterns)]
        let EmailCoroutineArg::Fs { batch } = arg else {
            return EmailCoroutineState::Complete(Err(M2dirEnvelopeListError::InvalidArg));
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
                            M2dirEnvelopeListError::UnexpectedBatch,
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
                        // Ask the driver for both the message body and
                        // the `.meta/<id>.flags` sidecar in one batch.
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
                        M2dirEnvelopeListError::UnexpectedBatch,
                    ));
                };
                let mut contents: BTreeMap<M2dirPath, Vec<u8>> =
                    contents.into_iter().map(|(k, v)| (k.into(), v)).collect();
                let parser = MessageParser::default();
                let mut envelopes: Vec<Envelope> = Vec::with_capacity(entries.len());
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
                        return EmailCoroutineState::Complete(Err(M2dirEnvelopeListError::Parse(
                            entry.path().clone(),
                        )));
                    };
                    let mut envelope = envelope_from(&entry, &flags, &parsed);
                    if self.with_attachment {
                        envelope.has_attachment = Some(parsed.attachment_count() > 0);
                    }
                    envelopes.push(envelope);
                }
                envelopes.sort_by(|a, b| b.date.cmp(&a.date));
                EmailCoroutineState::Complete(Ok(paginate(envelopes, self.page, self.page_size)))
            }
            State::Done => {
                EmailCoroutineState::Complete(Err(M2dirEnvelopeListError::ResumedAfterDone))
            }
        }
    }
}

enum State {
    Listing(InnerList),
    Reading(Vec<M2dirEntry>),
    Done,
}

/// Parses the contents of a `.meta/<id>.flags` file (one flag per
/// non-empty trimmed line) into an [`M2dirFlags`] payload.
fn parse_meta_flags(bytes: &[u8]) -> M2dirFlags {
    let Ok(text) = core::str::from_utf8(bytes) else {
        return M2dirFlags::default();
    };
    text.lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect()
}
