//! m2dir envelope-list coroutine: M2dirEntryList over the entry
//! directory then a batched WantsFileRead for message bytes plus the
//! .meta sidecar; headers parse via [`mail_parser`].
//!
//! Sorted by Date: header descending, paginated 1-indexed.
//!
//! # Example
//!
//! ```rust,ignore
//! use io_email::m2dir::envelope_list::M2dirEnvelopeList;
//!
//! let envs = client.run(M2dirEnvelopeList::new(&client.root, "INBOX", Some(1), Some(50), false)?)?;
//! ```

use alloc::{collections::BTreeSet, vec::Vec};
use core::mem;
use std::path::PathBuf;

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
    envelope::Envelope,
    m2dir::convert::{InvalidMailboxName, envelope_from, paginate, resolve_mailbox},
};

/// Errors produced by [`M2dirEnvelopeList`].
#[derive(Debug, Error)]
pub enum M2dirEnvelopeListError {
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

/// I/O-free coroutine listing every message in an m2dir, sorted by
/// Date: descending then paginated.
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
        let inner = InnerList::new(m2dir.clone(), InnerOpts::default());
        Ok(Self {
            state: State::Listing(inner),
            m2dir,
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

impl M2dirCoroutine for M2dirEnvelopeList {
    type Yield = M2dirYield;
    type Return = Result<Vec<Envelope>, M2dirEnvelopeListError>;

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
                        M2dirEnvelopeListError::UnexpectedArg,
                    ));
                };
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
                        return M2dirCoroutineState::Complete(Err(M2dirEnvelopeListError::Parse(
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
                M2dirCoroutineState::Complete(Ok(paginate(envelopes, self.page, self.page_size)))
            }
            State::Done => {
                M2dirCoroutineState::Complete(Err(M2dirEnvelopeListError::ResumedAfterDone))
            }
        }
    }
}
