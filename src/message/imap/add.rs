//! IMAP message-add coroutine wrapping APPEND (RFC 3501 §6.3.11).
//!
//! Two Message-ID safety nets:
//!
//! 1. A missing Message-ID: header is replaced by a synthetic
//!    <uuid@io-email.invalid> so the message stays addressable.
//! 2. When UIDPLUS (RFC 4315) is missing, falls back to SELECT +
//!    UID SEARCH HEADER Message-ID and takes the highest UID.
//!
//! # Example
//!
//! ```rust,ignore
//! use io_email::message::imap::add::ImapMessageAdd;
//!
//! let uid = client.run(ImapMessageAdd::new("INBOX", &flags, raw)?)?;
//! ```

use alloc::{
    format,
    string::{String, ToString},
    vec::Vec,
};
use core::mem;

use io_imap::{
    codec::fragmentizer::Fragmentizer,
    coroutine::{ImapCoroutine, ImapCoroutineState, ImapYield},
    rfc3501::{
        append::{ImapMessageAppend, ImapMessageAppendError, ImapMessageAppendOptions},
        search::{ImapMessageSearch, ImapMessageSearchError, ImapMessageSearchOptions},
        select::{ImapMailboxSelect, ImapMailboxSelectError, ImapMailboxSelectOptions},
    },
    types::{
        core::{AString, Literal, Vec1},
        extensions::binary::LiteralOrLiteral8,
        search::SearchKey,
    },
};
use log::trace;
use mail_parser::MessageParser;
use thiserror::Error;
use uuid::Uuid;

use crate::{
    flag::types::Flag,
    imap::convert::{InvalidMailboxName, flag_from, parse_mailbox},
};

/// Errors produced by [`ImapMessageAdd`].
#[derive(Debug, Error)]
pub enum ImapMessageAddError {
    #[error(transparent)]
    Append(#[from] ImapMessageAppendError),
    #[error(transparent)]
    Select(#[from] ImapMailboxSelectError),
    #[error(transparent)]
    Search(#[from] ImapMessageSearchError),
    #[error("invalid IMAP mailbox `{0}`")]
    InvalidMailbox(String),
    #[error("invalid message content: {0}")]
    InvalidContent(String),
    #[error("fallback UID search returned no match (server omitted UIDPLUS)")]
    UidLookupFailed,
    #[error("coroutine was resumed after completion")]
    ResumedAfterDone,
}

impl From<InvalidMailboxName> for ImapMessageAddError {
    fn from(err: InvalidMailboxName) -> Self {
        Self::InvalidMailbox(err.0)
    }
}

/// I/O-free coroutine appending a raw RFC 5322 message and resolving
/// the appended UID.
pub struct ImapMessageAdd {
    state: State,
    mailbox: String,
    message_id: String,
}

impl ImapMessageAdd {
    /// `flags` apply to the appended message; pass an empty slice for
    /// none. `raw` must be a valid RFC 5322 message.
    pub fn new(
        mailbox: &str,
        flags: &[Flag],
        mut raw: Vec<u8>,
    ) -> Result<Self, ImapMessageAddError> {
        trace!("prepare IMAP message add");

        // NOTE: extract or synthesize Message-ID up front; needed as a
        // fallback to recover the UID when UIDPLUS is missing.
        let parsed_id = MessageParser::default()
            .parse_headers(&raw)
            .and_then(|m| m.message_id().map(ToString::to_string))
            .filter(|s| !s.is_empty());

        let message_id = match parsed_id {
            Some(id) => id,
            None => {
                let generated = format!("{}@io-email.invalid", Uuid::new_v4());
                trace!("appended message had no Message-ID; injected <{generated}>");
                let header = format!("Message-ID: <{generated}>\r\n");
                raw.splice(0..0, header.bytes());
                generated
            }
        };

        let mbox = parse_mailbox(mailbox)?;
        let imap_flags: Vec<_> = flags.iter().map(flag_from).collect();
        let literal = Literal::try_from(raw)
            .map_err(|err| ImapMessageAddError::InvalidContent(err.to_string()))?;
        let message = LiteralOrLiteral8::Literal(literal);
        let append = ImapMessageAppend::new(
            mbox,
            message,
            ImapMessageAppendOptions {
                flags: imap_flags,
                date: None,
            },
        );

        Ok(Self {
            state: State::Appending(append),
            mailbox: mailbox.to_string(),
            message_id,
        })
    }
}

enum State {
    Appending(ImapMessageAppend),
    SelectingForLookup { select: ImapMailboxSelect },
    Searching(ImapMessageSearch),
    Done,
}

impl ImapCoroutine for ImapMessageAdd {
    type Yield = ImapYield;
    type Return = Result<String, ImapMessageAddError>;

    fn resume(
        &mut self,
        fragmentizer: &mut Fragmentizer,
        mut bytes: Option<&[u8]>,
    ) -> ImapCoroutineState<Self::Yield, Self::Return> {
        loop {
            match mem::replace(&mut self.state, State::Done) {
                State::Appending(mut append) => match append.resume(fragmentizer, bytes.take()) {
                    ImapCoroutineState::Yielded(yielded) => {
                        self.state = State::Appending(append);
                        return ImapCoroutineState::Yielded(yielded);
                    }
                    ImapCoroutineState::Complete(Ok((_exists, appenduid))) => {
                        if let Some((_, uid)) = appenduid {
                            return ImapCoroutineState::Complete(Ok(uid.to_string()));
                        }
                        let mbox = match parse_mailbox(&self.mailbox) {
                            Ok(m) => m,
                            Err(err) => return ImapCoroutineState::Complete(Err(err.into())),
                        };
                        self.state = State::SelectingForLookup {
                            select: ImapMailboxSelect::new(
                                mbox,
                                ImapMailboxSelectOptions::default(),
                            ),
                        };
                    }
                    ImapCoroutineState::Complete(Err(err)) => {
                        return ImapCoroutineState::Complete(Err(err.into()));
                    }
                },
                State::SelectingForLookup { mut select } => {
                    match select.resume(fragmentizer, bytes.take()) {
                        ImapCoroutineState::Yielded(yielded) => {
                            self.state = State::SelectingForLookup { select };
                            return ImapCoroutineState::Yielded(yielded);
                        }
                        ImapCoroutineState::Complete(Ok(_)) => {
                            let Ok(field) = AString::try_from("Message-ID") else {
                                return ImapCoroutineState::Complete(Err(
                                    ImapMessageAddError::UidLookupFailed,
                                ));
                            };
                            let Ok(value) = AString::try_from(self.message_id.clone()) else {
                                return ImapCoroutineState::Complete(Err(
                                    ImapMessageAddError::UidLookupFailed,
                                ));
                            };
                            let criteria = Vec1::from(SearchKey::Header(field, value));
                            self.state = State::Searching(ImapMessageSearch::new(
                                criteria,
                                ImapMessageSearchOptions { uid: true },
                            ));
                        }
                        ImapCoroutineState::Complete(Err(err)) => {
                            return ImapCoroutineState::Complete(Err(err.into()));
                        }
                    }
                }
                State::Searching(mut search) => match search.resume(fragmentizer, bytes.take()) {
                    ImapCoroutineState::Yielded(yielded) => {
                        self.state = State::Searching(search);
                        return ImapCoroutineState::Yielded(yielded);
                    }
                    ImapCoroutineState::Complete(Ok(uids)) => match uids.into_iter().max() {
                        Some(uid) => return ImapCoroutineState::Complete(Ok(uid.to_string())),
                        None => {
                            return ImapCoroutineState::Complete(Err(
                                ImapMessageAddError::UidLookupFailed,
                            ));
                        }
                    },
                    ImapCoroutineState::Complete(Err(err)) => {
                        return ImapCoroutineState::Complete(Err(err.into()));
                    }
                },
                State::Done => {
                    return ImapCoroutineState::Complete(Err(
                        ImapMessageAddError::ResumedAfterDone,
                    ));
                }
            }
        }
    }
}
