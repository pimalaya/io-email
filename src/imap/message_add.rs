//! IMAP message-add coroutine.
//!
//! `APPEND <mailbox> [flags] <message>` (RFC 3501 §6.3.11) with two
//! Message-ID safety nets:
//!
//! 1. Before sending, the body is parsed for a `Message-ID:` header;
//!    when missing, a synthetic `<uuid@io-email.invalid>` is injected
//!    so the message stays addressable later.
//! 2. When the server does not advertise UIDPLUS (no `APPENDUID`
//!    response code per RFC 4315), the coroutine falls back to
//!    `SELECT <mailbox>` + `UID SEARCH HEADER Message-ID <id>` and
//!    takes the highest matching UID.

use alloc::{
    format,
    string::{String, ToString},
    vec::Vec,
};
use core::mem;

use io_imap::{
    coroutine::{ImapCoroutine, ImapCoroutineState, ImapYield},
    rfc3501::{
        append::{ImapMessageAppend, ImapMessageAppendError},
        search::{ImapMessageSearch, ImapMessageSearchError},
        select::{ImapMailboxSelect, ImapMailboxSelectError},
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
    coroutine::{EmailBackend, EmailCoroutine, EmailCoroutineArg, EmailCoroutineState, ImapStep},
    flag::Flag,
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
    #[error("coroutine was resumed with the wrong EmailCoroutineArg variant")]
    InvalidArg,
    #[error("coroutine was resumed after completion")]
    ResumedAfterDone,
}

impl From<InvalidMailboxName> for ImapMessageAddError {
    fn from(err: InvalidMailboxName) -> Self {
        Self::InvalidMailbox(err.0)
    }
}

/// I/O-free coroutine appending a raw RFC 5322 message and resolving
/// the appended UID (UIDPLUS fast path or SEARCH fallback).
pub struct ImapMessageAdd {
    state: State,
    mailbox: String,
    message_id: String,
}

impl ImapMessageAdd {
    /// `flags` are applied to the appended message; pass an empty
    /// slice for none. `raw` must be a syntactically valid RFC 5322
    /// message; framing-level escaping is the server's job.
    pub fn new(
        mailbox: &str,
        flags: &[Flag],
        mut raw: Vec<u8>,
    ) -> Result<Self, ImapMessageAddError> {
        trace!("prepare IMAP message add");

        // Extract or synthesize a Message-ID up front: needed as a
        // fallback to recover the UID on servers that don't advertise
        // UIDPLUS (no APPENDUID response code, RFC 4315).
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
        let append = ImapMessageAppend::new(mbox, imap_flags, None, message);

        Ok(Self {
            state: State::Appending(append),
            mailbox: mailbox.to_string(),
            message_id,
        })
    }
}

impl EmailCoroutine for ImapMessageAdd {
    type Yield = ImapStep;
    type Return = Result<String, ImapMessageAddError>;

    const BACKEND: EmailBackend = EmailBackend::Imap;

    fn resume(
        &mut self,
        arg: EmailCoroutineArg<'_>,
    ) -> EmailCoroutineState<Self::Yield, Self::Return> {
        #[allow(irrefutable_let_patterns)]
        let EmailCoroutineArg::Imap {
            fragmentizer,
            mut bytes,
        } = arg
        else {
            return EmailCoroutineState::Complete(Err(ImapMessageAddError::InvalidArg));
        };

        loop {
            match mem::replace(&mut self.state, State::Done) {
                State::Appending(mut append) => match append.resume(fragmentizer, bytes.take()) {
                    ImapCoroutineState::Yielded(ImapYield::WantsRead) => {
                        self.state = State::Appending(append);
                        return EmailCoroutineState::Yielded(ImapStep::WantsRead);
                    }
                    ImapCoroutineState::Yielded(ImapYield::WantsWrite(out)) => {
                        self.state = State::Appending(append);
                        return EmailCoroutineState::Yielded(ImapStep::WantsWrite(out));
                    }
                    ImapCoroutineState::Complete(Ok((_exists, appenduid))) => {
                        if let Some((_, uid)) = appenduid {
                            return EmailCoroutineState::Complete(Ok(uid.to_string()));
                        }
                        // UIDPLUS missing: fall back to SELECT +
                        // UID SEARCH HEADER Message-ID <id>.
                        let mbox = match parse_mailbox(&self.mailbox) {
                            Ok(m) => m,
                            Err(err) => return EmailCoroutineState::Complete(Err(err.into())),
                        };
                        self.state = State::SelectingForLookup {
                            select: ImapMailboxSelect::new(mbox),
                        };
                    }
                    ImapCoroutineState::Complete(Err(err)) => {
                        return EmailCoroutineState::Complete(Err(err.into()));
                    }
                },
                State::SelectingForLookup { mut select } => {
                    match select.resume(fragmentizer, bytes.take()) {
                        ImapCoroutineState::Yielded(ImapYield::WantsRead) => {
                            self.state = State::SelectingForLookup { select };
                            return EmailCoroutineState::Yielded(ImapStep::WantsRead);
                        }
                        ImapCoroutineState::Yielded(ImapYield::WantsWrite(out)) => {
                            self.state = State::SelectingForLookup { select };
                            return EmailCoroutineState::Yielded(ImapStep::WantsWrite(out));
                        }
                        ImapCoroutineState::Complete(Ok(_)) => {
                            let Ok(field) = AString::try_from("Message-ID") else {
                                return EmailCoroutineState::Complete(Err(
                                    ImapMessageAddError::UidLookupFailed,
                                ));
                            };
                            let Ok(value) = AString::try_from(self.message_id.clone()) else {
                                return EmailCoroutineState::Complete(Err(
                                    ImapMessageAddError::UidLookupFailed,
                                ));
                            };
                            let criteria = Vec1::from(SearchKey::Header(field, value));
                            self.state = State::Searching(ImapMessageSearch::new(criteria, true));
                        }
                        ImapCoroutineState::Complete(Err(err)) => {
                            return EmailCoroutineState::Complete(Err(err.into()));
                        }
                    }
                }
                State::Searching(mut search) => match search.resume(fragmentizer, bytes.take()) {
                    ImapCoroutineState::Yielded(ImapYield::WantsRead) => {
                        self.state = State::Searching(search);
                        return EmailCoroutineState::Yielded(ImapStep::WantsRead);
                    }
                    ImapCoroutineState::Yielded(ImapYield::WantsWrite(out)) => {
                        self.state = State::Searching(search);
                        return EmailCoroutineState::Yielded(ImapStep::WantsWrite(out));
                    }
                    ImapCoroutineState::Complete(Ok(uids)) => match uids.into_iter().max() {
                        Some(uid) => return EmailCoroutineState::Complete(Ok(uid.to_string())),
                        None => {
                            return EmailCoroutineState::Complete(Err(
                                ImapMessageAddError::UidLookupFailed,
                            ));
                        }
                    },
                    ImapCoroutineState::Complete(Err(err)) => {
                        return EmailCoroutineState::Complete(Err(err.into()));
                    }
                },
                State::Done => {
                    return EmailCoroutineState::Complete(Err(
                        ImapMessageAddError::ResumedAfterDone,
                    ));
                }
            }
        }
    }
}

enum State {
    Appending(ImapMessageAppend),
    SelectingForLookup { select: ImapMailboxSelect },
    Searching(ImapMessageSearch),
    Done,
}
