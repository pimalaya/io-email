//! IMAP message add (`APPEND <mailbox> [flags] <bytes>`), wrapping
//! [`io_imap::rfc3501::append::ImapMessageAppend`] to add a raw RFC
//! 5322 message to a mailbox without selecting it first.

use alloc::{string::ToString, vec::Vec};

use io_imap::{
    context::ImapContext,
    rfc3501::append::{ImapMessageAppend, ImapMessageAppendError, ImapMessageAppendResult},
    types::{
        core::Literal, extensions::binary::LiteralOrLiteral8, flag::Flag as ImapFlag,
        mailbox::Mailbox as ImapMailbox,
    },
};
use log::trace;
use thiserror::Error;

/// Errors produced while running IMAP APPEND.
#[derive(Debug, Error)]
pub enum MessageAddError {
    #[error(transparent)]
    Append(#[from] ImapMessageAppendError),
    #[error("Failed to encode the message as an IMAP literal: {0}")]
    Literal(String),
}

/// Result returned by [`MessageAdd::resume`].
#[derive(Debug)]
pub enum MessageAddResult {
    Ok {
        /// UIDVALIDITY and UID of the appended message, if the server
        /// returned an `[APPENDUID …]` response code (RFC 4315).
        appenduid: Option<(u32, u32)>,
    },
    WantsRead,
    WantsWrite(Vec<u8>),
    Err(MessageAddError),
}

/// I/O-free coroutine wrapping `APPEND <mailbox> [flags] <bytes>`.
pub struct MessageAdd {
    inner: Option<ImapMessageAppend>,
}

impl MessageAdd {
    /// Builds the coroutine. `flags` are written verbatim into the
    /// APPEND command; pass an empty vec to leave the message
    /// unflagged.
    pub fn new(
        context: ImapContext,
        mailbox: ImapMailbox<'static>,
        flags: Vec<ImapFlag<'static>>,
        raw: Vec<u8>,
    ) -> Result<Self, MessageAddError> {
        trace!("prepare IMAP message add");
        let literal =
            Literal::try_from(raw).map_err(|err| MessageAddError::Literal(err.to_string()))?;
        let message = LiteralOrLiteral8::Literal(literal);
        let inner = ImapMessageAppend::new(context, mailbox, flags, None, message);
        Ok(Self { inner: Some(inner) })
    }

    /// Advances the coroutine.
    pub fn resume(&mut self, arg: Option<&[u8]>) -> MessageAddResult {
        let Some(mut append) = self.inner.take() else {
            return MessageAddResult::Err(MessageAddError::Literal(
                "IMAP message add resumed after completion".into(),
            ));
        };

        match append.resume(arg) {
            ImapMessageAppendResult::WantsRead => {
                self.inner = Some(append);
                MessageAddResult::WantsRead
            }
            ImapMessageAppendResult::WantsWrite(bytes) => {
                self.inner = Some(append);
                MessageAddResult::WantsWrite(bytes)
            }
            ImapMessageAppendResult::Err { err, .. } => MessageAddResult::Err(err.into()),
            ImapMessageAppendResult::Ok { appenduid, .. } => MessageAddResult::Ok { appenduid },
        }
    }
}
