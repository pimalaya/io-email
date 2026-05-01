//! IMAP message copy (`SELECT <src>` + `UID COPY <ids> <dst>`),
//! wrapping a private orchestrator that selects the source mailbox,
//! then issues a UID COPY command to copy the requested messages into
//! the destination mailbox.

use alloc::vec::Vec;
use core::mem;

use io_imap::{
    context::ImapContext,
    rfc3501::{
        copy::{ImapMessageCopy, ImapMessageCopyError, ImapMessageCopyResult},
        select::{
            ImapMailboxSelect, ImapMailboxSelectError as ImapSelectError, ImapMailboxSelectResult,
        },
    },
    types::{mailbox::Mailbox as ImapMailbox, sequence::SequenceSet},
};
use log::trace;
use thiserror::Error;

/// Errors produced while orchestrating SELECT + UID COPY for IMAP
/// message copy.
#[derive(Debug, Error)]
pub enum MessageCopyError {
    #[error(transparent)]
    Select(#[from] ImapSelectError),
    #[error(transparent)]
    Copy(#[from] ImapMessageCopyError),
    #[error("IMAP message copy was resumed after completion")]
    AlreadyDone,
}

/// Result returned by [`MessageCopy::resume`].
#[derive(Debug)]
pub enum MessageCopyResult {
    Ok,
    WantsRead,
    WantsWrite(Vec<u8>),
    Err(MessageCopyError),
}

/// I/O-free coroutine wrapping `SELECT <from>` followed by `UID COPY
/// <ids> <to>`. UIDs are used by default; pass `uid = false` to
/// interpret the sequence-set as message sequence numbers.
pub struct MessageCopy {
    inner: Inner,
    pending: Option<PendingCopy>,
}

struct PendingCopy {
    sequence_set: SequenceSet,
    target: ImapMailbox<'static>,
    uid: bool,
}

enum Inner {
    Selecting(ImapMailboxSelect),
    Copying(ImapMessageCopy),
    Done,
}

impl MessageCopy {
    /// Selects `from` read-write, then issues `UID COPY <ids> <to>`.
    pub fn new(
        context: ImapContext,
        from: ImapMailbox<'static>,
        to: ImapMailbox<'static>,
        sequence_set: SequenceSet,
        uid: bool,
    ) -> Self {
        trace!("prepare IMAP message copy");
        Self {
            inner: Inner::Selecting(ImapMailboxSelect::new(context, from)),
            pending: Some(PendingCopy {
                sequence_set,
                target: to,
                uid,
            }),
        }
    }

    /// Advances the orchestrator. Drives SELECT first, then UID COPY.
    pub fn resume(&mut self, arg: Option<&[u8]>) -> MessageCopyResult {
        let mut input = arg;

        loop {
            let inner = mem::replace(&mut self.inner, Inner::Done);

            match inner {
                Inner::Selecting(mut select) => match select.resume(input.take()) {
                    ImapMailboxSelectResult::WantsRead => {
                        self.inner = Inner::Selecting(select);
                        return MessageCopyResult::WantsRead;
                    }
                    ImapMailboxSelectResult::WantsWrite(bytes) => {
                        self.inner = Inner::Selecting(select);
                        return MessageCopyResult::WantsWrite(bytes);
                    }
                    ImapMailboxSelectResult::Err { err, .. } => {
                        return MessageCopyResult::Err(err.into());
                    }
                    ImapMailboxSelectResult::Ok { context, .. } => {
                        let pending = self.pending.take().expect("pending copy set on construct");
                        let copy = ImapMessageCopy::new(
                            context,
                            pending.sequence_set,
                            pending.target,
                            pending.uid,
                        );
                        self.inner = Inner::Copying(copy);
                    }
                },
                Inner::Copying(mut copy) => match copy.resume(input.take()) {
                    ImapMessageCopyResult::WantsRead => {
                        self.inner = Inner::Copying(copy);
                        return MessageCopyResult::WantsRead;
                    }
                    ImapMessageCopyResult::WantsWrite(bytes) => {
                        self.inner = Inner::Copying(copy);
                        return MessageCopyResult::WantsWrite(bytes);
                    }
                    ImapMessageCopyResult::Err { err, .. } => {
                        return MessageCopyResult::Err(err.into());
                    }
                    ImapMessageCopyResult::Ok { .. } => return MessageCopyResult::Ok,
                },
                Inner::Done => {
                    return MessageCopyResult::Err(MessageCopyError::AlreadyDone);
                }
            }
        }
    }
}
