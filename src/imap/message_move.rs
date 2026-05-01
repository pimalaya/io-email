//! IMAP message move (`SELECT <src>` + `UID MOVE <ids> <dst>`, RFC
//! 6851), wrapping a private orchestrator that selects the source
//! mailbox, then issues a UID MOVE command to relocate the requested
//! messages into the destination mailbox.

use alloc::vec::Vec;
use core::mem;

use io_imap::{
    context::ImapContext,
    rfc3501::select::{
        ImapMailboxSelect, ImapMailboxSelectError as ImapSelectError, ImapMailboxSelectResult,
    },
    rfc6851::r#move::{ImapMessageMove, ImapMessageMoveError, ImapMessageMoveResult},
    types::{mailbox::Mailbox as ImapMailbox, sequence::SequenceSet},
};
use log::trace;
use thiserror::Error;

/// Errors produced while orchestrating SELECT + UID MOVE for IMAP
/// message move.
#[derive(Debug, Error)]
pub enum MessageMoveError {
    #[error(transparent)]
    Select(#[from] ImapSelectError),
    #[error(transparent)]
    Move(#[from] ImapMessageMoveError),
    #[error("IMAP message move was resumed after completion")]
    AlreadyDone,
}

/// Result returned by [`MessageMove::resume`].
#[derive(Debug)]
pub enum MessageMoveResult {
    Ok,
    WantsRead,
    WantsWrite(Vec<u8>),
    Err(MessageMoveError),
}

/// I/O-free coroutine wrapping `SELECT <from>` followed by `UID MOVE
/// <ids> <to>`. UIDs are used by default; pass `uid = false` to
/// interpret the sequence-set as message sequence numbers.
pub struct MessageMove {
    inner: Inner,
    pending: Option<PendingMove>,
}

struct PendingMove {
    sequence_set: SequenceSet,
    target: ImapMailbox<'static>,
    uid: bool,
}

enum Inner {
    Selecting(ImapMailboxSelect),
    Moving(ImapMessageMove),
    Done,
}

impl MessageMove {
    /// Selects `from` read-write, then issues `UID MOVE <ids> <to>`.
    pub fn new(
        context: ImapContext,
        from: ImapMailbox<'static>,
        to: ImapMailbox<'static>,
        sequence_set: SequenceSet,
        uid: bool,
    ) -> Self {
        trace!("prepare IMAP message move");
        Self {
            inner: Inner::Selecting(ImapMailboxSelect::new(context, from)),
            pending: Some(PendingMove {
                sequence_set,
                target: to,
                uid,
            }),
        }
    }

    /// Advances the orchestrator. Drives SELECT first, then UID MOVE.
    pub fn resume(&mut self, arg: Option<&[u8]>) -> MessageMoveResult {
        let mut input = arg;

        loop {
            let inner = mem::replace(&mut self.inner, Inner::Done);

            match inner {
                Inner::Selecting(mut select) => match select.resume(input.take()) {
                    ImapMailboxSelectResult::WantsRead => {
                        self.inner = Inner::Selecting(select);
                        return MessageMoveResult::WantsRead;
                    }
                    ImapMailboxSelectResult::WantsWrite(bytes) => {
                        self.inner = Inner::Selecting(select);
                        return MessageMoveResult::WantsWrite(bytes);
                    }
                    ImapMailboxSelectResult::Err { err, .. } => {
                        return MessageMoveResult::Err(err.into());
                    }
                    ImapMailboxSelectResult::Ok { context, .. } => {
                        let pending = self.pending.take().expect("pending move set on construct");
                        let mv = ImapMessageMove::new(
                            context,
                            pending.sequence_set,
                            pending.target,
                            pending.uid,
                        );
                        self.inner = Inner::Moving(mv);
                    }
                },
                Inner::Moving(mut mv) => match mv.resume(input.take()) {
                    ImapMessageMoveResult::WantsRead => {
                        self.inner = Inner::Moving(mv);
                        return MessageMoveResult::WantsRead;
                    }
                    ImapMessageMoveResult::WantsWrite(bytes) => {
                        self.inner = Inner::Moving(mv);
                        return MessageMoveResult::WantsWrite(bytes);
                    }
                    ImapMessageMoveResult::Err { err, .. } => {
                        return MessageMoveResult::Err(err.into());
                    }
                    ImapMessageMoveResult::Ok { .. } => return MessageMoveResult::Ok,
                },
                Inner::Done => {
                    return MessageMoveResult::Err(MessageMoveError::AlreadyDone);
                }
            }
        }
    }
}
