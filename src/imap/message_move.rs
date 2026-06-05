//! IMAP message-move coroutine: optional SELECT then UID MOVE (RFC
//! 6851).
//!
//! # Example
//!
//! ```rust,ignore
//! use io_email::imap::message_move::ImapMessageMove;
//!
//! client.run(ImapMessageMove::new("INBOX", "Archive", &["12"], true)?)?;
//! ```

use alloc::string::String;
use core::mem;

use io_imap::{
    codec::fragmentizer::Fragmentizer,
    coroutine::{ImapCoroutine, ImapCoroutineState, ImapYield},
    rfc3501::select::{ImapMailboxSelect, ImapMailboxSelectError, ImapMailboxSelectOptions},
    rfc6851::r#move::{
        ImapMessageMove as InnerImapMessageMove, ImapMessageMoveError as InnerImapMessageMoveError,
        ImapMessageMoveOptions,
    },
};
use log::trace;
use thiserror::Error;

use crate::imap::convert::{InvalidMailboxName, InvalidUidSet, parse_mailbox, parse_uids};

/// Errors produced by [`ImapMessageMove`].
#[derive(Debug, Error)]
pub enum ImapMessageMoveError {
    #[error(transparent)]
    Select(#[from] ImapMailboxSelectError),
    #[error(transparent)]
    Move(#[from] InnerImapMessageMoveError),
    #[error("invalid IMAP mailbox `{0}`")]
    InvalidMailbox(String),
    #[error("invalid message UID `{0}`")]
    InvalidUid(String),
    #[error("empty UID set")]
    EmptyUidSet,
    #[error("coroutine was resumed after completion")]
    ResumedAfterDone,
}

impl From<InvalidMailboxName> for ImapMessageMoveError {
    fn from(err: InvalidMailboxName) -> Self {
        Self::InvalidMailbox(err.0)
    }
}

impl From<InvalidUidSet> for ImapMessageMoveError {
    fn from(err: InvalidUidSet) -> Self {
        match err {
            InvalidUidSet::Empty => Self::EmptyUidSet,
            InvalidUidSet::Invalid(s) => Self::InvalidUid(s),
        }
    }
}

/// I/O-free coroutine moving a UID set across mailboxes.
pub struct ImapMessageMove {
    state: State,
}

impl ImapMessageMove {
    pub fn new(
        from: &str,
        to: &str,
        ids: &[&str],
        auto_select: bool,
    ) -> Result<Self, ImapMessageMoveError> {
        trace!("prepare IMAP message move (auto_select={auto_select})");
        let src = parse_mailbox(from)?;
        let dst = parse_mailbox(to)?;
        let sequence_set = parse_uids(ids)?;
        let mv = InnerImapMessageMove::new(sequence_set, dst, ImapMessageMoveOptions { uid: true });
        let state = if auto_select {
            State::Selecting {
                select: ImapMailboxSelect::new(src, ImapMailboxSelectOptions::default()),
                mv,
            }
        } else {
            State::Moving(mv)
        };
        Ok(Self { state })
    }
}

#[allow(clippy::large_enum_variant)] // see flag_store.rs for rationale
enum State {
    Selecting {
        select: ImapMailboxSelect,
        mv: InnerImapMessageMove,
    },
    Moving(InnerImapMessageMove),
    Done,
}

impl ImapCoroutine for ImapMessageMove {
    type Yield = ImapYield;
    type Return = Result<(), ImapMessageMoveError>;

    fn resume(
        &mut self,
        fragmentizer: &mut Fragmentizer,
        mut bytes: Option<&[u8]>,
    ) -> ImapCoroutineState<Self::Yield, Self::Return> {
        loop {
            match mem::replace(&mut self.state, State::Done) {
                State::Selecting { mut select, mv } => {
                    match select.resume(fragmentizer, bytes.take()) {
                        ImapCoroutineState::Yielded(yielded) => {
                            self.state = State::Selecting { select, mv };
                            return ImapCoroutineState::Yielded(yielded);
                        }
                        ImapCoroutineState::Complete(Ok(_)) => {
                            self.state = State::Moving(mv);
                        }
                        ImapCoroutineState::Complete(Err(err)) => {
                            return ImapCoroutineState::Complete(Err(err.into()));
                        }
                    }
                }
                State::Moving(mut mv) => match mv.resume(fragmentizer, bytes.take()) {
                    ImapCoroutineState::Yielded(yielded) => {
                        self.state = State::Moving(mv);
                        return ImapCoroutineState::Yielded(yielded);
                    }
                    ImapCoroutineState::Complete(Ok(_copyuid)) => {
                        return ImapCoroutineState::Complete(Ok(()));
                    }
                    ImapCoroutineState::Complete(Err(err)) => {
                        return ImapCoroutineState::Complete(Err(err.into()));
                    }
                },
                State::Done => {
                    return ImapCoroutineState::Complete(Err(
                        ImapMessageMoveError::ResumedAfterDone,
                    ));
                }
            }
        }
    }
}
