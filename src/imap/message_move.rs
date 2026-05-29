//! IMAP message-move coroutine.
//!
//! Optional `SELECT <from>` (gated on `auto_select`) followed by
//! `UID MOVE <ids> <to>` (RFC 6851).

use alloc::string::String;
use core::mem;

use io_imap::{
    coroutine::{ImapCoroutine, ImapCoroutineState, ImapYield},
    rfc3501::select::{ImapMailboxSelect, ImapMailboxSelectError},
    rfc6851::r#move::{
        ImapMessageMove as InnerImapMessageMove, ImapMessageMoveError as InnerImapMessageMoveError,
    },
};
use log::trace;
use thiserror::Error;

use crate::{
    coroutine::{EmailBackend, EmailCoroutine, EmailCoroutineArg, EmailCoroutineState, ImapStep},
    imap::convert::{InvalidMailboxName, InvalidUidSet, parse_mailbox, parse_uids},
};

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
    #[error("coroutine was resumed with the wrong EmailCoroutineArg variant")]
    InvalidArg,
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
        let mv = InnerImapMessageMove::new(sequence_set, dst, true);
        let state = if auto_select {
            State::Selecting {
                select: ImapMailboxSelect::new(src),
                mv,
            }
        } else {
            State::Moving(mv)
        };
        Ok(Self { state })
    }
}

impl EmailCoroutine for ImapMessageMove {
    type Yield = ImapStep;
    type Return = Result<(), ImapMessageMoveError>;

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
            return EmailCoroutineState::Complete(Err(ImapMessageMoveError::InvalidArg));
        };

        loop {
            match mem::replace(&mut self.state, State::Done) {
                State::Selecting { mut select, mv } => {
                    match select.resume(fragmentizer, bytes.take()) {
                        ImapCoroutineState::Yielded(ImapYield::WantsRead) => {
                            self.state = State::Selecting { select, mv };
                            return EmailCoroutineState::Yielded(ImapStep::WantsRead);
                        }
                        ImapCoroutineState::Yielded(ImapYield::WantsWrite(out)) => {
                            self.state = State::Selecting { select, mv };
                            return EmailCoroutineState::Yielded(ImapStep::WantsWrite(out));
                        }
                        ImapCoroutineState::Complete(Ok(_)) => {
                            self.state = State::Moving(mv);
                        }
                        ImapCoroutineState::Complete(Err(err)) => {
                            return EmailCoroutineState::Complete(Err(err.into()));
                        }
                    }
                }
                State::Moving(mut mv) => match mv.resume(fragmentizer, bytes.take()) {
                    ImapCoroutineState::Yielded(ImapYield::WantsRead) => {
                        self.state = State::Moving(mv);
                        return EmailCoroutineState::Yielded(ImapStep::WantsRead);
                    }
                    ImapCoroutineState::Yielded(ImapYield::WantsWrite(out)) => {
                        self.state = State::Moving(mv);
                        return EmailCoroutineState::Yielded(ImapStep::WantsWrite(out));
                    }
                    ImapCoroutineState::Complete(Ok(_copyuid)) => {
                        return EmailCoroutineState::Complete(Ok(()));
                    }
                    ImapCoroutineState::Complete(Err(err)) => {
                        return EmailCoroutineState::Complete(Err(err.into()));
                    }
                },
                State::Done => {
                    return EmailCoroutineState::Complete(Err(
                        ImapMessageMoveError::ResumedAfterDone,
                    ));
                }
            }
        }
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
