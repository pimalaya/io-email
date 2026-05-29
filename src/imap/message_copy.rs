//! IMAP message-copy coroutine.
//!
//! Optional `SELECT <from>` (gated on `auto_select`) followed by
//! `UID COPY <ids> <to>` (RFC 3501 §6.4.7).

use alloc::string::String;
use core::mem;

use io_imap::{
    coroutine::{ImapCoroutine, ImapCoroutineState, ImapYield},
    rfc3501::{
        copy::{
            ImapMessageCopy as InnerImapMessageCopy,
            ImapMessageCopyError as InnerImapMessageCopyError,
        },
        select::{ImapMailboxSelect, ImapMailboxSelectError},
    },
};
use log::trace;
use thiserror::Error;

use crate::{
    coroutine::{EmailBackend, EmailCoroutine, EmailCoroutineArg, EmailCoroutineState, ImapStep},
    imap::convert::{InvalidMailboxName, InvalidUidSet, parse_mailbox, parse_uids},
};

/// Errors produced by [`ImapMessageCopy`].
#[derive(Debug, Error)]
pub enum ImapMessageCopyError {
    #[error(transparent)]
    Select(#[from] ImapMailboxSelectError),
    #[error(transparent)]
    Copy(#[from] InnerImapMessageCopyError),
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

impl From<InvalidMailboxName> for ImapMessageCopyError {
    fn from(err: InvalidMailboxName) -> Self {
        Self::InvalidMailbox(err.0)
    }
}

impl From<InvalidUidSet> for ImapMessageCopyError {
    fn from(err: InvalidUidSet) -> Self {
        match err {
            InvalidUidSet::Empty => Self::EmptyUidSet,
            InvalidUidSet::Invalid(s) => Self::InvalidUid(s),
        }
    }
}

/// I/O-free coroutine copying a UID set across mailboxes.
pub struct ImapMessageCopy {
    state: State,
}

impl ImapMessageCopy {
    pub fn new(
        from: &str,
        to: &str,
        ids: &[&str],
        auto_select: bool,
    ) -> Result<Self, ImapMessageCopyError> {
        trace!("prepare IMAP message copy (auto_select={auto_select})");
        let src = parse_mailbox(from)?;
        let dst = parse_mailbox(to)?;
        let sequence_set = parse_uids(ids)?;
        let copy = InnerImapMessageCopy::new(sequence_set, dst, true);
        let state = if auto_select {
            State::Selecting {
                select: ImapMailboxSelect::new(src),
                copy,
            }
        } else {
            State::Copying(copy)
        };
        Ok(Self { state })
    }
}

impl EmailCoroutine for ImapMessageCopy {
    type Yield = ImapStep;
    type Return = Result<(), ImapMessageCopyError>;

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
            return EmailCoroutineState::Complete(Err(ImapMessageCopyError::InvalidArg));
        };

        loop {
            match mem::replace(&mut self.state, State::Done) {
                State::Selecting { mut select, copy } => {
                    match select.resume(fragmentizer, bytes.take()) {
                        ImapCoroutineState::Yielded(ImapYield::WantsRead) => {
                            self.state = State::Selecting { select, copy };
                            return EmailCoroutineState::Yielded(ImapStep::WantsRead);
                        }
                        ImapCoroutineState::Yielded(ImapYield::WantsWrite(out)) => {
                            self.state = State::Selecting { select, copy };
                            return EmailCoroutineState::Yielded(ImapStep::WantsWrite(out));
                        }
                        ImapCoroutineState::Complete(Ok(_)) => {
                            self.state = State::Copying(copy);
                        }
                        ImapCoroutineState::Complete(Err(err)) => {
                            return EmailCoroutineState::Complete(Err(err.into()));
                        }
                    }
                }
                State::Copying(mut copy) => match copy.resume(fragmentizer, bytes.take()) {
                    ImapCoroutineState::Yielded(ImapYield::WantsRead) => {
                        self.state = State::Copying(copy);
                        return EmailCoroutineState::Yielded(ImapStep::WantsRead);
                    }
                    ImapCoroutineState::Yielded(ImapYield::WantsWrite(out)) => {
                        self.state = State::Copying(copy);
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
                        ImapMessageCopyError::ResumedAfterDone,
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
        copy: InnerImapMessageCopy,
    },
    Copying(InnerImapMessageCopy),
    Done,
}
