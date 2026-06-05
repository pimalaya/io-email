//! IMAP message-copy coroutine: optional SELECT then UID COPY (RFC
//! 3501 §6.4.7).
//!
//! # Example
//!
//! ```rust,ignore
//! use io_email::imap::message_copy::ImapMessageCopy;
//!
//! client.run(ImapMessageCopy::new("INBOX", "Archive", &["12"], true)?)?;
//! ```

use alloc::string::String;
use core::mem;

use io_imap::{
    codec::fragmentizer::Fragmentizer,
    coroutine::{ImapCoroutine, ImapCoroutineState, ImapYield},
    rfc3501::{
        copy::{
            ImapMessageCopy as InnerImapMessageCopy,
            ImapMessageCopyError as InnerImapMessageCopyError, ImapMessageCopyOptions,
        },
        select::{ImapMailboxSelect, ImapMailboxSelectError, ImapMailboxSelectOptions},
    },
};
use log::trace;
use thiserror::Error;

use crate::imap::convert::{InvalidMailboxName, InvalidUidSet, parse_mailbox, parse_uids};

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
        let copy =
            InnerImapMessageCopy::new(sequence_set, dst, ImapMessageCopyOptions { uid: true });
        let state = if auto_select {
            State::Selecting {
                select: ImapMailboxSelect::new(src, ImapMailboxSelectOptions::default()),
                copy,
            }
        } else {
            State::Copying(copy)
        };
        Ok(Self { state })
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

impl ImapCoroutine for ImapMessageCopy {
    type Yield = ImapYield;
    type Return = Result<(), ImapMessageCopyError>;

    fn resume(
        &mut self,
        fragmentizer: &mut Fragmentizer,
        mut bytes: Option<&[u8]>,
    ) -> ImapCoroutineState<Self::Yield, Self::Return> {
        loop {
            match mem::replace(&mut self.state, State::Done) {
                State::Selecting { mut select, copy } => {
                    match select.resume(fragmentizer, bytes.take()) {
                        ImapCoroutineState::Yielded(yielded) => {
                            self.state = State::Selecting { select, copy };
                            return ImapCoroutineState::Yielded(yielded);
                        }
                        ImapCoroutineState::Complete(Ok(_)) => {
                            self.state = State::Copying(copy);
                        }
                        ImapCoroutineState::Complete(Err(err)) => {
                            return ImapCoroutineState::Complete(Err(err.into()));
                        }
                    }
                }
                State::Copying(mut copy) => match copy.resume(fragmentizer, bytes.take()) {
                    ImapCoroutineState::Yielded(yielded) => {
                        self.state = State::Copying(copy);
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
                        ImapMessageCopyError::ResumedAfterDone,
                    ));
                }
            }
        }
    }
}
