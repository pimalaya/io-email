//! IMAP message-delete coroutine: optional SELECT, UID STORE
//! +FLAGS (\\Deleted), then EXPUNGE (RFC 3501 §6.4.3).
//!
//! The shared API treats delete as permanent, which on IMAP requires
//! both the flag-set and expunge.
//!
//! # Example
//!
//! ```rust,ignore
//! use io_email::imap::message_delete::ImapMessageDelete;
//!
//! client.run(ImapMessageDelete::new("INBOX", "12", true)?)?;
//! ```

use alloc::{string::String, vec};
use core::mem;

use io_imap::{
    codec::fragmentizer::Fragmentizer,
    coroutine::{ImapCoroutine, ImapCoroutineState, ImapYield},
    rfc3501::{
        expunge::{ImapMailboxExpunge, ImapMailboxExpungeError},
        select::{ImapMailboxSelect, ImapMailboxSelectError, ImapMailboxSelectOptions},
        store::{ImapMessageStore, ImapMessageStoreError, ImapMessageStoreOptions},
    },
    types::flag::StoreType,
};
use log::trace;
use thiserror::Error;

use crate::{
    flag::{Flag, IanaFlag},
    imap::convert::{InvalidMailboxName, InvalidUidSet, flag_from, parse_mailbox, parse_uids},
};

/// Errors produced by [`ImapMessageDelete`].
#[derive(Debug, Error)]
pub enum ImapMessageDeleteError {
    #[error(transparent)]
    Select(#[from] ImapMailboxSelectError),
    #[error(transparent)]
    Store(#[from] ImapMessageStoreError),
    #[error(transparent)]
    Expunge(#[from] ImapMailboxExpungeError),
    #[error("invalid IMAP mailbox `{0}`")]
    InvalidMailbox(String),
    #[error("invalid message UID `{0}`")]
    InvalidUid(String),
    #[error("empty UID set")]
    EmptyUidSet,
    #[error("coroutine was resumed after completion")]
    ResumedAfterDone,
}

impl From<InvalidMailboxName> for ImapMessageDeleteError {
    fn from(err: InvalidMailboxName) -> Self {
        Self::InvalidMailbox(err.0)
    }
}

impl From<InvalidUidSet> for ImapMessageDeleteError {
    fn from(err: InvalidUidSet) -> Self {
        match err {
            InvalidUidSet::Empty => Self::EmptyUidSet,
            InvalidUidSet::Invalid(s) => Self::InvalidUid(s),
        }
    }
}

/// I/O-free coroutine deleting and expunging a single UID.
pub struct ImapMessageDelete {
    state: State,
}

impl ImapMessageDelete {
    pub fn new(mailbox: &str, id: &str, auto_select: bool) -> Result<Self, ImapMessageDeleteError> {
        trace!("prepare IMAP message delete (auto_select={auto_select})");
        let mbox = parse_mailbox(mailbox)?;
        let sequence_set = parse_uids(&[id])?;
        let imap_flags = vec![flag_from(&Flag::from_iana(IanaFlag::Deleted))];
        let store = ImapMessageStore::new(
            sequence_set,
            StoreType::Add,
            imap_flags,
            ImapMessageStoreOptions { uid: true },
        );
        let state = if auto_select {
            State::Selecting {
                select: ImapMailboxSelect::new(mbox, ImapMailboxSelectOptions::default()),
                store,
            }
        } else {
            State::Storing(store)
        };
        Ok(Self { state })
    }
}

#[allow(clippy::large_enum_variant)] // see flag_store.rs for rationale
enum State {
    Selecting {
        select: ImapMailboxSelect,
        store: ImapMessageStore,
    },
    Storing(ImapMessageStore),
    Expunging(ImapMailboxExpunge),
    Done,
}

impl ImapCoroutine for ImapMessageDelete {
    type Yield = ImapYield;
    type Return = Result<(), ImapMessageDeleteError>;

    fn resume(
        &mut self,
        fragmentizer: &mut Fragmentizer,
        mut bytes: Option<&[u8]>,
    ) -> ImapCoroutineState<Self::Yield, Self::Return> {
        loop {
            match mem::replace(&mut self.state, State::Done) {
                State::Selecting { mut select, store } => {
                    match select.resume(fragmentizer, bytes.take()) {
                        ImapCoroutineState::Yielded(yielded) => {
                            self.state = State::Selecting { select, store };
                            return ImapCoroutineState::Yielded(yielded);
                        }
                        ImapCoroutineState::Complete(Ok(_)) => {
                            self.state = State::Storing(store);
                        }
                        ImapCoroutineState::Complete(Err(err)) => {
                            return ImapCoroutineState::Complete(Err(err.into()));
                        }
                    }
                }
                State::Storing(mut store) => match store.resume(fragmentizer, bytes.take()) {
                    ImapCoroutineState::Yielded(yielded) => {
                        self.state = State::Storing(store);
                        return ImapCoroutineState::Yielded(yielded);
                    }
                    ImapCoroutineState::Complete(Ok(_)) => {
                        self.state = State::Expunging(ImapMailboxExpunge::new());
                    }
                    ImapCoroutineState::Complete(Err(err)) => {
                        return ImapCoroutineState::Complete(Err(err.into()));
                    }
                },
                State::Expunging(mut expunge) => match expunge.resume(fragmentizer, bytes.take()) {
                    ImapCoroutineState::Yielded(yielded) => {
                        self.state = State::Expunging(expunge);
                        return ImapCoroutineState::Yielded(yielded);
                    }
                    ImapCoroutineState::Complete(Ok(_expunged)) => {
                        return ImapCoroutineState::Complete(Ok(()));
                    }
                    ImapCoroutineState::Complete(Err(err)) => {
                        return ImapCoroutineState::Complete(Err(err.into()));
                    }
                },
                State::Done => {
                    return ImapCoroutineState::Complete(Err(
                        ImapMessageDeleteError::ResumedAfterDone,
                    ));
                }
            }
        }
    }
}
