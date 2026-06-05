//! IMAP flag-store coroutine: optional SELECT then UID STORE (RFC
//! 3501 §6.4.6).
//!
//! `auto_select = false` skips SELECT for sync engines that
//! pre-select per batch. Shared by add/set/delete via [`FlagOp`].
//!
//! # Example
//!
//! ```rust,ignore
//! use io_email::{flag::FlagOp, imap::flag_store::ImapFlagStore};
//!
//! client.run(ImapFlagStore::new("INBOX", &["12"], &flags, FlagOp::Add, true)?)?;
//! ```

use alloc::{string::String, vec::Vec};
use core::mem;

use io_imap::{
    codec::fragmentizer::Fragmentizer,
    coroutine::{ImapCoroutine, ImapCoroutineState, ImapYield},
    rfc3501::{
        select::{ImapMailboxSelect, ImapMailboxSelectError, ImapMailboxSelectOptions},
        store::{ImapMessageStore, ImapMessageStoreError, ImapMessageStoreOptions},
    },
    types::flag::StoreType,
};
use log::trace;
use thiserror::Error;

use crate::{
    flag::{Flag, FlagOp},
    imap::convert::{InvalidMailboxName, InvalidUidSet, flag_from, parse_mailbox, parse_uids},
};

/// Errors produced by [`ImapFlagStore`].
#[derive(Debug, Error)]
pub enum ImapFlagStoreError {
    #[error(transparent)]
    Select(#[from] ImapMailboxSelectError),
    #[error(transparent)]
    Store(#[from] ImapMessageStoreError),
    #[error("invalid IMAP mailbox `{0}`")]
    InvalidMailbox(String),
    #[error("empty UID set")]
    EmptyUidSet,
    #[error("invalid message UID `{0}`")]
    InvalidUid(String),
    #[error("coroutine was resumed after completion")]
    ResumedAfterDone,
}

impl From<InvalidMailboxName> for ImapFlagStoreError {
    fn from(err: InvalidMailboxName) -> Self {
        Self::InvalidMailbox(err.0)
    }
}

impl From<InvalidUidSet> for ImapFlagStoreError {
    fn from(err: InvalidUidSet) -> Self {
        match err {
            InvalidUidSet::Empty => Self::EmptyUidSet,
            InvalidUidSet::Invalid(s) => Self::InvalidUid(s),
        }
    }
}

/// I/O-free coroutine adding/setting/removing flags on a UID set.
pub struct ImapFlagStore {
    state: State,
}

impl ImapFlagStore {
    /// `op` picks the STORE variant. When `auto_select` is set the
    /// mailbox is SELECTed first.
    pub fn new(
        mailbox: &str,
        ids: &[&str],
        flags: &[Flag],
        op: FlagOp,
        auto_select: bool,
    ) -> Result<Self, ImapFlagStoreError> {
        trace!("prepare IMAP flag store (auto_select={auto_select} op={op:?})");
        let mbox = parse_mailbox(mailbox)?;
        let sequence_set = parse_uids(ids)?;
        let imap_flags: Vec<_> = flags.iter().map(flag_from).collect();
        let kind = match op {
            FlagOp::Add => StoreType::Add,
            FlagOp::Set => StoreType::Replace,
            FlagOp::Remove => StoreType::Remove,
        };

        let store = ImapMessageStore::new(
            sequence_set,
            kind,
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

// NOTE: Selecting carries both coroutines; the size delta vs Storing
// is bounded by ImapMailboxSelect. Boxing isn't worth it for
// short-lived per-op state machines.
#[allow(clippy::large_enum_variant)]
enum State {
    Selecting {
        select: ImapMailboxSelect,
        store: ImapMessageStore,
    },
    Storing(ImapMessageStore),
    Done,
}

impl ImapCoroutine for ImapFlagStore {
    type Yield = ImapYield;
    type Return = Result<(), ImapFlagStoreError>;

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
                        return ImapCoroutineState::Complete(Ok(()));
                    }
                    ImapCoroutineState::Complete(Err(err)) => {
                        return ImapCoroutineState::Complete(Err(err.into()));
                    }
                },
                State::Done => {
                    return ImapCoroutineState::Complete(Err(ImapFlagStoreError::ResumedAfterDone));
                }
            }
        }
    }
}
