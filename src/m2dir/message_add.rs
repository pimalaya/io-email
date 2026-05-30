//! m2dir message-add coroutine.
//!
//! Wraps [`io_m2dir::coroutines::message_store::M2dirMessageStore`]
//! and, when `flags` is non-empty, chains a follow-up
//! [`M2dirFlagSet`] to persist them as
//! `.meta/<id>.flags`.
//!
//! The store coroutine probes pid + 4 random bytes to mint the entry
//! id (`<date>,<checksum>.<nonce>` per the m2dir spec).
//!
//! [`M2dirFlagSet`]: io_m2dir::coroutines::flag_set::M2dirFlagSet

use alloc::{string::String, vec::Vec};
use core::mem;
use std::path::PathBuf;

use io_m2dir::{
    coroutine::*,
    coroutines::{
        flag_set::{M2dirFlagSet as InnerFlagSet, M2dirFlagSetError as FlagSetErr},
        message_store::{M2dirMessageStore as InnerStore, M2dirMessageStoreError as StoreErr},
    },
    flag::M2dirFlags,
    m2dir::M2dir,
};
use log::trace;
use thiserror::Error;

use crate::{
    flag::Flag,
    m2dir::convert::{InvalidMailboxName, flags_to_m2dir, resolve_mailbox},
};

/// Errors produced by [`M2dirMessageAdd`].
#[derive(Debug, Error)]
pub enum M2dirMessageAddError {
    #[error(transparent)]
    Store(#[from] StoreErr),
    #[error(transparent)]
    SetFlags(#[from] FlagSetErr),
    #[error(transparent)]
    InvalidMailbox(#[from] InvalidMailboxName),
}

/// I/O-free coroutine appending a raw message to an m2dir mailbox.
pub struct M2dirMessageAdd {
    state: State,
    m2dir: M2dir,
    flags: M2dirFlags,
}

impl M2dirMessageAdd {
    pub fn new(
        root: impl Into<PathBuf>,
        mailbox: &str,
        flags: &[Flag],
        bytes: Vec<u8>,
    ) -> Result<Self, M2dirMessageAddError> {
        trace!("prepare m2dir message add");
        let m2dir = resolve_mailbox(root, mailbox)?;
        let store = InnerStore::new(m2dir.clone(), bytes);
        Ok(Self {
            state: State::Storing(store),
            m2dir,
            flags: flags_to_m2dir(flags),
        })
    }
}

enum State {
    Storing(InnerStore),
    SettingFlags { set: InnerFlagSet, id: String },
    Done,
}

impl M2dirCoroutine for M2dirMessageAdd {
    type Yield = M2dirYield;
    type Return = Result<String, M2dirMessageAddError>;

    fn resume(&mut self, arg: Option<M2dirArg>) -> M2dirCoroutineState<Self::Yield, Self::Return> {
        match mem::replace(&mut self.state, State::Done) {
            State::Storing(mut store) => match store.resume(arg) {
                M2dirCoroutineState::Yielded(y) => {
                    self.state = State::Storing(store);
                    M2dirCoroutineState::Yielded(y)
                }
                M2dirCoroutineState::Complete(Ok(entry)) => {
                    if self.flags.is_empty() {
                        M2dirCoroutineState::Complete(Ok(entry.id().into()))
                    } else {
                        let id: String = entry.id().into();
                        let set = InnerFlagSet::new(&self.m2dir, &id, self.flags.clone());
                        self.state = State::SettingFlags { set, id };
                        M2dirCoroutine::resume(self, None)
                    }
                }
                M2dirCoroutineState::Complete(Err(err)) => {
                    M2dirCoroutineState::Complete(Err(err.into()))
                }
            },
            State::SettingFlags { mut set, id } => match set.resume(arg) {
                M2dirCoroutineState::Yielded(y) => {
                    self.state = State::SettingFlags { set, id };
                    M2dirCoroutineState::Yielded(y)
                }
                M2dirCoroutineState::Complete(Ok(())) => M2dirCoroutineState::Complete(Ok(id)),
                M2dirCoroutineState::Complete(Err(err)) => {
                    M2dirCoroutineState::Complete(Err(err.into()))
                }
            },
            State::Done => unreachable!("M2dirMessageAdd resumed after completion"),
        }
    }
}
