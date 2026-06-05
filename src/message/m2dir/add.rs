//! m2dir message-add coroutine: wraps
//! [`io_m2dir::entry::store::M2dirEntryStore`] and, when `flags` is
//! non-empty, chains [`M2dirFlagSet`] to persist the sidecar.
//!
//! The store coroutine probes pid + 4 random bytes per the m2dir
//! `<date>,<checksum>.<nonce>` id convention.
//!
//! # Example
//!
//! ```rust,ignore
//! use io_email::message::m2dir::add::M2dirMessageAdd;
//!
//! let id = client.run(M2dirMessageAdd::new(&client.root, "INBOX", &flags, raw)?)?;
//! ```
//!
//! [`M2dirFlagSet`]: io_m2dir::flag::set::M2dirFlagSet

use alloc::{string::String, vec::Vec};
use core::mem;
use std::path::PathBuf;

use io_m2dir::{
    coroutine::*,
    entry::store::{
        M2dirEntryStore as InnerStore, M2dirEntryStoreError as StoreErr,
        M2dirEntryStoreOptions as StoreOpts,
    },
    flag::{
        set::{
            M2dirFlagSet as InnerFlagSet, M2dirFlagSetError as FlagSetErr,
            M2dirFlagSetOptions as FlagSetOpts,
        },
        types::M2dirFlags,
    },
    m2dir::types::M2dir,
};
use log::trace;
use thiserror::Error;

use crate::{
    flag::types::Flag,
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
        let store = InnerStore::new(m2dir.clone(), bytes, StoreOpts::default());
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
                        let set = InnerFlagSet::new(
                            &self.m2dir,
                            &id,
                            self.flags.clone(),
                            FlagSetOpts::default(),
                        );
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
