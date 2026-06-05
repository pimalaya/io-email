//! m2dir flag-store coroutine: walks every id and drives one per-id
//! inner ([`M2dirFlagAdd`] / [`M2dirFlagSet`] / [`M2dirFlagRemove`])
//! against the .meta/<id>.flags sidecar.
//!
//! # Example
//!
//! ```rust,ignore
//! use io_email::{flag::FlagOp, m2dir::flag_store::M2dirFlagStore};
//!
//! client.run(M2dirFlagStore::new(&client.root, "INBOX", &["msg-id"], &flags, FlagOp::Add)?)?;
//! ```
//!
//! [`M2dirFlagAdd`]: io_m2dir::flag::add::M2dirFlagAdd
//! [`M2dirFlagSet`]: io_m2dir::flag::set::M2dirFlagSet
//! [`M2dirFlagRemove`]: io_m2dir::flag::remove::M2dirFlagRemove

use alloc::{collections::VecDeque, string::String};
use std::path::PathBuf;

use io_m2dir::{
    coroutine::*,
    flag::{
        add::{
            M2dirFlagAdd as InnerAdd, M2dirFlagAddError as AddErr, M2dirFlagAddOptions as AddOpts,
        },
        remove::{
            M2dirFlagRemove as InnerRemove, M2dirFlagRemoveError as RemoveErr,
            M2dirFlagRemoveOptions as RemoveOpts,
        },
        set::{
            M2dirFlagSet as InnerSet, M2dirFlagSetError as SetErr, M2dirFlagSetOptions as SetOpts,
        },
        types::M2dirFlags,
    },
    m2dir::types::M2dir,
};
use log::trace;
use thiserror::Error;

use crate::{
    flag::{Flag, FlagOp},
    m2dir::convert::{InvalidMailboxName, flags_to_m2dir, resolve_mailbox},
};

/// Errors produced by [`M2dirFlagStore`].
#[derive(Debug, Error)]
pub enum M2dirFlagStoreError {
    #[error(transparent)]
    Add(#[from] AddErr),
    #[error(transparent)]
    Set(#[from] SetErr),
    #[error(transparent)]
    Remove(#[from] RemoveErr),
    #[error(transparent)]
    InvalidMailbox(#[from] InvalidMailboxName),
}

/// I/O-free coroutine applying a flag store across every id in turn.
pub struct M2dirFlagStore {
    m2dir: M2dir,
    flags: M2dirFlags,
    op: FlagOp,
    pending: VecDeque<String>,
    current: Option<Stage>,
}

impl M2dirFlagStore {
    pub fn new(
        root: impl Into<PathBuf>,
        mailbox: &str,
        ids: &[&str],
        flags: &[Flag],
        op: FlagOp,
    ) -> Result<Self, M2dirFlagStoreError> {
        trace!("prepare m2dir flag store ({op:?})");
        let m2dir = resolve_mailbox(root, mailbox)?;
        Ok(Self {
            m2dir,
            flags: flags_to_m2dir(flags),
            op,
            pending: ids.iter().map(|s| (*s).into()).collect(),
            current: None,
        })
    }
}

enum Stage {
    Add(InnerAdd),
    Set(InnerSet),
    Remove(InnerRemove),
}

impl Stage {
    fn start(m2dir: &M2dir, id: String, flags: M2dirFlags, op: FlagOp) -> Self {
        match op {
            FlagOp::Add => Stage::Add(InnerAdd::new(m2dir, id, flags, AddOpts::default())),
            FlagOp::Set => Stage::Set(InnerSet::new(m2dir, id, flags, SetOpts::default())),
            FlagOp::Remove => {
                Stage::Remove(InnerRemove::new(m2dir, id, flags, RemoveOpts::default()))
            }
        }
    }
}

impl M2dirCoroutine for M2dirFlagStore {
    type Yield = M2dirYield;
    type Return = Result<(), M2dirFlagStoreError>;

    fn resume(&mut self, arg: Option<M2dirArg>) -> M2dirCoroutineState<Self::Yield, Self::Return> {
        let mut arg = arg;
        loop {
            if self.current.is_none() {
                let Some(id) = self.pending.pop_front() else {
                    return M2dirCoroutineState::Complete(Ok(()));
                };
                self.current = Some(Stage::start(&self.m2dir, id, self.flags.clone(), self.op));
            }
            let stage = self.current.as_mut().unwrap();
            let inner_state = match stage {
                Stage::Add(inner) => match inner.resume(arg.take()) {
                    M2dirCoroutineState::Yielded(y) => M2dirCoroutineState::Yielded(y),
                    M2dirCoroutineState::Complete(r) => {
                        M2dirCoroutineState::Complete(r.map_err(M2dirFlagStoreError::from))
                    }
                },
                Stage::Set(inner) => match inner.resume(arg.take()) {
                    M2dirCoroutineState::Yielded(y) => M2dirCoroutineState::Yielded(y),
                    M2dirCoroutineState::Complete(r) => {
                        M2dirCoroutineState::Complete(r.map_err(M2dirFlagStoreError::from))
                    }
                },
                Stage::Remove(inner) => match inner.resume(arg.take()) {
                    M2dirCoroutineState::Yielded(y) => M2dirCoroutineState::Yielded(y),
                    M2dirCoroutineState::Complete(r) => {
                        M2dirCoroutineState::Complete(r.map_err(M2dirFlagStoreError::from))
                    }
                },
            };
            match inner_state {
                M2dirCoroutineState::Yielded(y) => return M2dirCoroutineState::Yielded(y),
                M2dirCoroutineState::Complete(Ok(())) => {
                    self.current = None;
                }
                M2dirCoroutineState::Complete(Err(err)) => {
                    return M2dirCoroutineState::Complete(Err(err));
                }
            }
        }
    }
}
