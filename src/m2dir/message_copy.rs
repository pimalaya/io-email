//! m2dir message-copy coroutine: per id chains [`M2dirEntryGet`] then
//! [`M2dirEntryStore`] between m2dirs.
//!
//! Flags are not propagated; callers needing flag-preserving copies
//! must follow up with [`crate::client::EmailClientStd::add_flags`].
//!
//! # Example
//!
//! ```rust,ignore
//! use io_email::m2dir::message_copy::M2dirMessageCopy;
//!
//! client.run(M2dirMessageCopy::new(&client.root, "INBOX", "Archive", &["msg-id"])?)?;
//! ```
//!
//! [`M2dirEntryGet`]: io_m2dir::entry::get::M2dirEntryGet
//! [`M2dirEntryStore`]: io_m2dir::entry::store::M2dirEntryStore

use alloc::{collections::VecDeque, string::String};
use core::mem;
use std::path::PathBuf;

use io_m2dir::{
    coroutine::*,
    entry::{
        get::{
            M2dirEntryGet as InnerGet, M2dirEntryGetError as GetErr,
            M2dirEntryGetOptions as GetOpts,
        },
        store::{
            M2dirEntryStore as InnerStore, M2dirEntryStoreError as StoreErr,
            M2dirEntryStoreOptions as StoreOpts,
        },
    },
    m2dir::types::M2dir,
};
use log::trace;
use thiserror::Error;

use crate::m2dir::convert::{InvalidMailboxName, resolve_mailbox};

/// Errors produced by [`M2dirMessageCopy`].
#[derive(Debug, Error)]
pub enum M2dirMessageCopyError {
    #[error(transparent)]
    Get(#[from] GetErr),
    #[error(transparent)]
    Store(#[from] StoreErr),
    #[error(transparent)]
    InvalidMailbox(#[from] InvalidMailboxName),
}

/// I/O-free coroutine copying every id from `from` to `to`.
pub struct M2dirMessageCopy {
    source: M2dir,
    target: M2dir,
    pending: VecDeque<String>,
    stage: Stage,
}

impl M2dirMessageCopy {
    pub fn new(
        root: impl Into<PathBuf>,
        from: &str,
        to: &str,
        ids: &[&str],
    ) -> Result<Self, M2dirMessageCopyError> {
        trace!("prepare m2dir message copy");
        let root = root.into();
        let source = resolve_mailbox(&root, from)?;
        let target = resolve_mailbox(&root, to)?;
        Ok(Self {
            source,
            target,
            pending: ids.iter().map(|s| (*s).into()).collect(),
            stage: Stage::Idle,
        })
    }
}

enum Stage {
    Idle,
    Getting(InnerGet),
    Storing(InnerStore),
}

impl M2dirCoroutine for M2dirMessageCopy {
    type Yield = M2dirYield;
    type Return = Result<(), M2dirMessageCopyError>;

    fn resume(&mut self, arg: Option<M2dirArg>) -> M2dirCoroutineState<Self::Yield, Self::Return> {
        let mut arg = arg;
        loop {
            if matches!(self.stage, Stage::Idle) {
                let Some(id) = self.pending.pop_front() else {
                    return M2dirCoroutineState::Complete(Ok(()));
                };
                self.stage =
                    Stage::Getting(InnerGet::new(self.source.clone(), id, GetOpts::default()));
            }
            match mem::replace(&mut self.stage, Stage::Idle) {
                Stage::Idle => unreachable!(),
                Stage::Getting(mut get) => match get.resume(arg.take()) {
                    M2dirCoroutineState::Yielded(y) => {
                        self.stage = Stage::Getting(get);
                        return M2dirCoroutineState::Yielded(y);
                    }
                    M2dirCoroutineState::Complete(Ok(ok)) => {
                        self.stage = Stage::Storing(InnerStore::new(
                            self.target.clone(),
                            ok.contents,
                            StoreOpts::default(),
                        ));
                    }
                    M2dirCoroutineState::Complete(Err(err)) => {
                        return M2dirCoroutineState::Complete(Err(err.into()));
                    }
                },
                Stage::Storing(mut store) => match store.resume(arg.take()) {
                    M2dirCoroutineState::Yielded(y) => {
                        self.stage = Stage::Storing(store);
                        return M2dirCoroutineState::Yielded(y);
                    }
                    M2dirCoroutineState::Complete(Ok(_entry)) => {
                        // NOTE: loop back to the next id or finish.
                    }
                    M2dirCoroutineState::Complete(Err(err)) => {
                        return M2dirCoroutineState::Complete(Err(err.into()));
                    }
                },
            }
        }
    }
}
