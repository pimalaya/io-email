//! m2dir message-move coroutine: per id chains [`M2dirEntryGet`] +
//! [`M2dirEntryStore`] + [`M2dirEntryDelete`].
//!
//! Flags are not propagated; the .meta/<id>.flags sidecar must be
//! re-applied after the move when needed.
//!
//! # Example
//!
//! ```rust,ignore
//! use io_email::m2dir::message_move::M2dirMessageMove;
//!
//! client.run(M2dirMessageMove::new(&client.root, "INBOX", "Archive", &["msg-id"])?)?;
//! ```
//!
//! [`M2dirEntryGet`]: io_m2dir::entry::get::M2dirEntryGet
//! [`M2dirEntryStore`]: io_m2dir::entry::store::M2dirEntryStore
//! [`M2dirEntryDelete`]: io_m2dir::entry::delete::M2dirEntryDelete

use alloc::{collections::VecDeque, string::String};
use core::mem;
use std::path::PathBuf;

use io_m2dir::{
    coroutine::*,
    entry::{
        delete::{
            M2dirEntryDelete as InnerDelete, M2dirEntryDeleteError as DeleteErr,
            M2dirEntryDeleteOptions as DeleteOpts,
        },
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

/// Errors produced by [`M2dirMessageMove`].
#[derive(Debug, Error)]
pub enum M2dirMessageMoveError {
    #[error(transparent)]
    Get(#[from] GetErr),
    #[error(transparent)]
    Store(#[from] StoreErr),
    #[error(transparent)]
    Delete(#[from] DeleteErr),
    #[error(transparent)]
    InvalidMailbox(#[from] InvalidMailboxName),
}

/// I/O-free coroutine moving every id from `from` to `to`.
pub struct M2dirMessageMove {
    source: M2dir,
    target: M2dir,
    pending: VecDeque<String>,
    current_id: Option<String>,
    stage: Stage,
}

impl M2dirMessageMove {
    pub fn new(
        root: impl Into<PathBuf>,
        from: &str,
        to: &str,
        ids: &[&str],
    ) -> Result<Self, M2dirMessageMoveError> {
        trace!("prepare m2dir message move");
        let root = root.into();
        let source = resolve_mailbox(&root, from)?;
        let target = resolve_mailbox(&root, to)?;
        Ok(Self {
            source,
            target,
            pending: ids.iter().map(|s| (*s).into()).collect(),
            current_id: None,
            stage: Stage::Idle,
        })
    }
}

enum Stage {
    Idle,
    Getting(InnerGet),
    Storing(InnerStore),
    Deleting(InnerDelete),
}

impl M2dirCoroutine for M2dirMessageMove {
    type Yield = M2dirYield;
    type Return = Result<(), M2dirMessageMoveError>;

    fn resume(&mut self, arg: Option<M2dirArg>) -> M2dirCoroutineState<Self::Yield, Self::Return> {
        let mut arg = arg;
        loop {
            if matches!(self.stage, Stage::Idle) {
                let Some(id) = self.pending.pop_front() else {
                    return M2dirCoroutineState::Complete(Ok(()));
                };
                self.stage = Stage::Getting(InnerGet::new(
                    self.source.clone(),
                    id.clone(),
                    GetOpts::default(),
                ));
                self.current_id = Some(id);
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
                        let id = self.current_id.take().expect("current_id set when storing");
                        self.stage = Stage::Deleting(InnerDelete::new(
                            self.source.clone(),
                            id,
                            DeleteOpts::default(),
                        ));
                    }
                    M2dirCoroutineState::Complete(Err(err)) => {
                        return M2dirCoroutineState::Complete(Err(err.into()));
                    }
                },
                Stage::Deleting(mut delete) => match delete.resume(arg.take()) {
                    M2dirCoroutineState::Yielded(y) => {
                        self.stage = Stage::Deleting(delete);
                        return M2dirCoroutineState::Yielded(y);
                    }
                    M2dirCoroutineState::Complete(Ok(())) => {
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
