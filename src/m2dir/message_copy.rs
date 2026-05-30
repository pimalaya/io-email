//! m2dir message-copy coroutine: per id, fetches the source bytes via
//! [`M2dirMessageGet`] then writes them to the target via
//! [`M2dirMessageStore`].
//!
//! Flags are not propagated across the copy: the m2dir flag layout
//! lives in a separate `.meta/<id>.flags` sidecar whose payload is
//! reread under different ids on the target side. Callers needing
//! flag-preserving copies should add them via
//! [`crate::client::EmailClientStd::add_flags`] after the copy
//! completes.
//!
//! [`M2dirMessageGet`]: io_m2dir::coroutines::message_get::M2dirMessageGet
//! [`M2dirMessageStore`]: io_m2dir::coroutines::message_store::M2dirMessageStore

use alloc::{collections::VecDeque, string::String};
use core::mem;
use std::path::PathBuf;

use io_m2dir::{
    coroutine::*,
    coroutines::{
        message_get::{M2dirMessageGet as InnerGet, M2dirMessageGetError as GetErr},
        message_store::{M2dirMessageStore as InnerStore, M2dirMessageStoreError as StoreErr},
    },
    m2dir::M2dir,
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
                self.stage = Stage::Getting(InnerGet::new(self.source.clone(), id));
            }
            match mem::replace(&mut self.stage, Stage::Idle) {
                Stage::Idle => unreachable!(),
                Stage::Getting(mut get) => match get.resume(arg.take()) {
                    M2dirCoroutineState::Yielded(y) => {
                        self.stage = Stage::Getting(get);
                        return M2dirCoroutineState::Yielded(y);
                    }
                    M2dirCoroutineState::Complete(Ok(ok)) => {
                        self.stage =
                            Stage::Storing(InnerStore::new(self.target.clone(), ok.contents));
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
                        // NOTE: loop back to pull the next id or finish.
                    }
                    M2dirCoroutineState::Complete(Err(err)) => {
                        return M2dirCoroutineState::Complete(Err(err.into()));
                    }
                },
            }
        }
    }
}
