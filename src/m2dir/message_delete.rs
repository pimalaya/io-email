//! m2dir message-delete coroutine.
//!
//! Wraps [`io_m2dir::coroutines::message_delete::M2dirMessageDelete`]:
//! locates the entry file, then removes both the entry and its
//! `.meta/<id>.*` sidecar files.

use std::path::PathBuf;

use io_m2dir::{
    coroutine::*,
    coroutines::message_delete::{
        M2dirMessageDelete as InnerDelete, M2dirMessageDeleteError as InnerErr,
    },
};
use log::trace;
use thiserror::Error;

use crate::m2dir::convert::{InvalidMailboxName, resolve_mailbox};

/// Errors produced by [`M2dirMessageDelete`].
#[derive(Debug, Error)]
pub enum M2dirMessageDeleteError {
    #[error(transparent)]
    Delete(#[from] InnerErr),
    #[error(transparent)]
    InvalidMailbox(#[from] InvalidMailboxName),
}

/// I/O-free coroutine deleting a single m2dir message.
pub struct M2dirMessageDelete {
    inner: InnerDelete,
}

impl M2dirMessageDelete {
    pub fn new(
        root: impl Into<PathBuf>,
        mailbox: &str,
        id: &str,
    ) -> Result<Self, M2dirMessageDeleteError> {
        trace!("prepare m2dir message delete");
        let m2dir = resolve_mailbox(root, mailbox)?;
        Ok(Self {
            inner: InnerDelete::new(m2dir, id),
        })
    }
}

impl M2dirCoroutine for M2dirMessageDelete {
    type Yield = M2dirYield;
    type Return = Result<(), M2dirMessageDeleteError>;

    fn resume(&mut self, arg: Option<M2dirArg>) -> M2dirCoroutineState<Self::Yield, Self::Return> {
        match self.inner.resume(arg) {
            M2dirCoroutineState::Yielded(y) => M2dirCoroutineState::Yielded(y),
            M2dirCoroutineState::Complete(r) => {
                M2dirCoroutineState::Complete(r.map_err(Into::into))
            }
        }
    }
}
