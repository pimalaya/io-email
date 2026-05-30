//! m2dir mailbox-create coroutine.
//!
//! Wraps [`io_m2dir::coroutines::mailbox_create::M2dirMailboxCreate`]:
//! creates `<root>/<name>/` plus the `.m2dir` marker and `.meta/`
//! subdirectory.

use std::path::PathBuf;

use io_m2dir::{
    coroutine::*,
    coroutines::mailbox_create::{
        M2dirMailboxCreate as InnerCreate, M2dirMailboxCreateError as InnerErr,
    },
    m2store::NewFolderError,
};
use log::trace;
use thiserror::Error;

use crate::m2dir::convert::store_from_root;

/// Errors produced by [`M2dirMailboxCreate`].
#[derive(Debug, Error)]
pub enum M2dirMailboxCreateError {
    #[error(transparent)]
    Create(#[from] InnerErr),
    #[error(transparent)]
    InvalidMailbox(#[from] NewFolderError),
}

/// I/O-free coroutine creating an m2dir mailbox under the m2store root.
pub struct M2dirMailboxCreate {
    inner: InnerCreate,
}

impl M2dirMailboxCreate {
    pub fn new(root: impl Into<PathBuf>, name: &str) -> Result<Self, M2dirMailboxCreateError> {
        trace!("prepare m2dir mailbox create");
        let store = store_from_root(root);
        let inner = InnerCreate::new(&store, name)?;
        Ok(Self { inner })
    }
}

impl M2dirCoroutine for M2dirMailboxCreate {
    type Yield = M2dirYield;
    type Return = Result<(), M2dirMailboxCreateError>;

    fn resume(&mut self, arg: Option<M2dirArg>) -> M2dirCoroutineState<Self::Yield, Self::Return> {
        match self.inner.resume(arg) {
            M2dirCoroutineState::Yielded(y) => M2dirCoroutineState::Yielded(y),
            M2dirCoroutineState::Complete(Ok(_m2dir)) => M2dirCoroutineState::Complete(Ok(())),
            M2dirCoroutineState::Complete(Err(err)) => {
                M2dirCoroutineState::Complete(Err(err.into()))
            }
        }
    }
}
