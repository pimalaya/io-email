//! m2dir mailbox-create coroutine wrapping
//! [`io_m2dir::m2dir::create::M2dirCreate`]: creates the mailbox
//! directory plus the .m2dir marker and .meta/ sidecar.
//!
//! # Example
//!
//! ```rust,ignore
//! use io_email::mailbox::m2dir::create::M2dirMailboxCreate;
//!
//! client.run(M2dirMailboxCreate::new(&client.root, "Archive")?)?;
//! ```

use std::path::PathBuf;

use io_m2dir::{
    coroutine::*,
    m2dir::create::{
        M2dirCreate as InnerCreate, M2dirCreateError as InnerErr, M2dirCreateOptions as InnerOpts,
    },
    store::M2dirStoreError,
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
    InvalidMailbox(#[from] M2dirStoreError),
}

/// I/O-free coroutine creating an m2dir mailbox under the store root.
pub struct M2dirMailboxCreate {
    inner: InnerCreate,
}

impl M2dirMailboxCreate {
    pub fn new(root: impl Into<PathBuf>, name: &str) -> Result<Self, M2dirMailboxCreateError> {
        trace!("prepare m2dir mailbox create");
        let store = store_from_root(root);
        let inner = InnerCreate::new(&store, name, InnerOpts::default())?;
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
