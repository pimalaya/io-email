//! m2dir mailbox-delete coroutine wrapping
//! [`io_m2dir::m2dir::delete::M2dirDelete`]: recursively removes the
//! mailbox directory.
//!
//! # Example
//!
//! ```rust,ignore
//! use io_email::mailbox::m2dir::delete::M2dirMailboxDelete;
//!
//! client.run(M2dirMailboxDelete::new(&client.root, "Archive")?)?;
//! ```

use std::path::PathBuf;

use io_m2dir::{
    coroutine::*,
    m2dir::delete::{
        M2dirDelete as InnerDelete, M2dirDeleteError as InnerErr, M2dirDeleteOptions as InnerOpts,
    },
};
use log::trace;
use thiserror::Error;

use crate::m2dir::convert::{InvalidMailboxName, resolve_mailbox};

/// Errors produced by [`M2dirMailboxDelete`].
#[derive(Debug, Error)]
pub enum M2dirMailboxDeleteError {
    #[error(transparent)]
    Delete(#[from] InnerErr),
    #[error(transparent)]
    InvalidMailbox(#[from] InvalidMailboxName),
}

/// I/O-free coroutine deleting an m2dir mailbox under the store root.
pub struct M2dirMailboxDelete {
    inner: InnerDelete,
}

impl M2dirMailboxDelete {
    pub fn new(root: impl Into<PathBuf>, name: &str) -> Result<Self, M2dirMailboxDeleteError> {
        trace!("prepare m2dir mailbox delete");
        let m2dir = resolve_mailbox(root, name)?;
        Ok(Self {
            inner: InnerDelete::new(m2dir.path().clone(), InnerOpts::default()),
        })
    }
}

impl M2dirCoroutine for M2dirMailboxDelete {
    type Yield = M2dirYield;
    type Return = Result<(), M2dirMailboxDeleteError>;

    fn resume(&mut self, arg: Option<M2dirArg>) -> M2dirCoroutineState<Self::Yield, Self::Return> {
        match self.inner.resume(arg) {
            M2dirCoroutineState::Yielded(y) => M2dirCoroutineState::Yielded(y),
            M2dirCoroutineState::Complete(r) => {
                M2dirCoroutineState::Complete(r.map_err(Into::into))
            }
        }
    }
}
