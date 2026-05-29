//! m2dir mailbox-create coroutine.
//!
//! Wraps [`io_m2dir::coroutines::mailbox_create::M2dirMailboxCreate`]:
//! creates `<root>/<name>/` plus the `.m2dir` marker and `.meta/`
//! subdirectory.

use std::path::PathBuf;

use io_m2dir::{
    coroutine::{M2dirArg, M2dirCoroutine, M2dirCoroutineState, M2dirYield},
    coroutines::mailbox_create::{
        M2dirMailboxCreate as InnerCreate, M2dirMailboxCreateError as InnerErr,
    },
    m2store::NewFolderError,
};
use log::trace;
use thiserror::Error;

use crate::{
    coroutine::{
        EmailBackend, EmailCoroutine, EmailCoroutineArg, EmailCoroutineState, FsBatch, FsStep,
    },
    m2dir::convert::{files_out, paths_out, store_from_root},
};

/// Errors produced by [`M2dirMailboxCreate`].
#[derive(Debug, Error)]
pub enum M2dirMailboxCreateError {
    #[error(transparent)]
    Create(#[from] InnerErr),
    #[error(transparent)]
    InvalidMailbox(#[from] NewFolderError),
    #[error("coroutine was resumed with the wrong EmailCoroutineArg variant")]
    InvalidArg,
    #[error("coroutine was resumed with an FsBatch variant it did not request")]
    UnexpectedBatch,
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

impl EmailCoroutine for M2dirMailboxCreate {
    type Yield = FsStep;
    type Return = Result<(), M2dirMailboxCreateError>;

    const BACKEND: EmailBackend = EmailBackend::M2dir;

    fn resume(
        &mut self,
        arg: EmailCoroutineArg<'_>,
    ) -> EmailCoroutineState<Self::Yield, Self::Return> {
        #[allow(irrefutable_let_patterns)]
        let EmailCoroutineArg::Fs { batch } = arg else {
            return EmailCoroutineState::Complete(Err(M2dirMailboxCreateError::InvalidArg));
        };

        let inner_arg = match batch {
            None => None,
            Some(FsBatch::DirCreate) => Some(M2dirArg::DirCreate),
            Some(FsBatch::FileCreate) => Some(M2dirArg::FileCreate),
            Some(_) => {
                return EmailCoroutineState::Complete(Err(
                    M2dirMailboxCreateError::UnexpectedBatch,
                ));
            }
        };

        match self.inner.resume(inner_arg) {
            M2dirCoroutineState::Complete(Ok(_m2dir)) => EmailCoroutineState::Complete(Ok(())),
            M2dirCoroutineState::Yielded(M2dirYield::WantsDirCreate(paths)) => {
                EmailCoroutineState::Yielded(FsStep::WantsDirCreate(paths_out(paths)))
            }
            M2dirCoroutineState::Yielded(M2dirYield::WantsFileCreate(files)) => {
                EmailCoroutineState::Yielded(FsStep::WantsFileCreate(files_out(files)))
            }
            M2dirCoroutineState::Complete(Err(err)) => {
                EmailCoroutineState::Complete(Err(err.into()))
            }
            other => {
                let _ = other;
                unreachable!("M2dirMailboxCreate only yields DirCreate / FileCreate / Done / Err");
            }
        }
    }
}
