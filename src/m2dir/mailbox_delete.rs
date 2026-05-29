//! m2dir mailbox-delete coroutine.
//!
//! Wraps [`io_m2dir::coroutines::mailbox_delete::M2dirMailboxDelete`]:
//! recursively removes the on-disk directory for the named mailbox.

use std::path::PathBuf;

use io_m2dir::{
    coroutine::{M2dirArg, M2dirCoroutine, M2dirCoroutineState, M2dirYield},
    coroutines::mailbox_delete::{
        M2dirMailboxDelete as InnerDelete, M2dirMailboxDeleteError as InnerErr,
    },
};
use log::trace;
use thiserror::Error;

use crate::{
    coroutine::{
        EmailBackend, EmailCoroutine, EmailCoroutineArg, EmailCoroutineState, FsBatch, FsStep,
    },
    m2dir::convert::{InvalidMailboxName, paths_out, resolve_mailbox},
};

/// Errors produced by [`M2dirMailboxDelete`].
#[derive(Debug, Error)]
pub enum M2dirMailboxDeleteError {
    #[error(transparent)]
    Delete(#[from] InnerErr),
    #[error(transparent)]
    InvalidMailbox(#[from] InvalidMailboxName),
    #[error("coroutine was resumed with the wrong EmailCoroutineArg variant")]
    InvalidArg,
    #[error("coroutine was resumed with an FsBatch variant it did not request")]
    UnexpectedBatch,
}

/// I/O-free coroutine deleting an m2dir mailbox under the m2store root.
pub struct M2dirMailboxDelete {
    inner: InnerDelete,
}

impl M2dirMailboxDelete {
    pub fn new(root: impl Into<PathBuf>, name: &str) -> Result<Self, M2dirMailboxDeleteError> {
        trace!("prepare m2dir mailbox delete");
        let m2dir = resolve_mailbox(root, name)?;
        Ok(Self {
            inner: InnerDelete::new(m2dir.path().clone()),
        })
    }
}

impl EmailCoroutine for M2dirMailboxDelete {
    type Yield = FsStep;
    type Return = Result<(), M2dirMailboxDeleteError>;

    const BACKEND: EmailBackend = EmailBackend::M2dir;

    fn resume(
        &mut self,
        arg: EmailCoroutineArg<'_>,
    ) -> EmailCoroutineState<Self::Yield, Self::Return> {
        #[allow(irrefutable_let_patterns)]
        let EmailCoroutineArg::Fs { batch } = arg else {
            return EmailCoroutineState::Complete(Err(M2dirMailboxDeleteError::InvalidArg));
        };

        let inner_arg = match batch {
            None => None,
            Some(FsBatch::DirRemove) => Some(M2dirArg::DirRemove),
            Some(_) => {
                return EmailCoroutineState::Complete(Err(
                    M2dirMailboxDeleteError::UnexpectedBatch,
                ));
            }
        };

        match self.inner.resume(inner_arg) {
            M2dirCoroutineState::Complete(Ok(())) => EmailCoroutineState::Complete(Ok(())),
            M2dirCoroutineState::Yielded(M2dirYield::WantsDirRemove(paths)) => {
                EmailCoroutineState::Yielded(FsStep::WantsDirRemove(paths_out(paths)))
            }
            M2dirCoroutineState::Complete(Err(err)) => {
                EmailCoroutineState::Complete(Err(err.into()))
            }
            other => {
                let _ = other;
                unreachable!("M2dirMailboxDelete only yields DirRemove / Done / Err");
            }
        }
    }
}
