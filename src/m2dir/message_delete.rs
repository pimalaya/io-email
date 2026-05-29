//! m2dir message-delete coroutine.
//!
//! Wraps [`io_m2dir::coroutines::message_delete::M2dirMessageDelete`]:
//! locates the entry file, then removes both the entry and its
//! `.meta/<id>.*` sidecar files.

use std::path::PathBuf;

use io_m2dir::{
    coroutine::{M2dirArg, M2dirCoroutine, M2dirCoroutineState, M2dirYield},
    coroutines::message_delete::{
        M2dirMessageDelete as InnerDelete, M2dirMessageDeleteError as InnerErr,
    },
};
use log::trace;
use thiserror::Error;

use crate::{
    coroutine::{
        EmailBackend, EmailCoroutine, EmailCoroutineArg, EmailCoroutineState, FsBatch, FsStep,
    },
    m2dir::convert::{InvalidMailboxName, dirread_in, paths_out, probes_in, resolve_mailbox},
};

/// Errors produced by [`M2dirMessageDelete`].
#[derive(Debug, Error)]
pub enum M2dirMessageDeleteError {
    #[error(transparent)]
    Delete(#[from] InnerErr),
    #[error(transparent)]
    InvalidMailbox(#[from] InvalidMailboxName),
    #[error("coroutine was resumed with the wrong EmailCoroutineArg variant")]
    InvalidArg,
    #[error("coroutine was resumed with an FsBatch variant it did not request")]
    UnexpectedBatch,
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

impl EmailCoroutine for M2dirMessageDelete {
    type Yield = FsStep;
    type Return = Result<(), M2dirMessageDeleteError>;

    const BACKEND: EmailBackend = EmailBackend::M2dir;

    fn resume(
        &mut self,
        arg: EmailCoroutineArg<'_>,
    ) -> EmailCoroutineState<Self::Yield, Self::Return> {
        #[allow(irrefutable_let_patterns)]
        let EmailCoroutineArg::Fs { batch } = arg else {
            return EmailCoroutineState::Complete(Err(M2dirMessageDeleteError::InvalidArg));
        };

        let inner_arg = match batch {
            None => None,
            Some(FsBatch::DirRead(entries)) => Some(M2dirArg::DirRead(dirread_in(entries))),
            Some(FsBatch::FileExists(probes)) => Some(M2dirArg::FileExists(probes_in(probes))),
            Some(FsBatch::FileRemove) => Some(M2dirArg::FileRemove),
            Some(_) => {
                return EmailCoroutineState::Complete(Err(
                    M2dirMessageDeleteError::UnexpectedBatch,
                ));
            }
        };

        match self.inner.resume(inner_arg) {
            M2dirCoroutineState::Complete(Ok(())) => EmailCoroutineState::Complete(Ok(())),
            M2dirCoroutineState::Yielded(M2dirYield::WantsDirRead(paths)) => {
                EmailCoroutineState::Yielded(FsStep::WantsDirRead(paths_out(paths)))
            }
            M2dirCoroutineState::Yielded(M2dirYield::WantsFileExists(paths)) => {
                EmailCoroutineState::Yielded(FsStep::WantsFileExists(paths_out(paths)))
            }
            M2dirCoroutineState::Yielded(M2dirYield::WantsFileRemove(paths)) => {
                EmailCoroutineState::Yielded(FsStep::WantsFileRemove(paths_out(paths)))
            }
            M2dirCoroutineState::Complete(Err(err)) => {
                EmailCoroutineState::Complete(Err(err.into()))
            }
            other => {
                let _ = other;
                unreachable!(
                    "M2dirMessageDelete only yields DirRead / FileExists / FileRemove / Done / Err"
                );
            }
        }
    }
}
