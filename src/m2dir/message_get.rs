//! m2dir message-get coroutine.
//!
//! Wraps [`io_m2dir::coroutines::message_get::M2dirMessageGet`]: walks
//! the m2dir entry directory until it finds the file whose id matches,
//! then reads its bytes. The checksum embedded in the filename is
//! validated by the inner coroutine before yielding.

use alloc::vec::Vec;
use std::path::PathBuf;

use io_m2dir::{
    coroutine::{M2dirArg, M2dirCoroutine, M2dirCoroutineState, M2dirYield},
    coroutines::message_get::{M2dirMessageGet as InnerGet, M2dirMessageGetError as InnerErr},
};
use log::trace;
use thiserror::Error;

use crate::{
    coroutine::{
        EmailBackend, EmailCoroutine, EmailCoroutineArg, EmailCoroutineState, FsBatch, FsStep,
    },
    m2dir::convert::{
        InvalidMailboxName, dirread_in, fileread_in, paths_out, probes_in, resolve_mailbox,
    },
};

/// Errors produced by [`M2dirMessageGet`].
#[derive(Debug, Error)]
pub enum M2dirMessageGetError {
    #[error(transparent)]
    Get(#[from] InnerErr),
    #[error(transparent)]
    InvalidMailbox(#[from] InvalidMailboxName),
    #[error("coroutine was resumed with the wrong EmailCoroutineArg variant")]
    InvalidArg,
    #[error("coroutine was resumed with an FsBatch variant it did not request")]
    UnexpectedBatch,
}

/// I/O-free coroutine reading a single m2dir message as raw bytes.
pub struct M2dirMessageGet {
    inner: InnerGet,
}

impl M2dirMessageGet {
    pub fn new(
        root: impl Into<PathBuf>,
        mailbox: &str,
        id: &str,
    ) -> Result<Self, M2dirMessageGetError> {
        trace!("prepare m2dir message get");
        let m2dir = resolve_mailbox(root, mailbox)?;
        Ok(Self {
            inner: InnerGet::new(m2dir, id),
        })
    }
}

impl EmailCoroutine for M2dirMessageGet {
    type Yield = FsStep;
    type Return = Result<Vec<u8>, M2dirMessageGetError>;

    const BACKEND: EmailBackend = EmailBackend::M2dir;

    fn resume(
        &mut self,
        arg: EmailCoroutineArg<'_>,
    ) -> EmailCoroutineState<Self::Yield, Self::Return> {
        #[allow(irrefutable_let_patterns)]
        let EmailCoroutineArg::Fs { batch } = arg else {
            return EmailCoroutineState::Complete(Err(M2dirMessageGetError::InvalidArg));
        };

        let inner_arg = match batch {
            None => None,
            Some(FsBatch::DirRead(entries)) => Some(M2dirArg::DirRead(dirread_in(entries))),
            Some(FsBatch::FileExists(probes)) => Some(M2dirArg::FileExists(probes_in(probes))),
            Some(FsBatch::FileRead(files)) => Some(M2dirArg::FileRead(fileread_in(files))),
            Some(_) => {
                return EmailCoroutineState::Complete(Err(M2dirMessageGetError::UnexpectedBatch));
            }
        };

        match self.inner.resume(inner_arg) {
            M2dirCoroutineState::Complete(Ok(ok)) => EmailCoroutineState::Complete(Ok(ok.contents)),
            M2dirCoroutineState::Yielded(M2dirYield::WantsDirRead(paths)) => {
                EmailCoroutineState::Yielded(FsStep::WantsDirRead(paths_out(paths)))
            }
            M2dirCoroutineState::Yielded(M2dirYield::WantsFileExists(paths)) => {
                EmailCoroutineState::Yielded(FsStep::WantsFileExists(paths_out(paths)))
            }
            M2dirCoroutineState::Yielded(M2dirYield::WantsFileRead(paths)) => {
                EmailCoroutineState::Yielded(FsStep::WantsFileRead(paths_out(paths)))
            }
            M2dirCoroutineState::Complete(Err(err)) => {
                EmailCoroutineState::Complete(Err(err.into()))
            }
            other => {
                let _ = other;
                unreachable!(
                    "M2dirMessageGet only yields DirRead / FileExists / FileRead / Done / Err"
                );
            }
        }
    }
}
