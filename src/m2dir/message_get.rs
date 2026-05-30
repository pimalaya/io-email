//! m2dir message-get coroutine.
//!
//! Wraps [`io_m2dir::coroutines::message_get::M2dirMessageGet`]: walks
//! the m2dir entry directory until it finds the file whose id matches,
//! then reads its bytes. The checksum embedded in the filename is
//! validated by the inner coroutine before yielding.

use alloc::vec::Vec;
use std::path::PathBuf;

use io_m2dir::{
    coroutine::*,
    coroutines::message_get::{M2dirMessageGet as InnerGet, M2dirMessageGetError as InnerErr},
};
use log::trace;
use thiserror::Error;

use crate::m2dir::convert::{InvalidMailboxName, resolve_mailbox};

/// Errors produced by [`M2dirMessageGet`].
#[derive(Debug, Error)]
pub enum M2dirMessageGetError {
    #[error(transparent)]
    Get(#[from] InnerErr),
    #[error(transparent)]
    InvalidMailbox(#[from] InvalidMailboxName),
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

impl M2dirCoroutine for M2dirMessageGet {
    type Yield = M2dirYield;
    type Return = Result<Vec<u8>, M2dirMessageGetError>;

    fn resume(&mut self, arg: Option<M2dirArg>) -> M2dirCoroutineState<Self::Yield, Self::Return> {
        match self.inner.resume(arg) {
            M2dirCoroutineState::Yielded(y) => M2dirCoroutineState::Yielded(y),
            M2dirCoroutineState::Complete(Ok(out)) => {
                M2dirCoroutineState::Complete(Ok(out.contents))
            }
            M2dirCoroutineState::Complete(Err(err)) => {
                M2dirCoroutineState::Complete(Err(err.into()))
            }
        }
    }
}
