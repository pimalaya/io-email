//! m2dir message-get coroutine wrapping
//! [`io_m2dir::entry::get::M2dirEntryGet`]: locates the entry file by
//! id, validates the checksum, and returns raw RFC 5322 bytes.
//!
//! # Example
//!
//! ```rust,ignore
//! use io_email::message::m2dir::get::M2dirMessageGet;
//!
//! let raw = client.run(M2dirMessageGet::new(&client.root, "INBOX", "msg-id")?)?;
//! ```

use alloc::vec::Vec;
use std::path::PathBuf;

use io_m2dir::{
    coroutine::*,
    entry::get::{
        M2dirEntryGet as InnerGet, M2dirEntryGetError as InnerErr,
        M2dirEntryGetOptions as InnerOpts,
    },
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
            inner: InnerGet::new(m2dir, id, InnerOpts::default()),
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
