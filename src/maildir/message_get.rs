//! Maildir message-get coroutine.
//!
//! Wraps [`io_maildir::coroutines::message_get::MaildirMessageGet`]:
//! locates the message inside the resolved mailbox via the embedded
//! `MaildirMessageLocate` (cur → new → tmp probing) and reads its raw
//! RFC 5322 bytes.

use alloc::vec::Vec;
use std::path::PathBuf;

use io_maildir::{
    coroutine::*,
    coroutines::message_get::{
        MaildirMessageGet as InnerMaildirMessageGet,
        MaildirMessageGetError as InnerMaildirMessageGetError,
    },
    maildir::Maildir,
};
use log::trace;
use thiserror::Error;

use crate::maildir::convert::{InvalidMailboxName, resolve_mailbox};

/// Errors produced by [`MaildirMessageGet`].
#[derive(Debug, Error)]
pub enum MaildirMessageGetError {
    #[error(transparent)]
    Get(#[from] InnerMaildirMessageGetError),
    #[error(transparent)]
    InvalidMailbox(#[from] InvalidMailboxName),
}

/// I/O-free coroutine reading a single Maildir message as raw bytes.
pub struct MaildirMessageGet {
    inner: InnerMaildirMessageGet,
}

impl MaildirMessageGet {
    pub fn new(
        root: impl Into<PathBuf>,
        maildir_plus: bool,
        mailbox: &str,
        id: &str,
    ) -> Result<Self, MaildirMessageGetError> {
        trace!("prepare Maildir message get");
        let path = resolve_mailbox(&root.into(), maildir_plus, mailbox)?;
        let maildir = Maildir::from_path(path);
        Ok(Self {
            inner: InnerMaildirMessageGet::new(maildir, id),
        })
    }
}

impl MaildirCoroutine for MaildirMessageGet {
    type Yield = MaildirYield;
    type Return = Result<Vec<u8>, MaildirMessageGetError>;

    fn resume(
        &mut self,
        arg: Option<MaildirReply>,
    ) -> MaildirCoroutineState<Self::Yield, Self::Return> {
        match self.inner.resume(arg) {
            MaildirCoroutineState::Yielded(y) => MaildirCoroutineState::Yielded(y),
            MaildirCoroutineState::Complete(Ok(message)) => {
                MaildirCoroutineState::Complete(Ok(message.into()))
            }
            MaildirCoroutineState::Complete(Err(err)) => {
                MaildirCoroutineState::Complete(Err(err.into()))
            }
        }
    }
}
