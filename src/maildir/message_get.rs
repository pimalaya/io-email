//! Maildir message-get coroutine.
//!
//! Wraps [`io_maildir::coroutines::message_get::MaildirMessageGet`]:
//! locates the message inside the resolved mailbox via the embedded
//! `MaildirMessageLocate` (cur → new → tmp probing) and reads its raw
//! RFC 5322 bytes.

use alloc::vec::Vec;
use std::path::PathBuf;

use io_maildir::{
    coroutine::{MaildirCoroutine, MaildirCoroutineState, MaildirReply, MaildirYield},
    coroutines::message_get::{
        MaildirMessageGet as InnerMaildirMessageGet,
        MaildirMessageGetError as InnerMaildirMessageGetError,
    },
    maildir::Maildir,
};
use log::trace;
use thiserror::Error;

use crate::{
    coroutine::{
        EmailBackend, EmailCoroutine, EmailCoroutineArg, EmailCoroutineState, FsBatch, FsStep,
    },
    maildir::convert::{
        InvalidMailboxName, dirread_in, fileread_in, paths_out, probes_in, resolve_mailbox,
    },
};

/// Errors produced by [`MaildirMessageGet`].
#[derive(Debug, Error)]
pub enum MaildirMessageGetError {
    #[error(transparent)]
    Get(#[from] InnerMaildirMessageGetError),
    #[error(transparent)]
    InvalidMailbox(#[from] InvalidMailboxName),
    #[error("coroutine was resumed with the wrong EmailCoroutineArg variant")]
    InvalidArg,
    #[error("coroutine was resumed with an FsBatch variant it did not request")]
    UnexpectedBatch,
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

impl EmailCoroutine for MaildirMessageGet {
    type Yield = FsStep;
    type Return = Result<Vec<u8>, MaildirMessageGetError>;

    const BACKEND: EmailBackend = EmailBackend::Maildir;

    fn resume(
        &mut self,
        arg: EmailCoroutineArg<'_>,
    ) -> EmailCoroutineState<Self::Yield, Self::Return> {
        #[allow(irrefutable_let_patterns)]
        let EmailCoroutineArg::Fs { batch } = arg else {
            return EmailCoroutineState::Complete(Err(MaildirMessageGetError::InvalidArg));
        };

        let inner_arg = match batch {
            None => None,
            Some(FsBatch::FileExists(probes)) => Some(MaildirReply::FileExists(probes_in(probes))),
            Some(FsBatch::DirRead(entries)) => Some(MaildirReply::DirRead(dirread_in(entries))),
            Some(FsBatch::FileRead(files)) => Some(MaildirReply::FileRead(fileread_in(files))),
            Some(_) => {
                return EmailCoroutineState::Complete(Err(MaildirMessageGetError::UnexpectedBatch));
            }
        };

        match self.inner.resume(inner_arg) {
            MaildirCoroutineState::Complete(Ok(message)) => {
                EmailCoroutineState::Complete(Ok(message.into()))
            }
            MaildirCoroutineState::Yielded(MaildirYield::WantsFileExists(paths)) => {
                EmailCoroutineState::Yielded(FsStep::WantsFileExists(paths_out(paths)))
            }
            MaildirCoroutineState::Yielded(MaildirYield::WantsDirRead(paths)) => {
                EmailCoroutineState::Yielded(FsStep::WantsDirRead(paths_out(paths)))
            }
            MaildirCoroutineState::Yielded(MaildirYield::WantsFileRead(paths)) => {
                EmailCoroutineState::Yielded(FsStep::WantsFileRead(paths_out(paths)))
            }
            MaildirCoroutineState::Complete(Err(err)) => {
                EmailCoroutineState::Complete(Err(err.into()))
            }
            other => {
                let _ = other;
                unreachable!(
                    "MaildirMessageGet only yields FileExists / DirRead / FileRead / Done / Err"
                );
            }
        }
    }
}
