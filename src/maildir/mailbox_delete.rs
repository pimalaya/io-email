//! Maildir mailbox-delete coroutine.
//!
//! Wraps [`io_maildir::coroutines::maildir_delete::MaildirDelete`]:
//! recursively removes the on-disk directory for the named mailbox.

use std::path::PathBuf;

use io_maildir::{
    coroutine::{MaildirCoroutine, MaildirCoroutineState, MaildirReply, MaildirYield},
    coroutines::maildir_delete::{
        MaildirDelete as InnerMaildirDelete, MaildirDeleteError as InnerMaildirDeleteError,
    },
    path::MaildirPath,
};
use log::trace;
use thiserror::Error;

use crate::{
    coroutine::{
        EmailBackend, EmailCoroutine, EmailCoroutineArg, EmailCoroutineState, FsBatch, FsStep,
    },
    maildir::convert::{InvalidMailboxName, resolve_mailbox},
};

/// Errors produced by [`MaildirMailboxDelete`].
#[derive(Debug, Error)]
pub enum MaildirMailboxDeleteError {
    #[error(transparent)]
    Delete(#[from] InnerMaildirDeleteError),
    #[error(transparent)]
    InvalidMailbox(#[from] InvalidMailboxName),
    #[error("coroutine was resumed with the wrong EmailCoroutineArg variant")]
    InvalidArg,
    #[error("coroutine was resumed with an FsBatch variant it did not request")]
    UnexpectedBatch,
}

/// I/O-free coroutine deleting a Maildir mailbox under the configured
/// root.
pub struct MaildirMailboxDelete {
    inner: InnerMaildirDelete,
}

impl MaildirMailboxDelete {
    pub fn new(
        root: impl Into<PathBuf>,
        maildir_plus: bool,
        name: &str,
    ) -> Result<Self, MaildirMailboxDeleteError> {
        trace!("prepare Maildir mailbox delete");
        let path = resolve_mailbox(&root.into(), maildir_plus, name)?;
        let path: MaildirPath = path.into();
        Ok(Self {
            inner: InnerMaildirDelete::new(path),
        })
    }
}

impl EmailCoroutine for MaildirMailboxDelete {
    type Yield = FsStep;
    type Return = Result<(), MaildirMailboxDeleteError>;

    const BACKEND: EmailBackend = EmailBackend::Maildir;

    fn resume(
        &mut self,
        arg: EmailCoroutineArg<'_>,
    ) -> EmailCoroutineState<Self::Yield, Self::Return> {
        #[allow(irrefutable_let_patterns)]
        let EmailCoroutineArg::Fs { batch } = arg else {
            return EmailCoroutineState::Complete(Err(MaildirMailboxDeleteError::InvalidArg));
        };

        let inner_arg = match batch {
            None => None,
            Some(FsBatch::DirRemove) => Some(MaildirReply::DirRemove),
            Some(_) => {
                return EmailCoroutineState::Complete(Err(
                    MaildirMailboxDeleteError::UnexpectedBatch,
                ));
            }
        };

        match self.inner.resume(inner_arg) {
            MaildirCoroutineState::Complete(Ok(())) => EmailCoroutineState::Complete(Ok(())),
            MaildirCoroutineState::Yielded(MaildirYield::WantsDirRemove(paths)) => {
                EmailCoroutineState::Yielded(FsStep::WantsDirRemove(
                    paths.into_iter().map(PathBuf::from).collect(),
                ))
            }
            MaildirCoroutineState::Complete(Err(err)) => {
                EmailCoroutineState::Complete(Err(err.into()))
            }
            other => {
                let _ = other;
                unreachable!("MaildirDelete only yields DirRemove / Done / Err");
            }
        }
    }
}
