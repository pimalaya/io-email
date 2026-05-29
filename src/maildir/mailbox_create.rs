//! Maildir mailbox-create coroutine.
//!
//! Wraps [`io_maildir::coroutines::maildir_create::MaildirCreate`]:
//! creates the `<mailbox>/`, `<mailbox>/cur/`, `<mailbox>/new/` and
//! `<mailbox>/tmp/` directories. The on-disk name is computed from
//! the shared `(root, maildir_plus)` pair via [`resolve_mailbox`].

use std::path::PathBuf;

use io_maildir::{
    coroutine::{MaildirCoroutine, MaildirCoroutineState, MaildirReply, MaildirYield},
    coroutines::maildir_create::{
        MaildirCreate as InnerMaildirCreate, MaildirCreateError as InnerMaildirCreateError,
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

/// Errors produced by [`MaildirMailboxCreate`].
#[derive(Debug, Error)]
pub enum MaildirMailboxCreateError {
    #[error(transparent)]
    Create(#[from] InnerMaildirCreateError),
    #[error(transparent)]
    InvalidMailbox(#[from] InvalidMailboxName),
    #[error("coroutine was resumed with the wrong EmailCoroutineArg variant")]
    InvalidArg,
    #[error("coroutine was resumed with an FsBatch variant it did not request")]
    UnexpectedBatch,
}

/// I/O-free coroutine creating a Maildir mailbox under the configured
/// root.
pub struct MaildirMailboxCreate {
    inner: InnerMaildirCreate,
}

impl MaildirMailboxCreate {
    pub fn new(
        root: impl Into<PathBuf>,
        maildir_plus: bool,
        name: &str,
    ) -> Result<Self, MaildirMailboxCreateError> {
        trace!("prepare Maildir mailbox create");
        let path = resolve_mailbox(&root.into(), maildir_plus, name)?;
        let path: MaildirPath = path.into();
        Ok(Self {
            inner: InnerMaildirCreate::new(path),
        })
    }
}

impl EmailCoroutine for MaildirMailboxCreate {
    type Yield = FsStep;
    type Return = Result<(), MaildirMailboxCreateError>;

    const BACKEND: EmailBackend = EmailBackend::Maildir;

    fn resume(
        &mut self,
        arg: EmailCoroutineArg<'_>,
    ) -> EmailCoroutineState<Self::Yield, Self::Return> {
        #[allow(irrefutable_let_patterns)]
        let EmailCoroutineArg::Fs { batch } = arg else {
            return EmailCoroutineState::Complete(Err(MaildirMailboxCreateError::InvalidArg));
        };

        let inner_arg = match batch {
            None => None,
            Some(FsBatch::DirCreate) => Some(MaildirReply::DirCreate),
            Some(_) => {
                return EmailCoroutineState::Complete(Err(
                    MaildirMailboxCreateError::UnexpectedBatch,
                ));
            }
        };

        match self.inner.resume(inner_arg) {
            MaildirCoroutineState::Complete(Ok(())) => EmailCoroutineState::Complete(Ok(())),
            MaildirCoroutineState::Yielded(MaildirYield::WantsDirCreate(paths)) => {
                EmailCoroutineState::Yielded(FsStep::WantsDirCreate(
                    paths.into_iter().map(PathBuf::from).collect(),
                ))
            }
            MaildirCoroutineState::Complete(Err(err)) => {
                EmailCoroutineState::Complete(Err(err.into()))
            }
            other => {
                let _ = other;
                unreachable!("MaildirCreate only yields DirCreate / Done / Err");
            }
        }
    }
}
