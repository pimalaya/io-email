//! Maildir message-delete coroutine.
//!
//! Maildir has no atomic "remove this message" primitive; the
//! conventional approach is to mark the message with the `T`
//! (Trashed) info-section letter and let a periodic expunge clean it
//! up. This coroutine wraps
//! [`io_maildir::coroutines::flags_add::MaildirFlagsAdd`] with the
//! [`MaildirFlag::Trashed`] flag so a `delete_message` call on the
//! shared API stays portable.
//!
//! [`MaildirFlag::Trashed`]: io_maildir::flag::MaildirFlag::Trashed

use core::iter::once;
use std::path::PathBuf;

use io_maildir::{
    coroutine::{MaildirCoroutine, MaildirCoroutineState, MaildirReply, MaildirYield},
    coroutines::flags_add::{MaildirFlagsAdd as InnerAdd, MaildirFlagsAddError as InnerErr},
    flag::MaildirFlag,
    maildir::Maildir,
};
use log::trace;
use thiserror::Error;

use crate::{
    coroutine::{
        EmailBackend, EmailCoroutine, EmailCoroutineArg, EmailCoroutineState, FsBatch, FsStep,
    },
    maildir::convert::{
        InvalidMailboxName, dirread_in, pairs_out, paths_out, probes_in, resolve_mailbox,
    },
};

/// Errors produced by [`MaildirMessageDelete`].
#[derive(Debug, Error)]
pub enum MaildirMessageDeleteError {
    #[error(transparent)]
    Trash(#[from] InnerErr),
    #[error(transparent)]
    InvalidMailbox(#[from] InvalidMailboxName),
    #[error("coroutine was resumed with the wrong EmailCoroutineArg variant")]
    InvalidArg,
    #[error("coroutine was resumed with an FsBatch variant it did not request")]
    UnexpectedBatch,
}

/// I/O-free coroutine flagging a Maildir message as Trashed.
pub struct MaildirMessageDelete {
    inner: InnerAdd,
}

impl MaildirMessageDelete {
    pub fn new(
        root: impl Into<PathBuf>,
        maildir_plus: bool,
        mailbox: &str,
        id: &str,
    ) -> Result<Self, MaildirMessageDeleteError> {
        trace!("prepare Maildir message delete (Trashed flag)");
        let path = resolve_mailbox(&root.into(), maildir_plus, mailbox)?;
        let maildir = Maildir::from_path(path);
        let trashed = once(MaildirFlag::Trashed).collect();
        Ok(Self {
            inner: InnerAdd::new(maildir, id, trashed),
        })
    }
}

impl EmailCoroutine for MaildirMessageDelete {
    type Yield = FsStep;
    type Return = Result<(), MaildirMessageDeleteError>;

    const BACKEND: EmailBackend = EmailBackend::Maildir;

    fn resume(
        &mut self,
        arg: EmailCoroutineArg<'_>,
    ) -> EmailCoroutineState<Self::Yield, Self::Return> {
        #[allow(irrefutable_let_patterns)]
        let EmailCoroutineArg::Fs { batch } = arg else {
            return EmailCoroutineState::Complete(Err(MaildirMessageDeleteError::InvalidArg));
        };

        let inner_arg = match batch {
            None => None,
            Some(FsBatch::FileExists(probes)) => Some(MaildirReply::FileExists(probes_in(probes))),
            Some(FsBatch::DirRead(entries)) => Some(MaildirReply::DirRead(dirread_in(entries))),
            Some(FsBatch::Rename) => Some(MaildirReply::Rename),
            Some(_) => {
                return EmailCoroutineState::Complete(Err(
                    MaildirMessageDeleteError::UnexpectedBatch,
                ));
            }
        };

        match self.inner.resume(inner_arg) {
            MaildirCoroutineState::Complete(Ok(())) => EmailCoroutineState::Complete(Ok(())),
            MaildirCoroutineState::Yielded(MaildirYield::WantsFileExists(paths)) => {
                EmailCoroutineState::Yielded(FsStep::WantsFileExists(paths_out(paths)))
            }
            MaildirCoroutineState::Yielded(MaildirYield::WantsDirRead(paths)) => {
                EmailCoroutineState::Yielded(FsStep::WantsDirRead(paths_out(paths)))
            }
            MaildirCoroutineState::Yielded(MaildirYield::WantsRename(pairs)) => {
                EmailCoroutineState::Yielded(FsStep::WantsRename(pairs_out(pairs)))
            }
            MaildirCoroutineState::Complete(Err(err)) => {
                EmailCoroutineState::Complete(Err(err.into()))
            }
            other => {
                let _ = other;
                unreachable!("MaildirFlagsAdd never yields this state");
            }
        }
    }
}
