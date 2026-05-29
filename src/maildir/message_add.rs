//! Maildir message-add coroutine.
//!
//! Wraps [`io_maildir::coroutines::message_store::MaildirMessageStore`]:
//! writes the raw bytes to `tmp/`, then renames into `cur/` with the
//! info-section letters derived from `flags`. The yielded id is the
//! Maildir filename minus the `:2,FLAGS` suffix.
//!
//! `MaildirMessageStore` itself probes time / pid / hostname to mint
//! the message identifier (RFC's `time.usec.hostname` convention), so
//! this coroutine relays those `Wants*` variants through.

use alloc::{string::String, vec::Vec};
use std::path::PathBuf;

use io_maildir::{
    coroutine::{MaildirCoroutine, MaildirCoroutineState, MaildirReply, MaildirYield},
    coroutines::message_store::{
        MaildirMessageStore as InnerStore, MaildirMessageStoreError as InnerErr,
    },
    maildir::{Maildir, MaildirSubdir},
};
use log::trace;
use thiserror::Error;

use crate::{
    coroutine::{
        EmailBackend, EmailCoroutine, EmailCoroutineArg, EmailCoroutineState, FsBatch, FsStep,
    },
    flag::Flag,
    maildir::convert::{
        InvalidMailboxName, files_out, flags_to_maildir, pairs_out, resolve_mailbox,
    },
};

/// Errors produced by [`MaildirMessageAdd`].
#[derive(Debug, Error)]
pub enum MaildirMessageAddError {
    #[error(transparent)]
    Store(#[from] InnerErr),
    #[error(transparent)]
    InvalidMailbox(#[from] InvalidMailboxName),
    #[error("coroutine was resumed with the wrong EmailCoroutineArg variant")]
    InvalidArg,
    #[error("coroutine was resumed with an FsBatch variant it did not request")]
    UnexpectedBatch,
}

/// I/O-free coroutine appending a raw message to a Maildir under `cur/`.
pub struct MaildirMessageAdd {
    inner: InnerStore,
}

impl MaildirMessageAdd {
    pub fn new(
        root: impl Into<PathBuf>,
        maildir_plus: bool,
        mailbox: &str,
        flags: &[Flag],
        bytes: Vec<u8>,
    ) -> Result<Self, MaildirMessageAddError> {
        trace!("prepare Maildir message add");
        let path = resolve_mailbox(&root.into(), maildir_plus, mailbox)?;
        let maildir = Maildir::from_path(path);
        let md_flags = flags_to_maildir(flags);
        Ok(Self {
            inner: InnerStore::new(maildir, MaildirSubdir::Cur, md_flags, bytes),
        })
    }
}

impl EmailCoroutine for MaildirMessageAdd {
    type Yield = FsStep;
    type Return = Result<String, MaildirMessageAddError>;

    const BACKEND: EmailBackend = EmailBackend::Maildir;

    fn resume(
        &mut self,
        arg: EmailCoroutineArg<'_>,
    ) -> EmailCoroutineState<Self::Yield, Self::Return> {
        #[allow(irrefutable_let_patterns)]
        let EmailCoroutineArg::Fs { batch } = arg else {
            return EmailCoroutineState::Complete(Err(MaildirMessageAddError::InvalidArg));
        };

        let inner_arg = match batch {
            None => None,
            Some(FsBatch::Time { secs, nanos }) => Some(MaildirReply::Time { secs, nanos }),
            Some(FsBatch::Pid(p)) => Some(MaildirReply::Pid(p)),
            Some(FsBatch::Hostname(h)) => Some(MaildirReply::Hostname(h)),
            Some(FsBatch::FileCreate) => Some(MaildirReply::FileCreate),
            Some(FsBatch::Rename) => Some(MaildirReply::Rename),
            Some(_) => {
                return EmailCoroutineState::Complete(Err(MaildirMessageAddError::UnexpectedBatch));
            }
        };

        match self.inner.resume(inner_arg) {
            MaildirCoroutineState::Complete(Ok(ok)) => EmailCoroutineState::Complete(Ok(ok.id)),
            MaildirCoroutineState::Yielded(MaildirYield::WantsTime) => {
                EmailCoroutineState::Yielded(FsStep::WantsTime)
            }
            MaildirCoroutineState::Yielded(MaildirYield::WantsPid) => {
                EmailCoroutineState::Yielded(FsStep::WantsPid)
            }
            MaildirCoroutineState::Yielded(MaildirYield::WantsHostname) => {
                EmailCoroutineState::Yielded(FsStep::WantsHostname)
            }
            MaildirCoroutineState::Yielded(MaildirYield::WantsFileCreate(files)) => {
                EmailCoroutineState::Yielded(FsStep::WantsFileCreate(files_out(files)))
            }
            MaildirCoroutineState::Yielded(MaildirYield::WantsRename(pairs)) => {
                EmailCoroutineState::Yielded(FsStep::WantsRename(pairs_out(pairs)))
            }
            MaildirCoroutineState::Complete(Err(err)) => {
                EmailCoroutineState::Complete(Err(err.into()))
            }
            other => {
                let _ = other;
                unreachable!("MaildirMessageStore never yields this state");
            }
        }
    }
}
