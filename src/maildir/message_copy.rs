//! Maildir message-copy coroutine: copies every id from one Maildir to
//! another by chaining
//! [`io_maildir::coroutines::message_copy::MaildirMessageCopy`] per id.
//! Target subdir reuses the source subdir.

use alloc::{collections::VecDeque, string::String};
use std::path::PathBuf;

use io_maildir::{
    coroutine::{MaildirCoroutine, MaildirCoroutineState, MaildirReply, MaildirYield},
    coroutines::message_copy::{
        MaildirMessageCopy as InnerCopy, MaildirMessageCopyError as InnerErr,
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
        InvalidMailboxName, dirread_in, pairs_out, paths_out, probes_in, resolve_mailbox,
    },
};

/// Errors produced by [`MaildirMessageCopy`].
#[derive(Debug, Error)]
pub enum MaildirMessageCopyError {
    #[error(transparent)]
    Copy(#[from] InnerErr),
    #[error(transparent)]
    InvalidMailbox(#[from] InvalidMailboxName),
    #[error("coroutine was resumed with the wrong EmailCoroutineArg variant")]
    InvalidArg,
    #[error("coroutine was resumed with an FsBatch variant it did not request")]
    UnexpectedBatch,
}

/// I/O-free coroutine copying every id from `from` to `to`.
pub struct MaildirMessageCopy {
    source: Maildir,
    target: Maildir,
    pending: VecDeque<String>,
    current: Option<InnerCopy>,
}

impl MaildirMessageCopy {
    pub fn new(
        root: impl Into<PathBuf>,
        maildir_plus: bool,
        from: &str,
        to: &str,
        ids: &[&str],
    ) -> Result<Self, MaildirMessageCopyError> {
        trace!("prepare Maildir message copy");
        let root = root.into();
        let source = Maildir::from_path(resolve_mailbox(&root, maildir_plus, from)?);
        let target = Maildir::from_path(resolve_mailbox(&root, maildir_plus, to)?);
        Ok(Self {
            source,
            target,
            pending: ids.iter().map(|s| (*s).into()).collect(),
            current: None,
        })
    }
}

impl EmailCoroutine for MaildirMessageCopy {
    type Yield = FsStep;
    type Return = Result<(), MaildirMessageCopyError>;

    const BACKEND: EmailBackend = EmailBackend::Maildir;

    fn resume(
        &mut self,
        arg: EmailCoroutineArg<'_>,
    ) -> EmailCoroutineState<Self::Yield, Self::Return> {
        #[allow(irrefutable_let_patterns)]
        let EmailCoroutineArg::Fs { batch } = arg else {
            return EmailCoroutineState::Complete(Err(MaildirMessageCopyError::InvalidArg));
        };

        let mut batch = batch;
        loop {
            if self.current.is_none() {
                let Some(id) = self.pending.pop_front() else {
                    return EmailCoroutineState::Complete(Ok(()));
                };
                self.current = Some(InnerCopy::new(
                    id,
                    self.source.clone(),
                    self.target.clone(),
                    None,
                ));
            }
            let inner = self.current.as_mut().unwrap();
            let inner_arg = match batch.take() {
                None => None,
                Some(FsBatch::FileExists(probes)) => {
                    Some(MaildirReply::FileExists(probes_in(probes)))
                }
                Some(FsBatch::DirRead(entries)) => Some(MaildirReply::DirRead(dirread_in(entries))),
                Some(FsBatch::Copy) => Some(MaildirReply::Copy),
                Some(_) => {
                    return EmailCoroutineState::Complete(Err(
                        MaildirMessageCopyError::UnexpectedBatch,
                    ));
                }
            };
            match inner.resume(inner_arg) {
                MaildirCoroutineState::Complete(Ok(())) => {
                    self.current = None;
                }
                MaildirCoroutineState::Yielded(MaildirYield::WantsFileExists(paths)) => {
                    return EmailCoroutineState::Yielded(FsStep::WantsFileExists(paths_out(paths)));
                }
                MaildirCoroutineState::Yielded(MaildirYield::WantsDirRead(paths)) => {
                    return EmailCoroutineState::Yielded(FsStep::WantsDirRead(paths_out(paths)));
                }
                MaildirCoroutineState::Yielded(MaildirYield::WantsCopy(pairs)) => {
                    return EmailCoroutineState::Yielded(FsStep::WantsCopy(pairs_out(pairs)));
                }
                MaildirCoroutineState::Complete(Err(err)) => {
                    return EmailCoroutineState::Complete(Err(err.into()));
                }
                other => {
                    let _ = other;
                    unreachable!("MaildirMessageCopy never yields this state");
                }
            }
        }
    }
}
