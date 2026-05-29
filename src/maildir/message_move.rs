//! Maildir message-move coroutine: moves every id from one Maildir to
//! another by chaining
//! [`io_maildir::coroutines::message_move::MaildirMessageMove`] per id.
//! Target subdir reuses the source subdir.

use alloc::{collections::VecDeque, string::String};
use std::path::PathBuf;

use io_maildir::{
    coroutine::{MaildirCoroutine, MaildirCoroutineState, MaildirReply, MaildirYield},
    coroutines::message_move::{
        MaildirMessageMove as InnerMove, MaildirMessageMoveError as InnerErr,
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

/// Errors produced by [`MaildirMessageMove`].
#[derive(Debug, Error)]
pub enum MaildirMessageMoveError {
    #[error(transparent)]
    Move(#[from] InnerErr),
    #[error(transparent)]
    InvalidMailbox(#[from] InvalidMailboxName),
    #[error("coroutine was resumed with the wrong EmailCoroutineArg variant")]
    InvalidArg,
    #[error("coroutine was resumed with an FsBatch variant it did not request")]
    UnexpectedBatch,
}

/// I/O-free coroutine moving every id from `from` to `to`.
pub struct MaildirMessageMove {
    source: Maildir,
    target: Maildir,
    pending: VecDeque<String>,
    current: Option<InnerMove>,
}

impl MaildirMessageMove {
    pub fn new(
        root: impl Into<PathBuf>,
        maildir_plus: bool,
        from: &str,
        to: &str,
        ids: &[&str],
    ) -> Result<Self, MaildirMessageMoveError> {
        trace!("prepare Maildir message move");
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

impl EmailCoroutine for MaildirMessageMove {
    type Yield = FsStep;
    type Return = Result<(), MaildirMessageMoveError>;

    const BACKEND: EmailBackend = EmailBackend::Maildir;

    fn resume(
        &mut self,
        arg: EmailCoroutineArg<'_>,
    ) -> EmailCoroutineState<Self::Yield, Self::Return> {
        #[allow(irrefutable_let_patterns)]
        let EmailCoroutineArg::Fs { batch } = arg else {
            return EmailCoroutineState::Complete(Err(MaildirMessageMoveError::InvalidArg));
        };

        let mut batch = batch;
        loop {
            if self.current.is_none() {
                let Some(id) = self.pending.pop_front() else {
                    return EmailCoroutineState::Complete(Ok(()));
                };
                self.current = Some(InnerMove::new(
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
                Some(FsBatch::Rename) => Some(MaildirReply::Rename),
                Some(_) => {
                    return EmailCoroutineState::Complete(Err(
                        MaildirMessageMoveError::UnexpectedBatch,
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
                MaildirCoroutineState::Yielded(MaildirYield::WantsRename(pairs)) => {
                    return EmailCoroutineState::Yielded(FsStep::WantsRename(pairs_out(pairs)));
                }
                MaildirCoroutineState::Complete(Err(err)) => {
                    return EmailCoroutineState::Complete(Err(err.into()));
                }
                other => {
                    let _ = other;
                    unreachable!("MaildirMessageMove never yields this state");
                }
            }
        }
    }
}
