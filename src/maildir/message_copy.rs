//! Maildir message-copy coroutine: copies every id from one Maildir to
//! another by chaining
//! [`io_maildir::coroutines::message_copy::MaildirMessageCopy`] per id.
//! Target subdir reuses the source subdir.

use alloc::{collections::VecDeque, string::String};
use std::path::PathBuf;

use io_maildir::{
    coroutine::*,
    coroutines::message_copy::{
        MaildirMessageCopy as InnerCopy, MaildirMessageCopyError as InnerErr,
    },
    maildir::Maildir,
};
use log::trace;
use thiserror::Error;

use crate::maildir::convert::{InvalidMailboxName, resolve_mailbox};

/// Errors produced by [`MaildirMessageCopy`].
#[derive(Debug, Error)]
pub enum MaildirMessageCopyError {
    #[error(transparent)]
    Copy(#[from] InnerErr),
    #[error(transparent)]
    InvalidMailbox(#[from] InvalidMailboxName),
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

impl MaildirCoroutine for MaildirMessageCopy {
    type Yield = MaildirYield;
    type Return = Result<(), MaildirMessageCopyError>;

    fn resume(
        &mut self,
        arg: Option<MaildirReply>,
    ) -> MaildirCoroutineState<Self::Yield, Self::Return> {
        let mut arg = arg;
        loop {
            if self.current.is_none() {
                let Some(id) = self.pending.pop_front() else {
                    return MaildirCoroutineState::Complete(Ok(()));
                };
                self.current = Some(InnerCopy::new(
                    id,
                    self.source.clone(),
                    self.target.clone(),
                    None,
                ));
            }

            let inner = self.current.as_mut().unwrap();
            match inner.resume(arg.take()) {
                MaildirCoroutineState::Complete(Ok(())) => {
                    self.current = None;
                }
                MaildirCoroutineState::Yielded(y) => return MaildirCoroutineState::Yielded(y),
                MaildirCoroutineState::Complete(Err(err)) => {
                    return MaildirCoroutineState::Complete(Err(err.into()));
                }
            }
        }
    }
}
