//! Maildir mailbox-delete coroutine.
//!
//! Wraps [`io_maildir::coroutines::maildir_delete::MaildirDelete`]:
//! recursively removes the on-disk directory for the named mailbox.

use std::path::PathBuf;

use io_maildir::{
    coroutine::*,
    coroutines::maildir_delete::{
        MaildirDelete as InnerMaildirDelete, MaildirDeleteError as InnerMaildirDeleteError,
    },
    path::MaildirPath,
};
use log::trace;
use thiserror::Error;

use crate::maildir::convert::{InvalidMailboxName, resolve_mailbox};

/// Errors produced by [`MaildirMailboxDelete`].
#[derive(Debug, Error)]
pub enum MaildirMailboxDeleteError {
    #[error(transparent)]
    Delete(#[from] InnerMaildirDeleteError),
    #[error(transparent)]
    InvalidMailbox(#[from] InvalidMailboxName),
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

impl MaildirCoroutine for MaildirMailboxDelete {
    type Yield = MaildirYield;
    type Return = Result<(), MaildirMailboxDeleteError>;

    fn resume(
        &mut self,
        arg: Option<MaildirReply>,
    ) -> MaildirCoroutineState<Self::Yield, Self::Return> {
        match self.inner.resume(arg) {
            MaildirCoroutineState::Yielded(y) => MaildirCoroutineState::Yielded(y),
            MaildirCoroutineState::Complete(r) => {
                MaildirCoroutineState::Complete(r.map_err(Into::into))
            }
        }
    }
}
