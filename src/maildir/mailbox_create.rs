//! Maildir mailbox-create coroutine.
//!
//! Wraps [`io_maildir::coroutines::maildir_create::MaildirCreate`]:
//! creates the `<mailbox>/`, `<mailbox>/cur/`, `<mailbox>/new/` and
//! `<mailbox>/tmp/` directories. The on-disk name is computed from
//! the shared `(root, maildir_plus)` pair via [`resolve_mailbox`].

use std::path::PathBuf;

use io_maildir::{
    coroutine::*,
    coroutines::maildir_create::{
        MaildirCreate as InnerMaildirCreate, MaildirCreateError as InnerMaildirCreateError,
    },
    path::MaildirPath,
};
use log::trace;
use thiserror::Error;

use crate::maildir::convert::{InvalidMailboxName, resolve_mailbox};

/// Errors produced by [`MaildirMailboxCreate`].
#[derive(Debug, Error)]
pub enum MaildirMailboxCreateError {
    #[error(transparent)]
    Create(#[from] InnerMaildirCreateError),
    #[error(transparent)]
    InvalidMailbox(#[from] InvalidMailboxName),
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

impl MaildirCoroutine for MaildirMailboxCreate {
    type Yield = MaildirYield;
    type Return = Result<(), MaildirMailboxCreateError>;

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
