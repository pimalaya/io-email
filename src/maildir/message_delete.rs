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
    coroutine::*,
    coroutines::flags_add::{MaildirFlagsAdd as InnerAdd, MaildirFlagsAddError as InnerErr},
    flag::MaildirFlag,
    maildir::Maildir,
};
use log::trace;
use thiserror::Error;

use crate::maildir::convert::{InvalidMailboxName, resolve_mailbox};

/// Errors produced by [`MaildirMessageDelete`].
#[derive(Debug, Error)]
pub enum MaildirMessageDeleteError {
    #[error(transparent)]
    Trash(#[from] InnerErr),
    #[error(transparent)]
    InvalidMailbox(#[from] InvalidMailboxName),
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

impl MaildirCoroutine for MaildirMessageDelete {
    type Yield = MaildirYield;
    type Return = Result<(), MaildirMessageDeleteError>;

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
