//! Maildir message-delete coroutine: adds the Trashed (T) info-section
//! letter; expunge is the caller's responsibility.
//!
//! Wraps [`io_maildir::flag::add::MaildirFlagsAdd`] with
//! [`MaildirFlag::Trashed`].
//!
//! # Example
//!
//! ```rust,ignore
//! use io_email::message::maildir::delete::MaildirMessageDelete;
//!
//! client.run(MaildirMessageDelete::new(&client.store, "INBOX", "msg-id")?)?;
//! ```
//!
//! [`MaildirFlag::Trashed`]: io_maildir::flag::types::MaildirFlag::Trashed

use core::iter::once;

use io_maildir::{
    coroutine::*,
    flag::{
        add::{MaildirFlagsAdd as InnerAdd, MaildirFlagsAddError as InnerErr},
        types::MaildirFlag,
    },
    maildir::types::Maildir,
    store::MaildirStore,
};
use log::trace;
use thiserror::Error;

use crate::maildir::convert::{InvalidMailboxName, mailbox_path};

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
        store: &MaildirStore,
        mailbox: &str,
        id: &str,
    ) -> Result<Self, MaildirMessageDeleteError> {
        trace!("prepare Maildir message delete (Trashed flag)");
        let path = mailbox_path(mailbox)?;
        let maildir = Maildir::from_path(store.resolve(&path));
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
