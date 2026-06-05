//! Maildir mailbox-delete coroutine wrapping
//! [`io_maildir::maildir::delete::MaildirDelete`]: recursively removes
//! the mailbox directory under the [`MaildirStore`].
//!
//! # Example
//!
//! ```rust,ignore
//! use io_email::maildir::mailbox_delete::MaildirMailboxDelete;
//!
//! client.run(MaildirMailboxDelete::new(&client.store, "Archive")?)?;
//! ```

use io_maildir::{
    coroutine::*,
    maildir::delete::{
        MaildirDelete as InnerMaildirDelete, MaildirDeleteError as InnerMaildirDeleteError,
    },
    store::MaildirStore,
};
use log::trace;
use thiserror::Error;

use crate::maildir::convert::{InvalidMailboxName, mailbox_path};

/// Errors produced by [`MaildirMailboxDelete`].
#[derive(Debug, Error)]
pub enum MaildirMailboxDeleteError {
    #[error(transparent)]
    Delete(#[from] InnerMaildirDeleteError),
    #[error(transparent)]
    InvalidMailbox(#[from] InvalidMailboxName),
}

/// I/O-free coroutine deleting a Maildir mailbox under the store.
pub struct MaildirMailboxDelete {
    inner: InnerMaildirDelete,
}

impl MaildirMailboxDelete {
    pub fn new(store: &MaildirStore, name: &str) -> Result<Self, MaildirMailboxDeleteError> {
        trace!("prepare Maildir mailbox delete");
        let path = mailbox_path(name)?;
        Ok(Self {
            inner: InnerMaildirDelete::new(store, path),
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
