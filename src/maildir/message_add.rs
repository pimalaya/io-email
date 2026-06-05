//! Maildir message-add coroutine wrapping
//! [`io_maildir::entry::store::MaildirEntryStore`]: writes raw bytes
//! to tmp/, then renames into cur/ with info-section letters.
//!
//! Returns the Maildir filename minus the `:2,FLAGS` suffix.
//!
//! # Example
//!
//! ```rust,ignore
//! use io_email::maildir::message_add::MaildirMessageAdd;
//!
//! let id = client.run(MaildirMessageAdd::new(&client.store, "INBOX", &flags, raw)?)?;
//! ```

use alloc::{string::String, vec::Vec};

use io_maildir::{
    coroutine::*,
    entry::store::{MaildirEntryStore as InnerStore, MaildirEntryStoreError as InnerErr},
    maildir::types::{Maildir, MaildirSubdir},
    store::MaildirStore,
};
use log::trace;
use thiserror::Error;

use crate::{
    flag::Flag,
    maildir::convert::{InvalidMailboxName, flags_to_maildir, mailbox_path},
};

/// Errors produced by [`MaildirMessageAdd`].
#[derive(Debug, Error)]
pub enum MaildirMessageAddError {
    #[error(transparent)]
    Store(#[from] InnerErr),
    #[error(transparent)]
    InvalidMailbox(#[from] InvalidMailboxName),
}

/// I/O-free coroutine appending a raw message to a Maildir cur/.
pub struct MaildirMessageAdd {
    inner: InnerStore,
}

impl MaildirMessageAdd {
    pub fn new(
        store: &MaildirStore,
        mailbox: &str,
        flags: &[Flag],
        bytes: Vec<u8>,
    ) -> Result<Self, MaildirMessageAddError> {
        trace!("prepare Maildir message add");
        let path = mailbox_path(mailbox)?;
        let maildir = Maildir::from_path(store.resolve(&path));
        let md_flags = flags_to_maildir(flags);
        Ok(Self {
            inner: InnerStore::new(maildir, MaildirSubdir::Cur, md_flags, bytes),
        })
    }
}

impl MaildirCoroutine for MaildirMessageAdd {
    type Yield = MaildirYield;
    type Return = Result<String, MaildirMessageAddError>;

    fn resume(
        &mut self,
        arg: Option<MaildirReply>,
    ) -> MaildirCoroutineState<Self::Yield, Self::Return> {
        match self.inner.resume(arg) {
            MaildirCoroutineState::Yielded(y) => MaildirCoroutineState::Yielded(y),
            MaildirCoroutineState::Complete(Ok(ok)) => MaildirCoroutineState::Complete(Ok(ok.id)),
            MaildirCoroutineState::Complete(Err(err)) => {
                MaildirCoroutineState::Complete(Err(err.into()))
            }
        }
    }
}
