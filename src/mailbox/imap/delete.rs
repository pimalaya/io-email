//! IMAP mailbox-delete coroutine wrapping DELETE (RFC 3501 §6.3.4).
//!
//! # Example
//!
//! ```rust,ignore
//! use io_email::mailbox::imap::delete::ImapMailboxDelete;
//!
//! client.run(ImapMailboxDelete::new("Archive")?)?;
//! ```

use alloc::string::String;

use io_imap::{
    codec::fragmentizer::Fragmentizer,
    coroutine::{ImapCoroutine, ImapCoroutineState, ImapYield},
    rfc3501::delete::{
        ImapMailboxDelete as InnerImapMailboxDelete,
        ImapMailboxDeleteError as InnerImapMailboxDeleteError,
    },
};
use log::trace;
use thiserror::Error;

use crate::imap::convert::{InvalidMailboxName, parse_mailbox};

/// Errors produced by [`ImapMailboxDelete`].
#[derive(Debug, Error)]
pub enum ImapMailboxDeleteError {
    #[error(transparent)]
    Delete(#[from] InnerImapMailboxDeleteError),
    #[error("invalid IMAP mailbox `{0}`")]
    InvalidMailbox(String),
}

impl From<InvalidMailboxName> for ImapMailboxDeleteError {
    fn from(err: InvalidMailboxName) -> Self {
        Self::InvalidMailbox(err.0)
    }
}

/// I/O-free coroutine deleting an IMAP mailbox.
pub struct ImapMailboxDelete {
    inner: InnerImapMailboxDelete,
}

impl ImapMailboxDelete {
    pub fn new(name: &str) -> Result<Self, ImapMailboxDeleteError> {
        trace!("prepare IMAP mailbox delete");
        let mbox = parse_mailbox(name)?;
        Ok(Self {
            inner: InnerImapMailboxDelete::new(mbox),
        })
    }
}

impl ImapCoroutine for ImapMailboxDelete {
    type Yield = ImapYield;
    type Return = Result<(), ImapMailboxDeleteError>;

    fn resume(
        &mut self,
        fragmentizer: &mut Fragmentizer,
        bytes: Option<&[u8]>,
    ) -> ImapCoroutineState<Self::Yield, Self::Return> {
        match self.inner.resume(fragmentizer, bytes) {
            ImapCoroutineState::Yielded(y) => ImapCoroutineState::Yielded(y),
            ImapCoroutineState::Complete(r) => ImapCoroutineState::Complete(r.map_err(Into::into)),
        }
    }
}
