//! IMAP mailbox-create coroutine wrapping CREATE (RFC 3501 §6.3.3).
//!
//! # Example
//!
//! ```rust,ignore
//! use io_email::mailbox::imap::create::ImapMailboxCreate;
//!
//! client.run(ImapMailboxCreate::new("Archive")?)?;
//! ```

use alloc::string::String;

use io_imap::{
    codec::fragmentizer::Fragmentizer,
    coroutine::{ImapCoroutine, ImapCoroutineState, ImapYield},
    rfc3501::create::{
        ImapMailboxCreate as InnerImapMailboxCreate,
        ImapMailboxCreateError as InnerImapMailboxCreateError,
    },
};
use log::trace;
use thiserror::Error;

use crate::imap::convert::{InvalidMailboxName, parse_mailbox};

/// Errors produced by [`ImapMailboxCreate`].
#[derive(Debug, Error)]
pub enum ImapMailboxCreateError {
    #[error(transparent)]
    Create(#[from] InnerImapMailboxCreateError),
    #[error("invalid IMAP mailbox `{0}`")]
    InvalidMailbox(String),
}

impl From<InvalidMailboxName> for ImapMailboxCreateError {
    fn from(err: InvalidMailboxName) -> Self {
        Self::InvalidMailbox(err.0)
    }
}

/// I/O-free coroutine creating an IMAP mailbox.
pub struct ImapMailboxCreate {
    inner: InnerImapMailboxCreate,
}

impl ImapMailboxCreate {
    pub fn new(name: &str) -> Result<Self, ImapMailboxCreateError> {
        trace!("prepare IMAP mailbox create");
        let mbox = parse_mailbox(name)?;
        Ok(Self {
            inner: InnerImapMailboxCreate::new(mbox),
        })
    }
}

impl ImapCoroutine for ImapMailboxCreate {
    type Yield = ImapYield;
    type Return = Result<(), ImapMailboxCreateError>;

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
