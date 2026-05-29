//! IMAP mailbox-create coroutine: `CREATE <mailbox>` (RFC 3501 §6.3.3).

use alloc::string::String;

use io_imap::{
    coroutine::{ImapCoroutine, ImapCoroutineState, ImapYield},
    rfc3501::create::{
        ImapMailboxCreate as InnerImapMailboxCreate,
        ImapMailboxCreateError as InnerImapMailboxCreateError,
    },
};
use log::trace;
use thiserror::Error;

use crate::{
    coroutine::{EmailBackend, EmailCoroutine, EmailCoroutineArg, EmailCoroutineState, ImapStep},
    imap::convert::{InvalidMailboxName, parse_mailbox},
};

/// Errors produced by [`ImapMailboxCreate`].
#[derive(Debug, Error)]
pub enum ImapMailboxCreateError {
    #[error(transparent)]
    Create(#[from] InnerImapMailboxCreateError),
    #[error("invalid IMAP mailbox `{0}`")]
    InvalidMailbox(String),
    #[error("coroutine was resumed with the wrong EmailCoroutineArg variant")]
    InvalidArg,
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

impl EmailCoroutine for ImapMailboxCreate {
    type Yield = ImapStep;
    type Return = Result<(), ImapMailboxCreateError>;

    const BACKEND: EmailBackend = EmailBackend::Imap;

    fn resume(
        &mut self,
        arg: EmailCoroutineArg<'_>,
    ) -> EmailCoroutineState<Self::Yield, Self::Return> {
        #[allow(irrefutable_let_patterns)]
        let EmailCoroutineArg::Imap {
            fragmentizer,
            bytes,
        } = arg
        else {
            return EmailCoroutineState::Complete(Err(ImapMailboxCreateError::InvalidArg));
        };

        match self.inner.resume(fragmentizer, bytes) {
            ImapCoroutineState::Yielded(ImapYield::WantsRead) => {
                EmailCoroutineState::Yielded(ImapStep::WantsRead)
            }
            ImapCoroutineState::Yielded(ImapYield::WantsWrite(out)) => {
                EmailCoroutineState::Yielded(ImapStep::WantsWrite(out))
            }
            ImapCoroutineState::Complete(Ok(())) => EmailCoroutineState::Complete(Ok(())),
            ImapCoroutineState::Complete(Err(err)) => {
                EmailCoroutineState::Complete(Err(err.into()))
            }
        }
    }
}
