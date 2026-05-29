//! IMAP mailbox-delete coroutine: `DELETE <mailbox>` (RFC 3501 §6.3.4).

use alloc::string::String;

use io_imap::{
    coroutine::{ImapCoroutine, ImapCoroutineState, ImapYield},
    rfc3501::delete::{
        ImapMailboxDelete as InnerImapMailboxDelete,
        ImapMailboxDeleteError as InnerImapMailboxDeleteError,
    },
};
use log::trace;
use thiserror::Error;

use crate::{
    coroutine::{EmailBackend, EmailCoroutine, EmailCoroutineArg, EmailCoroutineState, ImapStep},
    imap::convert::{InvalidMailboxName, parse_mailbox},
};

/// Errors produced by [`ImapMailboxDelete`].
#[derive(Debug, Error)]
pub enum ImapMailboxDeleteError {
    #[error(transparent)]
    Delete(#[from] InnerImapMailboxDeleteError),
    #[error("invalid IMAP mailbox `{0}`")]
    InvalidMailbox(String),
    #[error("coroutine was resumed with the wrong EmailCoroutineArg variant")]
    InvalidArg,
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

impl EmailCoroutine for ImapMailboxDelete {
    type Yield = ImapStep;
    type Return = Result<(), ImapMailboxDeleteError>;

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
            return EmailCoroutineState::Complete(Err(ImapMailboxDeleteError::InvalidArg));
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
