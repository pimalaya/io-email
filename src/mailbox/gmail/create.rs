//! Gmail mailbox-create coroutine wrapping `users.labels.create`.

use io_gmail::{
    coroutine::{GmailCoroutine, GmailCoroutineState, GmailYield},
    v1::rest::labels::{GmailLabel, create::GmailLabelCreate},
    v1::send::GmailSendError,
};
use io_http::rfc6750::bearer::HttpAuthBearer;
use log::trace;
use thiserror::Error;

/// Errors produced by [`GmailMailboxCreate`].
#[derive(Debug, Error)]
pub enum GmailMailboxCreateError {
    #[error(transparent)]
    Send(#[from] GmailSendError),
}

/// I/O-free coroutine creating a Gmail label named `name`.
pub struct GmailMailboxCreate {
    inner: GmailLabelCreate,
}

impl GmailMailboxCreate {
    pub fn new(
        auth: &HttpAuthBearer,
        user_id: &str,
        name: &str,
    ) -> Result<Self, GmailMailboxCreateError> {
        trace!("prepare Gmail mailbox create");
        let label = GmailLabel {
            name: name.into(),
            ..Default::default()
        };
        Ok(Self {
            inner: GmailLabelCreate::new(auth, user_id, &label)?,
        })
    }
}

impl GmailCoroutine for GmailMailboxCreate {
    type Yield = GmailYield;
    type Return = Result<(), GmailMailboxCreateError>;

    fn resume(&mut self, bytes: Option<&[u8]>) -> GmailCoroutineState<Self::Yield, Self::Return> {
        match self.inner.resume(bytes) {
            GmailCoroutineState::Yielded(y) => GmailCoroutineState::Yielded(y),
            GmailCoroutineState::Complete(Ok(_)) => GmailCoroutineState::Complete(Ok(())),
            GmailCoroutineState::Complete(Err(err)) => {
                GmailCoroutineState::Complete(Err(err.into()))
            }
        }
    }
}
