//! Gmail mailbox-delete coroutine wrapping `users.labels.delete`;
//! deleting a label removes it from every message that carried it.

use io_gmail::{
    coroutine::{GmailCoroutine, GmailCoroutineState, GmailYield},
    v1::rest::labels::delete::GmailLabelDelete,
    v1::send::GmailSendError,
};
use io_http::rfc6750::bearer::HttpAuthBearer;
use log::trace;
use thiserror::Error;

/// Errors produced by [`GmailMailboxDelete`].
#[derive(Debug, Error)]
pub enum GmailMailboxDeleteError {
    #[error(transparent)]
    Send(#[from] GmailSendError),
}

/// I/O-free coroutine deleting a Gmail label by id.
pub struct GmailMailboxDelete {
    inner: GmailLabelDelete,
}

impl GmailMailboxDelete {
    pub fn new(
        auth: &HttpAuthBearer,
        user_id: &str,
        id: &str,
    ) -> Result<Self, GmailMailboxDeleteError> {
        trace!("prepare Gmail mailbox delete");
        Ok(Self {
            inner: GmailLabelDelete::new(auth, user_id, id)?,
        })
    }
}

impl GmailCoroutine for GmailMailboxDelete {
    type Yield = GmailYield;
    type Return = Result<(), GmailMailboxDeleteError>;

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
