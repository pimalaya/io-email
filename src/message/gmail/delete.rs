//! Gmail message-delete coroutine wrapping `users.messages.delete`;
//! a permanent delete (not a move to TRASH).

use io_gmail::{
    coroutine::{GmailCoroutine, GmailCoroutineState, GmailYield},
    v1::rest::messages::delete::GmailMessageDelete as InnerDelete,
    v1::send::GmailSendError,
};
use io_http::rfc6750::bearer::HttpAuthBearer;
use log::trace;
use thiserror::Error;

/// Errors produced by [`GmailMessageDelete`].
#[derive(Debug, Error)]
pub enum GmailMessageDeleteError {
    #[error(transparent)]
    Send(#[from] GmailSendError),
}

/// I/O-free coroutine permanently deleting a Gmail message by id.
pub struct GmailMessageDelete {
    inner: InnerDelete,
}

impl GmailMessageDelete {
    /// `mailbox` is unused; kept for shared-API symmetry.
    pub fn new(
        auth: &HttpAuthBearer,
        user_id: &str,
        _mailbox: &str,
        id: &str,
    ) -> Result<Self, GmailMessageDeleteError> {
        trace!("prepare Gmail message delete");
        Ok(Self {
            inner: InnerDelete::new(auth, user_id, id)?,
        })
    }
}

impl GmailCoroutine for GmailMessageDelete {
    type Yield = GmailYield;
    type Return = Result<(), GmailMessageDeleteError>;

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
