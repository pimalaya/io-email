//! Gmail message-send coroutine wrapping `users.messages.send`; Gmail
//! both stores the message in Sent and delivers it.

use alloc::vec::Vec;

use io_gmail::{
    coroutine::{GmailCoroutine, GmailCoroutineState, GmailYield},
    v1::rest::messages::{GmailMessage, encode_raw, send::GmailMessageSend as InnerSend},
    v1::send::GmailSendError,
};
use io_http::rfc6750::bearer::HttpAuthBearer;
use log::trace;
use thiserror::Error;

/// Errors produced by [`GmailMessageSend`].
#[derive(Debug, Error)]
pub enum GmailMessageSendError {
    #[error(transparent)]
    Send(#[from] GmailSendError),
}

/// I/O-free coroutine sending a raw RFC 5322 message through Gmail.
pub struct GmailMessageSend {
    inner: InnerSend,
}

impl GmailMessageSend {
    pub fn new(
        auth: &HttpAuthBearer,
        user_id: &str,
        raw: Vec<u8>,
    ) -> Result<Self, GmailMessageSendError> {
        trace!("prepare Gmail message send");
        let message = GmailMessage {
            raw: Some(encode_raw(&raw)),
            ..Default::default()
        };
        Ok(Self {
            inner: InnerSend::new(auth, user_id, &message)?,
        })
    }
}

impl GmailCoroutine for GmailMessageSend {
    type Yield = GmailYield;
    type Return = Result<(), GmailMessageSendError>;

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
