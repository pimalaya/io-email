//! Gmail message-get coroutine wrapping `users.messages.get`
//! (format=RAW) and base64url-decoding the raw RFC 5322 bytes.

use alloc::{
    string::{String, ToString},
    vec::Vec,
};

use io_gmail::{
    coroutine::{GmailCoroutine, GmailCoroutineState, GmailYield},
    v1::rest::messages::{GmailMessageFormat, decode_raw, get::GmailMessageGet as InnerGet},
    v1::send::GmailSendError,
};
use io_http::rfc6750::bearer::HttpAuthBearer;
use log::trace;
use thiserror::Error;

/// Errors produced by [`GmailMessageGet`].
#[derive(Debug, Error)]
pub enum GmailMessageGetError {
    #[error(transparent)]
    Send(#[from] GmailSendError),
    #[error("Gmail message has no raw payload")]
    MissingRaw,
    #[error("Gmail raw message could not be decoded: {0}")]
    Decode(String),
}

/// I/O-free coroutine fetching the raw RFC 5322 bytes of a Gmail message.
pub struct GmailMessageGet {
    inner: InnerGet,
}

impl GmailMessageGet {
    /// `mailbox` is unused; kept for shared-API symmetry.
    pub fn new(
        auth: &HttpAuthBearer,
        user_id: &str,
        _mailbox: &str,
        id: &str,
    ) -> Result<Self, GmailMessageGetError> {
        trace!("prepare Gmail message get");
        Ok(Self {
            inner: InnerGet::new(auth, user_id, id, GmailMessageFormat::Raw, &[])?,
        })
    }
}

impl GmailCoroutine for GmailMessageGet {
    type Yield = GmailYield;
    type Return = Result<Vec<u8>, GmailMessageGetError>;

    fn resume(&mut self, bytes: Option<&[u8]>) -> GmailCoroutineState<Self::Yield, Self::Return> {
        match self.inner.resume(bytes) {
            GmailCoroutineState::Yielded(y) => GmailCoroutineState::Yielded(y),
            GmailCoroutineState::Complete(Err(err)) => {
                GmailCoroutineState::Complete(Err(err.into()))
            }
            GmailCoroutineState::Complete(Ok(out)) => {
                let Some(raw) = out.response.raw else {
                    return GmailCoroutineState::Complete(Err(GmailMessageGetError::MissingRaw));
                };
                match decode_raw(&raw) {
                    Ok(bytes) => GmailCoroutineState::Complete(Ok(bytes)),
                    Err(err) => GmailCoroutineState::Complete(Err(GmailMessageGetError::Decode(
                        err.to_string(),
                    ))),
                }
            }
        }
    }
}
