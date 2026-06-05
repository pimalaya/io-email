//! JMAP message-get coroutine: Email/get(blobId) then Blob/download
//! to return raw RFC 5322 bytes.
//!
//! Redirects (RFC 8620 §6.2) surface as an error rather than being
//! followed.
//!
//! # Example
//!
//! ```rust,ignore
//! use io_email::jmap::message_get::JmapMessageGet;
//!
//! let raw = client.run(JmapMessageGet::new(&session, &auth, "_", "email-id")?)?;
//! ```

use alloc::{
    string::{String, ToString},
    vec,
    vec::Vec,
};
use core::mem;

use io_jmap::{
    coroutine::{JmapCoroutine, JmapCoroutineState, JmapYield},
    rfc8620::{
        JmapSession,
        blob_download::{JmapBlobDownload, JmapBlobDownloadError, JmapBlobDownloadOutput},
        coroutine::JmapRedirectYield,
    },
    rfc8621::email::{
        JmapEmailProperty,
        get::{JmapEmailGet as InnerGet, JmapEmailGetError as InnerErr, JmapEmailGetOptions},
    },
};
use log::trace;
use secrecy::SecretString;
use thiserror::Error;
use url::Url;

use crate::jmap::convert::account_id_of;

/// Errors produced by [`JmapMessageGet`].
#[derive(Debug, Error)]
pub enum JmapMessageGetError {
    #[error(transparent)]
    EmailGet(#[from] InnerErr),
    #[error(transparent)]
    BlobDownload(#[from] JmapBlobDownloadError),
    #[error("Email/get returned no email for the requested id")]
    EmailNotFound,
    #[error("Email/get response did not include a blobId")]
    MissingBlobId,
    #[error("resolved JMAP download URL is invalid: {0}")]
    InvalidDownloadUrl(String),
    #[error("JMAP blob download was redirected; not yet supported")]
    UnsupportedRedirect,
    #[error("coroutine was resumed after completion")]
    ResumedAfterDone,
}

/// I/O-free coroutine fetching the raw RFC 5322 bytes of a JMAP email.
pub struct JmapMessageGet {
    state: State,
    http_auth: SecretString,
    download_url_template: String,
    account_id: String,
}

impl JmapMessageGet {
    pub fn new(
        session: &JmapSession,
        http_auth: &SecretString,
        _mailbox: &str,
        id: &str,
    ) -> Result<Self, JmapMessageGetError> {
        trace!("prepare JMAP message get");
        let opts = JmapEmailGetOptions {
            properties: Some(vec![JmapEmailProperty::BlobId]),
            ..Default::default()
        };
        let get = InnerGet::new(session, http_auth, vec![id.to_string()], opts)?;
        Ok(Self {
            state: State::GettingEmail(get),
            http_auth: http_auth.clone(),
            download_url_template: session.download_url.clone(),
            account_id: account_id_of(session),
        })
    }
}

enum State {
    GettingEmail(InnerGet),
    Downloading(JmapBlobDownload),
    Done,
}

impl JmapCoroutine for JmapMessageGet {
    type Yield = JmapYield;
    type Return = Result<Vec<u8>, JmapMessageGetError>;

    fn resume(&mut self, bytes: Option<&[u8]>) -> JmapCoroutineState<Self::Yield, Self::Return> {
        match mem::replace(&mut self.state, State::Done) {
            State::GettingEmail(mut get) => match get.resume(bytes) {
                JmapCoroutineState::Yielded(y) => {
                    self.state = State::GettingEmail(get);
                    JmapCoroutineState::Yielded(y)
                }
                JmapCoroutineState::Complete(Ok(ok)) => {
                    let Some(email) = ok.emails.into_iter().next() else {
                        return JmapCoroutineState::Complete(Err(
                            JmapMessageGetError::EmailNotFound,
                        ));
                    };
                    let Some(blob_id) = email.blob_id else {
                        return JmapCoroutineState::Complete(Err(
                            JmapMessageGetError::MissingBlobId,
                        ));
                    };
                    let url_str = resolve_download_url(
                        &self.download_url_template,
                        &self.account_id,
                        &blob_id,
                    );
                    let Ok(url) = Url::parse(&url_str) else {
                        return JmapCoroutineState::Complete(Err(
                            JmapMessageGetError::InvalidDownloadUrl(url_str),
                        ));
                    };
                    self.state = State::Downloading(JmapBlobDownload::new(&self.http_auth, &url));
                    JmapCoroutine::resume(self, None)
                }
                JmapCoroutineState::Complete(Err(err)) => {
                    JmapCoroutineState::Complete(Err(err.into()))
                }
            },
            State::Downloading(mut dl) => match dl.resume(bytes) {
                JmapCoroutineState::Yielded(JmapRedirectYield::WantsRead) => {
                    self.state = State::Downloading(dl);
                    JmapCoroutineState::Yielded(JmapYield::WantsRead)
                }
                JmapCoroutineState::Yielded(JmapRedirectYield::WantsWrite(out)) => {
                    self.state = State::Downloading(dl);
                    JmapCoroutineState::Yielded(JmapYield::WantsWrite(out))
                }
                JmapCoroutineState::Yielded(JmapRedirectYield::WantsRedirect { .. }) => {
                    JmapCoroutineState::Complete(Err(JmapMessageGetError::UnsupportedRedirect))
                }
                JmapCoroutineState::Complete(Ok(JmapBlobDownloadOutput { data, .. })) => {
                    JmapCoroutineState::Complete(Ok(data))
                }
                JmapCoroutineState::Complete(Err(err)) => {
                    JmapCoroutineState::Complete(Err(err.into()))
                }
            },
            State::Done => JmapCoroutineState::Complete(Err(JmapMessageGetError::ResumedAfterDone)),
        }
    }
}

/// Substitutes `{accountId, blobId, type, name}` in the download URL
/// template.
fn resolve_download_url(template: &str, account_id: &str, blob_id: &str) -> String {
    template
        .replace("{accountId}", account_id)
        .replace("{blobId}", blob_id)
        .replace("{type}", "message%2Frfc822")
        .replace("{name}", "message.eml")
}
