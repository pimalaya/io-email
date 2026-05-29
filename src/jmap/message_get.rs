//! JMAP message-get coroutine.
//!
//! Two-stage state machine:
//! 1. `Email/get(properties: [blobId])` resolves the email id to a
//!    download blob id.
//! 2. `Blob/download` against `{accountId, blobId}` returns the raw
//!    RFC 5322 bytes. Redirects (RFC 8620 §6.2 allows servers to
//!    relocate the blob) are not followed: the coroutine surfaces
//!    them as an error, matching the dead-code behaviour.

use alloc::{
    string::{String, ToString},
    vec,
    vec::Vec,
};
use core::mem;

use io_jmap::{
    coroutine::{JmapCoroutine, JmapCoroutineState, JmapYield},
    rfc8620::{
        blob_download::{JmapBlobDownload, JmapBlobDownloadError, JmapBlobDownloadOutput},
        redirect::JmapRedirectYield,
        session::JmapSession,
    },
    rfc8621::{
        email::EmailProperty,
        email_get::{JmapEmailGet as InnerGet, JmapEmailGetError as InnerErr},
    },
};
use log::trace;
use secrecy::SecretString;
use thiserror::Error;
use url::Url;

use crate::{
    coroutine::{EmailBackend, EmailCoroutine, EmailCoroutineArg, EmailCoroutineState, JmapStep},
    jmap::convert::account_id_of,
};

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
    #[error("coroutine was resumed with the wrong EmailCoroutineArg variant")]
    InvalidArg,
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
        let get = InnerGet::new(
            session,
            http_auth,
            vec![id.to_string()],
            Some(vec![EmailProperty::BlobId]),
            false,
            false,
            0,
        )?;
        Ok(Self {
            state: State::GettingEmail(get),
            http_auth: http_auth.clone(),
            download_url_template: session.download_url.clone(),
            account_id: account_id_of(session),
        })
    }
}

impl EmailCoroutine for JmapMessageGet {
    type Yield = JmapStep;
    type Return = Result<Vec<u8>, JmapMessageGetError>;

    const BACKEND: EmailBackend = EmailBackend::Jmap;

    fn resume(
        &mut self,
        arg: EmailCoroutineArg<'_>,
    ) -> EmailCoroutineState<Self::Yield, Self::Return> {
        #[allow(irrefutable_let_patterns)]
        let EmailCoroutineArg::Jmap { bytes } = arg else {
            return EmailCoroutineState::Complete(Err(JmapMessageGetError::InvalidArg));
        };

        match mem::replace(&mut self.state, State::Done) {
            State::GettingEmail(mut get) => match get.resume(bytes) {
                JmapCoroutineState::Yielded(JmapYield::WantsRead) => {
                    self.state = State::GettingEmail(get);
                    EmailCoroutineState::Yielded(JmapStep::WantsRead)
                }
                JmapCoroutineState::Yielded(JmapYield::WantsWrite(out)) => {
                    self.state = State::GettingEmail(get);
                    EmailCoroutineState::Yielded(JmapStep::WantsWrite(out))
                }
                JmapCoroutineState::Complete(Ok(ok)) => {
                    let Some(email) = ok.emails.into_iter().next() else {
                        return EmailCoroutineState::Complete(Err(
                            JmapMessageGetError::EmailNotFound,
                        ));
                    };
                    let Some(blob_id) = email.blob_id else {
                        return EmailCoroutineState::Complete(Err(
                            JmapMessageGetError::MissingBlobId,
                        ));
                    };
                    let url_str = resolve_download_url(
                        &self.download_url_template,
                        &self.account_id,
                        &blob_id,
                    );
                    let Ok(url) = Url::parse(&url_str) else {
                        return EmailCoroutineState::Complete(Err(
                            JmapMessageGetError::InvalidDownloadUrl(url_str),
                        ));
                    };
                    self.state = State::Downloading(JmapBlobDownload::new(&self.http_auth, &url));
                    self.resume(EmailCoroutineArg::Jmap { bytes: None })
                }
                JmapCoroutineState::Complete(Err(err)) => {
                    EmailCoroutineState::Complete(Err(err.into()))
                }
            },
            State::Downloading(mut dl) => match dl.resume(bytes) {
                JmapCoroutineState::Yielded(JmapRedirectYield::WantsRead) => {
                    self.state = State::Downloading(dl);
                    EmailCoroutineState::Yielded(JmapStep::WantsRead)
                }
                JmapCoroutineState::Yielded(JmapRedirectYield::WantsWrite(out)) => {
                    self.state = State::Downloading(dl);
                    EmailCoroutineState::Yielded(JmapStep::WantsWrite(out))
                }
                JmapCoroutineState::Yielded(JmapRedirectYield::WantsRedirect { .. }) => {
                    EmailCoroutineState::Complete(Err(JmapMessageGetError::UnsupportedRedirect))
                }
                JmapCoroutineState::Complete(Ok(JmapBlobDownloadOutput { data, .. })) => {
                    EmailCoroutineState::Complete(Ok(data))
                }
                JmapCoroutineState::Complete(Err(err)) => {
                    EmailCoroutineState::Complete(Err(err.into()))
                }
            },
            State::Done => {
                EmailCoroutineState::Complete(Err(JmapMessageGetError::ResumedAfterDone))
            }
        }
    }
}

enum State {
    GettingEmail(InnerGet),
    Downloading(JmapBlobDownload),
    Done,
}

/// Resolves the RFC 8620 download URL template against the live
/// `{accountId, blobId, type, name}` substitutions.
fn resolve_download_url(template: &str, account_id: &str, blob_id: &str) -> String {
    template
        .replace("{accountId}", account_id)
        .replace("{blobId}", blob_id)
        .replace("{type}", "message%2Frfc822")
        .replace("{name}", "message.eml")
}
