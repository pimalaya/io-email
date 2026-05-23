//! JMAP message get (`Email/get` + `Blob/download`), wrapping a private
//! orchestrator. Resolves the message blob id, then downloads its raw
//! RFC 5322 bytes.

use core::mem;

use alloc::{
    string::{String, ToString},
    vec,
    vec::Vec,
};

use io_jmap::{
    rfc8620::{
        blob_download::{JmapBlobDownload, JmapBlobDownloadError, JmapBlobDownloadResult},
        session::JmapSession,
    },
    rfc8621::{
        capabilities,
        email_get::{JmapEmailGet, JmapEmailGetError, JmapEmailGetResult},
    },
};
use log::trace;
use secrecy::SecretString;
use thiserror::Error;
use url::Url;

/// Errors produced while orchestrating Email/get + Blob/download for
/// JMAP raw message retrieval.
#[derive(Debug, Error)]
pub enum JmapMessageGetError {
    #[error(transparent)]
    EmailGet(#[from] JmapEmailGetError),
    #[error(transparent)]
    BlobDownload(#[from] JmapBlobDownloadError),
    #[error("Email/get returned no email for the requested id")]
    EmailNotFound,
    #[error("Email/get response did not include a blobId")]
    MissingBlobId,
    #[error("Resolved JMAP download URL is invalid: {0}")]
    InvalidDownloadUrl(String),
    #[error("JMAP blob download was redirected; not yet supported")]
    UnsupportedRedirect,
    #[error("JMAP message get was resumed after completion")]
    AlreadyDone,
}

/// Result returned by [`JmapMessageGet::resume`].
#[derive(Debug)]
pub enum JmapMessageGetResult {
    Ok(Vec<u8>),
    WantsRead,
    WantsWrite(Vec<u8>),
    Err(JmapMessageGetError),
}

enum State {
    GettingEmail {
        get: JmapEmailGet,
        download_url_template: String,
        account_id: String,
        http_auth: SecretString,
    },
    Downloading(JmapBlobDownload),
    Done,
}

/// I/O-free coroutine wrapping `Email/get` + `Blob/download`. Returns
/// raw RFC 5322 bytes.
pub struct JmapMessageGet {
    state: State,
}

impl JmapMessageGet {
    pub fn new(
        session: &JmapSession,
        http_auth: &SecretString,
        email_id: impl ToString,
    ) -> Result<Self, JmapMessageGetError> {
        trace!("prepare JMAP message get");

        let get = JmapEmailGet::new(
            session,
            http_auth,
            vec![email_id.to_string()],
            Some(vec![io_jmap::rfc8621::email::EmailProperty::BlobId]),
            false,
            false,
            0,
        )?;

        let account_id = session
            .primary_accounts
            .get(capabilities::MAIL)
            .cloned()
            .unwrap_or_default();

        Ok(Self {
            state: State::GettingEmail {
                get,
                download_url_template: session.download_url.clone(),
                account_id,
                http_auth: http_auth.clone(),
            },
        })
    }

    pub fn resume(&mut self, mut arg: Option<&[u8]>) -> JmapMessageGetResult {
        loop {
            match mem::replace(&mut self.state, State::Done) {
                State::GettingEmail {
                    mut get,
                    download_url_template,
                    account_id,
                    http_auth,
                } => match get.resume(arg.take()) {
                    JmapEmailGetResult::WantsRead => {
                        self.state = State::GettingEmail {
                            get,
                            download_url_template,
                            account_id,
                            http_auth,
                        };
                        return JmapMessageGetResult::WantsRead;
                    }
                    JmapEmailGetResult::WantsWrite(bytes) => {
                        self.state = State::GettingEmail {
                            get,
                            download_url_template,
                            account_id,
                            http_auth,
                        };
                        return JmapMessageGetResult::WantsWrite(bytes);
                    }
                    JmapEmailGetResult::Err(err) => return JmapMessageGetResult::Err(err.into()),
                    JmapEmailGetResult::Ok { mut emails, .. } => {
                        let Some(email) = emails.pop() else {
                            return JmapMessageGetResult::Err(JmapMessageGetError::EmailNotFound);
                        };

                        let Some(blob_id) = email.blob_id else {
                            return JmapMessageGetResult::Err(JmapMessageGetError::MissingBlobId);
                        };

                        let url_str =
                            resolve_download_url(&download_url_template, &account_id, &blob_id);

                        let url = match Url::parse(&url_str) {
                            Ok(u) => u,
                            Err(_) => {
                                return JmapMessageGetResult::Err(
                                    JmapMessageGetError::InvalidDownloadUrl(url_str),
                                );
                            }
                        };

                        let download = JmapBlobDownload::new(&http_auth, &url);
                        self.state = State::Downloading(download);
                    }
                },
                State::Downloading(mut download) => match download.resume(arg.take()) {
                    JmapBlobDownloadResult::WantsRead => {
                        self.state = State::Downloading(download);
                        return JmapMessageGetResult::WantsRead;
                    }
                    JmapBlobDownloadResult::WantsWrite(bytes) => {
                        self.state = State::Downloading(download);
                        return JmapMessageGetResult::WantsWrite(bytes);
                    }
                    JmapBlobDownloadResult::WantsRedirect { .. } => {
                        return JmapMessageGetResult::Err(JmapMessageGetError::UnsupportedRedirect);
                    }
                    JmapBlobDownloadResult::Err(err) => {
                        return JmapMessageGetResult::Err(err.into());
                    }
                    JmapBlobDownloadResult::Ok { data, .. } => {
                        return JmapMessageGetResult::Ok(data);
                    }
                },
                State::Done => {
                    return JmapMessageGetResult::Err(JmapMessageGetError::AlreadyDone);
                }
            }
        }
    }
}

pub(crate) fn resolve_download_url(template: &str, account_id: &str, blob_id: &str) -> String {
    // Use the RFC-suggested defaults for an `Email/get` raw blob
    // download: MIME type `message/rfc822` (URL-encoded) and file name
    // `message.eml`. Some providers (e.g. Fastmail) rely on the type
    // hint to route the request to the correct download authority.
    template
        .replace("{accountId}", account_id)
        .replace("{blobId}", blob_id)
        .replace("{type}", "message%2Frfc822")
        .replace("{name}", "message.eml")
}
