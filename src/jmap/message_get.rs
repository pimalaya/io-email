//! JMAP message get (`Email/get` + `Blob/download`), wrapping a private
//! orchestrator that resolves the message blob id then downloads its
//! raw RFC 5322 bytes.

use alloc::{
    string::{String, ToString},
    vec,
    vec::Vec,
};
use core::mem;

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

/// Errors produced while orchestrating `Email/get` + `Blob/download`
/// for JMAP raw message retrieval.
#[derive(Debug, Error)]
pub enum MessageGetError {
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

/// Result returned by [`MessageGet::resume`].
#[derive(Debug)]
pub enum MessageGetResult {
    Ok(Vec<u8>),
    WantsRead,
    WantsWrite(Vec<u8>),
    Err(MessageGetError),
}

/// I/O-free coroutine wrapping `Email/get` + `Blob/download`. Returns
/// the raw RFC 5322 bytes on completion.
pub struct MessageGet {
    inner: Inner,
    pending: Option<PendingDownload>,
}

struct PendingDownload {
    download_url_template: String,
    account_id: String,
    http_auth: SecretString,
}

enum Inner {
    GettingEmail(JmapEmailGet),
    Downloading(JmapBlobDownload),
    Done,
}

impl MessageGet {
    /// Builds the orchestrator from a JMAP session, the bearer/basic
    /// HTTP credential and the email id to download.
    pub fn new(
        session: &JmapSession,
        http_auth: &SecretString,
        email_id: impl ToString,
    ) -> Result<Self, MessageGetError> {
        trace!("prepare JMAP message get");

        let get = JmapEmailGet::new(
            session,
            http_auth,
            vec![email_id.to_string()],
            Some(vec!["blobId".into()]),
            false,
            false,
            0,
        )?;

        let pending = PendingDownload {
            download_url_template: session.download_url.clone(),
            account_id: session
                .primary_accounts
                .get(capabilities::MAIL)
                .cloned()
                .unwrap_or_default(),
            http_auth: http_auth.clone(),
        };

        Ok(Self {
            inner: Inner::GettingEmail(get),
            pending: Some(pending),
        })
    }

    /// Advances the orchestrator. Drives Email/get first, then
    /// Blob/download.
    pub fn resume(&mut self, arg: Option<&[u8]>) -> MessageGetResult {
        let mut input = arg;

        loop {
            let inner = mem::replace(&mut self.inner, Inner::Done);

            match inner {
                Inner::GettingEmail(mut get) => match get.resume(input.take()) {
                    JmapEmailGetResult::WantsRead => {
                        self.inner = Inner::GettingEmail(get);
                        return MessageGetResult::WantsRead;
                    }
                    JmapEmailGetResult::WantsWrite(bytes) => {
                        self.inner = Inner::GettingEmail(get);
                        return MessageGetResult::WantsWrite(bytes);
                    }
                    JmapEmailGetResult::Err(err) => return MessageGetResult::Err(err.into()),
                    JmapEmailGetResult::Ok { mut emails, .. } => {
                        let Some(email) = emails.pop() else {
                            return MessageGetResult::Err(MessageGetError::EmailNotFound);
                        };

                        let Some(blob_id) = email.blob_id else {
                            return MessageGetResult::Err(MessageGetError::MissingBlobId);
                        };

                        let pending = self
                            .pending
                            .take()
                            .expect("pending download set on construct");

                        let url_str = resolve_download_url(
                            &pending.download_url_template,
                            &pending.account_id,
                            &blob_id,
                        );

                        let url = match Url::parse(&url_str) {
                            Ok(u) => u,
                            Err(_) => {
                                return MessageGetResult::Err(MessageGetError::InvalidDownloadUrl(
                                    url_str,
                                ));
                            }
                        };

                        let download = JmapBlobDownload::new(&pending.http_auth, &url);
                        self.inner = Inner::Downloading(download);
                    }
                },
                Inner::Downloading(mut download) => match download.resume(input.take()) {
                    JmapBlobDownloadResult::WantsRead => {
                        self.inner = Inner::Downloading(download);
                        return MessageGetResult::WantsRead;
                    }
                    JmapBlobDownloadResult::WantsWrite(bytes) => {
                        self.inner = Inner::Downloading(download);
                        return MessageGetResult::WantsWrite(bytes);
                    }
                    JmapBlobDownloadResult::WantsRedirect { .. } => {
                        return MessageGetResult::Err(MessageGetError::UnsupportedRedirect);
                    }
                    JmapBlobDownloadResult::Err(err) => return MessageGetResult::Err(err.into()),
                    JmapBlobDownloadResult::Ok { data, .. } => {
                        return MessageGetResult::Ok(data);
                    }
                },
                Inner::Done => {
                    return MessageGetResult::Err(MessageGetError::AlreadyDone);
                }
            }
        }
    }
}

pub(crate) fn resolve_download_url(template: &str, account_id: &str, blob_id: &str) -> String {
    template
        .replace("{accountId}", account_id)
        .replace("{blobId}", blob_id)
        .replace("{type}", "application/octet-stream")
        .replace("{name}", blob_id)
}
