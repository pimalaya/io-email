//! JMAP message send (`Blob/upload` + `Email/import` +
//! `EmailSubmission/set`), wrapping a private orchestrator that
//! uploads the raw RFC 5322 bytes, imports them into the caller-
//! provided drafts mailbox with the `$draft` keyword, then submits
//! them with the caller-provided identity.

use alloc::{
    collections::BTreeMap,
    string::{String, ToString},
    vec::Vec,
};
use core::mem;

use io_jmap::{
    rfc8620::{
        blob_upload::{JmapBlobUpload, JmapBlobUploadError, JmapBlobUploadResult},
        session::JmapSession,
    },
    rfc8621::{
        capabilities,
        email::EmailImport,
        email_import::{JmapEmailImport, JmapEmailImportError, JmapEmailImportResult},
        email_submission::EmailSubmissionCreate,
        email_submission_set::{
            JmapEmailSubmissionSet, JmapEmailSubmissionSetError, JmapEmailSubmissionSetResult,
        },
    },
};
use log::trace;
use secrecy::SecretString;
use thiserror::Error;
use url::Url;

/// Errors produced while orchestrating `Blob/upload` + `Email/import` +
/// `EmailSubmission/set` for JMAP message submission.
#[derive(Debug, Error)]
pub enum MessageSendError {
    #[error(transparent)]
    BlobUpload(#[from] JmapBlobUploadError),
    #[error(transparent)]
    EmailImport(#[from] JmapEmailImportError),
    #[error(transparent)]
    EmailSubmission(#[from] JmapEmailSubmissionSetError),
    #[error("Email/import did not create the staged email")]
    ImportFailed,
    #[error("Email/import response did not include an email id")]
    MissingImportedEmailId,
    #[error("EmailSubmission/set did not submit the email")]
    SubmissionFailed,
    #[error("Resolved JMAP upload URL is invalid: {0}")]
    InvalidUploadUrl(String),
    #[error("JMAP message send was resumed after completion")]
    AlreadyDone,
}

/// Result returned by [`MessageSend::resume`].
#[derive(Debug)]
pub enum MessageSendResult {
    Ok,
    WantsRead,
    WantsWrite(Vec<u8>),
    Err(MessageSendError),
}

/// I/O-free coroutine wrapping `Blob/upload` → `Email/import` →
/// `EmailSubmission/set` to submit a JMAP message.
///
/// The caller must supply the identity to send as and the drafts
/// mailbox id to stage the email in. Both are typically obtained via
/// `Identity/get` and `Mailbox/query` (role = `"drafts"`) at session
/// startup.
pub struct MessageSend {
    inner: Inner,
    pending_after_upload: Option<PendingImport>,
    pending_after_import: Option<PendingSubmission>,
}

struct PendingImport {
    session: JmapSession,
    http_auth: SecretString,
    drafts_mailbox_id: String,
}

struct PendingSubmission {
    session: JmapSession,
    http_auth: SecretString,
    identity_id: String,
}

enum Inner {
    Uploading(JmapBlobUpload),
    Importing(JmapEmailImport),
    Submitting(JmapEmailSubmissionSet),
    Done,
}

impl MessageSend {
    /// Builds the orchestrator from a JMAP session, the bearer/basic
    /// HTTP credential, the raw RFC 5322 bytes to send, and the
    /// caller-provided identity + drafts mailbox ids.
    pub fn new(
        session: &JmapSession,
        http_auth: &SecretString,
        raw: Vec<u8>,
        identity_id: impl ToString,
        drafts_mailbox_id: impl ToString,
    ) -> Result<Self, MessageSendError> {
        trace!("prepare JMAP message send");

        let account_id = session
            .primary_accounts
            .get(capabilities::MAIL)
            .cloned()
            .unwrap_or_default();

        let upload_url_str = resolve_upload_url(&session.upload_url, &account_id);
        let upload_url = Url::parse(&upload_url_str)
            .map_err(|_| MessageSendError::InvalidUploadUrl(upload_url_str))?;

        let upload = JmapBlobUpload::new(http_auth, &upload_url, "message/rfc822", raw);

        Ok(Self {
            inner: Inner::Uploading(upload),
            pending_after_upload: Some(PendingImport {
                session: session.clone(),
                http_auth: http_auth.clone(),
                drafts_mailbox_id: drafts_mailbox_id.to_string(),
            }),
            pending_after_import: Some(PendingSubmission {
                session: session.clone(),
                http_auth: http_auth.clone(),
                identity_id: identity_id.to_string(),
            }),
        })
    }

    /// Advances the orchestrator. Drives upload, import, then submit.
    pub fn resume(&mut self, arg: Option<&[u8]>) -> MessageSendResult {
        let mut input = arg;

        loop {
            let inner = mem::replace(&mut self.inner, Inner::Done);

            match inner {
                Inner::Uploading(mut upload) => match upload.resume(input.take()) {
                    JmapBlobUploadResult::WantsRead => {
                        self.inner = Inner::Uploading(upload);
                        return MessageSendResult::WantsRead;
                    }
                    JmapBlobUploadResult::WantsWrite(bytes) => {
                        self.inner = Inner::Uploading(upload);
                        return MessageSendResult::WantsWrite(bytes);
                    }
                    JmapBlobUploadResult::Err(err) => {
                        return MessageSendResult::Err(err.into());
                    }
                    JmapBlobUploadResult::Ok { blob_id, .. } => {
                        let pending = self
                            .pending_after_upload
                            .take()
                            .expect("import params set on construct");

                        let mut mailbox_ids = BTreeMap::new();
                        mailbox_ids.insert(pending.drafts_mailbox_id, true);

                        let mut keywords = BTreeMap::new();
                        keywords.insert("$draft".into(), true);

                        let mut emails = BTreeMap::new();
                        emails.insert(
                            "outgoing".into(),
                            EmailImport {
                                blob_id,
                                mailbox_ids,
                                keywords: Some(keywords),
                                received_at: None,
                            },
                        );

                        let import = match JmapEmailImport::new(
                            &pending.session,
                            &pending.http_auth,
                            emails,
                        ) {
                            Ok(c) => c,
                            Err(err) => return MessageSendResult::Err(err.into()),
                        };

                        self.inner = Inner::Importing(import);
                    }
                },
                Inner::Importing(mut import) => match import.resume(input.take()) {
                    JmapEmailImportResult::WantsRead => {
                        self.inner = Inner::Importing(import);
                        return MessageSendResult::WantsRead;
                    }
                    JmapEmailImportResult::WantsWrite(bytes) => {
                        self.inner = Inner::Importing(import);
                        return MessageSendResult::WantsWrite(bytes);
                    }
                    JmapEmailImportResult::Err(err) => {
                        return MessageSendResult::Err(err.into());
                    }
                    JmapEmailImportResult::Ok {
                        mut created,
                        not_created,
                        ..
                    } => {
                        if !not_created.is_empty() {
                            return MessageSendResult::Err(MessageSendError::ImportFailed);
                        }

                        let Some(email) = created.remove("outgoing") else {
                            return MessageSendResult::Err(MessageSendError::ImportFailed);
                        };

                        let Some(email_id) = email.id else {
                            return MessageSendResult::Err(
                                MessageSendError::MissingImportedEmailId,
                            );
                        };

                        let pending = self
                            .pending_after_import
                            .take()
                            .expect("submission params set on construct");

                        let mut submissions = BTreeMap::new();
                        submissions.insert(
                            "outgoing".into(),
                            EmailSubmissionCreate {
                                identity_id: pending.identity_id,
                                email_id,
                                envelope: None,
                            },
                        );

                        let submit = match JmapEmailSubmissionSet::new(
                            &pending.session,
                            &pending.http_auth,
                            submissions,
                        ) {
                            Ok(c) => c,
                            Err(err) => return MessageSendResult::Err(err.into()),
                        };

                        self.inner = Inner::Submitting(submit);
                    }
                },
                Inner::Submitting(mut submit) => match submit.resume(input.take()) {
                    JmapEmailSubmissionSetResult::WantsRead => {
                        self.inner = Inner::Submitting(submit);
                        return MessageSendResult::WantsRead;
                    }
                    JmapEmailSubmissionSetResult::WantsWrite(bytes) => {
                        self.inner = Inner::Submitting(submit);
                        return MessageSendResult::WantsWrite(bytes);
                    }
                    JmapEmailSubmissionSetResult::Err(err) => {
                        return MessageSendResult::Err(err.into());
                    }
                    JmapEmailSubmissionSetResult::Ok { not_created, .. } => {
                        if !not_created.is_empty() {
                            return MessageSendResult::Err(MessageSendError::SubmissionFailed);
                        }

                        return MessageSendResult::Ok;
                    }
                },
                Inner::Done => {
                    return MessageSendResult::Err(MessageSendError::AlreadyDone);
                }
            }
        }
    }
}

fn resolve_upload_url(template: &str, account_id: &str) -> String {
    template.replace("{accountId}", account_id)
}
