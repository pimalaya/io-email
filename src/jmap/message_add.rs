//! JMAP message add (`Blob/upload` → `Mailbox/query` → `Email/import`),
//! wrapping a private orchestrator that uploads the raw RFC 5322
//! bytes, resolves the destination mailbox name to an id, then imports
//! the blob into that mailbox.

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
        mailbox_query::{JmapMailboxQuery, JmapMailboxQueryError, JmapMailboxQueryResult},
    },
};
use log::trace;
use secrecy::SecretString;
use thiserror::Error;
use url::Url;

use crate::jmap::message_copy::find_mailbox_id;

/// Errors produced while orchestrating Blob/upload + Mailbox/query +
/// Email/import.
#[derive(Debug, Error)]
pub enum MessageAddError {
    #[error(transparent)]
    BlobUpload(#[from] JmapBlobUploadError),
    #[error(transparent)]
    MailboxQuery(#[from] JmapMailboxQueryError),
    #[error(transparent)]
    EmailImport(#[from] JmapEmailImportError),
    #[error("no JMAP mailbox matched the name {0:?}")]
    UnknownMailbox(String),
    #[error("Email/import did not create the imported email")]
    ImportFailed,
    #[error("Resolved JMAP upload URL is invalid: {0}")]
    InvalidUploadUrl(String),
    #[error("JMAP message add was resumed after completion")]
    AlreadyDone,
}

/// Result returned by [`MessageAdd::resume`].
#[derive(Debug)]
pub enum MessageAddResult {
    Ok {
        /// JMAP id of the newly-created email, if the server returned
        /// it.
        email_id: Option<String>,
    },
    WantsRead,
    WantsWrite(Vec<u8>),
    Err(MessageAddError),
}

/// I/O-free orchestrator: Blob/upload → Mailbox/query (resolve name)
/// → Email/import.
pub struct MessageAdd {
    inner: Inner,
    pending: Option<Pending>,
}

struct Pending {
    session: JmapSession,
    http_auth: SecretString,
    mailbox_name: String,
    keywords: Vec<String>,
    blob_id: Option<String>,
}

enum Inner {
    Uploading(JmapBlobUpload),
    Resolving(JmapMailboxQuery),
    Importing(JmapEmailImport),
    Done,
}

impl MessageAdd {
    /// Builds the orchestrator.
    pub fn new(
        session: &JmapSession,
        http_auth: &SecretString,
        raw: Vec<u8>,
        mailbox_name: impl ToString,
        keywords: impl IntoIterator<Item = String>,
    ) -> Result<Self, MessageAddError> {
        trace!("prepare JMAP message add");

        let account_id = session
            .primary_accounts
            .get(capabilities::MAIL)
            .cloned()
            .unwrap_or_default();

        let upload_url_str = session.upload_url.replace("{accountId}", &account_id);
        let upload_url = Url::parse(&upload_url_str)
            .map_err(|_| MessageAddError::InvalidUploadUrl(upload_url_str))?;

        let upload = JmapBlobUpload::new(http_auth, &upload_url, "message/rfc822", raw);

        Ok(Self {
            inner: Inner::Uploading(upload),
            pending: Some(Pending {
                session: session.clone(),
                http_auth: http_auth.clone(),
                mailbox_name: mailbox_name.to_string(),
                keywords: keywords.into_iter().collect(),
                blob_id: None,
            }),
        })
    }

    /// Advances the orchestrator. Drives upload, then resolve, then
    /// import.
    pub fn resume(&mut self, arg: Option<&[u8]>) -> MessageAddResult {
        let mut input = arg;

        loop {
            let inner = mem::replace(&mut self.inner, Inner::Done);

            match inner {
                Inner::Uploading(mut upload) => match upload.resume(input.take()) {
                    JmapBlobUploadResult::WantsRead => {
                        self.inner = Inner::Uploading(upload);
                        return MessageAddResult::WantsRead;
                    }
                    JmapBlobUploadResult::WantsWrite(bytes) => {
                        self.inner = Inner::Uploading(upload);
                        return MessageAddResult::WantsWrite(bytes);
                    }
                    JmapBlobUploadResult::Err(err) => return MessageAddResult::Err(err.into()),
                    JmapBlobUploadResult::Ok { blob_id, .. } => {
                        let pending = self.pending.as_mut().expect("pending set on construct");
                        pending.blob_id = Some(blob_id);

                        let query = match JmapMailboxQuery::new(
                            &pending.session,
                            &pending.http_auth,
                            None,
                            None,
                            None,
                            None,
                            None,
                        ) {
                            Ok(q) => q,
                            Err(err) => return MessageAddResult::Err(err.into()),
                        };

                        self.inner = Inner::Resolving(query);
                    }
                },
                Inner::Resolving(mut query) => match query.resume(input.take()) {
                    JmapMailboxQueryResult::WantsRead => {
                        self.inner = Inner::Resolving(query);
                        return MessageAddResult::WantsRead;
                    }
                    JmapMailboxQueryResult::WantsWrite(bytes) => {
                        self.inner = Inner::Resolving(query);
                        return MessageAddResult::WantsWrite(bytes);
                    }
                    JmapMailboxQueryResult::Err(err) => return MessageAddResult::Err(err.into()),
                    JmapMailboxQueryResult::Ok { mailboxes, .. } => {
                        let pending = self.pending.take().expect("pending set above");

                        let Some(mailbox_id) = find_mailbox_id(&mailboxes, &pending.mailbox_name)
                        else {
                            return MessageAddResult::Err(MessageAddError::UnknownMailbox(
                                pending.mailbox_name,
                            ));
                        };

                        let blob_id = pending.blob_id.expect("blob id set after upload");

                        let mut mailbox_ids = BTreeMap::new();
                        mailbox_ids.insert(mailbox_id, true);

                        let keywords = if pending.keywords.is_empty() {
                            None
                        } else {
                            Some(pending.keywords.into_iter().map(|k| (k, true)).collect())
                        };

                        let mut emails = BTreeMap::new();
                        emails.insert(
                            "added".into(),
                            EmailImport {
                                blob_id,
                                mailbox_ids,
                                keywords,
                                received_at: None,
                            },
                        );

                        let import = match JmapEmailImport::new(
                            &pending.session,
                            &pending.http_auth,
                            emails,
                        ) {
                            Ok(c) => c,
                            Err(err) => return MessageAddResult::Err(err.into()),
                        };

                        self.inner = Inner::Importing(import);
                    }
                },
                Inner::Importing(mut import) => match import.resume(input.take()) {
                    JmapEmailImportResult::WantsRead => {
                        self.inner = Inner::Importing(import);
                        return MessageAddResult::WantsRead;
                    }
                    JmapEmailImportResult::WantsWrite(bytes) => {
                        self.inner = Inner::Importing(import);
                        return MessageAddResult::WantsWrite(bytes);
                    }
                    JmapEmailImportResult::Err(err) => return MessageAddResult::Err(err.into()),
                    JmapEmailImportResult::Ok {
                        mut created,
                        not_created,
                        ..
                    } => {
                        if !not_created.is_empty() || created.is_empty() {
                            return MessageAddResult::Err(MessageAddError::ImportFailed);
                        }

                        let email_id = created.remove("added").and_then(|e| e.id);
                        return MessageAddResult::Ok { email_id };
                    }
                },
                Inner::Done => return MessageAddResult::Err(MessageAddError::AlreadyDone),
            }
        }
    }
}
