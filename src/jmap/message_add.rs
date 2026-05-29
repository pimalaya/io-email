//! JMAP message-add coroutine.
//!
//! Three-stage state machine:
//! 1. `Blob/upload` posts the raw RFC 5322 bytes to the session
//!    `upload_url` and yields a blob id.
//! 2. `Mailbox/query + Mailbox/get` resolves the destination mailbox
//!    name to a JMAP id.
//! 3. `Email/import` materialises the blob into the resolved mailbox
//!    with the requested keywords.

use alloc::{collections::BTreeMap, string::String, vec, vec::Vec};
use core::mem;

use io_jmap::{
    coroutine::{JmapCoroutine, JmapCoroutineState, JmapYield},
    rfc8620::{
        blob_upload::{JmapBlobUpload, JmapBlobUploadError, JmapBlobUploadOutput},
        redirect::JmapRedirectYield,
        session::JmapSession,
    },
    rfc8621::{
        email::EmailImport,
        email_import::{JmapEmailImport as InnerImport, JmapEmailImportError as ImportErr},
        mailbox::{MailboxFilter, MailboxProperty},
        mailbox_query::{JmapMailboxQuery as InnerQuery, JmapMailboxQueryError as QueryErr},
    },
};
use log::trace;
use secrecy::SecretString;
use thiserror::Error;
use url::Url;

use crate::{
    coroutine::{EmailBackend, EmailCoroutine, EmailCoroutineArg, EmailCoroutineState, JmapStep},
    flag::Flag,
    jmap::convert::{find_mailbox_id, keyword_from},
};

/// Errors produced by [`JmapMessageAdd`].
#[derive(Debug, Error)]
pub enum JmapMessageAddError {
    #[error(transparent)]
    BlobUpload(#[from] JmapBlobUploadError),
    #[error(transparent)]
    Query(#[from] QueryErr),
    #[error(transparent)]
    Import(#[from] ImportErr),
    #[error("no JMAP mailbox named `{0}` found")]
    NotFound(String),
    #[error("resolved JMAP upload URL is invalid: {0}")]
    InvalidUploadUrl(String),
    #[error("Email/import did not create the imported email")]
    NotImported,
    #[error("JMAP blob upload reached unexpected redirection")]
    UnsupportedRedirect,
    #[error("coroutine was resumed with the wrong EmailCoroutineArg variant")]
    InvalidArg,
    #[error("coroutine was resumed after completion")]
    ResumedAfterDone,
}

/// I/O-free coroutine appending a raw RFC 5322 message to a JMAP
/// mailbox.
pub struct JmapMessageAdd {
    state: State,
    mailbox_name: String,
    keywords: BTreeMap<String, bool>,
    session: JmapSession,
    http_auth: SecretString,
}

impl JmapMessageAdd {
    pub fn new(
        session: &JmapSession,
        http_auth: &SecretString,
        mailbox: &str,
        flags: &[Flag],
        raw: Vec<u8>,
    ) -> Result<Self, JmapMessageAddError> {
        trace!("prepare JMAP message add");
        let upload_url = resolve_upload_url(session)?;
        let upload = JmapBlobUpload::new(http_auth, &upload_url, "message/rfc822", raw);
        let keywords = flags.iter().map(|f| (keyword_from(f), true)).collect();
        Ok(Self {
            state: State::Uploading(upload),
            mailbox_name: mailbox.into(),
            keywords,
            session: session.clone(),
            http_auth: http_auth.clone(),
        })
    }
}

impl EmailCoroutine for JmapMessageAdd {
    type Yield = JmapStep;
    type Return = Result<String, JmapMessageAddError>;

    const BACKEND: EmailBackend = EmailBackend::Jmap;

    fn resume(
        &mut self,
        arg: EmailCoroutineArg<'_>,
    ) -> EmailCoroutineState<Self::Yield, Self::Return> {
        #[allow(irrefutable_let_patterns)]
        let EmailCoroutineArg::Jmap { bytes } = arg else {
            return EmailCoroutineState::Complete(Err(JmapMessageAddError::InvalidArg));
        };

        match mem::replace(&mut self.state, State::Done) {
            State::Uploading(mut upload) => match upload.resume(bytes) {
                JmapCoroutineState::Yielded(JmapRedirectYield::WantsRead) => {
                    self.state = State::Uploading(upload);
                    EmailCoroutineState::Yielded(JmapStep::WantsRead)
                }
                JmapCoroutineState::Yielded(JmapRedirectYield::WantsWrite(out)) => {
                    self.state = State::Uploading(upload);
                    EmailCoroutineState::Yielded(JmapStep::WantsWrite(out))
                }
                JmapCoroutineState::Yielded(JmapRedirectYield::WantsRedirect { .. }) => {
                    EmailCoroutineState::Complete(Err(JmapMessageAddError::UnsupportedRedirect))
                }
                JmapCoroutineState::Complete(Ok(JmapBlobUploadOutput { blob_id, .. })) => {
                    let query = match InnerQuery::new(
                        &self.session,
                        &self.http_auth,
                        Some(MailboxFilter {
                            name: Some(self.mailbox_name.clone()),
                            ..MailboxFilter::default()
                        }),
                        None,
                        None,
                        None,
                        Some(vec![MailboxProperty::Id, MailboxProperty::Name]),
                    ) {
                        Ok(q) => q,
                        Err(err) => return EmailCoroutineState::Complete(Err(err.into())),
                    };
                    self.state = State::Resolving { query, blob_id };
                    self.resume(EmailCoroutineArg::Jmap { bytes: None })
                }
                JmapCoroutineState::Complete(Err(err)) => {
                    EmailCoroutineState::Complete(Err(err.into()))
                }
            },
            State::Resolving { mut query, blob_id } => match query.resume(bytes) {
                JmapCoroutineState::Complete(Ok(ok)) => {
                    let Some(id) = find_mailbox_id(&ok.mailboxes, &self.mailbox_name) else {
                        return EmailCoroutineState::Complete(Err(JmapMessageAddError::NotFound(
                            self.mailbox_name.clone(),
                        )));
                    };
                    let mut mailbox_ids = BTreeMap::new();
                    mailbox_ids.insert(id, true);
                    let client_id = String::from("new");
                    let mut imports = BTreeMap::new();
                    imports.insert(
                        client_id.clone(),
                        EmailImport {
                            blob_id,
                            mailbox_ids,
                            keywords: if self.keywords.is_empty() {
                                None
                            } else {
                                Some(self.keywords.clone())
                            },
                            received_at: None,
                        },
                    );
                    let import = match InnerImport::new(&self.session, &self.http_auth, imports) {
                        Ok(i) => i,
                        Err(err) => return EmailCoroutineState::Complete(Err(err.into())),
                    };
                    self.state = State::Importing { import, client_id };
                    self.resume(EmailCoroutineArg::Jmap { bytes: None })
                }
                JmapCoroutineState::Yielded(JmapYield::WantsRead) => {
                    self.state = State::Resolving { query, blob_id };
                    EmailCoroutineState::Yielded(JmapStep::WantsRead)
                }
                JmapCoroutineState::Yielded(JmapYield::WantsWrite(out)) => {
                    self.state = State::Resolving { query, blob_id };
                    EmailCoroutineState::Yielded(JmapStep::WantsWrite(out))
                }
                JmapCoroutineState::Complete(Err(err)) => {
                    EmailCoroutineState::Complete(Err(err.into()))
                }
            },
            State::Importing {
                mut import,
                client_id,
            } => match import.resume(bytes) {
                JmapCoroutineState::Complete(Ok(ok)) => {
                    let Some(email) = ok.created.get(&client_id) else {
                        return EmailCoroutineState::Complete(Err(
                            JmapMessageAddError::NotImported,
                        ));
                    };
                    EmailCoroutineState::Complete(Ok(email.id.clone().unwrap_or_default()))
                }
                JmapCoroutineState::Yielded(JmapYield::WantsRead) => {
                    self.state = State::Importing { import, client_id };
                    EmailCoroutineState::Yielded(JmapStep::WantsRead)
                }
                JmapCoroutineState::Yielded(JmapYield::WantsWrite(out)) => {
                    self.state = State::Importing { import, client_id };
                    EmailCoroutineState::Yielded(JmapStep::WantsWrite(out))
                }
                JmapCoroutineState::Complete(Err(err)) => {
                    EmailCoroutineState::Complete(Err(err.into()))
                }
            },
            State::Done => {
                EmailCoroutineState::Complete(Err(JmapMessageAddError::ResumedAfterDone))
            }
        }
    }
}

enum State {
    Uploading(JmapBlobUpload),
    Resolving {
        query: InnerQuery,
        blob_id: String,
    },
    Importing {
        import: InnerImport,
        client_id: String,
    },
    Done,
}

/// Resolves the RFC 8620 upload URL template against the live
/// `{accountId}` substitution; returns the parsed [`Url`].
fn resolve_upload_url(session: &JmapSession) -> Result<Url, JmapMessageAddError> {
    let account_id = session
        .primary_accounts
        .get(io_jmap::rfc8621::capabilities::MAIL)
        .cloned()
        .unwrap_or_default();
    let url_str = session.upload_url.replace("{accountId}", &account_id);
    Url::parse(&url_str).map_err(|_| JmapMessageAddError::InvalidUploadUrl(url_str))
}
