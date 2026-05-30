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

impl JmapCoroutine for JmapMessageAdd {
    type Yield = JmapYield;
    type Return = Result<String, JmapMessageAddError>;

    fn resume(&mut self, bytes: Option<&[u8]>) -> JmapCoroutineState<Self::Yield, Self::Return> {
        match mem::replace(&mut self.state, State::Done) {
            State::Uploading(mut upload) => match upload.resume(bytes) {
                JmapCoroutineState::Yielded(JmapRedirectYield::WantsRead) => {
                    self.state = State::Uploading(upload);
                    JmapCoroutineState::Yielded(JmapYield::WantsRead)
                }
                JmapCoroutineState::Yielded(JmapRedirectYield::WantsWrite(out)) => {
                    self.state = State::Uploading(upload);
                    JmapCoroutineState::Yielded(JmapYield::WantsWrite(out))
                }
                JmapCoroutineState::Yielded(JmapRedirectYield::WantsRedirect { .. }) => {
                    JmapCoroutineState::Complete(Err(JmapMessageAddError::UnsupportedRedirect))
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
                        Err(err) => return JmapCoroutineState::Complete(Err(err.into())),
                    };
                    self.state = State::Resolving { query, blob_id };
                    JmapCoroutine::resume(self, None)
                }
                JmapCoroutineState::Complete(Err(err)) => {
                    JmapCoroutineState::Complete(Err(err.into()))
                }
            },
            State::Resolving { mut query, blob_id } => match query.resume(bytes) {
                JmapCoroutineState::Complete(Ok(ok)) => {
                    let Some(id) = find_mailbox_id(&ok.mailboxes, &self.mailbox_name) else {
                        return JmapCoroutineState::Complete(Err(JmapMessageAddError::NotFound(
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
                        Err(err) => return JmapCoroutineState::Complete(Err(err.into())),
                    };
                    self.state = State::Importing { import, client_id };
                    JmapCoroutine::resume(self, None)
                }
                JmapCoroutineState::Yielded(y) => {
                    self.state = State::Resolving { query, blob_id };
                    JmapCoroutineState::Yielded(y)
                }
                JmapCoroutineState::Complete(Err(err)) => {
                    JmapCoroutineState::Complete(Err(err.into()))
                }
            },
            State::Importing {
                mut import,
                client_id,
            } => match import.resume(bytes) {
                JmapCoroutineState::Complete(Ok(ok)) => {
                    let Some(email) = ok.created.get(&client_id) else {
                        return JmapCoroutineState::Complete(Err(JmapMessageAddError::NotImported));
                    };
                    JmapCoroutineState::Complete(Ok(email.id.clone().unwrap_or_default()))
                }
                JmapCoroutineState::Yielded(y) => {
                    self.state = State::Importing { import, client_id };
                    JmapCoroutineState::Yielded(y)
                }
                JmapCoroutineState::Complete(Err(err)) => {
                    JmapCoroutineState::Complete(Err(err.into()))
                }
            },
            State::Done => JmapCoroutineState::Complete(Err(JmapMessageAddError::ResumedAfterDone)),
        }
    }
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
