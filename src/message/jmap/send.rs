//! JMAP message-send coroutine: Blob/upload, Email/import into the
//! drafts mailbox with `$draft`, then EmailSubmission/set against the
//! configured identity.
//!
//! `identity_id` and `drafts_mailbox_id` must be resolved (typically
//! from the JMAP session) before constructing the coroutine.
//!
//! # Example
//!
//! ```rust,ignore
//! use io_email::message::jmap::send::JmapMessageSend;
//!
//! client.run(JmapMessageSend::new(&session, &auth, "identity-id", "drafts-id", raw)?)?;
//! ```

use alloc::{collections::BTreeMap, string::String, vec::Vec};
use core::mem;

use io_jmap::{
    coroutine::{JmapCoroutine, JmapCoroutineState, JmapYield},
    rfc8620::{
        JmapSession,
        blob_upload::{JmapBlobUpload, JmapBlobUploadError, JmapBlobUploadOutput},
        coroutine::JmapRedirectYield,
    },
    rfc8621::{
        email::{
            JmapEmailImportArgs,
            import::{JmapEmailImport as InnerImport, JmapEmailImportError as ImportErr},
        },
        email_submission::{
            JmapEmailSubmissionCreate,
            set::{
                JmapEmailSubmissionSet as InnerSubmit, JmapEmailSubmissionSetError as SubmitErr,
            },
        },
    },
};
use log::trace;
use secrecy::SecretString;
use thiserror::Error;
use url::Url;

use crate::jmap::convert::account_id_of;

/// Errors produced by [`JmapMessageSend`].
#[derive(Debug, Error)]
pub enum JmapMessageSendError {
    #[error(transparent)]
    BlobUpload(#[from] JmapBlobUploadError),
    #[error(transparent)]
    Import(#[from] ImportErr),
    #[error(transparent)]
    Submit(#[from] SubmitErr),
    #[error("Email/import did not create the staged email")]
    NotImported,
    #[error("Email/import response did not include an email id")]
    MissingImportedEmailId,
    #[error("JMAP blob upload reached unexpected redirection")]
    UnsupportedRedirect,
    #[error("EmailSubmission/set did not submit the email")]
    NotSubmitted,
    #[error("resolved JMAP upload URL is invalid: {0}")]
    InvalidUploadUrl(String),
    #[error("coroutine was resumed after completion")]
    ResumedAfterDone,
}

/// I/O-free coroutine queuing a JMAP message for delivery.
pub struct JmapMessageSend {
    state: State,
    identity_id: String,
    drafts_mailbox_id: String,
    session: JmapSession,
    http_auth: SecretString,
}

impl JmapMessageSend {
    pub fn new(
        session: &JmapSession,
        http_auth: &SecretString,
        identity_id: &str,
        drafts_mailbox_id: &str,
        raw: Vec<u8>,
    ) -> Result<Self, JmapMessageSendError> {
        trace!("prepare JMAP message send");
        let upload_url = resolve_upload_url(session)?;
        let upload = JmapBlobUpload::new(http_auth, &upload_url, "message/rfc822", raw);
        Ok(Self {
            state: State::Uploading(upload),
            identity_id: identity_id.into(),
            drafts_mailbox_id: drafts_mailbox_id.into(),
            session: session.clone(),
            http_auth: http_auth.clone(),
        })
    }
}

enum State {
    Uploading(JmapBlobUpload),
    Importing(InnerImport),
    Submitting(InnerSubmit),
    Done,
}

const OUTGOING: &str = "outgoing";

impl JmapCoroutine for JmapMessageSend {
    type Yield = JmapYield;
    type Return = Result<(), JmapMessageSendError>;

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
                    JmapCoroutineState::Complete(Err(JmapMessageSendError::UnsupportedRedirect))
                }
                JmapCoroutineState::Complete(Ok(JmapBlobUploadOutput { blob_id, .. })) => {
                    let mut mailbox_ids = BTreeMap::new();
                    mailbox_ids.insert(self.drafts_mailbox_id.clone(), true);

                    let mut keywords = BTreeMap::new();
                    keywords.insert("$draft".into(), true);

                    let mut emails = BTreeMap::new();
                    emails.insert(
                        OUTGOING.into(),
                        JmapEmailImportArgs {
                            blob_id,
                            mailbox_ids,
                            keywords: Some(keywords),
                            received_at: None,
                        },
                    );

                    let import = match InnerImport::new(&self.session, &self.http_auth, emails) {
                        Ok(i) => i,
                        Err(err) => return JmapCoroutineState::Complete(Err(err.into())),
                    };
                    self.state = State::Importing(import);
                    JmapCoroutine::resume(self, None)
                }
                JmapCoroutineState::Complete(Err(err)) => {
                    JmapCoroutineState::Complete(Err(err.into()))
                }
            },
            State::Importing(mut import) => match import.resume(bytes) {
                JmapCoroutineState::Yielded(y) => {
                    self.state = State::Importing(import);
                    JmapCoroutineState::Yielded(y)
                }
                JmapCoroutineState::Complete(Ok(mut ok)) => {
                    if !ok.not_created.is_empty() {
                        return JmapCoroutineState::Complete(Err(
                            JmapMessageSendError::NotImported,
                        ));
                    }
                    let Some(email) = ok.created.remove(OUTGOING) else {
                        return JmapCoroutineState::Complete(Err(
                            JmapMessageSendError::NotImported,
                        ));
                    };
                    let Some(email_id) = email.id else {
                        return JmapCoroutineState::Complete(Err(
                            JmapMessageSendError::MissingImportedEmailId,
                        ));
                    };

                    let mut submissions = BTreeMap::new();
                    submissions.insert(
                        OUTGOING.into(),
                        JmapEmailSubmissionCreate {
                            identity_id: self.identity_id.clone(),
                            email_id,
                            envelope: None,
                        },
                    );
                    let submit = match InnerSubmit::new(&self.session, &self.http_auth, submissions)
                    {
                        Ok(s) => s,
                        Err(err) => return JmapCoroutineState::Complete(Err(err.into())),
                    };
                    self.state = State::Submitting(submit);
                    JmapCoroutine::resume(self, None)
                }
                JmapCoroutineState::Complete(Err(err)) => {
                    JmapCoroutineState::Complete(Err(err.into()))
                }
            },
            State::Submitting(mut submit) => match submit.resume(bytes) {
                JmapCoroutineState::Yielded(y) => {
                    self.state = State::Submitting(submit);
                    JmapCoroutineState::Yielded(y)
                }
                JmapCoroutineState::Complete(Ok(ok)) => {
                    if !ok.not_created.is_empty() {
                        JmapCoroutineState::Complete(Err(JmapMessageSendError::NotSubmitted))
                    } else {
                        JmapCoroutineState::Complete(Ok(()))
                    }
                }
                JmapCoroutineState::Complete(Err(err)) => {
                    JmapCoroutineState::Complete(Err(err.into()))
                }
            },
            State::Done => {
                JmapCoroutineState::Complete(Err(JmapMessageSendError::ResumedAfterDone))
            }
        }
    }
}

/// Substitutes `{accountId}` in the upload URL template.
fn resolve_upload_url(session: &JmapSession) -> Result<Url, JmapMessageSendError> {
    let account_id = account_id_of(session);
    let url_str = session.upload_url.replace("{accountId}", &account_id);
    Url::parse(&url_str).map_err(|_| JmapMessageSendError::InvalidUploadUrl(url_str))
}
