//! JMAP message-send coroutine.
//!
//! Three-stage state machine:
//! 1. `Blob/upload` posts the raw RFC 5322 bytes to the session
//!    `upload_url` and yields a blob id.
//! 2. `Email/import` materialises the blob into the configured
//!    drafts mailbox with the `$draft` keyword.
//! 3. `EmailSubmission/set` queues the resulting email for delivery
//!    under the configured identity. JMAP picks the SMTP envelope
//!    from the email headers (`envelope: None`).
//!
//! `identity_id` and `drafts_mailbox_id` come from the
//! [`JmapContext`] and must be populated before
//! [`EmailClientStd::send_message`] is called.
//!
//! [`JmapContext`]: crate::client::JmapContext
//! [`EmailClientStd::send_message`]: crate::client::EmailClientStd::send_message

use alloc::{collections::BTreeMap, string::String, vec::Vec};
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
        email_submission::EmailSubmissionCreate,
        email_submission_set::{
            JmapEmailSubmissionSet as InnerSubmit, JmapEmailSubmissionSetError as SubmitErr,
        },
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
    #[error("coroutine was resumed with the wrong EmailCoroutineArg variant")]
    InvalidArg,
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

impl EmailCoroutine for JmapMessageSend {
    type Yield = JmapStep;
    type Return = Result<(), JmapMessageSendError>;

    const BACKEND: EmailBackend = EmailBackend::Jmap;

    fn resume(
        &mut self,
        arg: EmailCoroutineArg<'_>,
    ) -> EmailCoroutineState<Self::Yield, Self::Return> {
        #[allow(irrefutable_let_patterns)]
        let EmailCoroutineArg::Jmap { bytes } = arg else {
            return EmailCoroutineState::Complete(Err(JmapMessageSendError::InvalidArg));
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
                    EmailCoroutineState::Complete(Err(JmapMessageSendError::UnsupportedRedirect))
                }
                JmapCoroutineState::Complete(Ok(JmapBlobUploadOutput { blob_id, .. })) => {
                    let mut mailbox_ids = BTreeMap::new();
                    mailbox_ids.insert(self.drafts_mailbox_id.clone(), true);

                    let mut keywords = BTreeMap::new();
                    keywords.insert("$draft".into(), true);

                    let mut emails = BTreeMap::new();
                    emails.insert(
                        OUTGOING.into(),
                        EmailImport {
                            blob_id,
                            mailbox_ids,
                            keywords: Some(keywords),
                            received_at: None,
                        },
                    );

                    let import = match InnerImport::new(&self.session, &self.http_auth, emails) {
                        Ok(i) => i,
                        Err(err) => return EmailCoroutineState::Complete(Err(err.into())),
                    };
                    self.state = State::Importing(import);
                    self.resume(EmailCoroutineArg::Jmap { bytes: None })
                }
                JmapCoroutineState::Complete(Err(err)) => {
                    EmailCoroutineState::Complete(Err(err.into()))
                }
            },
            State::Importing(mut import) => match import.resume(bytes) {
                JmapCoroutineState::Yielded(JmapYield::WantsRead) => {
                    self.state = State::Importing(import);
                    EmailCoroutineState::Yielded(JmapStep::WantsRead)
                }
                JmapCoroutineState::Yielded(JmapYield::WantsWrite(out)) => {
                    self.state = State::Importing(import);
                    EmailCoroutineState::Yielded(JmapStep::WantsWrite(out))
                }
                JmapCoroutineState::Complete(Ok(mut ok)) => {
                    if !ok.not_created.is_empty() {
                        return EmailCoroutineState::Complete(Err(
                            JmapMessageSendError::NotImported,
                        ));
                    }
                    let Some(email) = ok.created.remove(OUTGOING) else {
                        return EmailCoroutineState::Complete(Err(
                            JmapMessageSendError::NotImported,
                        ));
                    };
                    let Some(email_id) = email.id else {
                        return EmailCoroutineState::Complete(Err(
                            JmapMessageSendError::MissingImportedEmailId,
                        ));
                    };

                    let mut submissions = BTreeMap::new();
                    submissions.insert(
                        OUTGOING.into(),
                        EmailSubmissionCreate {
                            identity_id: self.identity_id.clone(),
                            email_id,
                            envelope: None,
                        },
                    );
                    let submit = match InnerSubmit::new(&self.session, &self.http_auth, submissions)
                    {
                        Ok(s) => s,
                        Err(err) => return EmailCoroutineState::Complete(Err(err.into())),
                    };
                    self.state = State::Submitting(submit);
                    self.resume(EmailCoroutineArg::Jmap { bytes: None })
                }
                JmapCoroutineState::Complete(Err(err)) => {
                    EmailCoroutineState::Complete(Err(err.into()))
                }
            },
            State::Submitting(mut submit) => match submit.resume(bytes) {
                JmapCoroutineState::Yielded(JmapYield::WantsRead) => {
                    self.state = State::Submitting(submit);
                    EmailCoroutineState::Yielded(JmapStep::WantsRead)
                }
                JmapCoroutineState::Yielded(JmapYield::WantsWrite(out)) => {
                    self.state = State::Submitting(submit);
                    EmailCoroutineState::Yielded(JmapStep::WantsWrite(out))
                }
                JmapCoroutineState::Complete(Ok(ok)) => {
                    if !ok.not_created.is_empty() {
                        EmailCoroutineState::Complete(Err(JmapMessageSendError::NotSubmitted))
                    } else {
                        EmailCoroutineState::Complete(Ok(()))
                    }
                }
                JmapCoroutineState::Complete(Err(err)) => {
                    EmailCoroutineState::Complete(Err(err.into()))
                }
            },
            State::Done => {
                EmailCoroutineState::Complete(Err(JmapMessageSendError::ResumedAfterDone))
            }
        }
    }
}

enum State {
    Uploading(JmapBlobUpload),
    Importing(InnerImport),
    Submitting(InnerSubmit),
    Done,
}

const OUTGOING: &str = "outgoing";

/// Resolves the RFC 8620 upload URL template against the live
/// `{accountId}` substitution.
fn resolve_upload_url(session: &JmapSession) -> Result<Url, JmapMessageSendError> {
    let account_id = account_id_of(session);
    let url_str = session.upload_url.replace("{accountId}", &account_id);
    Url::parse(&url_str).map_err(|_| JmapMessageSendError::InvalidUploadUrl(url_str))
}
