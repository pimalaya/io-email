//! JMAP mailbox-delete coroutine.
//!
//! Two-stage state machine:
//! 1. `Mailbox/query + Mailbox/get` with a name filter resolves the
//!    shared mailbox name to a JMAP id (substring filter on the wire,
//!    exact-name post-filter on the client).
//! 2. `Mailbox/set { destroy }` removes the resolved id. The
//!    `onDestroyRemoveEmails: true` flag drops every email that lives
//!    only in this mailbox so the resulting state is `delete the
//!    folder and everything in it`, matching the IMAP semantics.

use alloc::{string::String, vec};
use core::mem;

use io_jmap::{
    coroutine::{JmapCoroutine, JmapCoroutineState, JmapYield},
    rfc8620::session::JmapSession,
    rfc8621::{
        mailbox::MailboxProperty,
        mailbox_query::{JmapMailboxQuery as InnerQuery, JmapMailboxQueryError as QueryErr},
        mailbox_set::{
            JmapMailboxSet as InnerSet, JmapMailboxSetArgs, JmapMailboxSetError as SetErr,
        },
    },
};
use log::trace;
use secrecy::SecretString;
use thiserror::Error;

use crate::{
    coroutine::{EmailBackend, EmailCoroutine, EmailCoroutineArg, EmailCoroutineState, JmapStep},
    jmap::convert::find_mailbox_id,
};

/// Errors produced by [`JmapMailboxDelete`].
#[derive(Debug, Error)]
pub enum JmapMailboxDeleteError {
    #[error(transparent)]
    Query(#[from] QueryErr),
    #[error(transparent)]
    Set(#[from] SetErr),
    #[error("no JMAP mailbox named `{0}` found")]
    NotFound(String),
    #[error("Mailbox/set did not destroy `{0}`")]
    NotDestroyed(String),
    #[error("coroutine was resumed with the wrong EmailCoroutineArg variant")]
    InvalidArg,
    #[error("coroutine was resumed after completion")]
    ResumedAfterDone,
}

/// I/O-free coroutine deleting a JMAP mailbox by name.
pub struct JmapMailboxDelete {
    state: State,
    name: String,
    session: JmapSession,
    http_auth: SecretString,
}

impl JmapMailboxDelete {
    pub fn new(
        session: &JmapSession,
        http_auth: &SecretString,
        name: &str,
    ) -> Result<Self, JmapMailboxDeleteError> {
        trace!("prepare JMAP mailbox delete");
        let query = InnerQuery::new(
            session,
            http_auth,
            Some(io_jmap::rfc8621::mailbox::MailboxFilter {
                name: Some(name.into()),
                ..io_jmap::rfc8621::mailbox::MailboxFilter::default()
            }),
            None,
            None,
            None,
            Some(vec![MailboxProperty::Id, MailboxProperty::Name]),
        )?;
        Ok(Self {
            state: State::Resolving(query),
            name: name.into(),
            session: session.clone(),
            http_auth: http_auth.clone(),
        })
    }
}

impl EmailCoroutine for JmapMailboxDelete {
    type Yield = JmapStep;
    type Return = Result<(), JmapMailboxDeleteError>;

    const BACKEND: EmailBackend = EmailBackend::Jmap;

    fn resume(
        &mut self,
        arg: EmailCoroutineArg<'_>,
    ) -> EmailCoroutineState<Self::Yield, Self::Return> {
        #[allow(irrefutable_let_patterns)]
        let EmailCoroutineArg::Jmap { bytes } = arg else {
            return EmailCoroutineState::Complete(Err(JmapMailboxDeleteError::InvalidArg));
        };

        match mem::replace(&mut self.state, State::Done) {
            State::Resolving(mut query) => match query.resume(bytes) {
                JmapCoroutineState::Complete(Ok(ok)) => {
                    let Some(id) = find_mailbox_id(&ok.mailboxes, &self.name) else {
                        return EmailCoroutineState::Complete(Err(
                            JmapMailboxDeleteError::NotFound(self.name.clone()),
                        ));
                    };
                    let args = JmapMailboxSetArgs {
                        destroy: Some(vec![id.clone()]),
                        on_destroy_remove_emails: Some(true),
                        ..JmapMailboxSetArgs::default()
                    };
                    let set = match InnerSet::new(&self.session, &self.http_auth, args) {
                        Ok(set) => set,
                        Err(err) => return EmailCoroutineState::Complete(Err(err.into())),
                    };
                    self.state = State::Destroying { set, id };
                    self.resume(EmailCoroutineArg::Jmap { bytes: None })
                }
                JmapCoroutineState::Yielded(JmapYield::WantsRead) => {
                    self.state = State::Resolving(query);
                    EmailCoroutineState::Yielded(JmapStep::WantsRead)
                }
                JmapCoroutineState::Yielded(JmapYield::WantsWrite(out)) => {
                    self.state = State::Resolving(query);
                    EmailCoroutineState::Yielded(JmapStep::WantsWrite(out))
                }
                JmapCoroutineState::Complete(Err(err)) => {
                    EmailCoroutineState::Complete(Err(err.into()))
                }
            },
            State::Destroying { mut set, id } => match set.resume(bytes) {
                JmapCoroutineState::Complete(Ok(ok)) => {
                    if ok.destroyed.iter().any(|d| d == &id) {
                        EmailCoroutineState::Complete(Ok(()))
                    } else {
                        EmailCoroutineState::Complete(Err(JmapMailboxDeleteError::NotDestroyed(id)))
                    }
                }
                JmapCoroutineState::Yielded(JmapYield::WantsRead) => {
                    self.state = State::Destroying { set, id };
                    EmailCoroutineState::Yielded(JmapStep::WantsRead)
                }
                JmapCoroutineState::Yielded(JmapYield::WantsWrite(out)) => {
                    self.state = State::Destroying { set, id };
                    EmailCoroutineState::Yielded(JmapStep::WantsWrite(out))
                }
                JmapCoroutineState::Complete(Err(err)) => {
                    EmailCoroutineState::Complete(Err(err.into()))
                }
            },
            State::Done => {
                EmailCoroutineState::Complete(Err(JmapMailboxDeleteError::ResumedAfterDone))
            }
        }
    }
}

enum State {
    Resolving(InnerQuery),
    Destroying { set: InnerSet, id: String },
    Done,
}
