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

use crate::jmap::convert::find_mailbox_id;

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

enum State {
    Resolving(InnerQuery),
    Destroying { set: InnerSet, id: String },
    Done,
}

impl JmapCoroutine for JmapMailboxDelete {
    type Yield = JmapYield;
    type Return = Result<(), JmapMailboxDeleteError>;

    fn resume(&mut self, bytes: Option<&[u8]>) -> JmapCoroutineState<Self::Yield, Self::Return> {
        match mem::replace(&mut self.state, State::Done) {
            State::Resolving(mut query) => match query.resume(bytes) {
                JmapCoroutineState::Complete(Ok(ok)) => {
                    let Some(id) = find_mailbox_id(&ok.mailboxes, &self.name) else {
                        return JmapCoroutineState::Complete(Err(
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
                        Err(err) => return JmapCoroutineState::Complete(Err(err.into())),
                    };
                    self.state = State::Destroying { set, id };
                    JmapCoroutine::resume(self, None)
                }
                JmapCoroutineState::Yielded(y) => {
                    self.state = State::Resolving(query);
                    JmapCoroutineState::Yielded(y)
                }
                JmapCoroutineState::Complete(Err(err)) => {
                    JmapCoroutineState::Complete(Err(err.into()))
                }
            },
            State::Destroying { mut set, id } => match set.resume(bytes) {
                JmapCoroutineState::Complete(Ok(ok)) => {
                    if ok.destroyed.iter().any(|d| d == &id) {
                        JmapCoroutineState::Complete(Ok(()))
                    } else {
                        JmapCoroutineState::Complete(Err(JmapMailboxDeleteError::NotDestroyed(id)))
                    }
                }
                JmapCoroutineState::Yielded(y) => {
                    self.state = State::Destroying { set, id };
                    JmapCoroutineState::Yielded(y)
                }
                JmapCoroutineState::Complete(Err(err)) => {
                    JmapCoroutineState::Complete(Err(err.into()))
                }
            },
            State::Done => {
                JmapCoroutineState::Complete(Err(JmapMailboxDeleteError::ResumedAfterDone))
            }
        }
    }
}
