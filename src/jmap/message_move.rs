//! JMAP message-move coroutine.
//!
//! Two-stage state machine:
//! 1. `Mailbox/query + Mailbox/get` with no filter returns every
//!    mailbox the account can see; the client picks the `from` and
//!    `to` ids by exact-name match.
//! 2. `Email/set { update: AddToMailbox(to_id) + RemoveFromMailbox(from_id)
//!    per id }` rewires the message in a single round-trip.
//!
//! Fetching every mailbox in stage 1 is the same trade-off as the
//! unified `list_mailboxes`: one round-trip instead of two, at the
//! cost of a larger response payload. For accounts with thousands of
//! folders, a future implementation should batch two filtered
//! `Mailbox/query` calls instead.

use alloc::{string::String, vec::Vec};
use core::mem;

use io_jmap::{
    coroutine::{JmapCoroutine, JmapCoroutineState, JmapYield},
    rfc8620::session::JmapSession,
    rfc8621::{
        email_set::{JmapEmailSet as InnerSet, JmapEmailSetArgs, JmapEmailSetError as SetErr},
        mailbox_query::{JmapMailboxQuery as InnerQuery, JmapMailboxQueryError as QueryErr},
    },
};
use log::trace;
use secrecy::SecretString;
use thiserror::Error;

use crate::{
    coroutine::{EmailBackend, EmailCoroutine, EmailCoroutineArg, EmailCoroutineState, JmapStep},
    jmap::convert::find_mailbox_id,
};

/// Errors produced by [`JmapMessageMove`].
#[derive(Debug, Error)]
pub enum JmapMessageMoveError {
    #[error(transparent)]
    Query(#[from] QueryErr),
    #[error(transparent)]
    Set(#[from] SetErr),
    #[error("no JMAP mailbox named `{0}` found")]
    NotFound(String),
    #[error("Email/set returned per-id failures: {0:?}")]
    NotUpdated(Vec<String>),
    #[error("coroutine was resumed with the wrong EmailCoroutineArg variant")]
    InvalidArg,
    #[error("coroutine was resumed after completion")]
    ResumedAfterDone,
}

/// I/O-free coroutine moving every id from `from` to `to`.
pub struct JmapMessageMove {
    state: State,
    from_name: String,
    to_name: String,
    ids: Vec<String>,
    session: JmapSession,
    http_auth: SecretString,
}

impl JmapMessageMove {
    pub fn new(
        session: &JmapSession,
        http_auth: &SecretString,
        from: &str,
        to: &str,
        ids: &[&str],
    ) -> Result<Self, JmapMessageMoveError> {
        trace!("prepare JMAP message move");
        let query = InnerQuery::new(session, http_auth, None, None, None, None, None)?;
        Ok(Self {
            state: State::Resolving(query),
            from_name: from.into(),
            to_name: to.into(),
            ids: ids.iter().map(|s| (*s).into()).collect(),
            session: session.clone(),
            http_auth: http_auth.clone(),
        })
    }
}

impl EmailCoroutine for JmapMessageMove {
    type Yield = JmapStep;
    type Return = Result<(), JmapMessageMoveError>;

    const BACKEND: EmailBackend = EmailBackend::Jmap;

    fn resume(
        &mut self,
        arg: EmailCoroutineArg<'_>,
    ) -> EmailCoroutineState<Self::Yield, Self::Return> {
        #[allow(irrefutable_let_patterns)]
        let EmailCoroutineArg::Jmap { bytes } = arg else {
            return EmailCoroutineState::Complete(Err(JmapMessageMoveError::InvalidArg));
        };

        match mem::replace(&mut self.state, State::Done) {
            State::Resolving(mut query) => match query.resume(bytes) {
                JmapCoroutineState::Complete(Ok(ok)) => {
                    let Some(from_id) = find_mailbox_id(&ok.mailboxes, &self.from_name) else {
                        return EmailCoroutineState::Complete(Err(JmapMessageMoveError::NotFound(
                            self.from_name.clone(),
                        )));
                    };
                    let Some(to_id) = find_mailbox_id(&ok.mailboxes, &self.to_name) else {
                        return EmailCoroutineState::Complete(Err(JmapMessageMoveError::NotFound(
                            self.to_name.clone(),
                        )));
                    };
                    let mut args = JmapEmailSetArgs::default();
                    for id in &self.ids {
                        args.add_to_mailbox(id.clone(), to_id.clone());
                        args.remove_from_mailbox(id.clone(), from_id.clone());
                    }
                    let set = match InnerSet::new(&self.session, &self.http_auth, args) {
                        Ok(set) => set,
                        Err(err) => return EmailCoroutineState::Complete(Err(err.into())),
                    };
                    self.state = State::Patching(set);
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
            State::Patching(mut set) => match set.resume(bytes) {
                JmapCoroutineState::Complete(Ok(ok)) => {
                    if ok.not_updated.is_empty() {
                        EmailCoroutineState::Complete(Ok(()))
                    } else {
                        EmailCoroutineState::Complete(Err(JmapMessageMoveError::NotUpdated(
                            ok.not_updated.into_keys().collect(),
                        )))
                    }
                }
                JmapCoroutineState::Yielded(JmapYield::WantsRead) => {
                    self.state = State::Patching(set);
                    EmailCoroutineState::Yielded(JmapStep::WantsRead)
                }
                JmapCoroutineState::Yielded(JmapYield::WantsWrite(out)) => {
                    self.state = State::Patching(set);
                    EmailCoroutineState::Yielded(JmapStep::WantsWrite(out))
                }
                JmapCoroutineState::Complete(Err(err)) => {
                    EmailCoroutineState::Complete(Err(err.into()))
                }
            },
            State::Done => {
                EmailCoroutineState::Complete(Err(JmapMessageMoveError::ResumedAfterDone))
            }
        }
    }
}

enum State {
    Resolving(InnerQuery),
    Patching(InnerSet),
    Done,
}
