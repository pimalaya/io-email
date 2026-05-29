//! JMAP message-copy coroutine.
//!
//! Two-stage state machine:
//! 1. `Mailbox/query + Mailbox/get` with a name filter resolves the
//!    target mailbox name to an id.
//! 2. `Email/set { update: AddToMailbox(target_id) per id }` adds the
//!    target mailbox reference to every requested email while leaving
//!    the existing mailboxIds intact (JMAP's "copy" is conceptually
//!    "an email referenced from N mailboxes").
//!
//! The shared `from` parameter is only used for symmetry with
//! Maildir / m2dir: JMAP doesn't need it because the existing
//! mailboxIds carry the source reference automatically.

use alloc::{string::String, vec, vec::Vec};
use core::mem;

use io_jmap::{
    coroutine::{JmapCoroutine, JmapCoroutineState, JmapYield},
    rfc8620::session::JmapSession,
    rfc8621::{
        email_set::{JmapEmailSet as InnerSet, JmapEmailSetArgs, JmapEmailSetError as SetErr},
        mailbox::{MailboxFilter, MailboxProperty},
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

/// Errors produced by [`JmapMessageCopy`].
#[derive(Debug, Error)]
pub enum JmapMessageCopyError {
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

/// I/O-free coroutine adding `to` to the mailboxIds of every id.
pub struct JmapMessageCopy {
    state: State,
    target_name: String,
    ids: Vec<String>,
    session: JmapSession,
    http_auth: SecretString,
}

impl JmapMessageCopy {
    pub fn new(
        session: &JmapSession,
        http_auth: &SecretString,
        _from: &str,
        to: &str,
        ids: &[&str],
    ) -> Result<Self, JmapMessageCopyError> {
        trace!("prepare JMAP message copy");
        let query = InnerQuery::new(
            session,
            http_auth,
            Some(MailboxFilter {
                name: Some(to.into()),
                ..MailboxFilter::default()
            }),
            None,
            None,
            None,
            Some(vec![MailboxProperty::Id, MailboxProperty::Name]),
        )?;
        Ok(Self {
            state: State::Resolving(query),
            target_name: to.into(),
            ids: ids.iter().map(|s| (*s).into()).collect(),
            session: session.clone(),
            http_auth: http_auth.clone(),
        })
    }
}

impl EmailCoroutine for JmapMessageCopy {
    type Yield = JmapStep;
    type Return = Result<(), JmapMessageCopyError>;

    const BACKEND: EmailBackend = EmailBackend::Jmap;

    fn resume(
        &mut self,
        arg: EmailCoroutineArg<'_>,
    ) -> EmailCoroutineState<Self::Yield, Self::Return> {
        #[allow(irrefutable_let_patterns)]
        let EmailCoroutineArg::Jmap { bytes } = arg else {
            return EmailCoroutineState::Complete(Err(JmapMessageCopyError::InvalidArg));
        };

        match mem::replace(&mut self.state, State::Done) {
            State::Resolving(mut query) => match query.resume(bytes) {
                JmapCoroutineState::Complete(Ok(ok)) => {
                    let Some(target_id) = find_mailbox_id(&ok.mailboxes, &self.target_name) else {
                        return EmailCoroutineState::Complete(Err(JmapMessageCopyError::NotFound(
                            self.target_name.clone(),
                        )));
                    };
                    let mut args = JmapEmailSetArgs::default();
                    for id in &self.ids {
                        args.add_to_mailbox(id.clone(), target_id.clone());
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
                        EmailCoroutineState::Complete(Err(JmapMessageCopyError::NotUpdated(
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
                EmailCoroutineState::Complete(Err(JmapMessageCopyError::ResumedAfterDone))
            }
        }
    }
}

enum State {
    Resolving(InnerQuery),
    Patching(InnerSet),
    Done,
}
