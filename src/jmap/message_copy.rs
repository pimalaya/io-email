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

use crate::jmap::convert::find_mailbox_id;

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

enum State {
    Resolving(InnerQuery),
    Patching(InnerSet),
    Done,
}

impl JmapCoroutine for JmapMessageCopy {
    type Yield = JmapYield;
    type Return = Result<(), JmapMessageCopyError>;

    fn resume(&mut self, bytes: Option<&[u8]>) -> JmapCoroutineState<Self::Yield, Self::Return> {
        match mem::replace(&mut self.state, State::Done) {
            State::Resolving(mut query) => match query.resume(bytes) {
                JmapCoroutineState::Complete(Ok(ok)) => {
                    let Some(target_id) = find_mailbox_id(&ok.mailboxes, &self.target_name) else {
                        return JmapCoroutineState::Complete(Err(JmapMessageCopyError::NotFound(
                            self.target_name.clone(),
                        )));
                    };
                    let mut args = JmapEmailSetArgs::default();
                    for id in &self.ids {
                        args.add_to_mailbox(id.clone(), target_id.clone());
                    }
                    let set = match InnerSet::new(&self.session, &self.http_auth, args) {
                        Ok(set) => set,
                        Err(err) => return JmapCoroutineState::Complete(Err(err.into())),
                    };
                    self.state = State::Patching(set);
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
            State::Patching(mut set) => match set.resume(bytes) {
                JmapCoroutineState::Complete(Ok(ok)) => {
                    if ok.not_updated.is_empty() {
                        JmapCoroutineState::Complete(Ok(()))
                    } else {
                        JmapCoroutineState::Complete(Err(JmapMessageCopyError::NotUpdated(
                            ok.not_updated.into_keys().collect(),
                        )))
                    }
                }
                JmapCoroutineState::Yielded(y) => {
                    self.state = State::Patching(set);
                    JmapCoroutineState::Yielded(y)
                }
                JmapCoroutineState::Complete(Err(err)) => {
                    JmapCoroutineState::Complete(Err(err.into()))
                }
            },
            State::Done => {
                JmapCoroutineState::Complete(Err(JmapMessageCopyError::ResumedAfterDone))
            }
        }
    }
}
