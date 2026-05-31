//! JMAP message-copy coroutine.
//!
//! Single-stage state machine:
//! `Email/set { update: AddToMailbox(target_id) per id }` adds the
//! target mailbox reference to every requested email while leaving
//! the existing mailboxIds intact (JMAP's "copy" is conceptually
//! "an email referenced from N mailboxes").
//!
//! The shared `from` parameter is only used for symmetry with
//! Maildir / m2dir: JMAP doesn't need it because the existing
//! mailboxIds carry the source reference automatically.

use alloc::{string::String, vec::Vec};
use core::mem;

use io_jmap::{
    coroutine::{JmapCoroutine, JmapCoroutineState, JmapYield},
    rfc8620::session::JmapSession,
    rfc8621::email_set::{JmapEmailSet as InnerSet, JmapEmailSetArgs, JmapEmailSetError as SetErr},
};
use log::trace;
use secrecy::SecretString;
use thiserror::Error;

/// Errors produced by [`JmapMessageCopy`].
#[derive(Debug, Error)]
pub enum JmapMessageCopyError {
    #[error(transparent)]
    Set(#[from] SetErr),
    #[error("Email/set returned per-id failures: {0:?}")]
    NotUpdated(Vec<String>),
    #[error("coroutine was resumed after completion")]
    ResumedAfterDone,
}

/// I/O-free coroutine adding `to` (a JMAP mailbox id) to the
/// mailboxIds of every email id.
pub struct JmapMessageCopy {
    state: State,
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
        let mut args = JmapEmailSetArgs::default();
        for id in ids {
            args.add_to_mailbox(*id, to);
        }
        let set = InnerSet::new(session, http_auth, args)?;
        Ok(Self {
            state: State::Patching(set),
        })
    }
}

enum State {
    Patching(InnerSet),
    Done,
}

impl JmapCoroutine for JmapMessageCopy {
    type Yield = JmapYield;
    type Return = Result<(), JmapMessageCopyError>;

    fn resume(&mut self, bytes: Option<&[u8]>) -> JmapCoroutineState<Self::Yield, Self::Return> {
        match mem::replace(&mut self.state, State::Done) {
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
