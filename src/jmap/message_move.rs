//! JMAP message-move coroutine.
//!
//! Single-stage state machine:
//! `Email/set { update: AddToMailbox(to_id) + RemoveFromMailbox(from_id)
//! per id }` rewires the message in a single round-trip.

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

/// Errors produced by [`JmapMessageMove`].
#[derive(Debug, Error)]
pub enum JmapMessageMoveError {
    #[error(transparent)]
    Set(#[from] SetErr),
    #[error("Email/set returned per-id failures: {0:?}")]
    NotUpdated(Vec<String>),
    #[error("coroutine was resumed after completion")]
    ResumedAfterDone,
}

/// I/O-free coroutine moving every id from `from` to `to`. Both are
/// JMAP mailbox ids.
pub struct JmapMessageMove {
    state: State,
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
        let mut args = JmapEmailSetArgs::default();
        for id in ids {
            args.add_to_mailbox(*id, to);
            args.remove_from_mailbox(*id, from);
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

impl JmapCoroutine for JmapMessageMove {
    type Yield = JmapYield;
    type Return = Result<(), JmapMessageMoveError>;

    fn resume(&mut self, bytes: Option<&[u8]>) -> JmapCoroutineState<Self::Yield, Self::Return> {
        match mem::replace(&mut self.state, State::Done) {
            State::Patching(mut set) => match set.resume(bytes) {
                JmapCoroutineState::Complete(Ok(ok)) => {
                    if ok.not_updated.is_empty() {
                        JmapCoroutineState::Complete(Ok(()))
                    } else {
                        JmapCoroutineState::Complete(Err(JmapMessageMoveError::NotUpdated(
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
                JmapCoroutineState::Complete(Err(JmapMessageMoveError::ResumedAfterDone))
            }
        }
    }
}
