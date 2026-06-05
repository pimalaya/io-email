//! JMAP message-copy coroutine wrapping Email/set with an
//! AddToMailbox patch per id.
//!
//! JMAP copy is "an email referenced from N mailboxes"; the source
//! `from` is unused (only there for shared-API symmetry).
//!
//! # Example
//!
//! ```rust,ignore
//! use io_email::message::jmap::copy::JmapMessageCopy;
//!
//! client.run(JmapMessageCopy::new(&session, &auth, "_", "dst-id", &["email-id"])?)?;
//! ```

use alloc::{string::String, vec::Vec};
use core::mem;

use io_jmap::{
    coroutine::{JmapCoroutine, JmapCoroutineState, JmapYield},
    rfc8620::JmapSession,
    rfc8621::email::set::{
        JmapEmailSet as InnerSet, JmapEmailSetArgs, JmapEmailSetError as SetErr,
    },
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

/// I/O-free coroutine adding `to` to the mailboxIds of every email id.
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
