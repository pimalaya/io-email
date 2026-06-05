//! JMAP mailbox-delete coroutine wrapping Mailbox/set { destroy }
//! with onDestroyRemoveEmails to match IMAP semantics.
//!
//! # Example
//!
//! ```rust,ignore
//! use io_email::jmap::mailbox_delete::JmapMailboxDelete;
//!
//! client.run(JmapMailboxDelete::new(&session, &auth, "mailbox-id")?)?;
//! ```

use alloc::{string::String, vec};
use core::mem;

use io_jmap::{
    coroutine::{JmapCoroutine, JmapCoroutineState, JmapYield},
    rfc8620::JmapSession,
    rfc8621::mailbox::set::{
        JmapMailboxSet as InnerSet, JmapMailboxSetArgs, JmapMailboxSetError as SetErr,
    },
};
use log::trace;
use secrecy::SecretString;
use thiserror::Error;

/// Errors produced by [`JmapMailboxDelete`].
#[derive(Debug, Error)]
pub enum JmapMailboxDeleteError {
    #[error(transparent)]
    Set(#[from] SetErr),
    #[error("Mailbox/set did not destroy `{0}`")]
    NotDestroyed(String),
    #[error("coroutine was resumed after completion")]
    ResumedAfterDone,
}

/// I/O-free coroutine deleting a JMAP mailbox by id.
pub struct JmapMailboxDelete {
    state: State,
    id: String,
}

impl JmapMailboxDelete {
    pub fn new(
        session: &JmapSession,
        http_auth: &SecretString,
        id: &str,
    ) -> Result<Self, JmapMailboxDeleteError> {
        trace!("prepare JMAP mailbox delete");
        let args = JmapMailboxSetArgs {
            destroy: Some(vec![id.into()]),
            on_destroy_remove_emails: Some(true),
            ..JmapMailboxSetArgs::default()
        };
        let set = InnerSet::new(session, http_auth, args)?;
        Ok(Self {
            state: State::Destroying(set),
            id: id.into(),
        })
    }
}

enum State {
    Destroying(InnerSet),
    Done,
}

impl JmapCoroutine for JmapMailboxDelete {
    type Yield = JmapYield;
    type Return = Result<(), JmapMailboxDeleteError>;

    fn resume(&mut self, bytes: Option<&[u8]>) -> JmapCoroutineState<Self::Yield, Self::Return> {
        match mem::replace(&mut self.state, State::Done) {
            State::Destroying(mut set) => match set.resume(bytes) {
                JmapCoroutineState::Complete(Ok(ok)) => {
                    if ok.destroyed.iter().any(|d| d == &self.id) {
                        JmapCoroutineState::Complete(Ok(()))
                    } else {
                        JmapCoroutineState::Complete(Err(JmapMailboxDeleteError::NotDestroyed(
                            mem::take(&mut self.id),
                        )))
                    }
                }
                JmapCoroutineState::Yielded(y) => {
                    self.state = State::Destroying(set);
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
