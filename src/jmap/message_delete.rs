//! JMAP message-delete coroutine: `Email/set { destroy }` (RFC 8621
//! §4.7). Removes the email from every mailbox it references; JMAP's
//! data model treats `destroy` as a global delete.

use alloc::{string::String, vec};

use io_jmap::{
    coroutine::{JmapCoroutine, JmapCoroutineState, JmapYield},
    rfc8620::session::JmapSession,
    rfc8621::email_set::{
        JmapEmailSet as InnerSet, JmapEmailSetArgs, JmapEmailSetError as InnerErr,
    },
};
use log::trace;
use secrecy::SecretString;
use thiserror::Error;

/// Errors produced by [`JmapMessageDelete`].
#[derive(Debug, Error)]
pub enum JmapMessageDeleteError {
    #[error(transparent)]
    Set(#[from] InnerErr),
    #[error("Email/set did not destroy `{0}`")]
    NotDestroyed(String),
}

/// I/O-free coroutine destroying a single JMAP email by id.
pub struct JmapMessageDelete {
    inner: InnerSet,
    id: String,
}

impl JmapMessageDelete {
    /// `mailbox` is part of the shared signature for symmetry with
    /// IMAP / Maildir but is unused: JMAP destroy is global.
    pub fn new(
        session: &JmapSession,
        http_auth: &SecretString,
        _mailbox: &str,
        id: &str,
    ) -> Result<Self, JmapMessageDeleteError> {
        trace!("prepare JMAP message delete");
        let args = JmapEmailSetArgs {
            destroy: Some(vec![id.into()]),
            ..JmapEmailSetArgs::default()
        };
        Ok(Self {
            inner: InnerSet::new(session, http_auth, args)?,
            id: id.into(),
        })
    }
}

impl JmapCoroutine for JmapMessageDelete {
    type Yield = JmapYield;
    type Return = Result<(), JmapMessageDeleteError>;

    fn resume(&mut self, bytes: Option<&[u8]>) -> JmapCoroutineState<Self::Yield, Self::Return> {
        match self.inner.resume(bytes) {
            JmapCoroutineState::Yielded(y) => JmapCoroutineState::Yielded(y),
            JmapCoroutineState::Complete(Ok(ok)) => {
                if ok.destroyed.iter().any(|d| d == &self.id) {
                    JmapCoroutineState::Complete(Ok(()))
                } else {
                    JmapCoroutineState::Complete(Err(JmapMessageDeleteError::NotDestroyed(
                        self.id.clone(),
                    )))
                }
            }
            JmapCoroutineState::Complete(Err(err)) => JmapCoroutineState::Complete(Err(err.into())),
        }
    }
}
