//! JMAP mailbox-create coroutine: `Mailbox/set { create }` (RFC 8621
//! §2.6). Creates a top-level mailbox whose display name is `name`.
//! Parent / role / sort-order belong to a protocol-specific extension.

use alloc::{collections::BTreeMap, string::String};

use io_jmap::{
    coroutine::{JmapCoroutine, JmapCoroutineState, JmapYield},
    rfc8620::session::JmapSession,
    rfc8621::{
        mailbox::MailboxCreate,
        mailbox_set::{
            JmapMailboxSet as InnerSet, JmapMailboxSetArgs, JmapMailboxSetError as InnerErr,
        },
    },
};
use log::trace;
use secrecy::SecretString;
use thiserror::Error;

/// Errors produced by [`JmapMailboxCreate`].
#[derive(Debug, Error)]
pub enum JmapMailboxCreateError {
    #[error(transparent)]
    Set(#[from] InnerErr),
    #[error("Mailbox/set did not create a mailbox for `{0}`")]
    NotCreated(String),
}

/// I/O-free coroutine creating a JMAP mailbox named `name`.
pub struct JmapMailboxCreate {
    inner: InnerSet,
    client_id: String,
}

impl JmapMailboxCreate {
    pub fn new(
        session: &JmapSession,
        http_auth: &SecretString,
        name: &str,
    ) -> Result<Self, JmapMailboxCreateError> {
        trace!("prepare JMAP mailbox create");
        let client_id = String::from("new");
        let mut create = BTreeMap::new();
        create.insert(
            client_id.clone(),
            MailboxCreate {
                name: Some(name.into()),
                ..MailboxCreate::default()
            },
        );
        let args = JmapMailboxSetArgs {
            create: Some(create),
            ..JmapMailboxSetArgs::default()
        };
        let inner = InnerSet::new(session, http_auth, args)?;
        Ok(Self { inner, client_id })
    }
}

impl JmapCoroutine for JmapMailboxCreate {
    type Yield = JmapYield;
    type Return = Result<(), JmapMailboxCreateError>;

    fn resume(&mut self, bytes: Option<&[u8]>) -> JmapCoroutineState<Self::Yield, Self::Return> {
        match self.inner.resume(bytes) {
            JmapCoroutineState::Yielded(y) => JmapCoroutineState::Yielded(y),
            JmapCoroutineState::Complete(Ok(ok)) => {
                if ok.created.contains_key(&self.client_id) {
                    JmapCoroutineState::Complete(Ok(()))
                } else {
                    JmapCoroutineState::Complete(Err(JmapMailboxCreateError::NotCreated(
                        self.client_id.clone(),
                    )))
                }
            }
            JmapCoroutineState::Complete(Err(err)) => JmapCoroutineState::Complete(Err(err.into())),
        }
    }
}
