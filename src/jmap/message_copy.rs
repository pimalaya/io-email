//! JMAP message copy (`Mailbox/query` + `Mailbox/get` → `Email/set`),
//! wrapping a private orchestrator that first resolves the destination
//! mailbox name to an id, then patches each email's `mailboxIds` to
//! add the destination.
//!
//! Single-account, in-place copy. Cross-account copy would use
//! [`io_jmap::rfc8621::email_copy::JmapEmailCopy`] and is intentionally
//! out of scope here.

use alloc::{
    string::{String, ToString},
    vec::Vec,
};
use core::mem;

use io_jmap::{
    rfc8620::session::JmapSession,
    rfc8621::{
        email_set::{JmapEmailSet, JmapEmailSetArgs, JmapEmailSetError, JmapEmailSetResult},
        mailbox::Mailbox,
        mailbox_query::{JmapMailboxQuery, JmapMailboxQueryError, JmapMailboxQueryResult},
    },
};
use log::trace;
use secrecy::SecretString;
use thiserror::Error;

/// Errors produced while orchestrating Mailbox lookup + Email/set for
/// JMAP message copy.
#[derive(Debug, Error)]
pub enum MessageCopyError {
    #[error(transparent)]
    MailboxQuery(#[from] JmapMailboxQueryError),
    #[error(transparent)]
    EmailSet(#[from] JmapEmailSetError),
    #[error("no JMAP mailbox matched the name {0:?}")]
    UnknownMailbox(String),
    #[error("JMAP message copy was resumed after completion")]
    AlreadyDone,
}

/// Result returned by [`MessageCopy::resume`].
#[derive(Debug)]
pub enum MessageCopyResult {
    Ok,
    WantsRead,
    WantsWrite(Vec<u8>),
    Err(MessageCopyError),
}

/// I/O-free orchestrator that resolves the destination mailbox name
/// to an id, then issues `Email/set` to add every email id to that
/// mailbox.
pub struct MessageCopy {
    inner: Inner,
    pending: Option<Pending>,
}

struct Pending {
    session: JmapSession,
    http_auth: SecretString,
    ids: Vec<String>,
    to_name: String,
}

enum Inner {
    Resolving(JmapMailboxQuery),
    Setting(JmapEmailSet),
    Done,
}

impl MessageCopy {
    /// Builds the orchestrator.
    pub fn new(
        session: &JmapSession,
        http_auth: &SecretString,
        ids: impl IntoIterator<Item = String>,
        to_name: impl ToString,
    ) -> Result<Self, MessageCopyError> {
        trace!("prepare JMAP message copy");
        let query = JmapMailboxQuery::new(session, http_auth, None, None, None, None, None)?;
        let pending = Pending {
            session: session.clone(),
            http_auth: http_auth.clone(),
            ids: ids.into_iter().collect(),
            to_name: to_name.to_string(),
        };
        Ok(Self {
            inner: Inner::Resolving(query),
            pending: Some(pending),
        })
    }

    /// Advances the orchestrator. Drives Mailbox/query first, then
    /// Email/set.
    pub fn resume(&mut self, arg: Option<&[u8]>) -> MessageCopyResult {
        let mut input = arg;

        loop {
            let inner = mem::replace(&mut self.inner, Inner::Done);

            match inner {
                Inner::Resolving(mut query) => match query.resume(input.take()) {
                    JmapMailboxQueryResult::WantsRead => {
                        self.inner = Inner::Resolving(query);
                        return MessageCopyResult::WantsRead;
                    }
                    JmapMailboxQueryResult::WantsWrite(bytes) => {
                        self.inner = Inner::Resolving(query);
                        return MessageCopyResult::WantsWrite(bytes);
                    }
                    JmapMailboxQueryResult::Err(err) => {
                        return MessageCopyResult::Err(err.into());
                    }
                    JmapMailboxQueryResult::Ok { mailboxes, .. } => {
                        let pending = self.pending.take().expect("pending set on construct");

                        let Some(to_id) = find_mailbox_id(&mailboxes, &pending.to_name) else {
                            return MessageCopyResult::Err(MessageCopyError::UnknownMailbox(
                                pending.to_name,
                            ));
                        };

                        let mut args = JmapEmailSetArgs::default();
                        for id in &pending.ids {
                            args.add_to_mailbox(id.clone(), to_id.clone());
                        }

                        let set =
                            match JmapEmailSet::new(&pending.session, &pending.http_auth, args) {
                                Ok(s) => s,
                                Err(err) => return MessageCopyResult::Err(err.into()),
                            };
                        self.inner = Inner::Setting(set);
                    }
                },
                Inner::Setting(mut set) => match set.resume(input.take()) {
                    JmapEmailSetResult::WantsRead => {
                        self.inner = Inner::Setting(set);
                        return MessageCopyResult::WantsRead;
                    }
                    JmapEmailSetResult::WantsWrite(bytes) => {
                        self.inner = Inner::Setting(set);
                        return MessageCopyResult::WantsWrite(bytes);
                    }
                    JmapEmailSetResult::Err(err) => return MessageCopyResult::Err(err.into()),
                    JmapEmailSetResult::Ok { .. } => return MessageCopyResult::Ok,
                },
                Inner::Done => return MessageCopyResult::Err(MessageCopyError::AlreadyDone),
            }
        }
    }
}

/// Finds a mailbox by exact-match name and returns its id.
pub(crate) fn find_mailbox_id(mailboxes: &[Mailbox], name: &str) -> Option<String> {
    mailboxes
        .iter()
        .find(|m| m.name.as_deref() == Some(name))
        .and_then(|m| m.id.clone())
}
