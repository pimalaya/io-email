//! JMAP message move (`Mailbox/query` + `Mailbox/get` → `Email/set`),
//! wrapping a private orchestrator that resolves the source and
//! destination mailbox names to ids, then patches each email's
//! `mailboxIds` to remove the source and add the destination.

use alloc::{
    string::{String, ToString},
    vec::Vec,
};
use core::mem;

use io_jmap::{
    rfc8620::session::JmapSession,
    rfc8621::{
        email_set::{JmapEmailSet, JmapEmailSetArgs, JmapEmailSetError, JmapEmailSetResult},
        mailbox_query::{JmapMailboxQuery, JmapMailboxQueryError, JmapMailboxQueryResult},
    },
};
use log::trace;
use secrecy::SecretString;
use thiserror::Error;

use crate::jmap::message_copy::find_mailbox_id;

/// Errors produced while orchestrating Mailbox lookup + Email/set for
/// JMAP message move.
#[derive(Debug, Error)]
pub enum MessageMoveError {
    #[error(transparent)]
    MailboxQuery(#[from] JmapMailboxQueryError),
    #[error(transparent)]
    EmailSet(#[from] JmapEmailSetError),
    #[error("no JMAP mailbox matched the name {0:?}")]
    UnknownMailbox(String),
    #[error("JMAP message move was resumed after completion")]
    AlreadyDone,
}

/// Result returned by [`MessageMove::resume`].
#[derive(Debug)]
pub enum MessageMoveResult {
    Ok,
    WantsRead,
    WantsWrite(Vec<u8>),
    Err(MessageMoveError),
}

/// I/O-free orchestrator that resolves the source and destination
/// mailbox names to ids, then issues `Email/set` patches that remove
/// each email from the source mailbox and add it to the destination.
pub struct MessageMove {
    inner: Inner,
    pending: Option<Pending>,
}

struct Pending {
    session: JmapSession,
    http_auth: SecretString,
    ids: Vec<String>,
    from_name: String,
    to_name: String,
}

enum Inner {
    Resolving(JmapMailboxQuery),
    Setting(JmapEmailSet),
    Done,
}

impl MessageMove {
    /// Builds the orchestrator.
    pub fn new(
        session: &JmapSession,
        http_auth: &SecretString,
        ids: impl IntoIterator<Item = String>,
        from_name: impl ToString,
        to_name: impl ToString,
    ) -> Result<Self, MessageMoveError> {
        trace!("prepare JMAP message move");
        let query = JmapMailboxQuery::new(session, http_auth, None, None, None, None, None)?;
        let pending = Pending {
            session: session.clone(),
            http_auth: http_auth.clone(),
            ids: ids.into_iter().collect(),
            from_name: from_name.to_string(),
            to_name: to_name.to_string(),
        };
        Ok(Self {
            inner: Inner::Resolving(query),
            pending: Some(pending),
        })
    }

    /// Advances the orchestrator. Drives Mailbox/query first, then
    /// Email/set.
    pub fn resume(&mut self, arg: Option<&[u8]>) -> MessageMoveResult {
        let mut input = arg;

        loop {
            let inner = mem::replace(&mut self.inner, Inner::Done);

            match inner {
                Inner::Resolving(mut query) => match query.resume(input.take()) {
                    JmapMailboxQueryResult::WantsRead => {
                        self.inner = Inner::Resolving(query);
                        return MessageMoveResult::WantsRead;
                    }
                    JmapMailboxQueryResult::WantsWrite(bytes) => {
                        self.inner = Inner::Resolving(query);
                        return MessageMoveResult::WantsWrite(bytes);
                    }
                    JmapMailboxQueryResult::Err(err) => {
                        return MessageMoveResult::Err(err.into());
                    }
                    JmapMailboxQueryResult::Ok { mailboxes, .. } => {
                        let pending = self.pending.take().expect("pending set on construct");

                        let Some(from_id) = find_mailbox_id(&mailboxes, &pending.from_name) else {
                            return MessageMoveResult::Err(MessageMoveError::UnknownMailbox(
                                pending.from_name,
                            ));
                        };
                        let Some(to_id) = find_mailbox_id(&mailboxes, &pending.to_name) else {
                            return MessageMoveResult::Err(MessageMoveError::UnknownMailbox(
                                pending.to_name,
                            ));
                        };

                        let mut args = JmapEmailSetArgs::default();
                        for id in &pending.ids {
                            args.remove_from_mailbox(id.clone(), from_id.clone());
                            args.add_to_mailbox(id.clone(), to_id.clone());
                        }

                        let set =
                            match JmapEmailSet::new(&pending.session, &pending.http_auth, args) {
                                Ok(s) => s,
                                Err(err) => return MessageMoveResult::Err(err.into()),
                            };
                        self.inner = Inner::Setting(set);
                    }
                },
                Inner::Setting(mut set) => match set.resume(input.take()) {
                    JmapEmailSetResult::WantsRead => {
                        self.inner = Inner::Setting(set);
                        return MessageMoveResult::WantsRead;
                    }
                    JmapEmailSetResult::WantsWrite(bytes) => {
                        self.inner = Inner::Setting(set);
                        return MessageMoveResult::WantsWrite(bytes);
                    }
                    JmapEmailSetResult::Err(err) => return MessageMoveResult::Err(err.into()),
                    JmapEmailSetResult::Ok { .. } => return MessageMoveResult::Ok,
                },
                Inner::Done => return MessageMoveResult::Err(MessageMoveError::AlreadyDone),
            }
        }
    }
}
