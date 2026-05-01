//! JMAP mailbox listing (`Mailbox/query` + `Mailbox/get`), wrapping
//! [`io_jmap::rfc8621::mailbox_query::JmapMailboxQuery`] and producing
//! the shared [`Mailbox`](crate::mailbox::Mailbox) type on completion.
//!
//! `total`/`unread` are populated unconditionally because JMAP returns
//! them in the same `Mailbox/get` response — there is no extra
//! round-trip to skip.

use alloc::vec::Vec;

use io_jmap::{
    rfc8620::session::JmapSession,
    rfc8621::{
        mailbox::Mailbox as JmapMailbox,
        mailbox_query::{JmapMailboxQuery, JmapMailboxQueryError, JmapMailboxQueryResult},
    },
};
use log::trace;
use secrecy::SecretString;

use crate::mailbox::Mailbox;

/// I/O-free coroutine listing every JMAP mailbox in the session's
/// primary mail account. Issues `Mailbox/query` + `Mailbox/get`.
pub struct MailboxList {
    inner: JmapMailboxQuery,
}

impl MailboxList {
    /// Builds the coroutine from a JMAP session and the bearer/basic
    /// HTTP credential.
    pub fn new(
        session: &JmapSession,
        http_auth: &SecretString,
    ) -> Result<Self, JmapMailboxQueryError> {
        trace!("prepare JMAP mailbox listing");
        let inner = JmapMailboxQuery::new(session, http_auth, None, None, None, None, None)?;
        Ok(Self { inner })
    }

    /// Advances the coroutine.
    pub fn resume(&mut self, arg: Option<&[u8]>) -> MailboxListResult {
        match self.inner.resume(arg) {
            JmapMailboxQueryResult::WantsRead => MailboxListResult::WantsRead,
            JmapMailboxQueryResult::WantsWrite(bytes) => MailboxListResult::WantsWrite(bytes),
            JmapMailboxQueryResult::Ok { mailboxes, .. } => {
                let mailboxes = mailboxes.into_iter().map(Mailbox::from).collect();
                MailboxListResult::Ok(mailboxes)
            }
            JmapMailboxQueryResult::Err(err) => MailboxListResult::Err(err),
        }
    }
}

/// Result returned by [`MailboxList::resume`].
#[derive(Debug)]
pub enum MailboxListResult {
    Ok(Vec<Mailbox>),
    WantsRead,
    WantsWrite(Vec<u8>),
    Err(JmapMailboxQueryError),
}

impl From<JmapMailbox> for Mailbox {
    fn from(mbox: JmapMailbox) -> Self {
        Self {
            id: mbox.id.unwrap_or_default(),
            name: mbox.name.unwrap_or_default(),
            total: Some(u64::from(mbox.total_emails)),
            unread: Some(u64::from(mbox.unread_emails)),
        }
    }
}
