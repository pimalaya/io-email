//! JMAP list-mailboxes coroutine wrapping a batched Mailbox/query +
//! Mailbox/get (RFC 8621 §2.4 + §2.5).
//!
//! `with_counts` only widens the requested property set: JMAP returns
//! totalEmails/unreadEmails inline, no second round-trip.
//!
//! # Example
//!
//! ```rust,ignore
//! use io_email::mailbox::jmap::list::JmapMailboxList;
//!
//! let mailboxes = client.run(JmapMailboxList::new(&session, &auth, true)?)?;
//! ```

use alloc::{vec, vec::Vec};

use io_jmap::{
    coroutine::{JmapCoroutine, JmapCoroutineState, JmapYield},
    rfc8620::JmapSession,
    rfc8621::mailbox::{
        JmapMailbox as JmapMailboxObject, JmapMailboxProperty,
        query::{
            JmapMailboxQuery, JmapMailboxQueryError, JmapMailboxQueryOptions,
            JmapMailboxQueryOutput,
        },
    },
};
use log::trace;
use secrecy::SecretString;
use thiserror::Error;

use crate::mailbox::types::Mailbox;

/// Errors produced by [`JmapMailboxList`].
#[derive(Debug, Error)]
pub enum JmapMailboxListError {
    #[error(transparent)]
    Query(#[from] JmapMailboxQueryError),
}

/// I/O-free coroutine listing every JMAP mailbox in the primary mail
/// account.
pub struct JmapMailboxList {
    inner: JmapMailboxQuery,
}

impl JmapMailboxList {
    /// Requests id + name; adds totalEmails + unreadEmails when
    /// `with_counts` is set.
    pub fn new(
        session: &JmapSession,
        http_auth: &SecretString,
        with_counts: bool,
    ) -> Result<Self, JmapMailboxListError> {
        trace!("prepare JMAP mailbox listing (with_counts={with_counts})");
        let properties = if with_counts {
            vec![
                JmapMailboxProperty::Id,
                JmapMailboxProperty::Name,
                JmapMailboxProperty::TotalEmails,
                JmapMailboxProperty::UnreadEmails,
            ]
        } else {
            vec![JmapMailboxProperty::Id, JmapMailboxProperty::Name]
        };
        let opts = JmapMailboxQueryOptions {
            properties: Some(properties),
            ..Default::default()
        };
        Ok(Self {
            inner: JmapMailboxQuery::new(session, http_auth, opts)?,
        })
    }
}

/// Converts one JMAP Mailbox object into the shared [`Mailbox`] shape.
fn mailbox_from(mailbox: JmapMailboxObject) -> Mailbox {
    Mailbox {
        id: mailbox.id.unwrap_or_default(),
        name: mailbox.name.unwrap_or_default(),
        total: Some(u64::from(mailbox.total_emails)),
        unread: Some(u64::from(mailbox.unread_emails)),
    }
}

impl JmapCoroutine for JmapMailboxList {
    type Yield = JmapYield;
    type Return = Result<Vec<Mailbox>, JmapMailboxListError>;

    fn resume(&mut self, bytes: Option<&[u8]>) -> JmapCoroutineState<Self::Yield, Self::Return> {
        match self.inner.resume(bytes) {
            JmapCoroutineState::Yielded(y) => JmapCoroutineState::Yielded(y),
            JmapCoroutineState::Complete(Ok(JmapMailboxQueryOutput { mailboxes, .. })) => {
                let mailboxes = mailboxes.into_iter().map(mailbox_from).collect();
                JmapCoroutineState::Complete(Ok(mailboxes))
            }
            JmapCoroutineState::Complete(Err(err)) => JmapCoroutineState::Complete(Err(err.into())),
        }
    }
}
