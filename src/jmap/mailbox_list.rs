//! JMAP list-mailboxes coroutine.
//!
//! Wraps `Mailbox/query` + `Mailbox/get` (RFC 8621 §2.4 + §2.5), sent
//! as a single batched JMAP request so one HTTP round-trip covers the
//! whole listing. Unlike IMAP, JMAP returns counts inline as
//! `totalEmails` / `unreadEmails` on the same response, so the
//! `with_counts` switch only widens the requested property set; no
//! second round-trip.
//!
//! Emits the shared [`Mailbox`] shape directly; JMAP-specific data
//! (role, sort order, rights, threads, subscription) is dropped on
//! purpose to stay LCD.

use alloc::{vec, vec::Vec};

use io_jmap::{
    coroutine::{JmapCoroutine, JmapCoroutineState, JmapYield},
    rfc8620::session::JmapSession,
    rfc8621::{
        mailbox::{Mailbox as JmapMailbox, MailboxProperty},
        mailbox_query::{JmapMailboxQuery, JmapMailboxQueryError, JmapMailboxQueryOutput},
    },
};
use log::trace;
use secrecy::SecretString;
use thiserror::Error;

use crate::mailbox::Mailbox;

/// Errors produced by [`JmapMailboxList`].
#[derive(Debug, Error)]
pub enum JmapMailboxListError {
    #[error(transparent)]
    Query(#[from] JmapMailboxQueryError),
}

/// I/O-free coroutine listing every JMAP mailbox visible to the
/// session's primary mail account, optionally enriched with
/// per-mailbox total / unread counts.
pub struct JmapMailboxList {
    inner: JmapMailboxQuery,
}

impl JmapMailboxList {
    /// `Mailbox/query` (no filter, no sort, no pagination) chained
    /// with `Mailbox/get` against the matched ids. Requested
    /// properties trim the wire payload: `id` + `name` for the plain
    /// listing, plus `totalEmails` + `unreadEmails` when `with_counts`
    /// is set.
    pub fn new(
        session: &JmapSession,
        http_auth: &SecretString,
        with_counts: bool,
    ) -> Result<Self, JmapMailboxListError> {
        trace!("prepare JMAP mailbox listing (with_counts={with_counts})");
        let properties = if with_counts {
            vec![
                MailboxProperty::Id,
                MailboxProperty::Name,
                MailboxProperty::TotalEmails,
                MailboxProperty::UnreadEmails,
            ]
        } else {
            vec![MailboxProperty::Id, MailboxProperty::Name]
        };
        Ok(Self {
            inner: JmapMailboxQuery::new(
                session,
                http_auth,
                None,
                None,
                None,
                None,
                Some(properties),
            )?,
        })
    }
}

/// Converts one JMAP `Mailbox` object into the shared [`Mailbox`]
/// shape.
///
/// JMAP-specific fields (role, parent_id, sort_order, threads,
/// rights, subscription) are dropped on purpose: they're not part of
/// the LCD surface. Counts always populate from the wire — the caller
/// requested the matching properties via `with_counts`, so a
/// deserialized `0` from a "not requested" payload is on them, not us.
fn mailbox_from(mailbox: JmapMailbox) -> Mailbox {
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
