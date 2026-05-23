//! Std blocking unified email client: holds one or more backend
//! clients side by side and dispatches each shared op to the first
//! registered backend that supports it.
//!
//! Build with [`EmailClientStd::new`] then chain the per-backend
//! `with_*` builders. Registration order is the priority order for
//! dispatch: the first registered backend that supports a given op
//! wins. Reassigning a slot with a second `with_*` call leaves the
//! priority position unchanged.

use alloc::{string::String, vec::Vec};

use log::trace;
#[cfg(any(feature = "imap", feature = "smtp"))]
use pimalaya_stream::std::stream::StreamStd;
use thiserror::Error;

#[cfg(feature = "search")]
use crate::search::query::SearchEmailsQuery;
use crate::{
    envelope::{Envelope, EnvelopeDiff},
    flag::{Flag, FlagOp},
    mailbox::{Mailbox, MailboxDiff},
};

/// Errors returned by [`EmailClientStd`].
///
/// Backend-specific errors propagate transparently through the
/// per-backend variants. Everything else is a least-common-denominator
/// reason any backend may surface, named without protocol prefix to
/// match the shared input/output API.
#[derive(Debug, Error)]
pub enum EmailClientStdError {
    #[cfg(feature = "imap")]
    #[error(transparent)]
    Imap(#[from] io_imap::client::ImapClientStdError),
    #[cfg(feature = "jmap")]
    #[error(transparent)]
    Jmap(#[from] io_jmap::client::JmapClientStdError),
    #[cfg(feature = "maildir")]
    #[error(transparent)]
    Maildir(#[from] io_maildir::client::MaildirClientError),
    #[cfg(feature = "m2dir")]
    #[error(transparent)]
    M2dir(#[from] io_m2dir::client::M2dirClientError),
    #[cfg(feature = "smtp")]
    #[error(transparent)]
    Smtp(#[from] io_smtp::client::SmtpClientStdError),

    #[error("no registered backend supports this operation")]
    UnsupportedOperation,

    #[error("invalid mailbox `{0}`")]
    InvalidMailbox(String),
    #[error("invalid message id `{0}`")]
    InvalidId(String),
    #[error("invalid email address `{0}`")]
    InvalidAddress(String),
    #[error("invalid URL `{0}`")]
    InvalidUrl(String),
    #[error("invalid message content: {0}")]
    InvalidMessageContent(String),

    #[error("mailbox `{0}` not found")]
    MailboxNotFound(String),
    #[error("message `{0}` not found")]
    MessageNotFound(String),
    #[error("no identity configured for `{0}`")]
    IdentityNotFound(String),
    #[error("empty message body")]
    EmptyMessageBody,
    #[error("missing required input `{0}`")]
    MissingInput(&'static str),
    #[error("operation `{0}` failed")]
    OperationFailed(&'static str),
}

/// Tag identifying which backend slot to dispatch to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum BackendKind {
    #[cfg(feature = "imap")]
    Imap,
    #[cfg(feature = "jmap")]
    Jmap,
    #[cfg(feature = "maildir")]
    Maildir,
    #[cfg(feature = "m2dir")]
    M2dir,
    #[cfg(feature = "smtp")]
    Smtp,
}

/// Std-blocking unified email client.
///
/// Holds one slot per supported backend plus a registration-ordered
/// list. Each shared op walks the list and dispatches to the first
/// backend that supports it; backends in the list that don't support
/// the op are skipped. When no slot can handle the op,
/// [`EmailClientStdError::UnsupportedOperation`] is returned.
#[derive(Debug, Default)]
pub struct EmailClientStd {
    #[cfg(feature = "imap")]
    pub(crate) imap: Option<io_imap::client::ImapClientStd<StreamStd>>,
    #[cfg(feature = "jmap")]
    pub(crate) jmap: Option<io_jmap::client::JmapClientStd>,
    #[cfg(feature = "maildir")]
    pub(crate) maildir: Option<io_maildir::client::MaildirClient>,
    #[cfg(feature = "m2dir")]
    pub(crate) m2dir: Option<io_m2dir::client::M2dirClient>,
    #[cfg(feature = "smtp")]
    pub(crate) smtp: Option<io_smtp::client::SmtpClientStd<StreamStd>>,
    pub(crate) order: Vec<BackendKind>,
}

impl EmailClientStd {
    /// Creates an empty client with no backend registered. Use the
    /// `with_*` builders to populate slots.
    pub fn new() -> Self {
        Self::default()
    }

    /// Lists every mailbox available to the active account.
    ///
    /// When `with_counts` is `true`, [`Mailbox::total`] and
    /// [`Mailbox::unread`] are populated when the backend supports
    /// them; otherwise they are left as `None`. Backends that surface
    /// counts for free always populate the fields regardless of the
    /// flag.
    pub fn list_mailboxes(
        &mut self,
        with_counts: bool,
    ) -> Result<Vec<Mailbox>, EmailClientStdError> {
        trace!("list mailboxes with {self:?}");

        for kind in self.order.clone() {
            match kind {
                #[cfg(feature = "imap")]
                BackendKind::Imap => return self.list_mailboxes_imap(with_counts),
                #[cfg(feature = "jmap")]
                BackendKind::Jmap => return self.list_mailboxes_jmap(),
                #[cfg(feature = "maildir")]
                BackendKind::Maildir => return self.list_mailboxes_maildir(),
                #[cfg(feature = "m2dir")]
                BackendKind::M2dir => return self.list_mailboxes_m2dir(),
                #[cfg(feature = "smtp")]
                BackendKind::Smtp => continue,
            }
        }

        Err(EmailClientStdError::UnsupportedOperation)
    }

    /// Lists envelopes from the given mailbox.
    ///
    /// `mailbox` is the backend-specific mailbox identifier (name or
    /// id). `page` is 1-indexed; pass `None` to default to page 1.
    /// `page_size = None` returns the full window. When
    /// `with_attachment` is set, [`Envelope::has_attachment`] is
    /// populated when the backend reports it (otherwise left as
    /// `None`).
    ///
    /// Default ordering is date descending (most recent first). Use
    /// [`Self::search_envelopes`] to filter and/or sort with the shared
    /// search DSL.
    pub fn list_envelopes(
        &mut self,
        mailbox: &str,
        page: Option<u32>,
        page_size: Option<u32>,
        with_attachment: bool,
    ) -> Result<Vec<Envelope>, EmailClientStdError> {
        trace!("list envelopes with {self:?}");

        for kind in self.order.clone() {
            match kind {
                #[cfg(feature = "imap")]
                BackendKind::Imap => {
                    return self.list_envelopes_imap(mailbox, page, page_size, with_attachment);
                }
                #[cfg(feature = "jmap")]
                BackendKind::Jmap => {
                    return self.list_envelopes_jmap(mailbox, page, page_size);
                }
                #[cfg(feature = "maildir")]
                BackendKind::Maildir => {
                    return self.list_envelopes_maildir(mailbox, page, page_size);
                }
                #[cfg(feature = "m2dir")]
                BackendKind::M2dir => {
                    return self.list_envelopes_m2dir(mailbox, page, page_size, with_attachment);
                }
                #[cfg(feature = "smtp")]
                BackendKind::Smtp => continue,
            }
        }

        Err(EmailClientStdError::UnsupportedOperation)
    }

    /// Searches envelopes in the given mailbox using the shared search
    /// DSL (requires the `search` cargo feature).
    ///
    /// `query` carries an optional filter and/or sort. When the filter
    /// is `None`, every envelope in `mailbox` matches; when the sort is
    /// `None`, the default is date descending (most recent first).
    /// Pagination follows the same rules as [`Self::list_envelopes`].
    ///
    /// Per-protocol translation lives in the matching backend module:
    /// [`crate::imap::envelope_search`] (full grammar, `SEARCH` +
    /// `SORT`), [`crate::jmap::envelope_search`] (conjunctive only;
    /// `or`/`not` are rejected; dates over-approximate `receivedAt`
    /// then re-check `sentAt` client-side), and
    /// [`crate::maildir::envelope_search`] (full grammar except
    /// `body`, evaluated client-side).
    #[cfg(feature = "search")]
    pub fn search_envelopes(
        &mut self,
        mailbox: &str,
        query: Option<&SearchEmailsQuery>,
        page: Option<u32>,
        page_size: Option<u32>,
        with_attachment: bool,
    ) -> Result<Vec<Envelope>, EmailClientStdError> {
        trace!("search envelopes with {self:?}");

        for kind in self.order.clone() {
            match kind {
                #[cfg(feature = "imap")]
                BackendKind::Imap => {
                    return self.search_envelopes_imap(
                        mailbox,
                        query,
                        page,
                        page_size,
                        with_attachment,
                    );
                }
                #[cfg(feature = "jmap")]
                BackendKind::Jmap => {
                    return self.search_envelopes_jmap(mailbox, query, page, page_size);
                }
                #[cfg(feature = "maildir")]
                BackendKind::Maildir => {
                    return self.search_envelopes_maildir(mailbox, query, page, page_size);
                }
                #[cfg(feature = "m2dir")]
                BackendKind::M2dir => continue,
                #[cfg(feature = "smtp")]
                BackendKind::Smtp => continue,
            }
        }

        Err(EmailClientStdError::UnsupportedOperation)
    }

    /// Surfaces a pre-diffed envelope delta against the opaque per- backend
    /// `state` checkpoint.
    ///
    /// `state` is the blob returned by a previous successful call (or `None` on
    /// first sync). On success the caller stores the returned checkpoint for
    /// next time. Returns [`EnvelopeDiff::FullListRequired`] when the backend
    /// cannot produce an incremental view (capability missing, state
    /// invalidated, server bumped UIDVALIDITY).
    ///
    /// Currently routed to IMAP (QRESYNC / CONDSTORE) and JMAP
    /// (`Email/changes`). Maildir, m2dir and SMTP slots are skipped.
    pub fn diff_envelopes(
        &mut self,
        mailbox: &str,
        state: Option<&[u8]>,
    ) -> Result<EnvelopeDiff, EmailClientStdError> {
        trace!("diff envelopes with {self:?}");

        for kind in self.order.clone() {
            match kind {
                #[cfg(feature = "imap")]
                BackendKind::Imap => return self.diff_envelopes_imap(mailbox, state),
                #[cfg(feature = "jmap")]
                BackendKind::Jmap => return self.diff_envelopes_jmap(mailbox, state),
                #[cfg(feature = "maildir")]
                BackendKind::Maildir => continue,
                #[cfg(feature = "m2dir")]
                BackendKind::M2dir => continue,
                #[cfg(feature = "smtp")]
                BackendKind::Smtp => continue,
            }
        }

        Err(EmailClientStdError::UnsupportedOperation)
    }

    /// Probes whether the mailbox set has changed since `state`. JMAP
    /// uses `Mailbox/changes` for a constant-cost "anything changed?"
    /// answer; backends without an account-global mailbox state token
    /// fall through to [`EmailClientStdError::UnsupportedOperation`]
    /// so the caller drops to a normal [`Self::list_mailboxes`].
    pub fn diff_mailboxes(
        &mut self,
        state: Option<&[u8]>,
    ) -> Result<MailboxDiff, EmailClientStdError> {
        trace!("diff mailboxes with {self:?}");

        for kind in self.order.clone() {
            match kind {
                #[cfg(feature = "imap")]
                BackendKind::Imap => continue,
                #[cfg(feature = "jmap")]
                BackendKind::Jmap => return self.diff_mailboxes_jmap(state),
                #[cfg(feature = "maildir")]
                BackendKind::Maildir => continue,
                #[cfg(feature = "m2dir")]
                BackendKind::M2dir => continue,
                #[cfg(feature = "smtp")]
                BackendKind::Smtp => continue,
            }
        }

        Err(EmailClientStdError::UnsupportedOperation)
    }

    /// Adds the given flags to every message in `ids` inside
    /// `mailbox`, preserving any flags already set. `ids` is a slice
    /// of backend-specific message identifiers.
    pub fn add_flags(
        &mut self,
        mailbox: &str,
        ids: &[&str],
        flags: &[Flag],
    ) -> Result<(), EmailClientStdError> {
        self.store_flags(mailbox, ids, flags, FlagOp::Add)
    }

    /// Replaces the flag set of every message in `ids` inside
    /// `mailbox` with `flags` exactly. Any prior flag not in `flags`
    /// is removed.
    pub fn set_flags(
        &mut self,
        mailbox: &str,
        ids: &[&str],
        flags: &[Flag],
    ) -> Result<(), EmailClientStdError> {
        self.store_flags(mailbox, ids, flags, FlagOp::Set)
    }

    /// Removes the given flags from every message in `ids` inside
    /// `mailbox`. Flags not present on a message are silently skipped.
    pub fn delete_flags(
        &mut self,
        mailbox: &str,
        ids: &[&str],
        flags: &[Flag],
    ) -> Result<(), EmailClientStdError> {
        self.store_flags(mailbox, ids, flags, FlagOp::Remove)
    }

    fn store_flags(
        &mut self,
        mailbox: &str,
        ids: &[&str],
        flags: &[Flag],
        op: FlagOp,
    ) -> Result<(), EmailClientStdError> {
        trace!("store flags ({op:?}) with {self:?}");

        for kind in self.order.clone() {
            match kind {
                #[cfg(feature = "imap")]
                BackendKind::Imap => return self.store_flags_imap(mailbox, ids, flags, op),
                #[cfg(feature = "jmap")]
                BackendKind::Jmap => return self.store_flags_jmap(ids, flags, op),
                #[cfg(feature = "maildir")]
                BackendKind::Maildir => return self.store_flags_maildir(mailbox, ids, flags, op),
                #[cfg(feature = "m2dir")]
                BackendKind::M2dir => return self.store_flags_m2dir(mailbox, ids, flags, op),
                #[cfg(feature = "smtp")]
                BackendKind::Smtp => continue,
            }
        }

        Err(EmailClientStdError::UnsupportedOperation)
    }

    /// Fetches the raw RFC 5322 bytes of message `id` from `mailbox`.
    ///
    /// `mailbox` may be ignored by backends whose ids are globally
    /// scoped. Returns the message body as-is, with no modification
    /// to the seen/read state.
    pub fn get_message(&mut self, mailbox: &str, id: &str) -> Result<Vec<u8>, EmailClientStdError> {
        trace!("get message with {self:?}");

        for kind in self.order.clone() {
            match kind {
                #[cfg(feature = "imap")]
                BackendKind::Imap => return self.get_message_imap(mailbox, id),
                #[cfg(feature = "jmap")]
                BackendKind::Jmap => return self.get_message_jmap(id),
                #[cfg(feature = "maildir")]
                BackendKind::Maildir => return self.get_message_maildir(mailbox, id),
                #[cfg(feature = "m2dir")]
                BackendKind::M2dir => return self.get_message_m2dir(mailbox, id),
                #[cfg(feature = "smtp")]
                BackendKind::Smtp => continue,
            }
        }

        Err(EmailClientStdError::UnsupportedOperation)
    }

    /// Appends a raw RFC 5322 message to `mailbox`, tagged with the
    /// given `flags`. `raw` must be a syntactically valid RFC 5322
    /// message; framing-level escaping is handled by the backend.
    /// Returns the identifier the backend assigned to the stored
    /// message (maildir basename, JMAP email id, IMAP UID).
    pub fn add_message(
        &mut self,
        mailbox: &str,
        flags: &[Flag],
        raw: Vec<u8>,
    ) -> Result<String, EmailClientStdError> {
        trace!("add message with {self:?}");

        for kind in self.order.clone() {
            match kind {
                #[cfg(feature = "imap")]
                BackendKind::Imap => return self.add_message_imap(mailbox, flags, raw),
                #[cfg(feature = "jmap")]
                BackendKind::Jmap => return self.add_message_jmap(mailbox, flags, raw),
                #[cfg(feature = "maildir")]
                BackendKind::Maildir => return self.add_message_maildir(mailbox, flags, raw),
                #[cfg(feature = "m2dir")]
                BackendKind::M2dir => return self.add_message_m2dir(mailbox, flags, raw),
                #[cfg(feature = "smtp")]
                BackendKind::Smtp => continue,
            }
        }

        Err(EmailClientStdError::UnsupportedOperation)
    }

    /// Creates a mailbox named `name` (backend-specific layout).
    pub fn create_mailbox(&mut self, name: &str) -> Result<(), EmailClientStdError> {
        trace!("create mailbox with {self:?}");

        for kind in self.order.clone() {
            match kind {
                #[cfg(feature = "imap")]
                BackendKind::Imap => return self.create_mailbox_imap(name),
                #[cfg(feature = "jmap")]
                BackendKind::Jmap => return self.create_mailbox_jmap(name),
                #[cfg(feature = "maildir")]
                BackendKind::Maildir => return self.create_mailbox_maildir(name),
                #[cfg(feature = "m2dir")]
                BackendKind::M2dir => return self.create_mailbox_m2dir(name),
                #[cfg(feature = "smtp")]
                BackendKind::Smtp => continue,
            }
        }

        Err(EmailClientStdError::UnsupportedOperation)
    }

    /// Deletes the mailbox named `name` and every message inside it.
    pub fn delete_mailbox(&mut self, name: &str) -> Result<(), EmailClientStdError> {
        trace!("delete mailbox with {self:?}");

        for kind in self.order.clone() {
            match kind {
                #[cfg(feature = "imap")]
                BackendKind::Imap => return self.delete_mailbox_imap(name),
                #[cfg(feature = "jmap")]
                BackendKind::Jmap => return self.delete_mailbox_jmap(name),
                #[cfg(feature = "maildir")]
                BackendKind::Maildir => return self.delete_mailbox_maildir(name),
                #[cfg(feature = "m2dir")]
                BackendKind::M2dir => return self.delete_mailbox_m2dir(name),
                #[cfg(feature = "smtp")]
                BackendKind::Smtp => continue,
            }
        }

        Err(EmailClientStdError::UnsupportedOperation)
    }

    /// Permanently deletes message `id` from `mailbox`.
    ///
    /// Backends without a server-side delete primitive (Maildir) are
    /// skipped; expunge-via-flag is the caller's responsibility.
    pub fn delete_message(&mut self, mailbox: &str, id: &str) -> Result<(), EmailClientStdError> {
        trace!("delete message with {self:?}");

        for kind in self.order.clone() {
            match kind {
                #[cfg(feature = "imap")]
                BackendKind::Imap => return self.delete_message_imap(mailbox, id),
                #[cfg(feature = "jmap")]
                BackendKind::Jmap => return self.delete_message_jmap(id),
                #[cfg(feature = "maildir")]
                BackendKind::Maildir => continue,
                #[cfg(feature = "m2dir")]
                BackendKind::M2dir => return self.delete_message_m2dir(mailbox, id),
                #[cfg(feature = "smtp")]
                BackendKind::Smtp => continue,
            }
        }

        Err(EmailClientStdError::UnsupportedOperation)
    }

    /// Copies every message in `ids` from mailbox `from` to mailbox
    /// `to`, leaving the originals in place.
    pub fn copy_messages(
        &mut self,
        from: &str,
        to: &str,
        ids: &[&str],
    ) -> Result<(), EmailClientStdError> {
        trace!("copy messages with {self:?}");

        for kind in self.order.clone() {
            match kind {
                #[cfg(feature = "imap")]
                BackendKind::Imap => return self.copy_messages_imap(from, to, ids),
                #[cfg(feature = "jmap")]
                BackendKind::Jmap => return self.copy_messages_jmap(to, ids),
                #[cfg(feature = "maildir")]
                BackendKind::Maildir => return self.copy_messages_maildir(from, to, ids),
                #[cfg(feature = "m2dir")]
                BackendKind::M2dir => return self.copy_messages_m2dir(from, to, ids),
                #[cfg(feature = "smtp")]
                BackendKind::Smtp => continue,
            }
        }

        Err(EmailClientStdError::UnsupportedOperation)
    }

    /// Moves every message in `ids` from mailbox `from` to mailbox
    /// `to`. The originals are removed from `from`.
    pub fn move_messages(
        &mut self,
        from: &str,
        to: &str,
        ids: &[&str],
    ) -> Result<(), EmailClientStdError> {
        trace!("move messages with {self:?}");

        for kind in self.order.clone() {
            match kind {
                #[cfg(feature = "imap")]
                BackendKind::Imap => return self.move_messages_imap(from, to, ids),
                #[cfg(feature = "jmap")]
                BackendKind::Jmap => return self.move_messages_jmap(from, to, ids),
                #[cfg(feature = "maildir")]
                BackendKind::Maildir => return self.move_messages_maildir(from, to, ids),
                #[cfg(feature = "m2dir")]
                BackendKind::M2dir => return self.move_messages_m2dir(from, to, ids),
                #[cfg(feature = "smtp")]
                BackendKind::Smtp => continue,
            }
        }

        Err(EmailClientStdError::UnsupportedOperation)
    }

    /// Sends a raw RFC 5322 message.
    ///
    /// `from` is the envelope sender (bare `local@domain`, no angle
    /// brackets); `to` is the non-empty list of envelope recipients.
    /// Envelope addresses are independent of the `From:` / `To:` /
    /// `Cc:` / `Bcc:` headers inside `raw` and govern actual routing.
    ///
    /// Dispatched to the first registered backend that supports
    /// sending (SMTP or JMAP); IMAP, Maildir and m2dir slots are
    /// skipped.
    #[cfg_attr(not(any(feature = "smtp", feature = "jmap")), allow(unused_variables))]
    pub fn send_message(
        &mut self,
        raw: Vec<u8>,
        from: &str,
        to: &[&str],
    ) -> Result<(), EmailClientStdError> {
        trace!("send message with {self:?}");

        for kind in self.order.clone() {
            match kind {
                #[cfg(feature = "smtp")]
                BackendKind::Smtp => return self.send_message_smtp(raw, from, to),
                #[cfg(feature = "jmap")]
                BackendKind::Jmap => return self.send_message_jmap(raw, from, to),
                #[cfg(feature = "imap")]
                BackendKind::Imap => continue,
                #[cfg(feature = "maildir")]
                BackendKind::Maildir => continue,
                #[cfg(feature = "m2dir")]
                BackendKind::M2dir => continue,
            }
        }

        Err(EmailClientStdError::UnsupportedOperation)
    }
}
