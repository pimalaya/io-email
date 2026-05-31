//! Multi-protocol std-blocking email client.
//!
//! [`EmailClientStd`] is a thin container holding one optional
//! per-protocol client per supported backend. The shared-API surface
//! (`list_mailboxes`, `list_envelopes`, `get_message`, …) lives on
//! each per-protocol client; this struct gives callers a single typed
//! bag to pass around the backends they care about, plus a dispatch
//! layer that picks the highest-priority registered backend.
//!
//! Two construction paths:
//!
//! - `with_<protocol>(client)`: register a client built externally
//!   (typically via the per-protocol `new` or `connect`).
//! - `connect_<protocol>(…)`: TLS-gated convenience that opens the
//!   connection through the per-protocol `connect` and fills the slot
//!   in one shot.
//!
//! ## Dispatch priority
//!
//! Reads / mutations on storage backends:
//! `Maildir → M2dir → JMAP → IMAP` (local before network, cheap
//! before expensive).
//!
//! Sending: `JMAP → SMTP`.
//!
//! When no registered backend supports the op, the dispatch returns
//! [`EmailClientStdError::NoBackendRegistered`].

use alloc::{string::String, sync::Arc, vec::Vec};
use core::sync::atomic::AtomicBool;
use std::sync::mpsc::Sender;

use thiserror::Error;

#[cfg(feature = "search")]
use crate::search::query::SearchEmailsQuery;
use crate::{
    envelope::{Envelope, EnvelopeDiff},
    event::WatchEvent,
    flag::{Flag, FlagOp},
    mailbox::{Mailbox, MailboxDiff},
};

#[cfg(feature = "imap")]
use crate::imap::client::{ImapClientError, ImapClientStd};
#[cfg(feature = "jmap")]
use crate::jmap::client::{JmapClientError, JmapClientStd};
#[cfg(feature = "m2dir")]
use crate::m2dir::client::{M2dirClient, M2dirClientError};
#[cfg(feature = "maildir")]
use crate::maildir::client::{MaildirClient, MaildirClientError};
#[cfg(feature = "smtp")]
use crate::smtp::client::{SmtpClientError, SmtpClientStd};

#[cfg(feature = "imap")]
use io_imap::types::core::{IString, NString};
#[cfg(all(
    feature = "smtp",
    any(
        feature = "rustls-ring",
        feature = "rustls-aws",
        feature = "native-tls"
    )
))]
use io_smtp::rfc5321::types::ehlo_domain::EhloDomain;
#[cfg(all(
    feature = "imap",
    any(
        feature = "rustls-ring",
        feature = "rustls-aws",
        feature = "native-tls"
    )
))]
use pimalaya_stream::sasl::Sasl as ImapSasl;
#[cfg(all(
    feature = "jmap",
    any(
        feature = "rustls-ring",
        feature = "rustls-aws",
        feature = "native-tls"
    )
))]
use secrecy::SecretString;
#[cfg(all(
    any(feature = "imap", feature = "jmap", feature = "smtp"),
    any(
        feature = "rustls-ring",
        feature = "rustls-aws",
        feature = "native-tls"
    )
))]
use {pimalaya_stream::tls::Tls, url::Url};

/// Errors surfaced by [`EmailClientStd`].
///
/// Each variant flattens the per-protocol client's error type via
/// `#[from]` so the matching `?` operator works on the shared client.
#[derive(Debug, Error)]
pub enum EmailClientStdError {
    #[cfg(feature = "imap")]
    #[error(transparent)]
    Imap(#[from] ImapClientError),
    #[cfg(feature = "jmap")]
    #[error(transparent)]
    Jmap(#[from] JmapClientError),
    #[cfg(feature = "smtp")]
    #[error(transparent)]
    Smtp(#[from] SmtpClientError),
    #[cfg(feature = "maildir")]
    #[error(transparent)]
    Maildir(#[from] MaildirClientError),
    #[cfg(feature = "m2dir")]
    #[error(transparent)]
    M2dir(#[from] M2dirClientError),
    #[error("No backend supporting this operation is registered")]
    NoBackendRegistered,
    #[error("Registered backend does not support this operation")]
    UnsupportedOperation,
}

/// Std-blocking multi-protocol email client.
///
/// Each slot holds an optional per-protocol client. Empty by default;
/// register backends through the `with_<protocol>` builders or the
/// `connect_<protocol>` convenience helpers. Slots are `pub` so
/// callers can read the registered client back out (e.g. to tweak its
/// pub knobs after construction).
#[derive(Default)]
pub struct EmailClientStd {
    #[cfg(feature = "imap")]
    pub imap: Option<ImapClientStd>,
    #[cfg(feature = "jmap")]
    pub jmap: Option<JmapClientStd>,
    #[cfg(feature = "smtp")]
    pub smtp: Option<SmtpClientStd>,
    #[cfg(feature = "maildir")]
    pub maildir: Option<MaildirClient>,
    #[cfg(feature = "m2dir")]
    pub m2dir: Option<M2dirClient>,
}

impl EmailClientStd {
    /// Builds an empty client with no backend registered.
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers the IMAP backend.
    #[cfg(feature = "imap")]
    pub fn with_imap(mut self, client: ImapClientStd) -> Self {
        self.imap = Some(client);
        self
    }

    /// Registers the JMAP backend.
    #[cfg(feature = "jmap")]
    pub fn with_jmap(mut self, client: JmapClientStd) -> Self {
        self.jmap = Some(client);
        self
    }

    /// Registers the SMTP backend.
    #[cfg(feature = "smtp")]
    pub fn with_smtp(mut self, client: SmtpClientStd) -> Self {
        self.smtp = Some(client);
        self
    }

    /// Registers the Maildir backend.
    #[cfg(feature = "maildir")]
    pub fn with_maildir(mut self, client: MaildirClient) -> Self {
        self.maildir = Some(client);
        self
    }

    /// Registers the m2dir backend.
    #[cfg(feature = "m2dir")]
    pub fn with_m2dir(mut self, client: M2dirClient) -> Self {
        self.m2dir = Some(client);
        self
    }

    /// Opens an IMAP connection via [`ImapClientStd::connect`] and
    /// registers the resulting client. See [`ImapClientStd::connect`]
    /// for the `auto_id` semantics.
    #[cfg(all(
        feature = "imap",
        any(
            feature = "rustls-ring",
            feature = "rustls-aws",
            feature = "native-tls"
        )
    ))]
    pub fn connect_imap(
        self,
        url: &Url,
        tls: &Tls,
        starttls: bool,
        sasl: Option<impl Into<ImapSasl>>,
        auto_id: Option<Vec<(IString<'static>, NString<'static>)>>,
    ) -> Result<Self, EmailClientStdError> {
        Ok(self.with_imap(ImapClientStd::connect(url, tls, starttls, sasl, auto_id)?))
    }

    /// Opens a JMAP connection via [`JmapClientStd::connect`] and
    /// registers the resulting client (session already discovered).
    #[cfg(all(
        feature = "jmap",
        any(
            feature = "rustls-ring",
            feature = "rustls-aws",
            feature = "native-tls"
        )
    ))]
    pub fn connect_jmap(
        self,
        url: &Url,
        tls: &Tls,
        http_auth: SecretString,
    ) -> Result<Self, EmailClientStdError> {
        Ok(self.with_jmap(JmapClientStd::connect(url, tls, http_auth)?))
    }

    /// Opens an SMTP connection via [`SmtpClientStd::connect`] and
    /// registers the resulting client.
    #[cfg(all(
        feature = "smtp",
        any(
            feature = "rustls-ring",
            feature = "rustls-aws",
            feature = "native-tls"
        )
    ))]
    pub fn connect_smtp(
        self,
        url: &Url,
        tls: &Tls,
        starttls: bool,
        domain: EhloDomain<'_>,
        sasl: Option<impl Into<pimalaya_stream::sasl::Sasl>>,
    ) -> Result<Self, EmailClientStdError> {
        Ok(self.with_smtp(SmtpClientStd::connect(url, tls, starttls, domain, sasl)?))
    }

    /// Pings every registered network backend (IMAP, SMTP) to reset
    /// the server's inactivity timer on long-idle sessions. Storage
    /// backends (Maildir, M2dir) and JMAP (HTTP, stateless) have
    /// nothing to keep alive and are skipped. Returns the first
    /// error encountered, or `Ok(())` when every registered network
    /// backend acknowledged the NOOP.
    pub fn ping(&mut self) -> Result<(), EmailClientStdError> {
        #[cfg(feature = "imap")]
        if let Some(c) = self.imap.as_mut() {
            c.ping()?;
        }
        #[cfg(feature = "smtp")]
        if let Some(c) = self.smtp.as_mut() {
            c.ping()?;
        }
        Ok(())
    }

    // ---- Shared-API dispatch (storage: Maildir → M2dir → JMAP → IMAP) ----

    /// Lists every visible mailbox via the highest-priority registered
    /// storage backend.
    pub fn list_mailboxes(
        &mut self,
        with_counts: bool,
    ) -> Result<Vec<Mailbox>, EmailClientStdError> {
        #[cfg(feature = "maildir")]
        if let Some(c) = &self.maildir {
            return Ok(c.list_mailboxes(with_counts)?);
        }
        #[cfg(feature = "m2dir")]
        if let Some(c) = &self.m2dir {
            return Ok(c.list_mailboxes(with_counts)?);
        }
        #[cfg(feature = "jmap")]
        if let Some(c) = self.jmap.as_mut() {
            return Ok(c.list_mailboxes(with_counts)?);
        }
        #[cfg(feature = "imap")]
        if let Some(c) = self.imap.as_mut() {
            return Ok(c.list_mailboxes(with_counts)?);
        }
        Err(EmailClientStdError::NoBackendRegistered)
    }

    /// Lists envelopes from `mailbox`. `with_attachment` is honoured
    /// by IMAP / Maildir / M2dir; JMAP returns the attachment flag
    /// inline and ignores the parameter.
    pub fn list_envelopes(
        &mut self,
        mailbox: &str,
        page: Option<u32>,
        page_size: Option<u32>,
        with_attachment: bool,
    ) -> Result<Vec<Envelope>, EmailClientStdError> {
        #[cfg(feature = "maildir")]
        if let Some(c) = &self.maildir {
            return Ok(c.list_envelopes(mailbox, page, page_size, with_attachment)?);
        }
        #[cfg(feature = "m2dir")]
        if let Some(c) = &self.m2dir {
            return Ok(c.list_envelopes(mailbox, page, page_size, with_attachment)?);
        }
        #[cfg(feature = "jmap")]
        if let Some(c) = self.jmap.as_mut() {
            return Ok(c.list_envelopes(mailbox, page, page_size)?);
        }
        #[cfg(feature = "imap")]
        if let Some(c) = self.imap.as_mut() {
            return Ok(c.list_envelopes(mailbox, page, page_size, with_attachment)?);
        }
        let _ = (mailbox, page, page_size, with_attachment);
        Err(EmailClientStdError::NoBackendRegistered)
    }

    /// Searches envelopes against the shared query.
    #[cfg(feature = "search")]
    pub fn search_envelopes(
        &mut self,
        mailbox: &str,
        query: Option<&SearchEmailsQuery>,
        page: Option<u32>,
        page_size: Option<u32>,
        with_attachment: bool,
    ) -> Result<Vec<Envelope>, EmailClientStdError> {
        #[cfg(feature = "maildir")]
        if let Some(c) = &self.maildir {
            return Ok(c.search_envelopes(mailbox, query, page, page_size, with_attachment)?);
        }
        #[cfg(feature = "m2dir")]
        if let Some(c) = &self.m2dir {
            return Ok(c.search_envelopes(mailbox, query, page, page_size, with_attachment)?);
        }
        #[cfg(feature = "jmap")]
        if let Some(c) = self.jmap.as_mut() {
            return Ok(c.search_envelopes(mailbox, query, page, page_size)?);
        }
        #[cfg(feature = "imap")]
        if let Some(c) = self.imap.as_mut() {
            return Ok(c.search_envelopes(mailbox, query, page, page_size, with_attachment)?);
        }
        let _ = (mailbox, query, page, page_size, with_attachment);
        Err(EmailClientStdError::NoBackendRegistered)
    }

    /// Adds, sets or removes flags on `ids`.
    pub fn store_flags(
        &mut self,
        mailbox: &str,
        ids: &[&str],
        flags: &[Flag],
        op: FlagOp,
    ) -> Result<(), EmailClientStdError> {
        #[cfg(feature = "maildir")]
        if let Some(c) = &self.maildir {
            return Ok(c.store_flags(mailbox, ids, flags, op)?);
        }
        #[cfg(feature = "m2dir")]
        if let Some(c) = &self.m2dir {
            return Ok(c.store_flags(mailbox, ids, flags, op)?);
        }
        #[cfg(feature = "jmap")]
        if let Some(c) = self.jmap.as_mut() {
            return Ok(c.store_flags(mailbox, ids, flags, op)?);
        }
        #[cfg(feature = "imap")]
        if let Some(c) = self.imap.as_mut() {
            return Ok(c.store_flags(mailbox, ids, flags, op)?);
        }
        let _ = (mailbox, ids, flags, op);
        Err(EmailClientStdError::NoBackendRegistered)
    }

    /// Fetches one message's raw RFC 5322 bytes.
    pub fn get_message(&mut self, mailbox: &str, id: &str) -> Result<Vec<u8>, EmailClientStdError> {
        #[cfg(feature = "maildir")]
        if let Some(c) = &self.maildir {
            return Ok(c.get_message(mailbox, id)?);
        }
        #[cfg(feature = "m2dir")]
        if let Some(c) = &self.m2dir {
            return Ok(c.get_message(mailbox, id)?);
        }
        #[cfg(feature = "jmap")]
        if let Some(c) = self.jmap.as_mut() {
            return Ok(c.get_message(mailbox, id)?);
        }
        #[cfg(feature = "imap")]
        if let Some(c) = self.imap.as_mut() {
            return Ok(c.get_message(mailbox, id)?);
        }
        let _ = (mailbox, id);
        Err(EmailClientStdError::NoBackendRegistered)
    }

    /// Adds `raw` to `mailbox` with the given flags. Returns the
    /// newly-assigned id.
    pub fn add_message(
        &mut self,
        mailbox: &str,
        flags: &[Flag],
        raw: Vec<u8>,
    ) -> Result<String, EmailClientStdError> {
        #[cfg(feature = "maildir")]
        if let Some(c) = &self.maildir {
            return Ok(c.add_message(mailbox, flags, raw)?);
        }
        #[cfg(feature = "m2dir")]
        if let Some(c) = &self.m2dir {
            return Ok(c.add_message(mailbox, flags, raw)?);
        }
        #[cfg(feature = "jmap")]
        if let Some(c) = self.jmap.as_mut() {
            return Ok(c.add_message(mailbox, flags, raw)?);
        }
        #[cfg(feature = "imap")]
        if let Some(c) = self.imap.as_mut() {
            return Ok(c.add_message(mailbox, flags, raw)?);
        }
        let _ = (mailbox, flags, raw);
        Err(EmailClientStdError::NoBackendRegistered)
    }

    /// Creates `name` as a new mailbox.
    pub fn create_mailbox(&mut self, name: &str) -> Result<(), EmailClientStdError> {
        #[cfg(feature = "maildir")]
        if let Some(c) = &self.maildir {
            return Ok(c.create_mailbox(name)?);
        }
        #[cfg(feature = "m2dir")]
        if let Some(c) = &self.m2dir {
            return Ok(c.create_mailbox(name)?);
        }
        #[cfg(feature = "jmap")]
        if let Some(c) = self.jmap.as_mut() {
            return Ok(c.create_mailbox(name)?);
        }
        #[cfg(feature = "imap")]
        if let Some(c) = self.imap.as_mut() {
            return Ok(c.create_mailbox(name)?);
        }
        let _ = name;
        Err(EmailClientStdError::NoBackendRegistered)
    }

    /// Deletes mailbox `name`.
    pub fn delete_mailbox(&mut self, name: &str) -> Result<(), EmailClientStdError> {
        #[cfg(feature = "maildir")]
        if let Some(c) = &self.maildir {
            return Ok(c.delete_mailbox(name)?);
        }
        #[cfg(feature = "m2dir")]
        if let Some(c) = &self.m2dir {
            return Ok(c.delete_mailbox(name)?);
        }
        #[cfg(feature = "jmap")]
        if let Some(c) = self.jmap.as_mut() {
            return Ok(c.delete_mailbox(name)?);
        }
        #[cfg(feature = "imap")]
        if let Some(c) = self.imap.as_mut() {
            return Ok(c.delete_mailbox(name)?);
        }
        let _ = name;
        Err(EmailClientStdError::NoBackendRegistered)
    }

    /// Deletes one message permanently.
    pub fn delete_message(&mut self, mailbox: &str, id: &str) -> Result<(), EmailClientStdError> {
        #[cfg(feature = "maildir")]
        if let Some(c) = &self.maildir {
            return Ok(c.delete_message(mailbox, id)?);
        }
        #[cfg(feature = "m2dir")]
        if let Some(c) = &self.m2dir {
            return Ok(c.delete_message(mailbox, id)?);
        }
        #[cfg(feature = "jmap")]
        if let Some(c) = self.jmap.as_mut() {
            return Ok(c.delete_message(mailbox, id)?);
        }
        #[cfg(feature = "imap")]
        if let Some(c) = self.imap.as_mut() {
            return Ok(c.delete_message(mailbox, id)?);
        }
        let _ = (mailbox, id);
        Err(EmailClientStdError::NoBackendRegistered)
    }

    /// Copies `ids` from `from` to `to`.
    pub fn copy_messages(
        &mut self,
        from: &str,
        to: &str,
        ids: &[&str],
    ) -> Result<(), EmailClientStdError> {
        #[cfg(feature = "maildir")]
        if let Some(c) = &self.maildir {
            return Ok(c.copy_messages(from, to, ids)?);
        }
        #[cfg(feature = "m2dir")]
        if let Some(c) = &self.m2dir {
            return Ok(c.copy_messages(from, to, ids)?);
        }
        #[cfg(feature = "jmap")]
        if let Some(c) = self.jmap.as_mut() {
            return Ok(c.copy_messages(from, to, ids)?);
        }
        #[cfg(feature = "imap")]
        if let Some(c) = self.imap.as_mut() {
            return Ok(c.copy_messages(from, to, ids)?);
        }
        let _ = (from, to, ids);
        Err(EmailClientStdError::NoBackendRegistered)
    }

    /// Moves `ids` from `from` to `to`.
    pub fn move_messages(
        &mut self,
        from: &str,
        to: &str,
        ids: &[&str],
    ) -> Result<(), EmailClientStdError> {
        #[cfg(feature = "maildir")]
        if let Some(c) = &self.maildir {
            return Ok(c.move_messages(from, to, ids)?);
        }
        #[cfg(feature = "m2dir")]
        if let Some(c) = &self.m2dir {
            return Ok(c.move_messages(from, to, ids)?);
        }
        #[cfg(feature = "jmap")]
        if let Some(c) = self.jmap.as_mut() {
            return Ok(c.move_messages(from, to, ids)?);
        }
        #[cfg(feature = "imap")]
        if let Some(c) = self.imap.as_mut() {
            return Ok(c.move_messages(from, to, ids)?);
        }
        let _ = (from, to, ids);
        Err(EmailClientStdError::NoBackendRegistered)
    }

    /// Surfaces a pre-diffed envelope delta against the opaque
    /// per-backend `state` checkpoint, when the registered backend
    /// supports it.
    ///
    /// `state` is the blob returned by a previous successful call (or
    /// `None` on first sync); the caller stores the returned checkpoint
    /// for next time. Returns [`EnvelopeDiff::FullListRequired`] when
    /// the backend cannot produce an incremental view (capability
    /// missing, state invalidated, server bumped UIDVALIDITY).
    ///
    /// Currently routed to IMAP (QRESYNC / CONDSTORE) and JMAP
    /// (`Email/changes`); Maildir, m2dir and SMTP fall through to
    /// [`EmailClientStdError::UnsupportedOperation`].
    pub fn diff_envelopes(
        &mut self,
        mailbox: &str,
        state: Option<&[u8]>,
    ) -> Result<EnvelopeDiff, EmailClientStdError> {
        #[cfg(feature = "jmap")]
        if let Some(c) = self.jmap.as_mut() {
            return Ok(c.diff_envelopes(mailbox, state)?);
        }
        #[cfg(feature = "imap")]
        if let Some(c) = self.imap.as_mut() {
            return Ok(c.diff_envelopes(mailbox, state)?);
        }
        let _ = (mailbox, state);
        Err(EmailClientStdError::UnsupportedOperation)
    }

    /// Probes whether the mailbox set has changed since `state`. JMAP
    /// uses `Mailbox/changes` for a constant-cost "anything changed?"
    /// answer; backends without an account-global mailbox state token
    /// (IMAP, Maildir, m2dir) fall through to
    /// [`EmailClientStdError::UnsupportedOperation`] so the caller can
    /// drop to a normal [`Self::list_mailboxes`].
    pub fn diff_mailboxes(
        &mut self,
        state: Option<&[u8]>,
    ) -> Result<MailboxDiff, EmailClientStdError> {
        #[cfg(feature = "jmap")]
        if let Some(c) = self.jmap.as_mut() {
            return Ok(c.diff_mailboxes(state)?);
        }
        let _ = state;
        Err(EmailClientStdError::UnsupportedOperation)
    }

    /// Watches `mailbox` for envelope-level deltas, forwarding events
    /// through `tx`. Priority: JMAP → IMAP (no filesystem watch yet).
    #[cfg(any(feature = "imap", feature = "jmap"))]
    pub fn watch_mailbox(
        &mut self,
        mailbox: &str,
        shutdown: Arc<AtomicBool>,
        tx: Sender<WatchEvent>,
    ) -> Result<(), EmailClientStdError> {
        #[cfg(feature = "jmap")]
        if let Some(c) = self.jmap.as_mut() {
            return Ok(c.watch_mailbox(mailbox, shutdown, tx)?);
        }
        #[cfg(feature = "imap")]
        if let Some(c) = self.imap.as_mut() {
            return Ok(c.watch_mailbox(mailbox, shutdown, tx)?);
        }
        let _ = (mailbox, shutdown, tx);
        Err(EmailClientStdError::NoBackendRegistered)
    }

    // ---- Sending (JMAP → SMTP) -------------------------------------

    /// Sends a raw RFC 5322 message. JMAP routes via
    /// `EmailSubmission/set` when registered; otherwise SMTP runs the
    /// RFC 5321 mail transaction.
    #[cfg(any(feature = "jmap", feature = "smtp"))]
    pub fn send_message(&mut self, raw: Vec<u8>) -> Result<(), EmailClientStdError> {
        #[cfg(feature = "jmap")]
        if let Some(c) = self.jmap.as_mut() {
            return Ok(c.send_message(raw)?);
        }
        #[cfg(feature = "smtp")]
        if let Some(c) = self.smtp.as_mut() {
            return Ok(c.send_message(raw)?);
        }
        let _ = raw;
        Err(EmailClientStdError::NoBackendRegistered)
    }
}
