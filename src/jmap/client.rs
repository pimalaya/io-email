//! Std-blocking JMAP client.
//!
//! Holds an inner [`JmapClientStd`] (from io-jmap) wrapping the boxed
//! stream, the bearer / basic HTTP credential and the discovered
//! [`JmapSession`]. Unlike IMAP there is no `auto_select` policy
//! (JMAP destroys are global) and no separate capability list (the
//! capabilities live inside [`JmapSession::capabilities`]).
//!
//! [`JmapClientStd::run`] pumps io-email JMAP coroutines directly
//! against the inner client's stream; the inner client's own
//! request/response helpers stay reachable through
//! [`JmapClientStd::inner`] for protocol-specific paths (blob_upload,
//! event_source, raw send_raw, ...) that the shared API does not cover.
//!
//! [`JmapClientStd`]: io_jmap::client::JmapClientStd
//! [`JmapSession`]: io_jmap::rfc8620::JmapSession
//! [`JmapSession::capabilities`]: io_jmap::rfc8620::JmapSession::capabilities

use alloc::{string::String, sync::Arc, vec::Vec};
use core::sync::atomic::AtomicBool;
use std::{
    io::{self, ErrorKind, Read, Write},
    sync::mpsc::Sender,
};

use io_jmap::{
    client::{JmapClientStd as InnerJmapClientStd, JmapClientStdError as InnerJmapClientStdError},
    coroutine::*,
    rfc8620::{JmapMethodError, JmapSession, changes::JmapChangesError},
    rfc8621::{
        email::{changes::JmapEmailChangesOptions, get::JmapEmailGetOptions},
        mailbox::{
            changes::{JmapMailboxChangesError, JmapMailboxChangesOptions},
            get::JmapMailboxGetOptions,
        },
    },
};
#[cfg(any(
    feature = "rustls-ring",
    feature = "rustls-aws",
    feature = "native-tls"
))]
use pimalaya_stream::tls::Tls;
use secrecy::SecretString;
use thiserror::Error;
use url::Url;

#[cfg(feature = "search")]
use crate::{
    envelope::jmap::search::{JmapEnvelopeSearch, JmapEnvelopeSearchError},
    search::query::SearchEmailsQuery,
};
use crate::{
    envelope::{
        event::WatchEvent,
        jmap::{
            diff as envelope_diff,
            list::{JmapEnvelopeList, JmapEnvelopeListError},
            watch::{JmapWatchMailbox, JmapWatchMailboxError, JmapWatchMailboxYield},
        },
        types::{Envelope, EnvelopeDiff, FlagUpdate},
    },
    flag::{
        jmap::store::{JmapFlagStore, JmapFlagStoreError},
        types::{Flag, FlagOp},
    },
    jmap::convert::{envelope_from, envelope_properties},
    mailbox::{
        jmap::{
            create::{JmapMailboxCreate, JmapMailboxCreateError},
            delete::{JmapMailboxDelete, JmapMailboxDeleteError},
            list::{JmapMailboxList, JmapMailboxListError},
        },
        types::{Mailbox, MailboxDiff},
    },
    message::jmap::{
        add::{JmapMessageAdd, JmapMessageAddError},
        copy::{JmapMessageCopy, JmapMessageCopyError},
        delete::{JmapMessageDelete, JmapMessageDeleteError},
        get::{JmapMessageGet, JmapMessageGetError},
        r#move::{JmapMessageMove, JmapMessageMoveError},
        send::{JmapMessageSend, JmapMessageSendError},
    },
};

/// Errors surfaced by [`JmapClientStd`] while running a coroutine.
///
/// One variant per shared-API JMAP coroutine.
#[derive(Debug, Error)]
pub enum JmapClientError {
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error("JMAP session is not initialised; call connect or session_get first")]
    MissingSession,
    #[error(transparent)]
    MailboxList(#[from] JmapMailboxListError),
    #[error(transparent)]
    EnvelopeList(#[from] JmapEnvelopeListError),
    #[cfg(feature = "search")]
    #[error(transparent)]
    EnvelopeSearch(#[from] JmapEnvelopeSearchError),
    #[error(transparent)]
    FlagStore(#[from] JmapFlagStoreError),
    #[error(transparent)]
    MailboxCreate(#[from] JmapMailboxCreateError),
    #[error(transparent)]
    MailboxDelete(#[from] JmapMailboxDeleteError),
    #[error(transparent)]
    MessageAdd(#[from] JmapMessageAddError),
    #[error(transparent)]
    MessageCopy(#[from] JmapMessageCopyError),
    #[error(transparent)]
    MessageDelete(#[from] JmapMessageDeleteError),
    #[error(transparent)]
    MessageGet(#[from] JmapMessageGetError),
    #[error(transparent)]
    MessageMove(#[from] JmapMessageMoveError),
    #[error(transparent)]
    MessageSend(#[from] JmapMessageSendError),
    #[error(transparent)]
    WatchMailbox(#[from] JmapWatchMailboxError),
    #[error(transparent)]
    Inner(#[from] InnerJmapClientStdError),
}

const READ_BUFFER_SIZE: usize = 16 * 1024;

/// Light JMAP client built on top of the io-jmap type-erased inner.
///
/// Unlike IMAP, JMAP carries no `auto_select` (destroys are global)
/// and no separate capability list (capabilities are exposed via the
/// inner client's cached [`JmapSession`]).
///
/// Two extra options are required for [`Self::send_message`]: the
/// JMAP identity to submit under (`Identity/get` `type=role:identity`)
/// and the drafts mailbox id (`Mailbox/query` `role: drafts`).
/// Populate them after [`Self::session_get`] when sending is in scope.
pub struct JmapClientStd {
    pub inner: InnerJmapClientStd,
    pub identity_id: Option<String>,
    pub drafts_mailbox_id: Option<String>,
}

impl JmapClientStd {
    /// Wraps an already-connected stream with the bearer / basic HTTP
    /// credential. The session must be discovered via
    /// [`Self::session_get`] before any shared-API method is called.
    pub fn new<S: Read + Write + Send + 'static>(stream: S, http_auth: SecretString) -> Self {
        Self {
            inner: InnerJmapClientStd::new(stream, http_auth),
            identity_id: None,
            drafts_mailbox_id: None,
        }
    }

    /// Pumps any standard-shape JMAP coroutine
    /// (`Yield = JmapYield`, `Return = Result<T, E>`) against the
    /// inner client's stream until it terminates.
    ///
    /// Reaches into [`Self::inner`] for raw field access rather than
    /// delegating to [`InnerJmapClientStd::run`] so error variants
    /// route through [`JmapClientError`] directly.
    pub fn run<C, T, E>(&mut self, mut coroutine: C) -> Result<T, JmapClientError>
    where
        C: JmapCoroutine<Yield = JmapYield, Return = Result<T, E>>,
        JmapClientError: From<E>,
    {
        let mut buf = [0u8; READ_BUFFER_SIZE];
        let mut arg: Option<&[u8]> = None;

        loop {
            match coroutine.resume(arg.take()) {
                JmapCoroutineState::Complete(Ok(out)) => return Ok(out),
                JmapCoroutineState::Complete(Err(err)) => return Err(err.into()),
                JmapCoroutineState::Yielded(JmapYield::WantsRead) => {
                    let n = self.inner.stream.read(&mut buf)?;
                    arg = Some(&buf[..n]);
                }
                JmapCoroutineState::Yielded(JmapYield::WantsWrite(bytes)) => {
                    self.inner.stream.write_all(&bytes)?;
                }
            }
        }
    }

    /// Discovers the JMAP session against `url` and caches it on the
    /// inner client. Pass either a base URL for `/.well-known/jmap`
    /// discovery or a direct session endpoint URL.
    pub fn session_get(&mut self, url: &Url) -> Result<(), JmapClientError> {
        self.inner.session_get(url)?;
        Ok(())
    }

    /// Lists every JMAP mailbox visible to the session's primary
    /// mail account. When `with_counts` is set, includes
    /// `totalEmails` / `unreadEmails` (still one round-trip; JMAP
    /// returns counts inline).
    pub fn list_mailboxes(&mut self, with_counts: bool) -> Result<Vec<Mailbox>, JmapClientError> {
        let coroutine = {
            let session = self.session_or_err()?;
            let http_auth = &self.inner.http_auth;
            JmapMailboxList::new(session, http_auth, with_counts)?
        };
        self.run(coroutine)
    }

    /// Lists envelopes from `mailbox`. `page = None` and
    /// `page_size = None` fetch the whole mailbox.
    pub fn list_envelopes(
        &mut self,
        mailbox: &str,
        page: Option<u32>,
        page_size: Option<u32>,
    ) -> Result<Vec<Envelope>, JmapClientError> {
        let coroutine = {
            let session = self.session_or_err()?;
            let http_auth = &self.inner.http_auth;
            JmapEnvelopeList::new(session, http_auth, mailbox, page, page_size)?
        };
        self.run(coroutine)
    }

    /// Searches envelopes in `mailbox` against the shared query.
    /// Pagination is applied to the SORT-ordered id list; date
    /// predicates are re-checked client-side because JMAP filters on
    /// `receivedAt` while the shared DSL targets `sentAt`.
    #[cfg(feature = "search")]
    pub fn search_envelopes(
        &mut self,
        mailbox: &str,
        query: Option<&SearchEmailsQuery>,
        page: Option<u32>,
        page_size: Option<u32>,
    ) -> Result<Vec<Envelope>, JmapClientError> {
        let coroutine = {
            let session = self.session_or_err()?;
            let http_auth = &self.inner.http_auth;
            JmapEnvelopeSearch::new(session, http_auth, mailbox, query, page, page_size)?
        };
        self.run(coroutine)
    }

    /// Adds, sets, or removes `flags` (JMAP keywords) on a JMAP email
    /// id set. `mailbox` is unused: JMAP keywords are global per
    /// email, not per mailbox.
    pub fn store_flags(
        &mut self,
        mailbox: &str,
        ids: &[&str],
        flags: &[Flag],
        op: FlagOp,
    ) -> Result<(), JmapClientError> {
        let coroutine = {
            let session = self.session_or_err()?;
            let http_auth = &self.inner.http_auth;
            JmapFlagStore::new(session, http_auth, mailbox, ids, flags, op)?
        };
        self.run(coroutine)
    }

    /// Fetches one message's raw RFC 5322 bytes via `Email/get`
    /// (resolving the blob id) then `Blob/download`.
    pub fn get_message(&mut self, mailbox: &str, id: &str) -> Result<Vec<u8>, JmapClientError> {
        let coroutine = {
            let session = self.session_or_err()?;
            let http_auth = &self.inner.http_auth;
            JmapMessageGet::new(session, http_auth, mailbox, id)?
        };
        self.run(coroutine)
    }

    /// Uploads `raw` as a blob then imports it into `mailbox` with
    /// the requested keywords. Returns the created email id.
    pub fn add_message(
        &mut self,
        mailbox: &str,
        flags: &[Flag],
        raw: Vec<u8>,
    ) -> Result<String, JmapClientError> {
        let coroutine = {
            let session = self.session_or_err()?;
            let http_auth = &self.inner.http_auth;
            JmapMessageAdd::new(session, http_auth, mailbox, flags, raw)?
        };
        self.run(coroutine)
    }

    /// Creates `name` as a top-level JMAP mailbox.
    pub fn create_mailbox(&mut self, name: &str) -> Result<(), JmapClientError> {
        let coroutine = {
            let session = self.session_or_err()?;
            let http_auth = &self.inner.http_auth;
            JmapMailboxCreate::new(session, http_auth, name)?
        };
        self.run(coroutine)
    }

    /// Deletes the JMAP mailbox named `name`; drops every email that
    /// lives only in that mailbox (`onDestroyRemoveEmails: true`).
    pub fn delete_mailbox(&mut self, name: &str) -> Result<(), JmapClientError> {
        let coroutine = {
            let session = self.session_or_err()?;
            let http_auth = &self.inner.http_auth;
            JmapMailboxDelete::new(session, http_auth, name)?
        };
        self.run(coroutine)
    }

    /// Destroys the JMAP email by id (global delete; removes the
    /// email from every mailbox it references).
    pub fn delete_message(&mut self, mailbox: &str, id: &str) -> Result<(), JmapClientError> {
        let coroutine = {
            let session = self.session_or_err()?;
            let http_auth = &self.inner.http_auth;
            JmapMessageDelete::new(session, http_auth, mailbox, id)?
        };
        self.run(coroutine)
    }

    /// Copies a JMAP email id set into `to` by adding `to`'s
    /// mailbox-id reference to each email. The `from` argument is
    /// part of the shared signature for symmetry with IMAP / Maildir
    /// but unused: existing `mailboxIds` carry the source reference.
    pub fn copy_messages(
        &mut self,
        from: &str,
        to: &str,
        ids: &[&str],
    ) -> Result<(), JmapClientError> {
        let coroutine = {
            let session = self.session_or_err()?;
            let http_auth = &self.inner.http_auth;
            JmapMessageCopy::new(session, http_auth, from, to, ids)?
        };
        self.run(coroutine)
    }

    /// Moves a JMAP email id set from `from` to `to`; adds `to`'s
    /// id and removes `from`'s id in the same `Email/set` patch.
    pub fn move_messages(
        &mut self,
        from: &str,
        to: &str,
        ids: &[&str],
    ) -> Result<(), JmapClientError> {
        let coroutine = {
            let session = self.session_or_err()?;
            let http_auth = &self.inner.http_auth;
            JmapMessageMove::new(session, http_auth, from, to, ids)?
        };
        self.run(coroutine)
    }

    /// Queues `raw` for delivery via `EmailSubmission/set`.
    /// Requires [`Self::identity_id`] and [`Self::drafts_mailbox_id`]
    /// to be populated.
    pub fn send_message(&mut self, raw: Vec<u8>) -> Result<(), JmapClientError> {
        let coroutine = {
            let session = self.session_or_err()?;
            let http_auth = &self.inner.http_auth;
            let identity_id = self
                .identity_id
                .as_deref()
                .ok_or(JmapClientError::MissingSession)?;
            let drafts_id = self
                .drafts_mailbox_id
                .as_deref()
                .ok_or(JmapClientError::MissingSession)?;
            JmapMessageSend::new(session, http_auth, identity_id, drafts_id, raw)?
        };
        self.run(coroutine)
    }

    /// Watches `mailbox` for envelope-level deltas via the JMAP
    /// EventSource (`closeafter=state`) + `Email/changes` +
    /// `Email/get` loop, forwarding every event through `tx`.
    ///
    /// **Blocks** the current thread. Returns `Ok(())` when
    /// `shutdown` flips, when the receiver behind `tx` is dropped,
    /// or when the protocol layer errors out.
    pub fn watch_mailbox(
        &mut self,
        mailbox: &str,
        shutdown: Arc<AtomicBool>,
        tx: Sender<WatchEvent>,
    ) -> Result<(), JmapClientError> {
        let mut coroutine = {
            let session = self.session_or_err()?;
            let http_auth = &self.inner.http_auth;
            JmapWatchMailbox::new(session, http_auth, mailbox, shutdown)?
        };
        let mut buf = [0u8; READ_BUFFER_SIZE];
        let mut bytes: Option<&[u8]> = None;

        loop {
            match coroutine.resume(bytes) {
                JmapCoroutineState::Complete(result) => return Ok(result?),
                JmapCoroutineState::Yielded(JmapWatchMailboxYield::WantsRead) => {
                    match self.inner.stream.read(&mut buf) {
                        Ok(n) => bytes = Some(&buf[..n]),
                        Err(err) if err.kind() == ErrorKind::WouldBlock => bytes = None,
                        Err(err) if err.kind() == ErrorKind::TimedOut => bytes = None,
                        Err(err) => return Err(err.into()),
                    }
                }
                JmapCoroutineState::Yielded(JmapWatchMailboxYield::WantsWrite(out)) => {
                    self.inner.stream.write_all(&out)?;
                    bytes = None;
                }
                JmapCoroutineState::Yielded(JmapWatchMailboxYield::Event(evt)) => {
                    if tx.send(evt).is_err() {
                        return Ok(());
                    }
                    bytes = None;
                }
            }
        }
    }

    /// Returns the `Email/changes`-driven envelope delta against the
    /// opaque per-backend `state` checkpoint.
    ///
    /// Decodes `state` as the cached `Email/state` token, then walks
    /// `Email/changes` rounds until `hasMoreChanges` clears. Created
    /// ids are turned into new envelopes via `Email/get`; updated ids
    /// surface as [`FlagUpdate`] entries. The `mailbox` argument is
    /// part of the shared signature: JMAP `Email/changes` is global
    /// per account and ignores it. Falls back to
    /// [`EnvelopeDiff::FullListRequired`] when the cached state is
    /// unusable or the server returns `cannotCalculateChanges`.
    pub fn diff_envelopes(
        &mut self,
        _mailbox: &str,
        state: Option<&[u8]>,
    ) -> Result<EnvelopeDiff, JmapClientError> {
        let Some(since_state) = state.and_then(envelope_diff::decode) else {
            return self.diff_baseline();
        };

        let mut created_ids: Vec<String> = Vec::new();
        let mut updated_ids: Vec<String> = Vec::new();
        let mut destroyed_ids: Vec<String> = Vec::new();
        let mut cursor = since_state;

        loop {
            let changes = match self
                .inner
                .email_changes(cursor.clone(), JmapEmailChangesOptions::default())
            {
                Ok(c) => c,
                Err(err) if envelope_diff::is_cannot_calculate_changes(&err) => {
                    return self.diff_baseline();
                }
                Err(err) => return Err(err.into()),
            };

            created_ids.extend(changes.created);
            updated_ids.extend(changes.updated);
            destroyed_ids.extend(changes.destroyed);

            cursor = changes.new_state;
            if !changes.has_more_changes {
                break;
            }
        }

        let properties = envelope_properties();

        let new_envelopes = if created_ids.is_empty() {
            Vec::new()
        } else {
            let opts = JmapEmailGetOptions {
                properties: Some(properties.clone()),
                ..Default::default()
            };
            let output = self.inner.email_get(created_ids, opts)?;
            output.emails.into_iter().map(envelope_from).collect()
        };

        let flag_updates = if updated_ids.is_empty() {
            Vec::new()
        } else {
            let opts = JmapEmailGetOptions {
                properties: Some(properties),
                ..Default::default()
            };
            let output = self.inner.email_get(updated_ids, opts)?;
            output
                .emails
                .into_iter()
                .map(envelope_from)
                .map(|env| FlagUpdate {
                    id: env.id,
                    flags: env.flags,
                })
                .collect()
        };

        Ok(EnvelopeDiff::Incremental {
            new_state: envelope_diff::encode(&cursor),
            flag_updates,
            new_envelopes,
            vanished_ids: destroyed_ids,
        })
    }

    /// Returns the `Mailbox/changes`-driven mailbox-set delta against
    /// the opaque per-backend `state` checkpoint. A single round trip:
    /// any non-empty bucket (or the server bumping the state) maps to
    /// [`MailboxDiff::Changed`]; an idle bucket maps to
    /// [`MailboxDiff::Unchanged`]. On `cannotCalculateChanges` the
    /// caller is told to re-list via `Changed { new_state: None }`.
    pub fn diff_mailboxes(&mut self, state: Option<&[u8]>) -> Result<MailboxDiff, JmapClientError> {
        let Some(since_state) = state.and_then(envelope_diff::decode) else {
            let opts = JmapMailboxGetOptions {
                ids: Some(Vec::new()),
                ..Default::default()
            };
            let output = self.inner.mailbox_get(opts)?;
            return Ok(MailboxDiff::Changed {
                new_state: Some(envelope_diff::encode(&output.new_state)),
            });
        };

        match self
            .inner
            .mailbox_changes(since_state, JmapMailboxChangesOptions::default())
        {
            Ok(changes)
                if !changes.has_more_changes
                    && changes.created.is_empty()
                    && changes.updated.is_empty()
                    && changes.destroyed.is_empty() =>
            {
                Ok(MailboxDiff::Unchanged {
                    new_state: envelope_diff::encode(&changes.new_state),
                })
            }
            Ok(changes) => Ok(MailboxDiff::Changed {
                new_state: Some(envelope_diff::encode(&changes.new_state)),
            }),
            Err(InnerJmapClientStdError::MailboxChanges(JmapMailboxChangesError::Changes(
                JmapChangesError::Method(JmapMethodError::CannotCalculateChanges { .. }),
            ))) => Ok(MailboxDiff::Changed { new_state: None }),
            Err(err) => Err(err.into()),
        }
    }

    /// Captures a fresh `Email/state` checkpoint via an empty
    /// `Email/get`. Used on first sync, when the cached state is
    /// unusable, or when the server returns `cannotCalculateChanges`.
    fn diff_baseline(&mut self) -> Result<EnvelopeDiff, JmapClientError> {
        let output = self
            .inner
            .email_get(Vec::new(), JmapEmailGetOptions::default())?;
        Ok(EnvelopeDiff::FullListRequired {
            new_state: Some(envelope_diff::encode(&output.new_state)),
        })
    }

    fn session_or_err(&self) -> Result<&JmapSession, JmapClientError> {
        self.inner
            .session
            .as_ref()
            .ok_or(JmapClientError::MissingSession)
    }
}

#[cfg(any(
    feature = "rustls-ring",
    feature = "rustls-aws",
    feature = "native-tls"
))]
impl JmapClientStd {
    /// Opens a TCP / TLS connection to `url`, builds the inner
    /// client around it, then runs `session_get` to discover the
    /// JMAP session.
    ///
    /// `url` is either a base URL for `/.well-known/jmap` discovery
    /// or a direct session endpoint.
    pub fn connect(url: &Url, tls: &Tls, http_auth: SecretString) -> Result<Self, JmapClientError> {
        let mut inner = InnerJmapClientStd::connect(url, tls, http_auth)?;
        inner.session_get(url)?;
        Ok(Self {
            inner,
            identity_id: None,
            drafts_mailbox_id: None,
        })
    }
}
