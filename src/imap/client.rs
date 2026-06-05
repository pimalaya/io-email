//! Std-blocking IMAP client.
//!
//! Holds an inner [`ImapClientStd`] (from io-imap) wrapping the stream
//! and its fragmentizer, plus the per-connection knobs the shared-API
//! IMAP methods need: the `auto_select` policy, the optional `auto_id`
//! payload, and the last-known capability list discovered at login.
//!
//! [`ImapClientStd::run`] pumps io-email IMAP coroutines directly
//! against the inner client's stream and fragmentizer; the inner
//! client's own request/response helpers stay reachable through
//! [`ImapClientStd::inner`] for protocol-specific paths (select, idle,
//! enable, ...) that the shared API does not cover.

use core::{num::NonZeroU32, sync::atomic::AtomicBool};

use alloc::{
    string::{String, ToString},
    sync::Arc,
    vec,
    vec::Vec,
};

use std::{
    io::{self, ErrorKind, Read, Write},
    sync::mpsc::Sender,
};

use io_imap::{
    client::{ImapClientStd as InnerImapClientStd, ImapClientStdError as InnerImapClientStdError},
    coroutine::*,
    types::{
        core::{IString, NString},
        fetch::{MacroOrMessageDataItemNames, MessageDataItem, MessageDataItemName},
        mailbox::Mailbox as ImapMailbox,
        response::Capability,
        sequence::SequenceSet,
    },
};
#[cfg(any(
    feature = "rustls-ring",
    feature = "rustls-aws",
    feature = "native-tls"
))]
use pimalaya_stream::{sasl::Sasl, tls::Tls};
use thiserror::Error;
#[cfg(any(
    feature = "rustls-ring",
    feature = "rustls-aws",
    feature = "native-tls"
))]
use url::Url;

#[cfg(feature = "search")]
use crate::{
    envelope::imap::search::{ImapEnvelopeSearch, ImapEnvelopeSearchError},
    search::query::SearchEmailsQuery,
};
use crate::{
    envelope::{
        event::WatchEvent,
        imap::{
            diff::{
                ImapState, envelope_from_items, flag_update_from_items, new_message_item_names,
                new_message_window,
            },
            list::{ImapEnvelopeList, ImapEnvelopeListError},
            watch::{ImapWatchMailbox, ImapWatchMailboxError, ImapWatchMailboxYield},
        },
        types::{Envelope, EnvelopeDiff, FlagUpdate},
    },
    flag::{
        imap::store::{ImapFlagStore, ImapFlagStoreError},
        types::{Flag, FlagOp},
    },
    imap::convert::parse_mailbox,
    mailbox::{
        imap::{
            create::{ImapMailboxCreate, ImapMailboxCreateError},
            delete::{ImapMailboxDelete, ImapMailboxDeleteError},
            list::{ImapMailboxList, ImapMailboxListError},
        },
        types::Mailbox,
    },
    message::imap::{
        add::{ImapMessageAdd, ImapMessageAddError},
        copy::{ImapMessageCopy, ImapMessageCopyError},
        delete::{ImapMessageDelete, ImapMessageDeleteError},
        get::{ImapMessageGet, ImapMessageGetError},
        r#move::{ImapMessageMove, ImapMessageMoveError},
    },
};

/// Errors surfaced by [`ImapClientStd`] while running a coroutine.
///
/// One variant per shared-API IMAP coroutine.
#[derive(Debug, Error)]
pub enum ImapClientError {
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    MailboxList(#[from] ImapMailboxListError),
    #[error(transparent)]
    EnvelopeList(#[from] ImapEnvelopeListError),
    #[cfg(feature = "search")]
    #[error(transparent)]
    EnvelopeSearch(#[from] ImapEnvelopeSearchError),
    #[error(transparent)]
    FlagStore(#[from] ImapFlagStoreError),
    #[error(transparent)]
    MailboxCreate(#[from] ImapMailboxCreateError),
    #[error(transparent)]
    MailboxDelete(#[from] ImapMailboxDeleteError),
    #[error(transparent)]
    MessageAdd(#[from] ImapMessageAddError),
    #[error(transparent)]
    MessageCopy(#[from] ImapMessageCopyError),
    #[error(transparent)]
    MessageDelete(#[from] ImapMessageDeleteError),
    #[error(transparent)]
    MessageGet(#[from] ImapMessageGetError),
    #[error(transparent)]
    MessageMove(#[from] ImapMessageMoveError),
    #[error(transparent)]
    WatchMailbox(#[from] ImapWatchMailboxError),
    #[error(transparent)]
    Inner(#[from] InnerImapClientStdError),
}

const READ_BUFFER_SIZE: usize = 16 * 1024;

/// Light IMAP client built on top of the io-imap type-erased inner.
///
/// `auto_select` is the per-message policy flag the IMAP coroutines
/// read at construction time; flip it off when the caller already
/// pre-selects the target mailbox. `capabilities` is the live list
/// discovered at login; `watch_mailbox` needs `QRESYNC` to be
/// present.
///
/// The RFC 2971 `auto_id` knob lives on the inner io-imap client
/// (`inner.auto_id`) because the auth coroutines themselves chain
/// the `ID` round-trip; set it before any auth_*/login call (or pass
/// it through [`Self::connect`]).
pub struct ImapClientStd {
    pub inner: InnerImapClientStd,
    pub auto_select: bool,
    pub capabilities: Vec<Capability<'static>>,
}

impl ImapClientStd {
    /// Wraps an already-connected stream with a fresh inner client,
    /// the default `auto_select = true` policy, and an empty
    /// capability list. Callers that intend to use `watch_mailbox`
    /// should populate `capabilities` after login.
    pub fn new<S: Read + Write + Send + 'static>(stream: S) -> Self {
        Self {
            inner: InnerImapClientStd::new(stream),
            auto_select: true,
            capabilities: Vec::new(),
        }
    }

    /// Pumps any standard-shape IMAP coroutine
    /// (`Yield = ImapYield`, `Return = Result<T, E>`) against the
    /// inner client's stream and fragmentizer until it terminates.
    ///
    /// Reaches into [`Self::inner`] for raw field access rather than
    /// delegating to [`InnerImapClientStd::run`] so error variants
    /// route through [`ImapClientError`] directly.
    pub fn run<C, T, E>(&mut self, mut coroutine: C) -> Result<T, ImapClientError>
    where
        C: ImapCoroutine<Yield = ImapYield, Return = Result<T, E>>,
        ImapClientError: From<E>,
    {
        let mut buf = [0u8; READ_BUFFER_SIZE];
        let mut arg: Option<&[u8]> = None;

        loop {
            match coroutine.resume(&mut self.inner.fragmentizer, arg.take()) {
                ImapCoroutineState::Complete(Ok(out)) => return Ok(out),
                ImapCoroutineState::Complete(Err(err)) => return Err(err.into()),
                ImapCoroutineState::Yielded(ImapYield::WantsRead) => {
                    let n = self.inner.stream.read(&mut buf)?;
                    arg = Some(&buf[..n]);
                }
                ImapCoroutineState::Yielded(ImapYield::WantsWrite(bytes)) => {
                    self.inner.stream.write_all(&bytes)?;
                }
            }
        }
    }

    /// Sends a NOOP to keep the connection alive (RFC 3501 §6.1.2).
    /// Sole purpose is to reset the server's inactivity timer on
    /// long-idle TUI sessions; the response is discarded.
    pub fn ping(&mut self) -> Result<(), ImapClientError> {
        Ok(self.inner.noop()?)
    }

    /// Lists every mailbox visible to the session. When
    /// `with_counts` is set, follows up with one `STATUS` per row
    /// to populate [`Mailbox::total`] / [`Mailbox::unread`].
    pub fn list_mailboxes(&mut self, with_counts: bool) -> Result<Vec<Mailbox>, ImapClientError> {
        self.run(ImapMailboxList::new(with_counts))
    }

    /// Lists envelopes from `mailbox`. `page = None` and
    /// `page_size = None` fetch the whole mailbox. Page 1 is the
    /// most recent window.
    pub fn list_envelopes(
        &mut self,
        mailbox: &str,
        page: Option<u32>,
        page_size: Option<u32>,
        with_attachment: bool,
    ) -> Result<Vec<Envelope>, ImapClientError> {
        self.run(ImapEnvelopeList::new(
            mailbox,
            page,
            page_size,
            with_attachment,
        )?)
    }

    /// Searches envelopes in `mailbox` against the shared query.
    /// Pagination is applied to the SORT-ordered UID list before
    /// FETCH.
    #[cfg(feature = "search")]
    pub fn search_envelopes(
        &mut self,
        mailbox: &str,
        query: Option<&SearchEmailsQuery>,
        page: Option<u32>,
        page_size: Option<u32>,
        with_attachment: bool,
    ) -> Result<Vec<Envelope>, ImapClientError> {
        self.run(ImapEnvelopeSearch::new(
            mailbox,
            query,
            page,
            page_size,
            with_attachment,
        )?)
    }

    /// Adds, sets, or removes `flags` on a UID set. When
    /// [`Self::auto_select`] is on, the target mailbox is SELECTed
    /// first; sync engines flip it off and pre-select once per
    /// batch.
    pub fn store_flags(
        &mut self,
        mailbox: &str,
        ids: &[&str],
        flags: &[Flag],
        op: FlagOp,
    ) -> Result<(), ImapClientError> {
        let auto_select = self.auto_select;
        self.run(ImapFlagStore::new(mailbox, ids, flags, op, auto_select)?)
    }

    /// Fetches one message's raw RFC 5322 bytes without flipping
    /// the `\Seen` flag. Honours [`Self::auto_select`].
    pub fn get_message(&mut self, mailbox: &str, id: &str) -> Result<Vec<u8>, ImapClientError> {
        let auto_select = self.auto_select;
        self.run(ImapMessageGet::new(mailbox, id, auto_select)?)
    }

    /// Appends `raw` to `mailbox` with the given flags. Returns the
    /// appended UID, resolved via UIDPLUS when available or via
    /// `UID SEARCH HEADER Message-ID` as a fallback.
    pub fn add_message(
        &mut self,
        mailbox: &str,
        flags: &[Flag],
        raw: Vec<u8>,
    ) -> Result<String, ImapClientError> {
        self.run(ImapMessageAdd::new(mailbox, flags, raw)?)
    }

    /// Creates `name` as a new mailbox (RFC 3501 §6.3.3).
    pub fn create_mailbox(&mut self, name: &str) -> Result<(), ImapClientError> {
        self.run(ImapMailboxCreate::new(name)?)
    }

    /// Deletes `name` (RFC 3501 §6.3.4).
    pub fn delete_mailbox(&mut self, name: &str) -> Result<(), ImapClientError> {
        self.run(ImapMailboxDelete::new(name)?)
    }

    /// Marks `id` as `\Deleted` then EXPUNGEs. Honours
    /// [`Self::auto_select`].
    pub fn delete_message(&mut self, mailbox: &str, id: &str) -> Result<(), ImapClientError> {
        let auto_select = self.auto_select;
        self.run(ImapMessageDelete::new(mailbox, id, auto_select)?)
    }

    /// Copies a UID set from `from` to `to` (RFC 3501 §6.4.7).
    /// Honours [`Self::auto_select`].
    pub fn copy_messages(
        &mut self,
        from: &str,
        to: &str,
        ids: &[&str],
    ) -> Result<(), ImapClientError> {
        let auto_select = self.auto_select;
        self.run(ImapMessageCopy::new(from, to, ids, auto_select)?)
    }

    /// Moves a UID set from `from` to `to` (RFC 6851). Honours
    /// [`Self::auto_select`].
    pub fn move_messages(
        &mut self,
        from: &str,
        to: &str,
        ids: &[&str],
    ) -> Result<(), ImapClientError> {
        let auto_select = self.auto_select;
        self.run(ImapMessageMove::new(from, to, ids, auto_select)?)
    }

    /// Watches `mailbox` for envelope-level deltas, forwarding every
    /// event through the caller-supplied [`Sender`].
    ///
    /// **Blocks** the current thread: drives the IDLE + QRESYNC
    /// coroutine in a loop, fans socket reads / writes against
    /// [`Self::inner`]'s stream, and pushes each yielded
    /// [`WatchEvent`] into `tx`. Returns `Ok(())` when `shutdown`
    /// flips (cooperative: the inner watcher winds IDLE down at the
    /// next loop tick) or when the receiver behind `tx` is dropped;
    /// returns `Err` when the protocol layer errors out.
    ///
    /// The caller must set a read timeout on the inner stream before
    /// invoking this method so the shutdown flag is polled at every
    /// timeout tick instead of only on server traffic. `WouldBlock`
    /// and `TimedOut` errors are treated as "no new bytes" and let
    /// the coroutine re-yield `WantsRead`.
    ///
    /// [`Self::capabilities`] must advertise `QRESYNC` (RFC 7162);
    /// populate it via login or an explicit `CAPABILITY` round-trip
    /// before reaching here.
    pub fn watch_mailbox(
        &mut self,
        mailbox: &str,
        shutdown: Arc<AtomicBool>,
        tx: Sender<WatchEvent>,
    ) -> Result<(), ImapClientError> {
        let mut coroutine = ImapWatchMailbox::new(mailbox, &self.capabilities, shutdown)?;
        let mut buf = [0u8; READ_BUFFER_SIZE];
        let mut bytes: Option<&[u8]> = None;

        loop {
            match coroutine.resume(&mut self.inner.fragmentizer, bytes) {
                ImapCoroutineState::Complete(result) => return Ok(result?),
                ImapCoroutineState::Yielded(ImapWatchMailboxYield::WantsRead) => {
                    match self.inner.stream.read(&mut buf) {
                        Ok(n) => bytes = Some(&buf[..n]),
                        Err(err) if err.kind() == ErrorKind::WouldBlock => bytes = None,
                        Err(err) if err.kind() == ErrorKind::TimedOut => bytes = None,
                        Err(err) => return Err(err.into()),
                    }
                }
                ImapCoroutineState::Yielded(ImapWatchMailboxYield::WantsWrite(out)) => {
                    self.inner.stream.write_all(&out)?;
                    bytes = None;
                }
                ImapCoroutineState::Yielded(ImapWatchMailboxYield::Event(evt)) => {
                    if tx.send(evt).is_err() {
                        return Ok(());
                    }
                    bytes = None;
                }
            }
        }
    }

    /// Returns the QRESYNC-driven envelope delta for `mailbox`.
    ///
    /// Decodes `state` into a checkpoint, opens `SELECT (QRESYNC …)`
    /// with `(uid_validity, highest_mod_seq)`, then fetches new UIDs
    /// above the cached high-water mark. Surfaces
    /// [`EnvelopeDiff::FullListRequired`] when QRESYNC is missing from
    /// [`Self::capabilities`], when UIDVALIDITY bumped, or when no
    /// usable checkpoint was supplied; otherwise returns
    /// [`EnvelopeDiff::Incremental`] with the new state, the flag
    /// updates, the new envelopes and the vanished UIDs.
    pub fn diff_envelopes(
        &mut self,
        mailbox: &str,
        state: Option<&[u8]>,
    ) -> Result<EnvelopeDiff, ImapClientError> {
        let mbox = parse_mailbox(mailbox).map_err(ImapEnvelopeListError::from)?;

        if !self.capabilities.contains(&Capability::QResync) {
            return Ok(EnvelopeDiff::FullListRequired { new_state: None });
        }

        let cached = state.and_then(ImapState::decode);

        let Some(cached) = cached else {
            return self.diff_baseline(mbox);
        };

        let Some(uid_validity_nz) = NonZeroU32::new(cached.uid_validity) else {
            return self.diff_baseline(mbox);
        };

        let capabilities = self.capabilities.clone();
        let select_data = match self.inner.select_qresync(
            mbox.clone(),
            uid_validity_nz,
            cached.highest_mod_seq,
            &capabilities,
        ) {
            Ok(data) => data,
            Err(_) => return self.diff_baseline(mbox),
        };

        let server_uid_validity = select_data
            .uid_validity
            .map(NonZeroU32::get)
            .unwrap_or(cached.uid_validity);
        if server_uid_validity != cached.uid_validity {
            return self.diff_baseline(mbox);
        }

        let flag_updates: Vec<FlagUpdate> = select_data
            .changed
            .iter()
            .filter_map(|fetch| flag_update_from_items(fetch.items.as_ref()))
            .collect();

        let vanished_ids: Vec<String> = select_data
            .vanished_earlier
            .iter()
            .map(|uid| uid.get().to_string())
            .collect();

        let mut new_envelopes: Vec<Envelope> = Vec::new();
        if let Some(window) = new_message_window(cached.highest_uid) {
            if let Ok(sequence_set) = SequenceSet::try_from(window.as_str()) {
                let data = self
                    .inner
                    .fetch(sequence_set, new_message_item_names(), true)?;
                new_envelopes = data
                    .into_iter()
                    .map(|(_, items)| envelope_from_items(items.into_inner()))
                    .collect();
            }
        }

        let highest_uid = new_envelopes
            .iter()
            .filter_map(|e| e.id.parse::<u32>().ok())
            .max()
            .unwrap_or(cached.highest_uid);

        let new_highest_mod_seq = select_data
            .highest_mod_seq
            .unwrap_or(cached.highest_mod_seq);

        let new_state = ImapState {
            uid_validity: server_uid_validity,
            highest_mod_seq: new_highest_mod_seq,
            highest_uid,
        }
        .encode();

        Ok(EnvelopeDiff::Incremental {
            new_state,
            flag_updates,
            new_envelopes,
            vanished_ids,
        })
    }

    /// Captures a fresh IMAP checkpoint via a plain SELECT plus a
    /// `UID FETCH *` to read the highest UID. Used on first sync, when
    /// the stored state is unusable, or when UIDVALIDITY bumped.
    fn diff_baseline(
        &mut self,
        mbox: ImapMailbox<'static>,
    ) -> Result<EnvelopeDiff, ImapClientError> {
        let select = self.inner.select(mbox)?;
        let Some(uid_validity) = select.uid_validity.map(NonZeroU32::get) else {
            return Ok(EnvelopeDiff::FullListRequired { new_state: None });
        };

        let exists = select.exists.unwrap_or(0);
        let mut highest_uid: u32 = 0;
        if exists > 0 {
            let sequence_set: SequenceSet = "*"
                .try_into()
                .expect("`*` is a valid sequence set spelling");
            let item_names =
                MacroOrMessageDataItemNames::MessageDataItemNames(vec![MessageDataItemName::Uid]);
            let data = self.inner.fetch(sequence_set, item_names, false)?;
            highest_uid = data
                .into_values()
                .flat_map(|items| items.into_inner().into_iter())
                .filter_map(|item| match item {
                    MessageDataItem::Uid(u) => Some(u.get()),
                    _ => None,
                })
                .max()
                .unwrap_or(0);
        }

        let highest_mod_seq = select.highest_mod_seq.unwrap_or(0);

        let new_state = ImapState {
            uid_validity,
            highest_mod_seq,
            highest_uid,
        }
        .encode();

        Ok(EnvelopeDiff::FullListRequired {
            new_state: Some(new_state),
        })
    }
}

#[cfg(any(
    feature = "rustls-ring",
    feature = "rustls-aws",
    feature = "native-tls"
))]
impl ImapClientStd {
    /// Opens a TCP / TLS connection to `url`, runs the optional STARTTLS
    /// upgrade and the SASL authentication, then wraps the authenticated stream
    /// with the io-email knobs.
    ///
    /// Delegates the protocol dance to [`InnerImapClientStd::connect`], which
    /// also sets a 5 s read timeout on the underlying socket so
    /// [`Self::watch_mailbox`] can poll its shutdown flag at every timeout
    /// tick. `auto_id` is forwarded to the inner connect and triggers an RFC
    /// 2971 `ID` round-trip after authentication (see
    /// [`InnerImapClientStd::auto_id`]).
    pub fn connect(
        url: &Url,
        tls: &Tls,
        starttls: bool,
        sasl: Option<impl Into<Sasl>>,
        auto_id: Option<Vec<(IString<'static>, NString<'static>)>>,
    ) -> Result<Self, ImapClientError> {
        let (inner, capabilities) = InnerImapClientStd::connect(url, tls, starttls, sasl, auto_id)?;

        Ok(Self {
            inner,
            auto_select: true,
            capabilities,
        })
    }
}
