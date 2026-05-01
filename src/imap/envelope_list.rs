//! IMAP envelope listing (`SELECT` + `FETCH UID FLAGS ENVELOPE
//! RFC822.SIZE [BODYSTRUCTURE]`), wrapping a private orchestrator and
//! producing the shared [`Envelope`](crate::envelope::Envelope) type
//! on completion.
//!
//! The `Date:` header is taken from `ENVELOPE.date` and parsed as RFC
//! 2822 — `INTERNALDATE` is intentionally not requested since
//! server-arrival time is not consistent across backends.
//!
//! Pagination is computed against the mailbox `EXISTS` count returned
//! by `SELECT`. Page 1 is the most recent window (highest sequence
//! numbers); page 2 the previous window, and so on.

use alloc::{
    collections::BTreeSet,
    format,
    string::{String, ToString},
    vec::Vec,
};
use core::mem;

use chrono::{DateTime, FixedOffset};
use io_imap::{
    context::ImapContext,
    rfc3501::{
        fetch::{
            ImapMessageFetch, ImapMessageFetchError as ImapFetchError, ImapMessageFetchResult,
        },
        select::{
            ImapMailboxSelect, ImapMailboxSelectError as ImapSelectError, ImapMailboxSelectResult,
        },
    },
    types::{
        body::BodyStructure,
        envelope::Address as ImapAddress,
        fetch::{MacroOrMessageDataItemNames, MessageDataItem, MessageDataItemName},
        flag::FlagFetch,
        mailbox::Mailbox as ImapMailbox,
    },
};
use log::trace;
use thiserror::Error;

use crate::{address::Address, envelope::Envelope, flag::Flag};

/// Errors produced while orchestrating SELECT + FETCH for IMAP envelope
/// listing.
#[derive(Debug, Error)]
pub enum EnvelopeListError {
    #[error(transparent)]
    Select(#[from] ImapSelectError),
    #[error(transparent)]
    Fetch(#[from] ImapFetchError),
    #[error("invalid IMAP sequence-set window {0:?}: {1}")]
    InvalidWindow(String, &'static str),
    #[error("IMAP envelope listing was resumed after completion")]
    AlreadyDone,
}

/// Result returned by [`EnvelopeList::resume`].
#[derive(Debug)]
pub enum EnvelopeListResult {
    Ok(Vec<Envelope>),
    WantsRead,
    WantsWrite(Vec<u8>),
    Err(EnvelopeListError),
}

/// I/O-free coroutine wrapping `SELECT <mailbox>` followed by `FETCH
/// <window> (UID FLAGS ENVELOPE RFC822.SIZE [BODYSTRUCTURE])`.
///
/// The window is computed from `EXISTS` and the requested
/// `page`/`page_size` (both 1-indexed; page 1 = most recent).
/// The mailbox is selected read-write by default; use
/// [`EnvelopeList::read_only`] for `EXAMINE`. Pass `with_attachment =
/// true` to additionally fetch `BODYSTRUCTURE` and populate
/// [`Envelope::has_attachment`].
pub struct EnvelopeList {
    inner: Inner,
    pending: Option<PendingFetch>,
}

struct PendingFetch {
    page: Option<u32>,
    page_size: Option<u32>,
    item_names: MacroOrMessageDataItemNames<'static>,
}

enum Inner {
    Selecting(ImapMailboxSelect),
    Fetching(ImapMessageFetch),
    Done,
}

impl EnvelopeList {
    /// Selects the mailbox read-write, then fetches the window
    /// described by `page`/`page_size`. Pass `page_size = None` to
    /// fetch the whole mailbox; `page = None` is treated as page 1.
    pub fn new(
        context: ImapContext,
        mailbox: ImapMailbox<'static>,
        page: Option<u32>,
        page_size: Option<u32>,
        with_attachment: bool,
    ) -> Self {
        trace!("prepare IMAP envelope listing");
        Self::with_select(
            ImapMailboxSelect::new(context, mailbox),
            page,
            page_size,
            with_attachment,
        )
    }

    /// Same as [`EnvelopeList::new`] but issues `EXAMINE` instead of
    /// `SELECT` (read-only).
    pub fn read_only(
        context: ImapContext,
        mailbox: ImapMailbox<'static>,
        page: Option<u32>,
        page_size: Option<u32>,
        with_attachment: bool,
    ) -> Self {
        trace!("prepare IMAP envelope listing (read-only)");
        Self::with_select(
            ImapMailboxSelect::read_only(context, mailbox),
            page,
            page_size,
            with_attachment,
        )
    }

    fn with_select(
        select: ImapMailboxSelect,
        page: Option<u32>,
        page_size: Option<u32>,
        with_attachment: bool,
    ) -> Self {
        let item_names = build_item_names(with_attachment);

        Self {
            inner: Inner::Selecting(select),
            pending: Some(PendingFetch {
                page,
                page_size,
                item_names,
            }),
        }
    }

    /// Advances the orchestrator. Drives SELECT first, then FETCH.
    pub fn resume(&mut self, arg: Option<&[u8]>) -> EnvelopeListResult {
        let mut input = arg;

        loop {
            let inner = mem::replace(&mut self.inner, Inner::Done);

            match inner {
                Inner::Selecting(mut select) => match select.resume(input.take()) {
                    ImapMailboxSelectResult::WantsRead => {
                        self.inner = Inner::Selecting(select);
                        return EnvelopeListResult::WantsRead;
                    }
                    ImapMailboxSelectResult::WantsWrite(bytes) => {
                        self.inner = Inner::Selecting(select);
                        return EnvelopeListResult::WantsWrite(bytes);
                    }
                    ImapMailboxSelectResult::Err { err, .. } => {
                        return EnvelopeListResult::Err(err.into());
                    }
                    ImapMailboxSelectResult::Ok { context, data } => {
                        let pending = self.pending.take().expect("pending fetch set on construct");
                        let exists = data.exists.unwrap_or(0);

                        let Some(window) = compute_window(exists, pending.page, pending.page_size)
                        else {
                            return EnvelopeListResult::Ok(Vec::new());
                        };

                        let sequence_set = match window.as_str().try_into() {
                            Ok(set) => set,
                            Err(_) => {
                                return EnvelopeListResult::Err(EnvelopeListError::InvalidWindow(
                                    window,
                                    "could not parse computed sequence-set",
                                ));
                            }
                        };

                        let fetch =
                            ImapMessageFetch::new(context, sequence_set, pending.item_names, false);
                        self.inner = Inner::Fetching(fetch);
                    }
                },
                Inner::Fetching(mut fetch) => match fetch.resume(input.take()) {
                    ImapMessageFetchResult::WantsRead => {
                        self.inner = Inner::Fetching(fetch);
                        return EnvelopeListResult::WantsRead;
                    }
                    ImapMessageFetchResult::WantsWrite(bytes) => {
                        self.inner = Inner::Fetching(fetch);
                        return EnvelopeListResult::WantsWrite(bytes);
                    }
                    ImapMessageFetchResult::Err { err, .. } => {
                        return EnvelopeListResult::Err(err.into());
                    }
                    ImapMessageFetchResult::Ok { data, .. } => {
                        // BTreeMap iteration is ascending by sequence
                        // number (oldest first); reverse so the
                        // freshest message comes first.
                        let envelopes = data
                            .into_iter()
                            .rev()
                            .map(|(seq, items)| envelope_from(seq.get(), items.into_inner()))
                            .collect();
                        return EnvelopeListResult::Ok(envelopes);
                    }
                },
                Inner::Done => {
                    return EnvelopeListResult::Err(EnvelopeListError::AlreadyDone);
                }
            }
        }
    }
}

/// Builds the static FETCH item-name list used by both the wrapped
/// coroutine and the `EmailClient` shortcut path. Always requests UID
/// + FLAGS + ENVELOPE + RFC822.SIZE; appends BODYSTRUCTURE only when
/// the caller opted in to attachment detection.
pub(crate) fn build_item_names(with_attachment: bool) -> MacroOrMessageDataItemNames<'static> {
    let mut names = vec![
        MessageDataItemName::Uid,
        MessageDataItemName::Flags,
        MessageDataItemName::Envelope,
        MessageDataItemName::Rfc822Size,
    ];
    if with_attachment {
        names.push(MessageDataItemName::BodyStructure);
    }
    MacroOrMessageDataItemNames::MessageDataItemNames(names)
}

/// Computes the IMAP sequence-set string for a `(page, page_size)`
/// pair against `exists` total messages. Returns `None` when the
/// window is empty (mailbox empty, page-size 0, or page beyond the
/// last one).
pub(crate) fn compute_window(
    exists: u32,
    page: Option<u32>,
    page_size: Option<u32>,
) -> Option<String> {
    if exists == 0 {
        return None;
    }

    let page = page.unwrap_or(1).max(1);

    let Some(size) = page_size else {
        return Some("1:*".to_string());
    };

    if size == 0 {
        return None;
    }

    let skip = (page - 1).saturating_mul(size);
    if skip >= exists {
        return None;
    }

    let end = exists - skip;
    let start = end.saturating_sub(size - 1).max(1);
    Some(format!("{start}:{end}"))
}

pub(crate) fn envelope_from(seq: u32, items: Vec<MessageDataItem<'static>>) -> Envelope {
    let mut id = String::new();
    let mut flags = BTreeSet::new();
    let mut subject = String::new();
    let mut from = Vec::new();
    let mut to = Vec::new();
    let mut date: Option<DateTime<FixedOffset>> = None;
    let mut size: u64 = 0;
    let mut has_attachment: Option<bool> = None;

    for item in items {
        match item {
            MessageDataItem::Uid(uid) => {
                id = uid.get().to_string();
            }
            MessageDataItem::Flags(items) => {
                flags = items.into_iter().filter_map(flag_from).collect();
            }
            MessageDataItem::Envelope(env) => {
                if let Some(s) = env.subject.into_option() {
                    subject = bytes_to_string(s.as_ref());
                }
                if let Some(d) = env.date.into_option() {
                    let raw = bytes_to_string(d.as_ref());
                    date = parse_rfc2822_date(&raw);
                }
                from = env.from.iter().map(address_from).collect();
                to = env.to.iter().map(address_from).collect();
            }
            MessageDataItem::Rfc822Size(n) => {
                size = u64::from(n);
            }
            MessageDataItem::BodyStructure(structure) => {
                has_attachment = Some(body_structure_has_attachment(&structure));
            }
            _ => {}
        }
    }

    if id.is_empty() {
        id = seq.to_string();
    }

    Envelope {
        id,
        flags,
        subject,
        from,
        to,
        date,
        size,
        has_attachment,
    }
}

pub(crate) fn flag_from(fetch: FlagFetch<'_>) -> Option<Flag> {
    let FlagFetch::Flag(flag) = fetch else {
        // `\Recent` is server-managed and not a user keyword.
        return None;
    };

    Flag::parse(&flag.to_string())
}

pub(crate) fn address_from(addr: &ImapAddress<'_>) -> Address {
    let name = addr
        .name
        .0
        .as_ref()
        .map(|s| bytes_to_string(s.as_ref()))
        .filter(|s| !s.is_empty());

    let mailbox = addr
        .mailbox
        .0
        .as_ref()
        .map(|s| bytes_to_string(s.as_ref()))
        .unwrap_or_default();
    let host = addr
        .host
        .0
        .as_ref()
        .map(|s| bytes_to_string(s.as_ref()))
        .unwrap_or_default();

    let email = if mailbox.is_empty() {
        host
    } else if host.is_empty() {
        mailbox
    } else {
        let mut s = String::with_capacity(mailbox.len() + 1 + host.len());
        s.push_str(&mailbox);
        s.push('@');
        s.push_str(&host);
        s
    };

    Address { name, email }
}

/// Walks an IMAP `BODYSTRUCTURE` looking for any leaf marked with a
/// `Content-Disposition: attachment`. Returns `false` when no leaf
/// reports that disposition (including when the disposition extension
/// data is absent — common with bare `BODY` fetches, less so with
/// `BODYSTRUCTURE`).
pub(crate) fn body_structure_has_attachment(structure: &BodyStructure<'_>) -> bool {
    match structure {
        BodyStructure::Single { extension_data, .. } => {
            let Some(ext) = extension_data.as_ref() else {
                return false;
            };
            let Some(disposition) = ext.tail.as_ref() else {
                return false;
            };
            let Some((kind, _)) = disposition.disposition.as_ref() else {
                return false;
            };
            kind.as_ref().eq_ignore_ascii_case(b"attachment")
        }
        BodyStructure::Multi { bodies, .. } => {
            bodies.as_ref().iter().any(body_structure_has_attachment)
        }
    }
}

fn parse_rfc2822_date(raw: &str) -> Option<DateTime<FixedOffset>> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }
    DateTime::parse_from_rfc2822(trimmed).ok()
}

fn bytes_to_string(bytes: &[u8]) -> String {
    core::str::from_utf8(bytes)
        .map(ToString::to_string)
        .unwrap_or_else(|_| {
            let mut out = String::with_capacity(bytes.len());
            for b in bytes {
                out.push(*b as char);
            }
            out
        })
}
