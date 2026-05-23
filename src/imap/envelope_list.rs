//! IMAP envelope listing (`SELECT` + `FETCH UID FLAGS ENVELOPE
//! RFC822.SIZE [BODYSTRUCTURE]`), wrapping a private orchestrator.
//!
//! Page 1 is the most recent window (highest sequence numbers). The
//! `Date:` header is parsed from `ENVELOPE.date` as RFC 2822;
//! `INTERNALDATE` is not requested (server-arrival time is not
//! consistent across backends).

use core::{mem, str::from_utf8};

use alloc::{
    collections::BTreeSet,
    format,
    string::{String, ToString},
    vec::Vec,
};

use chrono::{DateTime, FixedOffset};
use io_imap::{
    context::ImapContext,
    rfc3501::{
        examine::ImapMailboxExamine,
        fetch::{ImapMessageFetch, ImapMessageFetchError, ImapMessageFetchResult},
        select::{ImapMailboxSelect, ImapMailboxSelectError, ImapMailboxSelectResult},
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
use rfc2047_decoder::{Decoder, RecoverStrategy};
use thiserror::Error;

use crate::{
    address::Address,
    envelope::{Envelope, normalize_message_id},
    flag::Flag,
};

/// Errors produced while orchestrating SELECT + FETCH for IMAP envelope
/// listing.
#[derive(Debug, Error)]
pub enum ImapEnvelopeListError {
    #[error(transparent)]
    Select(#[from] ImapMailboxSelectError),
    #[error(transparent)]
    Fetch(#[from] ImapMessageFetchError),
    #[error("invalid IMAP sequence-set window {0:?}: {1}")]
    InvalidWindow(String, &'static str),
    #[error("IMAP envelope listing was resumed after completion")]
    AlreadyDone,
}

/// Result returned by [`ImapEnvelopeList::resume`].
#[derive(Debug)]
pub enum ImapEnvelopeListResult {
    Ok(Vec<Envelope>),
    WantsRead,
    WantsWrite(Vec<u8>),
    Err(ImapEnvelopeListError),
}

/// Wraps either [`ImapMailboxSelect`] or [`ImapMailboxExamine`] so the
/// downstream coroutine state machine can share a single match arm.
/// Both coroutines surface the same `ImapMailboxSelectResult` shape
/// (the EXAMINE result/error types are aliases of the SELECT ones).
enum SelectOrExamine {
    Select(ImapMailboxSelect),
    Examine(ImapMailboxExamine),
}

impl SelectOrExamine {
    fn resume(&mut self, arg: Option<&[u8]>) -> ImapMailboxSelectResult {
        match self {
            Self::Select(s) => s.resume(arg),
            Self::Examine(e) => e.resume(arg),
        }
    }
}

enum State {
    Selecting {
        select: SelectOrExamine,
        page: Option<u32>,
        page_size: Option<u32>,
        item_names: MacroOrMessageDataItemNames<'static>,
    },
    Fetching(ImapMessageFetch),
    Done,
}

/// I/O-free coroutine wrapping `SELECT <mailbox>` followed by `FETCH
/// <window> (UID FLAGS ENVELOPE RFC822.SIZE [BODYSTRUCTURE])`. The
/// window is computed from `EXISTS` and the requested `page`/`page_size`
/// (1-indexed; page 1 = most recent). Read-write by default; use
/// [`ImapEnvelopeList::read_only`] for `EXAMINE`.
pub struct ImapEnvelopeList {
    state: State,
}

impl ImapEnvelopeList {
    /// `page_size = None` fetches the whole mailbox; `page = None` is
    /// treated as page 1. `with_attachment = true` additionally fetches
    /// `BODYSTRUCTURE` to populate [`Envelope::has_attachment`].
    pub fn new(
        context: ImapContext,
        mailbox: ImapMailbox<'static>,
        page: Option<u32>,
        page_size: Option<u32>,
        with_attachment: bool,
    ) -> Self {
        trace!("prepare IMAP envelope listing");
        Self::with_select(
            SelectOrExamine::Select(ImapMailboxSelect::new(context, mailbox)),
            page,
            page_size,
            with_attachment,
        )
    }

    /// Read-only variant: issues `EXAMINE` instead of `SELECT`.
    pub fn read_only(
        context: ImapContext,
        mailbox: ImapMailbox<'static>,
        page: Option<u32>,
        page_size: Option<u32>,
        with_attachment: bool,
    ) -> Self {
        trace!("prepare IMAP envelope listing (read-only)");
        Self::with_select(
            SelectOrExamine::Examine(ImapMailboxExamine::new(context, mailbox)),
            page,
            page_size,
            with_attachment,
        )
    }

    fn with_select(
        select: SelectOrExamine,
        page: Option<u32>,
        page_size: Option<u32>,
        with_attachment: bool,
    ) -> Self {
        let item_names = build_item_names(with_attachment);
        Self {
            state: State::Selecting {
                select,
                page,
                page_size,
                item_names,
            },
        }
    }

    pub fn resume(&mut self, mut arg: Option<&[u8]>) -> ImapEnvelopeListResult {
        loop {
            match mem::replace(&mut self.state, State::Done) {
                State::Selecting {
                    mut select,
                    page,
                    page_size,
                    item_names,
                } => match select.resume(arg.take()) {
                    ImapMailboxSelectResult::WantsRead => {
                        self.state = State::Selecting {
                            select,
                            page,
                            page_size,
                            item_names,
                        };
                        return ImapEnvelopeListResult::WantsRead;
                    }
                    ImapMailboxSelectResult::WantsWrite(bytes) => {
                        self.state = State::Selecting {
                            select,
                            page,
                            page_size,
                            item_names,
                        };
                        return ImapEnvelopeListResult::WantsWrite(bytes);
                    }
                    ImapMailboxSelectResult::Err { err, .. } => {
                        return ImapEnvelopeListResult::Err(err.into());
                    }
                    ImapMailboxSelectResult::Ok { context, data } => {
                        let exists = data.exists.unwrap_or(0);

                        let Some(window) = compute_window(exists, page, page_size) else {
                            return ImapEnvelopeListResult::Ok(Vec::new());
                        };

                        let sequence_set = match window.as_str().try_into() {
                            Ok(set) => set,
                            Err(_) => {
                                return ImapEnvelopeListResult::Err(
                                    ImapEnvelopeListError::InvalidWindow(
                                        window,
                                        "could not parse computed sequence-set",
                                    ),
                                );
                            }
                        };

                        let fetch = ImapMessageFetch::new(context, sequence_set, item_names, false);
                        self.state = State::Fetching(fetch);
                    }
                },
                State::Fetching(mut fetch) => match fetch.resume(arg.take()) {
                    ImapMessageFetchResult::WantsRead => {
                        self.state = State::Fetching(fetch);
                        return ImapEnvelopeListResult::WantsRead;
                    }
                    ImapMessageFetchResult::WantsWrite(bytes) => {
                        self.state = State::Fetching(fetch);
                        return ImapEnvelopeListResult::WantsWrite(bytes);
                    }
                    ImapMessageFetchResult::Err { err, .. } => {
                        return ImapEnvelopeListResult::Err(err.into());
                    }
                    ImapMessageFetchResult::Ok { data, .. } => {
                        // BTreeMap iterates ascending by sequence number
                        // (oldest first); reverse so the freshest comes
                        // first.
                        let envelopes = data
                            .into_iter()
                            .rev()
                            .map(|(seq, items)| envelope_from(seq.get(), items.into_inner()))
                            .collect();
                        return ImapEnvelopeListResult::Ok(envelopes);
                    }
                },
                State::Done => {
                    return ImapEnvelopeListResult::Err(ImapEnvelopeListError::AlreadyDone);
                }
            }
        }
    }
}

/// Builds the FETCH item-name list used by both the wrapped coroutine
/// and the `EmailClientStd` shortcut path. Always requests UID + FLAGS +
/// ENVELOPE + RFC822.SIZE; appends BODYSTRUCTURE when the caller opted
/// in to attachment detection.
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

/// Computes the IMAP sequence-set string for `(page, page_size)` against
/// `exists`. `None` means an empty window (empty mailbox, page-size 0,
/// or page beyond the last one).
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
    let mut message_id: Option<String> = None;
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
                    subject = decode_mime_bytes(s.as_ref());
                }
                if let Some(d) = env.date.into_option() {
                    let raw = bytes_to_string(d.as_ref());
                    date = parse_rfc2822_date(&raw);
                }
                if let Some(m) = env.message_id.into_option() {
                    let raw = bytes_to_string(m.as_ref());
                    message_id = normalize_message_id(&raw);
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
        message_id,
        flags,
        subject,
        from,
        to,
        date,
        size,
        has_attachment,
    }
}

fn flag_from(fetch: FlagFetch<'_>) -> Option<Flag> {
    let FlagFetch::Flag(flag) = fetch else {
        return None;
    };
    Some(Flag::from_raw(flag.to_string()))
}

fn address_from(addr: &ImapAddress<'_>) -> Address {
    let name = addr
        .name
        .0
        .as_ref()
        .map(|s| decode_mime_bytes(s.as_ref()))
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

fn body_structure_has_attachment(structure: &BodyStructure<'_>) -> bool {
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
    from_utf8(bytes)
        .map(ToString::to_string)
        .unwrap_or_else(|_| {
            let mut out = String::with_capacity(bytes.len());

            for b in bytes {
                out.push(*b as char);
            }

            out
        })
}

/// Decodes RFC 2047 MIME-encoded words (e.g. `=?utf-8?B?...?=`) that
/// commonly appear in IMAP `ENVELOPE` subjects and address display
/// names. Falls back to [`bytes_to_string`] when the input is not a
/// well-formed encoded-word sequence; the decoder also retains literal
/// runs that surround encoded tokens.
fn decode_mime_bytes(bytes: &[u8]) -> String {
    let decoder = Decoder::new().too_long_encoded_word_strategy(RecoverStrategy::Decode);
    match decoder.decode(bytes) {
        Ok(s) => s,
        Err(err) => {
            trace!("cannot decode RFC 2047 bytes: {err}");
            bytes_to_string(bytes)
        }
    }
}
