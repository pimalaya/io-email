//! IMAP list-envelopes coroutine: SELECT then a windowed FETCH (UID
//! FLAGS ENVELOPE RFC822.SIZE, plus BODYSTRUCTURE when
//! `with_attachment`). Page 1 is the most recent window (RFC 3501
//! §6.4.5).
//!
//! # Example
//!
//! ```rust,ignore
//! use io_email::imap::envelope_list::ImapEnvelopeList;
//!
//! let envs = client.run(ImapEnvelopeList::new("INBOX", Some(1), Some(50), false)?)?;
//! ```

use alloc::{
    collections::BTreeSet,
    format,
    string::{String, ToString},
    vec,
    vec::Vec,
};
use core::{mem, str::from_utf8};

use chrono::{DateTime, FixedOffset};
use io_imap::{
    codec::fragmentizer::Fragmentizer,
    coroutine::{ImapCoroutine, ImapCoroutineState, ImapYield},
    rfc3501::{
        fetch::{ImapMessageFetch, ImapMessageFetchError, ImapMessageFetchOptions},
        select::{ImapMailboxSelect, ImapMailboxSelectError, ImapMailboxSelectOptions},
    },
    types::{
        body::BodyStructure,
        envelope::Address as ImapAddress,
        fetch::{MacroOrMessageDataItemNames, MessageDataItem, MessageDataItemName},
        flag::FlagFetch,
    },
};
use log::trace;
use rfc2047_decoder::{Decoder, RecoverStrategy};
use thiserror::Error;

use crate::{
    address::Address,
    envelope::{Envelope, normalize_message_id},
    flag::Flag,
    imap::convert::{InvalidMailboxName, parse_mailbox},
};

/// Errors produced by [`ImapEnvelopeList`].
#[derive(Debug, Error)]
pub enum ImapEnvelopeListError {
    #[error(transparent)]
    Select(#[from] ImapMailboxSelectError),
    #[error(transparent)]
    Fetch(#[from] ImapMessageFetchError),
    #[error("invalid IMAP mailbox `{0}`")]
    InvalidMailbox(String),
    #[error("computed sequence-set window {0:?} is invalid")]
    InvalidWindow(String),
    #[error("coroutine was resumed after completion")]
    ResumedAfterDone,
}

impl From<InvalidMailboxName> for ImapEnvelopeListError {
    fn from(err: InvalidMailboxName) -> Self {
        Self::InvalidMailbox(err.0)
    }
}

/// I/O-free coroutine listing envelopes from a mailbox.
pub struct ImapEnvelopeList {
    state: State,
}

impl ImapEnvelopeList {
    /// `page_size = None` fetches the whole mailbox; `page = None` is
    /// page 1.
    pub fn new(
        mailbox: &str,
        page: Option<u32>,
        page_size: Option<u32>,
        with_attachment: bool,
    ) -> Result<Self, ImapEnvelopeListError> {
        trace!("prepare IMAP envelope listing");
        let mbox = parse_mailbox(mailbox)?;
        Ok(Self {
            state: State::Selecting {
                select: ImapMailboxSelect::new(mbox, ImapMailboxSelectOptions::default()),
                page,
                page_size,
                item_names: build_item_names(with_attachment),
            },
        })
    }
}

enum State {
    Selecting {
        select: ImapMailboxSelect,
        page: Option<u32>,
        page_size: Option<u32>,
        item_names: MacroOrMessageDataItemNames<'static>,
    },
    Fetching(ImapMessageFetch),
    Done,
}

/// FETCH item-name list: UID + FLAGS + ENVELOPE + RFC822.SIZE, plus
/// BODYSTRUCTURE when `with_attachment` is set.
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

/// Sequence-set string for `(page, page_size)` against `exists`,
/// or `None` for an empty window.
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

/// Folds one FETCH row into a shared [`Envelope`].
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
            MessageDataItem::Flags(fs) => {
                flags = fs.into_iter().filter_map(flag_from_fetch).collect();
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

fn flag_from_fetch(fetch: FlagFetch<'_>) -> Option<Flag> {
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

/// Decodes RFC 2047 MIME-encoded words from IMAP ENVELOPE strings;
/// falls back to [`bytes_to_string`] on malformed input.
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

impl ImapCoroutine for ImapEnvelopeList {
    type Yield = ImapYield;
    type Return = Result<Vec<Envelope>, ImapEnvelopeListError>;

    fn resume(
        &mut self,
        fragmentizer: &mut Fragmentizer,
        mut bytes: Option<&[u8]>,
    ) -> ImapCoroutineState<Self::Yield, Self::Return> {
        loop {
            match mem::replace(&mut self.state, State::Done) {
                State::Selecting {
                    mut select,
                    page,
                    page_size,
                    item_names,
                } => match select.resume(fragmentizer, bytes.take()) {
                    ImapCoroutineState::Yielded(yielded) => {
                        self.state = State::Selecting {
                            select,
                            page,
                            page_size,
                            item_names,
                        };
                        return ImapCoroutineState::Yielded(yielded);
                    }
                    ImapCoroutineState::Complete(Ok(data)) => {
                        let exists = data.exists.unwrap_or(0);
                        let Some(window) = compute_window(exists, page, page_size) else {
                            return ImapCoroutineState::Complete(Ok(Vec::new()));
                        };
                        let sequence_set = match window.as_str().try_into() {
                            Ok(set) => set,
                            Err(_) => {
                                return ImapCoroutineState::Complete(Err(
                                    ImapEnvelopeListError::InvalidWindow(window),
                                ));
                            }
                        };
                        self.state = State::Fetching(ImapMessageFetch::new(
                            sequence_set,
                            item_names,
                            ImapMessageFetchOptions::default(),
                        ));
                    }
                    ImapCoroutineState::Complete(Err(err)) => {
                        return ImapCoroutineState::Complete(Err(err.into()));
                    }
                },
                State::Fetching(mut fetch) => match fetch.resume(fragmentizer, bytes.take()) {
                    ImapCoroutineState::Yielded(yielded) => {
                        self.state = State::Fetching(fetch);
                        return ImapCoroutineState::Yielded(yielded);
                    }
                    ImapCoroutineState::Complete(Ok(data)) => {
                        let envelopes = data
                            .into_iter()
                            .rev()
                            .map(|(seq, items)| envelope_from(seq.get(), items.into_inner()))
                            .collect();
                        return ImapCoroutineState::Complete(Ok(envelopes));
                    }
                    ImapCoroutineState::Complete(Err(err)) => {
                        return ImapCoroutineState::Complete(Err(err.into()));
                    }
                },
                State::Done => {
                    return ImapCoroutineState::Complete(Err(
                        ImapEnvelopeListError::ResumedAfterDone,
                    ));
                }
            }
        }
    }
}
