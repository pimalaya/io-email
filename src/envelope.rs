//! Envelope shared across all protocols.

use alloc::{collections::BTreeSet, string::String, vec::Vec};

use chrono::{DateTime, FixedOffset};

use crate::{address::Address, flag::Flag};

/// Per-message flag mutation surfaced by an incremental fetch
/// (IMAP CHANGEDSINCE / QRESYNC implicit FETCH, JMAP `Email/changes`
/// → `updated`). `flags` is the new authoritative set; callers
/// diff against their stored ancestor to derive add / remove
/// operations.
#[derive(Clone, Debug)]
pub struct FlagUpdate {
    pub id: String,
    pub flags: BTreeSet<Flag>,
}

/// Outcome of [`crate::client::EmailClientStd::diff_envelopes`].
///
/// `new_state` is the opaque per-backend checkpoint to persist for the
/// next call; format is private to the backend impl (IMAP packs
/// `(uid_validity, highest_mod_seq, highest_uid)`; JMAP stores the raw
/// `Email/state` bytes). Callers treat it as a `Vec<u8>` blob.
#[derive(Clone, Debug)]
pub enum EnvelopeDiff {
    /// First sync, server-side state invalidated, or capability
    /// missing. The caller must fall back to a regular
    /// `list_envelopes` round-trip and treat the result as the new
    /// baseline. `new_state` may be `None` when the backend could not
    /// cheaply capture a baseline without listing.
    FullListRequired { new_state: Option<Vec<u8>> },

    /// The backend returned a pre-diffed delta. The three buckets are
    /// disjoint; the caller applies them against the stored ancestor.
    Incremental {
        new_state: Vec<u8>,
        flag_updates: Vec<FlagUpdate>,
        new_envelopes: Vec<Envelope>,
        vanished_ids: Vec<String>,
    },
}

/// Lightweight summary of a message: enough to display in a list
/// without fetching the full body.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "kebab-case"))]
pub struct Envelope {
    /// Backend-specific identifier of the message.
    ///
    /// IMAP UID, JMAP email ID or Maildir filename id.
    pub id: String,

    /// `Message-ID:` header value (RFC 5322 §3.6.4), `None` when the
    /// header is missing or the backend did not surface it. Stable
    /// across every backend that stores the message, so sync uses it
    /// as the cross-side content key when available.
    #[cfg_attr(feature = "serde", serde(default))]
    pub message_id: Option<String>,

    /// Flags set on the message. Stored as a sorted set since wire
    /// order is not meaningful and duplicates are nonsensical; the
    /// derived `Ord` on `Flag` gives deterministic iteration.
    #[cfg_attr(feature = "serde", serde(default))]
    pub flags: BTreeSet<Flag>,

    /// Subject header value.
    #[cfg_attr(feature = "serde", serde(default))]
    pub subject: String,

    /// Sender(s).
    #[cfg_attr(feature = "serde", serde(default))]
    pub from: Vec<Address>,

    /// Primary recipient(s).
    #[cfg_attr(feature = "serde", serde(default))]
    pub to: Vec<Address>,

    /// Author-claimed send time, taken from the `Date:` header
    /// (IMAP `ENVELOPE.date`, JMAP `sentAt`, parsed `Date:` for
    /// Maildir).
    ///
    /// Server-arrival timestamps (IMAP `INTERNALDATE`, JMAP
    /// `receivedAt`, filesystem mtime) are intentionally not used:
    /// Maildir mtime is rewritten by every fresh sync, and the
    /// `Date:` header is the only timestamp consistent across every
    /// backend. `None` when the header is missing or unparseable.
    #[cfg_attr(feature = "serde", serde(default))]
    pub date: Option<DateTime<FixedOffset>>,

    /// Size of the raw RFC 5322 message in bytes. Free on every
    /// backend (IMAP `RFC822.SIZE`, JMAP `size`, filesystem entry
    /// size).
    #[cfg_attr(feature = "serde", serde(default))]
    pub size: u64,

    /// Whether the message has at least one attachment, when the
    /// caller opted in. JMAP returns this for free; IMAP requires a
    /// `BODYSTRUCTURE` fetch; Maildir requires parsing the message
    /// body. `None` when not requested or when detection is not
    /// implemented for the active backend.
    #[cfg_attr(feature = "serde", serde(default))]
    pub has_attachment: Option<bool>,
}

/// Strips RFC 5322 `msg-id` wrappers from the raw `Message-ID:` value
/// so every backend's [`Envelope::message_id`] is comparable
/// byte-for-byte. IMAP returns the header with the surrounding
/// `<...>`; `mail_parser` (m2dir, maildir) strips them; JMAP usually
/// strips them too. Whitespace and a single pair of angle brackets are
/// removed; an empty result becomes `None`.
pub fn normalize_message_id(raw: &str) -> Option<String> {
    use alloc::string::ToString;
    let trimmed = raw.trim();
    let inner = trimmed
        .strip_prefix('<')
        .and_then(|s| s.strip_suffix('>'))
        .unwrap_or(trimmed)
        .trim();
    if inner.is_empty() {
        None
    } else {
        Some(inner.to_string())
    }
}
