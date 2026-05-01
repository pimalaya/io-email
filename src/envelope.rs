//! Envelope shared across all protocols.

use alloc::{collections::BTreeSet, string::String, vec::Vec};

use chrono::{DateTime, FixedOffset};

use crate::{address::Address, flag::Flag};

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
