//! Mailbox shared across all protocols.

use alloc::string::String;

/// A mailbox (a.k.a. folder).
///
/// Strict least-common-denominator shape: only fields that are
/// first-class in every protocol the crate targets (IMAP, JMAP,
/// Maildir, m2dir, mbox, notmuch). Protocol-specific data (IMAP
/// delimiter and SPECIAL-USE attributes, JMAP role and rights,
/// Maildir path, …) is intentionally absent — for these, use the
/// corresponding protocol-specific crate directly.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "kebab-case"))]
pub struct Mailbox {
    /// Backend-specific identifier.
    ///
    /// JMAP exposes a real opaque ID; for IMAP, Maildir, mbox and
    /// notmuch this is the same as [`Self::name`]. Use this when
    /// issuing follow-up commands that refer to the mailbox.
    pub id: String,

    /// Human-readable mailbox name.
    pub name: String,

    /// Total number of messages, when the caller requested counts.
    /// `None` when the backend was not asked or cannot answer
    /// cheaply.
    #[cfg_attr(feature = "serde", serde(default))]
    pub total: Option<u64>,

    /// Number of unread messages, when the caller requested counts.
    /// `None` when the backend was not asked or cannot answer
    /// cheaply.
    #[cfg_attr(feature = "serde", serde(default))]
    pub unread: Option<u64>,
}

/// Special-use role of a mailbox.
///
/// Mirrors the IANA JMAP mailbox roles and the IMAP SPECIAL-USE
/// attributes (RFC 6154). [`MailboxRole::Other`] holds any value that
/// does not match a known role.
///
/// Not part of the shared [`Mailbox`] shape — only IMAP and JMAP
/// expose roles natively. Protocol-specific commands consume this
/// enum directly when they need to render or filter by role.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "kebab-case"))]
pub enum MailboxRole {
    Inbox,
    Archive,
    Drafts,
    Flagged,
    Important,
    Junk,
    Sent,
    Trash,
    Other(String),
}

impl MailboxRole {
    pub fn parse(raw: &str) -> Self {
        match raw.trim_start_matches('\\').to_ascii_lowercase().as_str() {
            "inbox" => Self::Inbox,
            "archive" => Self::Archive,
            "drafts" => Self::Drafts,
            "flagged" => Self::Flagged,
            "important" => Self::Important,
            "junk" | "spam" => Self::Junk,
            "sent" => Self::Sent,
            "trash" => Self::Trash,
            _ => Self::Other(raw.into()),
        }
    }
}
