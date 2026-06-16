//! Shared helpers for the Gmail coroutines: system-label constants,
//! flag/label translation, address parsing, and Gmail message to
//! shared [`Envelope`] conversion.

use alloc::{
    collections::BTreeSet,
    string::{String, ToString},
    vec::Vec,
};

use chrono::{DateTime, FixedOffset};
use io_gmail::v1::rest::messages::GmailMessage;

use crate::{
    address::Address,
    envelope::types::{Envelope, normalize_message_id},
    flag::types::{Flag, FlagOp, IanaFlag},
};

/// Gmail system label marking an unread message; its absence means the
/// message has been seen.
pub(crate) const UNREAD: &str = "UNREAD";
/// Gmail system label backing the shared `\Flagged` flag.
pub(crate) const STARRED: &str = "STARRED";
/// Gmail system label backing the shared `$Important` flag.
pub(crate) const IMPORTANT: &str = "IMPORTANT";
/// Gmail system label backing the shared `\Draft` flag.
pub(crate) const DRAFT: &str = "DRAFT";
/// Gmail system label backing the shared `$Junk` flag.
pub(crate) const SPAM: &str = "SPAM";

/// Whether listing `mailbox` requires `includeSpamTrash`: Gmail hides
/// SPAM and TRASH messages from `messages.list` unless asked.
pub(crate) fn include_spam_trash(mailbox: &str) -> bool {
    matches!(mailbox, SPAM | "TRASH")
}

/// Derives the shared flag set from a Gmail message's label ids.
///
/// Only the flag-like system labels are surfaced; INBOX, SENT, custom
/// labels and categories are mailboxes, not flags.
pub(crate) fn flags_from_labels(label_ids: &[String]) -> BTreeSet<Flag> {
    let has = |label: &str| label_ids.iter().any(|id| id == label);

    let mut flags = BTreeSet::new();
    if !has(UNREAD) {
        flags.insert(Flag::from_iana(IanaFlag::Seen));
    }
    if has(STARRED) {
        flags.insert(Flag::from_iana(IanaFlag::Flagged));
    }
    if has(IMPORTANT) {
        flags.insert(Flag::from_iana(IanaFlag::Important));
    }
    if has(DRAFT) {
        flags.insert(Flag::from_iana(IanaFlag::Draft));
    }
    if has(SPAM) {
        flags.insert(Flag::from_iana(IanaFlag::Junk));
    }
    flags
}

/// Translates a flag-store operation into Gmail `(addLabelIds,
/// removeLabelIds)`.
///
/// `\Seen` maps to the *absence* of `UNREAD`, so its polarity is
/// inverted. Flags with no Gmail equivalent (Answered, Forwarded, …)
/// are dropped; custom keywords are passed through as label ids.
pub(crate) fn label_patch(flags: &[Flag], op: FlagOp) -> (Vec<String>, Vec<String>) {
    let mut add = Vec::new();
    let mut remove = Vec::new();

    match op {
        FlagOp::Add => {
            for flag in flags {
                if let Some((label, inverted)) = label_of(flag) {
                    if inverted { &mut remove } else { &mut add }.push(label);
                }
            }
        }
        FlagOp::Remove => {
            for flag in flags {
                if let Some((label, inverted)) = label_of(flag) {
                    if inverted { &mut add } else { &mut remove }.push(label);
                }
            }
        }
        FlagOp::Set => {
            // Drive each known flag-label to its target presence; Gmail
            // modify is idempotent so blind add/remove is safe.
            target(
                flags.iter().any(Flag::is_seen),
                UNREAD,
                true,
                &mut add,
                &mut remove,
            );
            target(
                flags.iter().any(Flag::is_flagged),
                STARRED,
                false,
                &mut add,
                &mut remove,
            );
            target(
                flags.iter().any(Flag::is_important),
                IMPORTANT,
                false,
                &mut add,
                &mut remove,
            );
            target(
                flags.iter().any(Flag::is_draft),
                DRAFT,
                false,
                &mut add,
                &mut remove,
            );
            target(
                flags.iter().any(Flag::is_junk),
                SPAM,
                false,
                &mut add,
                &mut remove,
            );

            for flag in flags {
                if flag.iana().is_none() {
                    add.push(flag.raw().to_string());
                }
            }
        }
    }

    (add, remove)
}

/// Folds a Gmail [`GmailMessage`] (metadata format) into the shared
/// [`Envelope`] shape. `has_attachment` is left `None`: Gmail metadata
/// does not expose the MIME structure.
pub(crate) fn envelope_from(message: GmailMessage) -> Envelope {
    let flags = flags_from_labels(&message.label_ids);
    let size = message.size_estimate.unwrap_or(0);

    let mut subject = String::new();
    let mut from = Vec::new();
    let mut to = Vec::new();
    let mut date = None;
    let mut message_id = None;

    if let Some(payload) = &message.payload {
        subject = payload.header("Subject").unwrap_or_default().to_string();
        from = parse_addresses(payload.header("From").unwrap_or_default());
        to = parse_addresses(payload.header("To").unwrap_or_default());
        date = payload.header("Date").and_then(parse_rfc2822);
        message_id = payload.header("Message-ID").and_then(normalize_message_id);
    }

    Envelope {
        id: message.id,
        message_id,
        flags,
        subject,
        from,
        to,
        date,
        size,
        has_attachment: None,
    }
}

/// Maps a shared flag to its Gmail `(label, inverted)` pair, or `None`
/// when Gmail has no equivalent and the flag should be dropped.
fn label_of(flag: &Flag) -> Option<(String, bool)> {
    match flag.iana() {
        Some(IanaFlag::Seen) => Some((UNREAD.into(), true)),
        Some(IanaFlag::Flagged) => Some((STARRED.into(), false)),
        Some(IanaFlag::Important) => Some((IMPORTANT.into(), false)),
        Some(IanaFlag::Draft) => Some((DRAFT.into(), false)),
        Some(IanaFlag::Junk) => Some((SPAM.into(), false)),
        Some(_) => None,
        None => Some((flag.raw().to_string(), false)),
    }
}

/// Maps a Gmail flag-like system label to its shared `(flag, inverted)`
/// pair, or `None` when the label is not a flag (INBOX, custom labels,
/// categories). The reverse of [`label_of`], used to translate
/// `history.list` label deltas into flag events.
pub(crate) fn flag_of_label(label: &str) -> Option<(Flag, bool)> {
    match label {
        UNREAD => Some((Flag::from_iana(IanaFlag::Seen), true)),
        STARRED => Some((Flag::from_iana(IanaFlag::Flagged), false)),
        IMPORTANT => Some((Flag::from_iana(IanaFlag::Important), false)),
        DRAFT => Some((Flag::from_iana(IanaFlag::Draft), false)),
        SPAM => Some((Flag::from_iana(IanaFlag::Junk), false)),
        _ => None,
    }
}

/// Pushes `label` onto `add` or `remove` so that its final presence
/// matches `wanted`, honouring the inverted polarity of `UNREAD`.
fn target(
    wanted: bool,
    label: &str,
    inverted: bool,
    add: &mut Vec<String>,
    remove: &mut Vec<String>,
) {
    let present = wanted ^ inverted;
    if present { add } else { remove }.push(label.into());
}

/// Parses an RFC 5322 address-list header value into shared addresses.
///
/// Best-effort: splits on commas and extracts the `<addr>` plus an
/// optional display name. Good enough for envelope display; full
/// grammar parsing belongs to a protocol-specific path.
fn parse_addresses(raw: &str) -> Vec<Address> {
    raw.split(',')
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .map(parse_address)
        .collect()
}

fn parse_address(part: &str) -> Address {
    if let Some(open) = part.rfind('<') {
        if let Some(end) = part[open..].find('>') {
            let email = part[open + 1..open + end].trim().to_string();
            let name = part[..open].trim().trim_matches('"').trim();
            let name = (!name.is_empty()).then(|| name.to_string());
            return Address { name, email };
        }
    }
    Address {
        name: None,
        email: part.to_string(),
    }
}

fn parse_rfc2822(raw: &str) -> Option<DateTime<FixedOffset>> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }
    DateTime::parse_from_rfc2822(trimmed).ok()
}
