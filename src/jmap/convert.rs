//! Shared helpers for the JMAP coroutines: keyword translation,
//! pagination, account id extraction, and the `Email` → [`Envelope`]
//! conversion used by `envelope_list` and `message_*`.

use alloc::{
    string::{String, ToString},
    vec,
    vec::Vec,
};

use chrono::{DateTime, FixedOffset};
use io_jmap::rfc8620::session::JmapSession;
use io_jmap::rfc8621::{
    capabilities,
    email::{Email, EmailAddress as JmapAddress, EmailProperty},
};

use crate::{
    address::Address,
    envelope::{Envelope, normalize_message_id},
    flag::{Flag, IanaFlag},
};

/// Maps a shared [`Flag`] to its JMAP keyword (RFC 8621 §4.1.1).
/// IANA-classified flags use the lowercase canonical form; custom
/// keywords pass through their raw wire spelling.
pub(crate) fn keyword_from(flag: &Flag) -> String {
    match flag.iana() {
        Some(IanaFlag::Seen) => "$seen".into(),
        Some(IanaFlag::Answered) => "$answered".into(),
        Some(IanaFlag::Flagged) => "$flagged".into(),
        Some(IanaFlag::Draft) => "$draft".into(),
        Some(IanaFlag::Deleted) => "$deleted".into(),
        Some(IanaFlag::Forwarded) => "$forwarded".into(),
        Some(IanaFlag::Junk) => "$junk".into(),
        Some(IanaFlag::NotJunk) => "$notjunk".into(),
        Some(IanaFlag::Phishing) => "$phishing".into(),
        Some(IanaFlag::Important) => "$important".into(),
        Some(IanaFlag::MdnSent) => "$mdnsent".into(),
        None => flag.raw().to_string(),
    }
}

/// Translates 1-indexed `(page, page_size)` into JMAP
/// `(position, limit)`. Both stay `None` when no page size is
/// requested.
pub(crate) fn compute_position_limit(
    page: Option<u32>,
    page_size: Option<u32>,
) -> (Option<u64>, Option<u64>) {
    let Some(size) = page_size else {
        return (None, None);
    };
    let page = page.unwrap_or(1).max(1);
    let position = ((page - 1) as u64).saturating_mul(size as u64);
    (Some(position), Some(size as u64))
}

/// Extracts the primary mail account id from a JMAP session, falling
/// back to an empty string when the session advertises no mail
/// account (mostly a defensive default).
pub(crate) fn account_id_of(session: &JmapSession) -> String {
    session
        .primary_accounts
        .get(capabilities::MAIL)
        .cloned()
        .unwrap_or_default()
}

/// Properties requested from `Email/get` to populate an [`Envelope`].
/// Uses `sentAt` (author-claimed `Date:`) rather than `receivedAt` for
/// cross-backend consistency.
pub(crate) fn envelope_properties() -> Vec<EmailProperty> {
    vec![
        EmailProperty::Id,
        EmailProperty::Keywords,
        EmailProperty::Subject,
        EmailProperty::From,
        EmailProperty::To,
        EmailProperty::SentAt,
        EmailProperty::Size,
        EmailProperty::HasAttachment,
        EmailProperty::MessageId,
    ]
}

/// Folds a JMAP [`Email`] object into the shared [`Envelope`] shape.
pub(crate) fn envelope_from(email: Email) -> Envelope {
    let id = email.id.unwrap_or_default();
    let flags = email
        .keywords
        .unwrap_or_default()
        .into_iter()
        .filter_map(|(k, v)| if v { Some(Flag::from_raw(k)) } else { None })
        .collect();
    let subject = email.subject.unwrap_or_default();
    let from = email
        .from
        .unwrap_or_default()
        .into_iter()
        .map(address_from)
        .collect();
    let to = email
        .to
        .unwrap_or_default()
        .into_iter()
        .map(address_from)
        .collect();
    let date = email.sent_at.as_deref().and_then(parse_rfc3339);
    let size = email.size.unwrap_or(0);
    let has_attachment = email.has_attachment;
    // JMAP returns `messageId` as a list (RFC 5322 allows multiple
    // header instances). The first non-empty entry is the canonical
    // value.
    let message_id = email
        .message_id
        .and_then(|ids| ids.into_iter().find_map(|s| normalize_message_id(&s)));
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

fn address_from(addr: JmapAddress) -> Address {
    Address {
        name: addr.name,
        email: addr.email,
    }
}

fn parse_rfc3339(raw: &str) -> Option<DateTime<FixedOffset>> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }
    DateTime::parse_from_rfc3339(trimmed).ok()
}
