//! Shared helpers for the JMAP coroutines: keywords, pagination,
//! account id, and JMAP email to shared [`Envelope`] conversion.

use alloc::{
    string::{String, ToString},
    vec,
    vec::Vec,
};

use chrono::{DateTime, FixedOffset};
use io_jmap::{
    rfc8620::JmapSession,
    rfc8621::{
        MAIL_CAPABILITY,
        email::{JmapEmail, JmapEmailAddress, JmapEmailProperty},
    },
};

use crate::{
    address::Address,
    envelope::{Envelope, normalize_message_id},
    flag::{Flag, IanaFlag},
};

/// Maps a shared [`Flag`] to its JMAP keyword (RFC 8621 §4.1.1).
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

/// Translates 1-indexed `(page, page_size)` to JMAP `(position, limit)`.
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

/// Primary mail account id; empty when the session has none.
pub(crate) fn account_id_of(session: &JmapSession) -> String {
    session
        .primary_accounts
        .get(MAIL_CAPABILITY)
        .cloned()
        .unwrap_or_default()
}

/// Email/get properties for an [`Envelope`]. Uses sentAt
/// (author-claimed Date:) for cross-backend consistency.
pub(crate) fn envelope_properties() -> Vec<JmapEmailProperty> {
    vec![
        JmapEmailProperty::Id,
        JmapEmailProperty::Keywords,
        JmapEmailProperty::Subject,
        JmapEmailProperty::From,
        JmapEmailProperty::To,
        JmapEmailProperty::SentAt,
        JmapEmailProperty::Size,
        JmapEmailProperty::HasAttachment,
        JmapEmailProperty::MessageId,
    ]
}

/// Folds a JMAP [`JmapEmail`] object into the shared [`Envelope`] shape.
pub(crate) fn envelope_from(email: JmapEmail) -> Envelope {
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
    // NOTE: JMAP returns messageId as a list (RFC 5322 allows multiple
    // header instances); first non-empty entry is canonical.
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

fn address_from(addr: JmapEmailAddress) -> Address {
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
