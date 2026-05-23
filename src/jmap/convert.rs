//! Conversions between JMAP wire types and the shared types used by
//! [`EmailClientStd`], plus the `From` impl that wraps an
//! already-built [`JmapClientStd`] into a fresh unified client with
//! JMAP as the only registered backend.

use alloc::string::{String, ToString};

use io_jmap::{client::JmapClientStd, rfc8621::email::EmailFilter};

use crate::{client::EmailClientStd, flag::Flag};

impl From<JmapClientStd> for EmailClientStd {
    fn from(client: JmapClientStd) -> Self {
        Self::new().with_jmap(client)
    }
}

/// Maps a shared [`Flag`] to its JMAP keyword.
///
/// IANA-classified flags render as the lowercase canonical keyword
/// (`$seen`, `$forwarded`, …) per RFC 8621 §4.1.1; custom user
/// keywords pass through their raw wire spelling unchanged.
pub(crate) fn keyword_from(flag: &Flag) -> String {
    use crate::flag::IanaFlag;

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

/// Translates a shared mailbox name into a JMAP [`EmailFilter`].
/// Returns `None` when no mailbox is selected (queries the whole
/// account).
pub(crate) fn mailbox_filter(mailbox: &str) -> Option<EmailFilter> {
    if mailbox.is_empty() {
        return None;
    }
    Some(EmailFilter {
        in_mailbox: Some(mailbox.to_string()),
        ..EmailFilter::default()
    })
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
