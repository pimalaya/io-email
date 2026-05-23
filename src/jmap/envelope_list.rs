//! JMAP envelope listing (`Email/query` + `Email/get`), wrapping
//! [`io_jmap::rfc8621::email_query::JmapEmailQuery`].

use alloc::vec::Vec;

use chrono::{DateTime, FixedOffset};
use io_jmap::{
    rfc8620::session::JmapSession,
    rfc8621::{
        email::{Email, EmailAddress as JmapAddress, EmailProperty},
        email_query::{JmapEmailQuery, JmapEmailQueryError, JmapEmailQueryResult},
    },
};
use log::trace;
use secrecy::SecretString;

use crate::{
    address::Address,
    envelope::{Envelope, normalize_message_id},
    flag::Flag,
};

/// Result returned by [`JmapEnvelopeList::resume`].
#[derive(Debug)]
pub enum JmapEnvelopeListResult {
    Ok(Vec<Envelope>),
    WantsRead,
    WantsWrite(Vec<u8>),
    Err(JmapEmailQueryError),
}

/// I/O-free coroutine listing JMAP envelopes for the session's primary
/// mail account; one batched `Email/query` + `Email/get` request.
pub struct JmapEnvelopeList {
    inner: JmapEmailQuery,
}

impl JmapEnvelopeList {
    /// `page` is 1-indexed; `page_size = None` lets the server pick.
    pub fn new(
        session: &JmapSession,
        http_auth: &SecretString,
        page: Option<u32>,
        page_size: Option<u32>,
    ) -> Result<Self, JmapEmailQueryError> {
        trace!("prepare JMAP envelope listing");
        let (position, limit) = compute_position_limit(page, page_size);
        let inner = JmapEmailQuery::new(
            session,
            http_auth,
            None,
            None,
            position,
            limit,
            Some(envelope_properties()),
        )?;
        Ok(Self { inner })
    }

    pub fn resume(&mut self, arg: Option<&[u8]>) -> JmapEnvelopeListResult {
        match self.inner.resume(arg) {
            JmapEmailQueryResult::WantsRead => JmapEnvelopeListResult::WantsRead,
            JmapEmailQueryResult::WantsWrite(bytes) => JmapEnvelopeListResult::WantsWrite(bytes),
            JmapEmailQueryResult::Ok { emails, .. } => {
                let envelopes = emails.into_iter().map(Envelope::from).collect();
                JmapEnvelopeListResult::Ok(envelopes)
            }
            JmapEmailQueryResult::Err(err) => JmapEnvelopeListResult::Err(err),
        }
    }
}

fn compute_position_limit(page: Option<u32>, page_size: Option<u32>) -> (Option<u64>, Option<u64>) {
    let Some(size) = page_size else {
        return (None, None);
    };

    let page = page.unwrap_or(1).max(1);
    let position = ((page - 1) as u64).saturating_mul(size as u64);

    (Some(position), Some(size as u64))
}

/// Properties requested from `Email/get` to populate an [`Envelope`].
/// Uses `sentAt` (author-claimed `Date:`) rather than `receivedAt` for
/// cross-backend consistency.
pub fn envelope_properties() -> Vec<EmailProperty> {
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

impl From<Email> for Envelope {
    fn from(email: Email) -> Self {
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
            .map(Address::from)
            .collect();

        let to = email
            .to
            .unwrap_or_default()
            .into_iter()
            .map(Address::from)
            .collect();

        let date = email.sent_at.as_deref().and_then(parse_rfc3339);

        let size = email.size.unwrap_or(0);
        let has_attachment = email.has_attachment;

        // JMAP returns `messageId` as a list (RFC 5322 allows multiple
        // header instances). The first non-empty entry is the
        // canonical value.
        let message_id = email
            .message_id
            .and_then(|ids| ids.into_iter().find_map(|s| normalize_message_id(&s)));

        Self {
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
}

impl From<JmapAddress> for Address {
    fn from(addr: JmapAddress) -> Self {
        Self {
            name: addr.name,
            email: addr.email,
        }
    }
}

fn parse_rfc3339(raw: &str) -> Option<DateTime<FixedOffset>> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }
    DateTime::parse_from_rfc3339(trimmed).ok()
}
