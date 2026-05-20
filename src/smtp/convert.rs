//! Conversions between SMTP wire types and the shared types used by
//! [`EmailClientStd`], plus the `From` impl that wraps an
//! already-connected [`SmtpClientStd<StreamStd>`] into a fresh unified
//! client with SMTP as the only registered backend.

use alloc::{
    borrow::{Cow, ToOwned},
    string::ToString,
};

use io_smtp::{
    client::SmtpClientStd,
    rfc5321::types::{
        domain::Domain, ehlo_domain::EhloDomain, local_part::LocalPart,
        mailbox::Mailbox as SmtpMailbox,
    },
};
use pimalaya_stream::std::stream::StreamStd;

use crate::client::{EmailClientStd, EmailClientStdError};

impl From<SmtpClientStd<StreamStd>> for EmailClientStd {
    fn from(client: SmtpClientStd<StreamStd>) -> Self {
        Self::new().with_smtp(client)
    }
}

/// Parses a bare `local@domain` string into an SMTP [`Mailbox`].
pub(crate) fn parse_mailbox(addr: &str) -> Result<SmtpMailbox<'static>, EmailClientStdError> {
    let (local, domain) = addr
        .split_once('@')
        .ok_or_else(|| EmailClientStdError::InvalidAddress(addr.to_string()))?;
    Ok(SmtpMailbox {
        local_part: LocalPart(Cow::Owned(local.to_owned())),
        domain: EhloDomain::Domain(Domain(Cow::Owned(domain.to_owned()))),
    })
}
