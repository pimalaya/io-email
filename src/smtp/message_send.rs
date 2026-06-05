//! SMTP message-send coroutine wrapping
//! [`io_smtp::message::SmtpMessageSend`]: runs the RFC 5321 mail
//! transaction (MAIL FROM, RCPT TO, DATA) on an authenticated stream.
//!
//! Reverse path is the From: header; forward paths are To:/Cc:/Bcc:.
//! Override the envelope sender via
//! [`SmtpContext::default_reverse_path`].
//!
//! # Example
//!
//! ```rust,ignore
//! use io_email::smtp::message_send::SmtpMessageSend;
//!
//! client.run(SmtpMessageSend::new(raw, None)?)?;
//! ```
//!
//! [`SmtpContext::default_reverse_path`]: crate::client::SmtpContext::default_reverse_path

use alloc::{
    borrow::Cow,
    string::{String, ToString},
    vec::Vec,
};

use io_smtp::{
    coroutine::{SmtpCoroutine, SmtpCoroutineState, SmtpYield},
    message::{SmtpMessageSend as InnerSend, SmtpMessageSendError as InnerErr},
    rfc5321::types::{
        domain::Domain, ehlo_domain::EhloDomain, forward_path::ForwardPath, local_part::LocalPart,
        mailbox::Mailbox as SmtpMailbox, reverse_path::ReversePath,
    },
};
use log::trace;
use mail_parser::{Address as MailParserAddress, MessageParser};
use thiserror::Error;

/// Errors produced by [`SmtpMessageSend`].
#[derive(Debug, Error)]
pub enum SmtpMessageSendError {
    #[error(transparent)]
    Send(#[from] InnerErr),
    #[error("could not parse raw RFC 5322 message")]
    Parse,
    #[error("no `From:` header found in raw message and no SMTP override set")]
    MissingReversePath,
    #[error("no `To:` / `Cc:` / `Bcc:` recipients found in raw message")]
    MissingForwardPaths,
    #[error("invalid email address `{0}` in envelope")]
    InvalidAddress(String),
}

/// I/O-free coroutine running the RFC 5321 mail transaction over an
/// already authenticated stream.
pub struct SmtpMessageSend {
    inner: InnerSend,
}

impl SmtpMessageSend {
    /// `override_reverse_path` (e.g. from
    /// [`SmtpContext::default_reverse_path`]) takes precedence over
    /// the message's From: header.
    ///
    /// [`SmtpContext::default_reverse_path`]: crate::client::SmtpContext::default_reverse_path
    pub fn new(
        raw: Vec<u8>,
        override_reverse_path: Option<&str>,
    ) -> Result<Self, SmtpMessageSendError> {
        trace!("prepare SMTP message send");

        let parsed = MessageParser::default()
            .parse_headers(&raw)
            .ok_or(SmtpMessageSendError::Parse)?;

        let reverse = match override_reverse_path {
            Some(addr) => parse_smtp_mailbox(addr)?,
            None => parsed
                .from()
                .and_then(first_address)
                .ok_or(SmtpMessageSendError::MissingReversePath)
                .and_then(|addr| parse_smtp_mailbox(&addr))?,
        };

        let mut forwards: Vec<SmtpMailbox<'static>> = Vec::new();
        for addrs in [parsed.to(), parsed.cc(), parsed.bcc()]
            .into_iter()
            .flatten()
        {
            for addr in addresses(addrs) {
                forwards.push(parse_smtp_mailbox(&addr)?);
            }
        }
        if forwards.is_empty() {
            return Err(SmtpMessageSendError::MissingForwardPaths);
        }

        let reverse_path = ReversePath::Mailbox(reverse);
        let forward_paths = forwards.into_iter().map(ForwardPath::from);
        Ok(Self {
            inner: InnerSend::new(reverse_path, forward_paths, raw),
        })
    }
}

/// Flattens a mail-parser address group into bare local-part@domain
/// strings.
fn addresses(addrs: &MailParserAddress<'_>) -> Vec<String> {
    addrs
        .clone()
        .into_list()
        .into_iter()
        .filter_map(|a| {
            let email = a.address?.into_owned();
            if email.is_empty() { None } else { Some(email) }
        })
        .collect()
}

/// First address in a group; used to pick the From: envelope sender.
fn first_address(addrs: &MailParserAddress<'_>) -> Option<String> {
    addresses(addrs).into_iter().next()
}

impl SmtpCoroutine for SmtpMessageSend {
    type Yield = SmtpYield;
    type Return = Result<(), SmtpMessageSendError>;

    fn resume(&mut self, bytes: Option<&[u8]>) -> SmtpCoroutineState<Self::Yield, Self::Return> {
        match self.inner.resume(bytes) {
            SmtpCoroutineState::Yielded(y) => SmtpCoroutineState::Yielded(y),
            SmtpCoroutineState::Complete(r) => SmtpCoroutineState::Complete(r.map_err(Into::into)),
        }
    }
}

/// Parses local-part@domain into a 'static [`SmtpMailbox`] (owned).
fn parse_smtp_mailbox(addr: &str) -> Result<SmtpMailbox<'static>, SmtpMessageSendError> {
    let (local, domain) = addr
        .rsplit_once('@')
        .ok_or_else(|| SmtpMessageSendError::InvalidAddress(addr.to_string()))?;
    if local.is_empty() || domain.is_empty() {
        return Err(SmtpMessageSendError::InvalidAddress(addr.to_string()));
    }
    Ok(SmtpMailbox {
        local_part: LocalPart(Cow::Owned(local.to_string())),
        domain: EhloDomain::Domain(Domain(Cow::Owned(domain.to_string()))),
    })
}
