//! SMTP message-send coroutine.
//!
//! Wraps [`io_smtp::send::SmtpMessageSend`]: runs an RFC 5321 mail
//! transaction (MAIL FROM → RCPT TO → DATA) against an already
//! authenticated SMTP stream. Reverse and forward paths are extracted
//! from the raw RFC 5322 message itself (`From:` → reverse path,
//! `To:` + `Cc:` + `Bcc:` → forward paths) via mail-parser, so the
//! shared API takes only the raw bytes as input.
//!
//! Callers needing a different envelope sender (alias accounts,
//! bounce-address rewriting) override the reverse path on
//! [`SmtpContext::default_reverse_path`].
//!
//! [`SmtpContext::default_reverse_path`]: crate::client::SmtpContext::default_reverse_path

use alloc::{
    borrow::Cow,
    string::{String, ToString},
    vec::Vec,
};

use io_smtp::{
    coroutine::{SmtpCoroutine, SmtpCoroutineState, SmtpYield},
    rfc5321::types::{
        domain::Domain, ehlo_domain::EhloDomain, forward_path::ForwardPath, local_part::LocalPart,
        mailbox::Mailbox as SmtpMailbox, reverse_path::ReversePath,
    },
    send::{SmtpMessageSend as InnerSend, SmtpMessageSendError as InnerErr},
};
use log::trace;
use mail_parser::{Address as MailParserAddress, MessageParser};
use thiserror::Error;

use crate::coroutine::{
    EmailBackend, EmailCoroutine, EmailCoroutineArg, EmailCoroutineState, SmtpStep,
};

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
    #[error("coroutine was resumed with the wrong EmailCoroutineArg variant")]
    InvalidArg,
}

/// I/O-free coroutine running the RFC 5321 mail transaction over an
/// already authenticated stream.
pub struct SmtpMessageSend {
    inner: InnerSend,
}

impl SmtpMessageSend {
    /// Builds the coroutine from the raw RFC 5322 bytes. When
    /// `override_reverse_path` is set (e.g. via
    /// [`SmtpContext::default_reverse_path`]), it takes precedence
    /// over the message's `From:` header.
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

impl EmailCoroutine for SmtpMessageSend {
    type Yield = SmtpStep;
    type Return = Result<(), SmtpMessageSendError>;

    const BACKEND: EmailBackend = EmailBackend::Smtp;

    fn resume(
        &mut self,
        arg: EmailCoroutineArg<'_>,
    ) -> EmailCoroutineState<Self::Yield, Self::Return> {
        #[allow(irrefutable_let_patterns)]
        let EmailCoroutineArg::Smtp { bytes } = arg else {
            return EmailCoroutineState::Complete(Err(SmtpMessageSendError::InvalidArg));
        };

        match self.inner.resume(bytes) {
            SmtpCoroutineState::Complete(Ok(())) => EmailCoroutineState::Complete(Ok(())),
            SmtpCoroutineState::Yielded(SmtpYield::WantsRead) => {
                EmailCoroutineState::Yielded(SmtpStep::WantsRead)
            }
            SmtpCoroutineState::Yielded(SmtpYield::WantsWrite(out)) => {
                EmailCoroutineState::Yielded(SmtpStep::WantsWrite(out))
            }
            SmtpCoroutineState::Complete(Err(err)) => {
                EmailCoroutineState::Complete(Err(err.into()))
            }
        }
    }
}

/// Flattens a mail-parser address group into a list of bare
/// `local-part@domain` strings.
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

/// First address in an address group, or `None` when the group is
/// empty. Used to pick the lone `From:` envelope sender.
fn first_address(addrs: &MailParserAddress<'_>) -> Option<String> {
    addresses(addrs).into_iter().next()
}

/// Parses `local-part@domain` into a static-lifetime [`SmtpMailbox`].
/// Both halves are wrapped in `Cow::Owned` so the result outlives the
/// input slice.
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
