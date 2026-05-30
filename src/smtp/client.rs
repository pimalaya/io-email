//! Std-blocking SMTP client.
//!
//! Holds an inner [`SmtpClientStd`] (from io-smtp) wrapping the
//! authenticated stream, plus the io-email-specific
//! `default_reverse_path` knob (envelope-sender override used by
//! alias accounts and DKIM-aligned bounce-address rewriting).
//!
//! [`Self::send_message`] runs the RFC 5321 mail transaction
//! (MAIL FROM → RCPT TO → DATA) extracting reverse / forward paths
//! from the raw RFC 5322 bytes. The inner client's command helpers
//! (`greeting`, `ehlo`, `mail`, `rcpt`, `data`, …) stay reachable
//! through [`Self::inner`] for protocol-specific paths the shared
//! API does not cover.
//!
//! [`SmtpClientStd`]: io_smtp::client::SmtpClientStd

use alloc::{string::String, vec::Vec};
use std::io::{self, Read, Write};

#[cfg(any(
    feature = "rustls-ring",
    feature = "rustls-aws",
    feature = "native-tls"
))]
use io_smtp::rfc5321::types::ehlo_domain::EhloDomain;
use io_smtp::{
    client::{SmtpClientStd as InnerSmtpClientStd, SmtpClientStdError as InnerSmtpClientStdError},
    coroutine::*,
};
#[cfg(any(
    feature = "rustls-ring",
    feature = "rustls-aws",
    feature = "native-tls"
))]
use pimalaya_stream::{sasl::Sasl, tls::Tls};
use thiserror::Error;
#[cfg(any(
    feature = "rustls-ring",
    feature = "rustls-aws",
    feature = "native-tls"
))]
use url::Url;

use crate::smtp::message_send::{SmtpMessageSend, SmtpMessageSendError};

/// Errors surfaced by [`SmtpClientStd`] while running a coroutine.
#[derive(Debug, Error)]
pub enum SmtpClientError {
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    MessageSend(#[from] SmtpMessageSendError),
    #[error(transparent)]
    Inner(#[from] InnerSmtpClientStdError),
}

const READ_BUFFER_SIZE: usize = 16 * 1024;

/// Light SMTP client built on a generic blocking stream.
///
/// `default_reverse_path` overrides the `MAIL FROM` envelope sender
/// for accounts whose header `From:` differs from the SMTP sender
/// (DKIM-aligned gateways, bounce-address rewriting).
pub struct SmtpClientStd {
    pub inner: InnerSmtpClientStd,
    pub default_reverse_path: Option<String>,
}

impl SmtpClientStd {
    /// Wraps an already-authenticated SMTP stream with no envelope
    /// override.
    pub fn new<S: Read + Write + Send + 'static>(stream: S) -> Self {
        Self {
            inner: InnerSmtpClientStd::new(stream),
            default_reverse_path: None,
        }
    }

    /// Pumps any standard-shape SMTP coroutine
    /// (`Yield = SmtpYield`, `Return = Result<T, E>`) against the
    /// inner client's stream until it terminates.
    ///
    /// Reaches into [`Self::inner`] for raw field access rather than
    /// delegating to [`InnerSmtpClientStd::run`] so error variants
    /// route through [`SmtpClientError`] directly.
    pub fn run<C, T, E>(&mut self, mut coroutine: C) -> Result<T, SmtpClientError>
    where
        C: SmtpCoroutine<Yield = SmtpYield, Return = Result<T, E>>,
        SmtpClientError: From<E>,
    {
        let mut buf = [0u8; READ_BUFFER_SIZE];
        let mut arg: Option<&[u8]> = None;

        loop {
            match coroutine.resume(arg.take()) {
                SmtpCoroutineState::Complete(Ok(out)) => return Ok(out),
                SmtpCoroutineState::Complete(Err(err)) => return Err(err.into()),
                SmtpCoroutineState::Yielded(SmtpYield::WantsRead) => {
                    let n = self.inner.stream.read(&mut buf)?;
                    arg = Some(&buf[..n]);
                }
                SmtpCoroutineState::Yielded(SmtpYield::WantsWrite(bytes)) => {
                    self.inner.stream.write_all(&bytes)?;
                }
            }
        }
    }

    /// Sends the raw RFC 5322 `raw` message through the authenticated
    /// stream. Reverse path comes from
    /// [`Self::default_reverse_path`] when set, otherwise from the
    /// message's `From:` header; forward paths come from
    /// `To:` + `Cc:` + `Bcc:`.
    pub fn send_message(&mut self, raw: Vec<u8>) -> Result<(), SmtpClientError> {
        let coroutine = {
            let override_reverse = self.default_reverse_path.as_deref();
            SmtpMessageSend::new(raw, override_reverse)?
        };
        self.run(coroutine)
    }
}

#[cfg(any(
    feature = "rustls-ring",
    feature = "rustls-aws",
    feature = "native-tls"
))]
impl SmtpClientStd {
    /// Opens a TCP / TLS connection to `url`, runs the optional
    /// STARTTLS upgrade plus EHLO + SASL authentication, then wraps
    /// the authenticated stream with the io-email knobs (empty
    /// `default_reverse_path`).
    pub fn connect(
        url: &Url,
        tls: &Tls,
        starttls: bool,
        domain: EhloDomain<'_>,
        sasl: Option<impl Into<Sasl>>,
    ) -> Result<Self, SmtpClientError> {
        let inner = InnerSmtpClientStd::connect(url, tls, starttls, domain, sasl)?;
        Ok(Self {
            inner,
            default_reverse_path: None,
        })
    }
}
