//! SMTP backend implementation of the [`EmailClientStd`] private
//! dispatch method for sending messages.

use alloc::vec::Vec;

use io_smtp::rfc5321::types::{forward_path::ForwardPath, reverse_path::ReversePath};
use pimalaya_stream::std::stream::StreamStd;

use crate::{
    client::{EmailClientStd, EmailClientStdError},
    smtp::convert::parse_mailbox,
};

impl EmailClientStd {
    /// Registers the SMTP backend. See [`Self::with_imap`] for the
    /// ordering rule.
    pub fn with_smtp(mut self, client: io_smtp::client::SmtpClientStd<StreamStd>) -> Self {
        self.smtp = Some(client);
        if !self.order.contains(&crate::client::BackendKind::Smtp) {
            self.order.push(crate::client::BackendKind::Smtp);
        }
        self
    }

    /// Borrows the underlying SMTP client when registered. Same
    /// portability caveat as [`Self::as_imap`].
    pub fn as_smtp(&self) -> Option<&io_smtp::client::SmtpClientStd<StreamStd>> {
        self.smtp.as_ref()
    }

    /// Mutable variant of [`Self::as_smtp`].
    pub fn as_smtp_mut(&mut self) -> Option<&mut io_smtp::client::SmtpClientStd<StreamStd>> {
        self.smtp.as_mut()
    }

    pub(crate) fn send_message_smtp(
        &mut self,
        raw: Vec<u8>,
        from: &str,
        to: &[&str],
    ) -> Result<(), EmailClientStdError> {
        if to.is_empty() {
            return Err(EmailClientStdError::MissingInput("to"));
        }

        let client = self.smtp.as_mut().expect("smtp slot registered");

        let reverse_path = ReversePath::Mailbox(parse_mailbox(from)?);
        let forward_paths: Vec<ForwardPath<'static>> = to
            .iter()
            .map(|addr| parse_mailbox(addr).map(ForwardPath))
            .collect::<Result<_, _>>()?;

        client.send(reverse_path, forward_paths, raw)?;

        Ok(())
    }
}
