//! SMTP message send (`MAIL FROM` / `RCPT TO` / `DATA`), wrapping
//! [`io_smtp::send::SmtpMessageSend`].

use alloc::vec::Vec;

use io_smtp::{
    rfc5321::types::{forward_path::ForwardPath, reverse_path::ReversePath},
    send::{SmtpMessageSend, SmtpMessageSendError, SmtpMessageSendResult},
};
use log::trace;

/// I/O-free coroutine sending a complete RFC 5321 SMTP message
/// transaction (MAIL FROM, RCPT TO for each recipient, DATA).
pub struct MessageSend {
    inner: SmtpMessageSend,
}

impl MessageSend {
    /// Builds the coroutine from the envelope sender, the list of
    /// recipients, and the raw RFC 5322 bytes to send.
    pub fn new<'a>(
        reverse_path: ReversePath,
        forward_paths: impl IntoIterator<Item = ForwardPath<'a>>,
        message: Vec<u8>,
    ) -> Self {
        trace!("prepare SMTP message send");
        Self {
            inner: SmtpMessageSend::new(reverse_path, forward_paths, message),
        }
    }

    /// Advances the coroutine.
    pub fn resume(&mut self, arg: Option<&[u8]>) -> MessageSendResult {
        match self.inner.resume(arg) {
            SmtpMessageSendResult::Ok => MessageSendResult::Ok,
            SmtpMessageSendResult::WantsRead => MessageSendResult::WantsRead,
            SmtpMessageSendResult::WantsWrite(bytes) => MessageSendResult::WantsWrite(bytes),
            SmtpMessageSendResult::Err(err) => MessageSendResult::Err(err),
        }
    }
}

/// Result returned by [`MessageSend::resume`].
#[derive(Debug)]
pub enum MessageSendResult {
    Ok,
    WantsRead,
    WantsWrite(Vec<u8>),
    Err(SmtpMessageSendError),
}
