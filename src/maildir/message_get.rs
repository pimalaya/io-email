//! Maildir message get, wrapping
//! [`io_maildir::coroutines::message_get::MaildirMessageGet`]. Returns
//! the raw RFC 5322 bytes on completion.

use alloc::{
    collections::{BTreeMap, BTreeSet},
    string::{String, ToString},
    vec::Vec,
};

use io_maildir::{
    coroutines::message_get::{
        MaildirMessageGet, MaildirMessageGetArg, MaildirMessageGetError, MaildirMessageGetResult,
    },
    maildir::Maildir,
};
use log::trace;
use thiserror::Error;

/// Errors produced while retrieving a Maildir message.
#[derive(Debug, Error)]
pub enum MessageGetError {
    #[error(transparent)]
    Maildir(#[from] MaildirMessageGetError),
}

/// I/O-free coroutine reading a single Maildir message.
pub struct MessageGet {
    inner: MaildirMessageGet,
}

impl MessageGet {
    /// Builds the coroutine from the target Maildir and the message id.
    pub fn new(maildir: Maildir, id: impl ToString) -> Self {
        trace!("prepare Maildir message get");
        Self {
            inner: MaildirMessageGet::new(maildir, id),
        }
    }

    /// Advances the coroutine.
    pub fn resume(&mut self, arg: Option<MessageGetArg>) -> MessageGetResult {
        let inner_arg = arg.map(|arg| match arg {
            MessageGetArg::DirRead(entries) => MaildirMessageGetArg::DirRead(entries),
            MessageGetArg::FileRead(contents) => MaildirMessageGetArg::FileRead(contents),
        });

        match self.inner.resume(inner_arg) {
            MaildirMessageGetResult::WantsDirRead(paths) => MessageGetResult::WantsDirRead(paths),
            MaildirMessageGetResult::WantsFileRead(paths) => MessageGetResult::WantsFileRead(paths),
            MaildirMessageGetResult::Ok(message) => MessageGetResult::Ok(message.into()),
            MaildirMessageGetResult::Err(err) => MessageGetResult::Err(err.into()),
        }
    }
}

/// Result returned by [`MessageGet::resume`].
#[derive(Debug)]
pub enum MessageGetResult {
    Ok(Vec<u8>),
    WantsDirRead(BTreeSet<String>),
    WantsFileRead(BTreeSet<String>),
    Err(MessageGetError),
}

/// Argument fed back to [`MessageGet::resume`] after the caller
/// performed the requested filesystem operation.
#[derive(Debug)]
pub enum MessageGetArg {
    /// Response to [`MessageGetResult::WantsDirRead`].
    DirRead(BTreeMap<String, BTreeSet<String>>),

    /// Response to [`MessageGetResult::WantsFileRead`].
    FileRead(BTreeMap<String, Vec<u8>>),
}
