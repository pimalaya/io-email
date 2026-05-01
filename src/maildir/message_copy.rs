//! Maildir message copy, wrapping
//! [`io_maildir::coroutines::message_copy::MaildirMessageCopy`].

use alloc::{
    collections::{BTreeMap, BTreeSet},
    string::{String, ToString},
    vec::Vec,
};

use io_maildir::{
    coroutines::message_copy::{
        MaildirMessageCopy, MaildirMessageCopyArg, MaildirMessageCopyError,
        MaildirMessageCopyResult,
    },
    maildir::{Maildir, MaildirSubdir},
};
use log::trace;

/// I/O-free coroutine copying a single Maildir message into another
/// Maildir, preserving the source subdir by default.
pub struct MessageCopy {
    inner: MaildirMessageCopy,
}

impl MessageCopy {
    /// Builds the coroutine. `target_subdir` of `None` reuses the
    /// source subdir (`cur` / `new` / `tmp`).
    pub fn new(
        id: impl ToString,
        source: Maildir,
        target: Maildir,
        target_subdir: Option<MaildirSubdir>,
    ) -> Self {
        trace!("prepare Maildir message copy");
        Self {
            inner: MaildirMessageCopy::new(id, source, target, target_subdir),
        }
    }

    /// Advances the coroutine.
    pub fn resume(&mut self, arg: Option<MessageCopyArg>) -> MessageCopyResult {
        let inner_arg = arg.map(|arg| match arg {
            MessageCopyArg::DirRead(entries) => MaildirMessageCopyArg::DirRead(entries),
            MessageCopyArg::Copy => MaildirMessageCopyArg::Copy,
        });

        match self.inner.resume(inner_arg) {
            MaildirMessageCopyResult::Ok => MessageCopyResult::Ok,
            MaildirMessageCopyResult::WantsDirRead(paths) => MessageCopyResult::WantsDirRead(paths),
            MaildirMessageCopyResult::WantsCopy(pairs) => MessageCopyResult::WantsCopy(pairs),
            MaildirMessageCopyResult::Err(err) => MessageCopyResult::Err(err),
        }
    }
}

/// Result returned by [`MessageCopy::resume`].
#[derive(Debug)]
pub enum MessageCopyResult {
    Ok,
    WantsDirRead(BTreeSet<String>),
    WantsCopy(Vec<(String, String)>),
    Err(MaildirMessageCopyError),
}

/// Argument fed back to [`MessageCopy::resume`] after the caller
/// performed the requested filesystem operation.
#[derive(Debug)]
pub enum MessageCopyArg {
    /// Response to [`MessageCopyResult::WantsDirRead`].
    DirRead(BTreeMap<String, BTreeSet<String>>),
    /// Response to [`MessageCopyResult::WantsCopy`].
    Copy,
}
