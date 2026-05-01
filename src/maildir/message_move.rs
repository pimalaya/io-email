//! Maildir message move, wrapping
//! [`io_maildir::coroutines::message_move::MaildirMessageMove`].

use alloc::{
    collections::{BTreeMap, BTreeSet},
    string::{String, ToString},
    vec::Vec,
};

use io_maildir::{
    coroutines::message_move::{
        MaildirMessageMove, MaildirMessageMoveArg, MaildirMessageMoveError,
        MaildirMessageMoveResult,
    },
    maildir::{Maildir, MaildirSubdir},
};
use log::trace;

/// I/O-free coroutine moving a single Maildir message into another
/// Maildir, preserving the source subdir by default.
pub struct MessageMove {
    inner: MaildirMessageMove,
}

impl MessageMove {
    /// Builds the coroutine. `target_subdir` of `None` reuses the
    /// source subdir (`cur` / `new` / `tmp`).
    pub fn new(
        id: impl ToString,
        source: Maildir,
        target: Maildir,
        target_subdir: Option<MaildirSubdir>,
    ) -> Self {
        trace!("prepare Maildir message move");
        Self {
            inner: MaildirMessageMove::new(id, source, target, target_subdir),
        }
    }

    /// Advances the coroutine.
    pub fn resume(&mut self, arg: Option<MessageMoveArg>) -> MessageMoveResult {
        let inner_arg = arg.map(|arg| match arg {
            MessageMoveArg::DirRead(entries) => MaildirMessageMoveArg::DirRead(entries),
            MessageMoveArg::Rename => MaildirMessageMoveArg::Rename,
        });

        match self.inner.resume(inner_arg) {
            MaildirMessageMoveResult::Ok => MessageMoveResult::Ok,
            MaildirMessageMoveResult::WantsDirRead(paths) => MessageMoveResult::WantsDirRead(paths),
            MaildirMessageMoveResult::WantsRename(pairs) => MessageMoveResult::WantsRename(pairs),
            MaildirMessageMoveResult::Err(err) => MessageMoveResult::Err(err),
        }
    }
}

/// Result returned by [`MessageMove::resume`].
#[derive(Debug)]
pub enum MessageMoveResult {
    Ok,
    WantsDirRead(BTreeSet<String>),
    WantsRename(Vec<(String, String)>),
    Err(MaildirMessageMoveError),
}

/// Argument fed back to [`MessageMove::resume`] after the caller
/// performed the requested filesystem operation.
#[derive(Debug)]
pub enum MessageMoveArg {
    /// Response to [`MessageMoveResult::WantsDirRead`].
    DirRead(BTreeMap<String, BTreeSet<String>>),
    /// Response to [`MessageMoveResult::WantsRename`].
    Rename,
}
