//! Maildir message move, wrapping
//! [`io_maildir::coroutines::message_move::MaildirMessageMove`].

use alloc::{
    collections::{BTreeMap, BTreeSet},
    string::ToString,
    vec::Vec,
};

use io_maildir::{
    coroutines::message_move::{
        MaildirMessageMove as InnerMaildirMessageMove, MaildirMessageMoveArg as InnerArg,
        MaildirMessageMoveError, MaildirMessageMoveResult as InnerResult,
    },
    maildir::{Maildir, MaildirSubdir},
    path::MaildirPath,
};
use log::trace;

/// Argument fed back to [`MaildirMessageMove::resume`].
#[derive(Debug)]
pub enum MaildirMessageMoveArg {
    FileExists(BTreeMap<MaildirPath, bool>),
    DirRead(BTreeMap<MaildirPath, BTreeSet<MaildirPath>>),
    Rename,
}

/// Result returned by [`MaildirMessageMove::resume`].
#[derive(Debug)]
pub enum MaildirMessageMoveResult {
    Ok,
    WantsFileExists(BTreeSet<MaildirPath>),
    WantsDirRead(BTreeSet<MaildirPath>),
    WantsRename(Vec<(MaildirPath, MaildirPath)>),
    Err(MaildirMessageMoveError),
}

/// I/O-free coroutine moving a single Maildir message into another
/// Maildir, preserving the source subdir by default.
pub struct MaildirMessageMove {
    inner: InnerMaildirMessageMove,
}

impl MaildirMessageMove {
    /// `target_subdir = None` reuses the source subdir.
    pub fn new(
        id: impl ToString,
        source: Maildir,
        target: Maildir,
        target_subdir: Option<MaildirSubdir>,
    ) -> Self {
        trace!("prepare Maildir message move");
        Self {
            inner: InnerMaildirMessageMove::new(id, source, target, target_subdir),
        }
    }

    pub fn resume(&mut self, arg: Option<MaildirMessageMoveArg>) -> MaildirMessageMoveResult {
        let inner_arg = arg.map(|arg| match arg {
            MaildirMessageMoveArg::FileExists(probes) => InnerArg::FileExists(probes),
            MaildirMessageMoveArg::DirRead(entries) => InnerArg::DirRead(entries),
            MaildirMessageMoveArg::Rename => InnerArg::Rename,
        });

        match self.inner.resume(inner_arg) {
            InnerResult::Ok => MaildirMessageMoveResult::Ok,
            InnerResult::WantsFileExists(probes) => {
                MaildirMessageMoveResult::WantsFileExists(probes)
            }
            InnerResult::WantsDirRead(paths) => MaildirMessageMoveResult::WantsDirRead(paths),
            InnerResult::WantsRename(pairs) => MaildirMessageMoveResult::WantsRename(pairs),
            InnerResult::Err(err) => MaildirMessageMoveResult::Err(err),
        }
    }
}
