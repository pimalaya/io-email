//! Maildir message copy, wrapping
//! [`io_maildir::coroutines::message_copy::MaildirMessageCopy`].

use alloc::{
    collections::{BTreeMap, BTreeSet},
    string::ToString,
    vec::Vec,
};

use io_maildir::{
    coroutines::message_copy::{
        MaildirMessageCopy as InnerMaildirMessageCopy, MaildirMessageCopyArg as InnerArg,
        MaildirMessageCopyError, MaildirMessageCopyResult as InnerResult,
    },
    maildir::{Maildir, MaildirSubdir},
    path::MaildirPath,
};
use log::trace;

/// Argument fed back to [`MaildirMessageCopy::resume`].
#[derive(Debug)]
pub enum MaildirMessageCopyArg {
    FileExists(BTreeMap<MaildirPath, bool>),
    DirRead(BTreeMap<MaildirPath, BTreeSet<MaildirPath>>),
    Copy,
}

/// Result returned by [`MaildirMessageCopy::resume`].
#[derive(Debug)]
pub enum MaildirMessageCopyResult {
    Ok,
    WantsFileExists(BTreeSet<MaildirPath>),
    WantsDirRead(BTreeSet<MaildirPath>),
    WantsCopy(Vec<(MaildirPath, MaildirPath)>),
    Err(MaildirMessageCopyError),
}

/// I/O-free coroutine copying a single Maildir message into another
/// Maildir, preserving the source subdir by default.
pub struct MaildirMessageCopy {
    inner: InnerMaildirMessageCopy,
}

impl MaildirMessageCopy {
    /// `target_subdir = None` reuses the source subdir.
    pub fn new(
        id: impl ToString,
        source: Maildir,
        target: Maildir,
        target_subdir: Option<MaildirSubdir>,
    ) -> Self {
        trace!("prepare Maildir message copy");
        Self {
            inner: InnerMaildirMessageCopy::new(id, source, target, target_subdir),
        }
    }

    pub fn resume(&mut self, arg: Option<MaildirMessageCopyArg>) -> MaildirMessageCopyResult {
        let inner_arg = arg.map(|arg| match arg {
            MaildirMessageCopyArg::FileExists(probes) => InnerArg::FileExists(probes),
            MaildirMessageCopyArg::DirRead(entries) => InnerArg::DirRead(entries),
            MaildirMessageCopyArg::Copy => InnerArg::Copy,
        });

        match self.inner.resume(inner_arg) {
            InnerResult::Ok => MaildirMessageCopyResult::Ok,
            InnerResult::WantsFileExists(probes) => {
                MaildirMessageCopyResult::WantsFileExists(probes)
            }
            InnerResult::WantsDirRead(paths) => MaildirMessageCopyResult::WantsDirRead(paths),
            InnerResult::WantsCopy(pairs) => MaildirMessageCopyResult::WantsCopy(pairs),
            InnerResult::Err(err) => MaildirMessageCopyResult::Err(err),
        }
    }
}
