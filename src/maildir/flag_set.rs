//! Maildir flag set, wrapping
//! [`io_maildir::coroutines::flags_set::MaildirFlagsSet`].

use alloc::{
    collections::{BTreeMap, BTreeSet},
    string::ToString,
    vec::Vec,
};

use io_maildir::{
    coroutines::flags_set::{
        MaildirFlagsSet as InnerMaildirFlagsSet, MaildirFlagsSetArg, MaildirFlagsSetError,
    },
    flag::Flags,
    maildir::Maildir,
    path::MaildirPath,
};
use log::trace;

/// Argument fed back to [`MaildirFlagSet::resume`].
#[derive(Debug)]
pub enum MaildirFlagSetArg {
    FileExists(BTreeMap<MaildirPath, bool>),
    DirRead(BTreeMap<MaildirPath, BTreeSet<MaildirPath>>),
    Rename,
}

/// Result returned by [`MaildirFlagSet::resume`].
#[derive(Debug)]
pub enum MaildirFlagSetResult {
    Ok,
    WantsFileExists(BTreeSet<MaildirPath>),
    WantsDirRead(BTreeSet<MaildirPath>),
    WantsRename(Vec<(MaildirPath, MaildirPath)>),
    Err(MaildirFlagsSetError),
}

/// I/O-free coroutine replacing the flags on a single Maildir message.
pub struct MaildirFlagSet {
    inner: InnerMaildirFlagsSet,
}

impl MaildirFlagSet {
    pub fn new(maildir: Maildir, id: impl ToString, flags: Flags) -> Self {
        trace!("prepare Maildir flag set");
        Self {
            inner: InnerMaildirFlagsSet::new(maildir, id, flags),
        }
    }

    pub fn resume(&mut self, arg: Option<MaildirFlagSetArg>) -> MaildirFlagSetResult {
        use io_maildir::coroutines::flags_set::MaildirFlagsSetResult;

        let inner_arg = arg.map(|arg| match arg {
            MaildirFlagSetArg::FileExists(probes) => MaildirFlagsSetArg::FileExists(probes),
            MaildirFlagSetArg::DirRead(entries) => MaildirFlagsSetArg::DirRead(entries),
            MaildirFlagSetArg::Rename => MaildirFlagsSetArg::Rename,
        });

        match self.inner.resume(inner_arg) {
            MaildirFlagsSetResult::WantsFileExists(probes) => {
                MaildirFlagSetResult::WantsFileExists(probes)
            }
            MaildirFlagsSetResult::WantsDirRead(paths) => MaildirFlagSetResult::WantsDirRead(paths),
            MaildirFlagsSetResult::WantsRename(pairs) => MaildirFlagSetResult::WantsRename(pairs),
            MaildirFlagsSetResult::Ok => MaildirFlagSetResult::Ok,
            MaildirFlagsSetResult::Err(err) => MaildirFlagSetResult::Err(err),
        }
    }
}
