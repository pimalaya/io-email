//! Maildir flag add, wrapping
//! [`io_maildir::coroutines::flags_add::MaildirFlagsAdd`].

use alloc::{
    collections::{BTreeMap, BTreeSet},
    string::ToString,
    vec::Vec,
};

use io_maildir::{
    coroutines::flags_add::{
        MaildirFlagsAdd as InnerMaildirFlagsAdd, MaildirFlagsAddArg, MaildirFlagsAddError,
    },
    flag::Flags,
    maildir::Maildir,
    path::MaildirPath,
};
use log::trace;

/// Argument fed back to [`MaildirFlagAdd::resume`].
#[derive(Debug)]
pub enum MaildirFlagAddArg {
    FileExists(BTreeMap<MaildirPath, bool>),
    DirRead(BTreeMap<MaildirPath, BTreeSet<MaildirPath>>),
    Rename,
}

/// Result returned by [`MaildirFlagAdd::resume`].
#[derive(Debug)]
pub enum MaildirFlagAddResult {
    Ok,
    WantsFileExists(BTreeSet<MaildirPath>),
    WantsDirRead(BTreeSet<MaildirPath>),
    WantsRename(Vec<(MaildirPath, MaildirPath)>),
    Err(MaildirFlagsAddError),
}

/// I/O-free coroutine adding flags to a single Maildir message.
pub struct MaildirFlagAdd {
    inner: InnerMaildirFlagsAdd,
}

impl MaildirFlagAdd {
    pub fn new(maildir: Maildir, id: impl ToString, flags: Flags) -> Self {
        trace!("prepare Maildir flag add");
        Self {
            inner: InnerMaildirFlagsAdd::new(maildir, id, flags),
        }
    }

    pub fn resume(&mut self, arg: Option<MaildirFlagAddArg>) -> MaildirFlagAddResult {
        use io_maildir::coroutines::flags_add::MaildirFlagsAddResult;

        let inner_arg = arg.map(|arg| match arg {
            MaildirFlagAddArg::FileExists(probes) => MaildirFlagsAddArg::FileExists(probes),
            MaildirFlagAddArg::DirRead(entries) => MaildirFlagsAddArg::DirRead(entries),
            MaildirFlagAddArg::Rename => MaildirFlagsAddArg::Rename,
        });

        match self.inner.resume(inner_arg) {
            MaildirFlagsAddResult::WantsFileExists(probes) => {
                MaildirFlagAddResult::WantsFileExists(probes)
            }
            MaildirFlagsAddResult::WantsDirRead(paths) => MaildirFlagAddResult::WantsDirRead(paths),
            MaildirFlagsAddResult::WantsRename(pairs) => MaildirFlagAddResult::WantsRename(pairs),
            MaildirFlagsAddResult::Ok => MaildirFlagAddResult::Ok,
            MaildirFlagsAddResult::Err(err) => MaildirFlagAddResult::Err(err),
        }
    }
}
