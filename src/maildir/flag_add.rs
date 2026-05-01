//! Maildir flag add, wrapping
//! [`io_maildir::coroutines::flags_add::MaildirFlagsAdd`].

use alloc::{
    collections::{BTreeMap, BTreeSet},
    string::{String, ToString},
    vec::Vec,
};

use io_maildir::{
    coroutines::flags_add::{
        MaildirFlagsAdd, MaildirFlagsAddArg, MaildirFlagsAddError, MaildirFlagsAddResult,
    },
    flag::Flags,
    maildir::Maildir,
};
use log::trace;

/// I/O-free coroutine adding flags to a single Maildir message.
pub struct FlagAdd {
    inner: MaildirFlagsAdd,
}

impl FlagAdd {
    /// Builds the coroutine from the target Maildir, the message id and
    /// the flags to add.
    pub fn new(maildir: Maildir, id: impl ToString, flags: Flags) -> Self {
        trace!("prepare Maildir flag add");
        Self {
            inner: MaildirFlagsAdd::new(maildir, id, flags),
        }
    }

    /// Advances the coroutine.
    pub fn resume(&mut self, arg: Option<FlagAddArg>) -> FlagAddResult {
        let inner_arg = arg.map(|arg| match arg {
            FlagAddArg::DirRead(entries) => MaildirFlagsAddArg::DirRead(entries),
            FlagAddArg::Rename => MaildirFlagsAddArg::Rename,
        });

        match self.inner.resume(inner_arg) {
            MaildirFlagsAddResult::WantsDirRead(paths) => FlagAddResult::WantsDirRead(paths),
            MaildirFlagsAddResult::WantsRename(pairs) => FlagAddResult::WantsRename(pairs),
            MaildirFlagsAddResult::Ok => FlagAddResult::Ok,
            MaildirFlagsAddResult::Err(err) => FlagAddResult::Err(err),
        }
    }
}

/// Result returned by [`FlagAdd::resume`].
#[derive(Debug)]
pub enum FlagAddResult {
    Ok,
    WantsDirRead(BTreeSet<String>),
    WantsRename(Vec<(String, String)>),
    Err(MaildirFlagsAddError),
}

/// Argument fed back to [`FlagAdd::resume`] after the caller performed
/// the requested filesystem operation.
#[derive(Debug)]
pub enum FlagAddArg {
    /// Response to [`FlagAddResult::WantsDirRead`].
    DirRead(BTreeMap<String, BTreeSet<String>>),

    /// Response to [`FlagAddResult::WantsRename`].
    Rename,
}
