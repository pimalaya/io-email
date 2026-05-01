//! Maildir flag set, wrapping
//! [`io_maildir::coroutines::flags_set::MaildirFlagsSet`].

use alloc::{
    collections::{BTreeMap, BTreeSet},
    string::{String, ToString},
    vec::Vec,
};

use io_maildir::{
    coroutines::flags_set::{
        MaildirFlagsSet, MaildirFlagsSetArg, MaildirFlagsSetError, MaildirFlagsSetResult,
    },
    flag::Flags,
    maildir::Maildir,
};
use log::trace;

/// I/O-free coroutine replacing the flags on a single Maildir message.
pub struct FlagSet {
    inner: MaildirFlagsSet,
}

impl FlagSet {
    /// Builds the coroutine from the target Maildir, the message id and
    /// the flags to set.
    pub fn new(maildir: Maildir, id: impl ToString, flags: Flags) -> Self {
        trace!("prepare Maildir flag set");
        Self {
            inner: MaildirFlagsSet::new(maildir, id, flags),
        }
    }

    /// Advances the coroutine.
    pub fn resume(&mut self, arg: Option<FlagSetArg>) -> FlagSetResult {
        let inner_arg = arg.map(|arg| match arg {
            FlagSetArg::DirRead(entries) => MaildirFlagsSetArg::DirRead(entries),
            FlagSetArg::Rename => MaildirFlagsSetArg::Rename,
        });

        match self.inner.resume(inner_arg) {
            MaildirFlagsSetResult::WantsDirRead(paths) => FlagSetResult::WantsDirRead(paths),
            MaildirFlagsSetResult::WantsRename(pairs) => FlagSetResult::WantsRename(pairs),
            MaildirFlagsSetResult::Ok => FlagSetResult::Ok,
            MaildirFlagsSetResult::Err(err) => FlagSetResult::Err(err),
        }
    }
}

/// Result returned by [`FlagSet::resume`].
#[derive(Debug)]
pub enum FlagSetResult {
    Ok,
    WantsDirRead(BTreeSet<String>),
    WantsRename(Vec<(String, String)>),
    Err(MaildirFlagsSetError),
}

/// Argument fed back to [`FlagSet::resume`] after the caller performed
/// the requested filesystem operation.
#[derive(Debug)]
pub enum FlagSetArg {
    /// Response to [`FlagSetResult::WantsDirRead`].
    DirRead(BTreeMap<String, BTreeSet<String>>),

    /// Response to [`FlagSetResult::WantsRename`].
    Rename,
}
