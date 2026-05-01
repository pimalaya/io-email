//! Maildir flag delete, wrapping
//! [`io_maildir::coroutines::flags_remove::MaildirFlagsRemove`].

use alloc::{
    collections::{BTreeMap, BTreeSet},
    string::{String, ToString},
    vec::Vec,
};

use io_maildir::{
    coroutines::flags_remove::{
        MaildirFlagsRemove, MaildirFlagsRemoveArg, MaildirFlagsRemoveError,
        MaildirFlagsRemoveResult,
    },
    flag::Flags,
    maildir::Maildir,
};
use log::trace;

/// I/O-free coroutine removing flags from a single Maildir message.
pub struct FlagDelete {
    inner: MaildirFlagsRemove,
}

impl FlagDelete {
    /// Builds the coroutine from the target Maildir, the message id and
    /// the flags to remove.
    pub fn new(maildir: Maildir, id: impl ToString, flags: Flags) -> Self {
        trace!("prepare Maildir flag delete");
        Self {
            inner: MaildirFlagsRemove::new(maildir, id, flags),
        }
    }

    /// Advances the coroutine.
    pub fn resume(&mut self, arg: Option<FlagDeleteArg>) -> FlagDeleteResult {
        let inner_arg = arg.map(|arg| match arg {
            FlagDeleteArg::DirRead(entries) => MaildirFlagsRemoveArg::DirRead(entries),
            FlagDeleteArg::Rename => MaildirFlagsRemoveArg::Rename,
        });

        match self.inner.resume(inner_arg) {
            MaildirFlagsRemoveResult::WantsDirRead(paths) => FlagDeleteResult::WantsDirRead(paths),
            MaildirFlagsRemoveResult::WantsRename(pairs) => FlagDeleteResult::WantsRename(pairs),
            MaildirFlagsRemoveResult::Ok => FlagDeleteResult::Ok,
            MaildirFlagsRemoveResult::Err(err) => FlagDeleteResult::Err(err),
        }
    }
}

/// Result returned by [`FlagDelete::resume`].
#[derive(Debug)]
pub enum FlagDeleteResult {
    Ok,
    WantsDirRead(BTreeSet<String>),
    WantsRename(Vec<(String, String)>),
    Err(MaildirFlagsRemoveError),
}

/// Argument fed back to [`FlagDelete::resume`] after the caller
/// performed the requested filesystem operation.
#[derive(Debug)]
pub enum FlagDeleteArg {
    /// Response to [`FlagDeleteResult::WantsDirRead`].
    DirRead(BTreeMap<String, BTreeSet<String>>),

    /// Response to [`FlagDeleteResult::WantsRename`].
    Rename,
}
