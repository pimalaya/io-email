//! Maildir message add, wrapping
//! [`io_maildir::coroutines::message_store::MaildirMessageStore`].

use alloc::{collections::BTreeMap, string::String, vec::Vec};

use io_maildir::{
    coroutines::message_store::{
        MaildirMessageStore as InnerMaildirMessageStore, MaildirMessageStoreArg as InnerArg,
        MaildirMessageStoreError, MaildirMessageStoreResult as InnerResult,
    },
    flag::Flags,
    maildir::{Maildir, MaildirSubdir},
    path::MaildirPath,
};
use log::trace;

/// Argument fed back to [`MaildirMessageAdd::resume`].
#[derive(Debug)]
pub enum MaildirMessageAddArg {
    Time { secs: u64, nanos: u32 },
    Pid(u32),
    Hostname(String),
    FileCreate,
    Rename,
}

/// Result returned by [`MaildirMessageAdd::resume`].
#[derive(Debug)]
pub enum MaildirMessageAddResult {
    Ok { id: String, path: MaildirPath },
    WantsTime,
    WantsPid,
    WantsHostname,
    WantsFileCreate(BTreeMap<MaildirPath, Vec<u8>>),
    WantsRename(Vec<(MaildirPath, MaildirPath)>),
    Err(MaildirMessageStoreError),
}

/// I/O-free coroutine writing a raw RFC 5322 message into a Maildir.
/// Follows the standard maildir delivery protocol (write to `tmp`,
/// then rename).
pub struct MaildirMessageAdd {
    inner: InnerMaildirMessageStore,
}

impl MaildirMessageAdd {
    /// Pass `subdir = None` to default to [`MaildirSubdir::Cur`].
    pub fn new(
        maildir: Maildir,
        subdir: Option<MaildirSubdir>,
        flags: Flags,
        contents: Vec<u8>,
    ) -> Self {
        trace!("prepare Maildir message add");
        let subdir = subdir.unwrap_or(MaildirSubdir::Cur);
        Self {
            inner: InnerMaildirMessageStore::new(maildir, subdir, flags, contents),
        }
    }

    pub fn resume(&mut self, arg: Option<MaildirMessageAddArg>) -> MaildirMessageAddResult {
        let inner_arg = arg.map(|arg| match arg {
            MaildirMessageAddArg::Time { secs, nanos } => InnerArg::Time { secs, nanos },
            MaildirMessageAddArg::Pid(pid) => InnerArg::Pid(pid),
            MaildirMessageAddArg::Hostname(h) => InnerArg::Hostname(h),
            MaildirMessageAddArg::FileCreate => InnerArg::FileCreate,
            MaildirMessageAddArg::Rename => InnerArg::Rename,
        });

        match self.inner.resume(inner_arg) {
            InnerResult::Ok { id, path } => MaildirMessageAddResult::Ok { id, path },
            InnerResult::WantsTime => MaildirMessageAddResult::WantsTime,
            InnerResult::WantsPid => MaildirMessageAddResult::WantsPid,
            InnerResult::WantsHostname => MaildirMessageAddResult::WantsHostname,
            InnerResult::WantsFileCreate(files) => MaildirMessageAddResult::WantsFileCreate(files),
            InnerResult::WantsRename(pairs) => MaildirMessageAddResult::WantsRename(pairs),
            InnerResult::Err(err) => MaildirMessageAddResult::Err(err),
        }
    }
}
