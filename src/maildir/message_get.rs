//! Maildir message get, wrapping
//! [`io_maildir::coroutines::message_get::MaildirMessageGet`]. Returns
//! raw RFC 5322 bytes.

use alloc::{
    collections::{BTreeMap, BTreeSet},
    string::ToString,
    vec::Vec,
};

use io_maildir::{
    coroutines::message_get::{
        MaildirMessageGet as InnerMaildirMessageGet, MaildirMessageGetArg as InnerArg,
        MaildirMessageGetError, MaildirMessageGetResult as InnerResult,
    },
    maildir::Maildir,
    path::MaildirPath,
};
use log::trace;

/// Argument fed back to [`MaildirMessageGet::resume`].
#[derive(Debug)]
pub enum MaildirMessageGetArg {
    FileExists(BTreeMap<MaildirPath, bool>),
    DirRead(BTreeMap<MaildirPath, BTreeSet<MaildirPath>>),
    FileRead(BTreeMap<MaildirPath, Vec<u8>>),
}

/// Result returned by [`MaildirMessageGet::resume`].
#[derive(Debug)]
pub enum MaildirMessageGetResult {
    Ok(Vec<u8>),
    WantsFileExists(BTreeSet<MaildirPath>),
    WantsDirRead(BTreeSet<MaildirPath>),
    WantsFileRead(BTreeSet<MaildirPath>),
    Err(MaildirMessageGetError),
}

/// I/O-free coroutine reading a single Maildir message.
pub struct MaildirMessageGet {
    inner: InnerMaildirMessageGet,
}

impl MaildirMessageGet {
    pub fn new(maildir: Maildir, id: impl ToString) -> Self {
        trace!("prepare Maildir message get");
        Self {
            inner: InnerMaildirMessageGet::new(maildir, id),
        }
    }

    pub fn resume(&mut self, arg: Option<MaildirMessageGetArg>) -> MaildirMessageGetResult {
        let inner_arg = arg.map(|arg| match arg {
            MaildirMessageGetArg::FileExists(probes) => InnerArg::FileExists(probes),
            MaildirMessageGetArg::DirRead(entries) => InnerArg::DirRead(entries),
            MaildirMessageGetArg::FileRead(contents) => InnerArg::FileRead(contents),
        });

        match self.inner.resume(inner_arg) {
            InnerResult::WantsFileExists(probes) => {
                MaildirMessageGetResult::WantsFileExists(probes)
            }
            InnerResult::WantsDirRead(paths) => MaildirMessageGetResult::WantsDirRead(paths),
            InnerResult::WantsFileRead(paths) => MaildirMessageGetResult::WantsFileRead(paths),
            InnerResult::Ok(message) => MaildirMessageGetResult::Ok(message.into()),
            InnerResult::Err(err) => MaildirMessageGetResult::Err(err),
        }
    }
}
