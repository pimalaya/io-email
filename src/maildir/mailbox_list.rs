//! Maildir mailbox listing, wrapping
//! [`io_maildir::coroutines::maildir_list::MaildirList`] and producing
//! the shared [`Mailbox`](crate::mailbox::Mailbox) type on completion.
//!
//! Per-mailbox counts are not populated here — counting requires
//! enumerating each maildir's `cur/`+`new/` entries, which is driven
//! at the [`crate::client::EmailClient`] level when the caller opts
//! in.

use alloc::{
    collections::{BTreeMap, BTreeSet},
    string::{String, ToString},
    vec::Vec,
};
use std::path::Path;

use io_maildir::{
    coroutines::maildir_list::{MaildirList, MaildirListArg, MaildirListError, MaildirListResult},
    maildir::Maildir,
};
use log::trace;

use crate::mailbox::Mailbox;

/// I/O-free coroutine listing every Maildir under a root directory.
pub struct MailboxList {
    inner: MaildirList,
}

impl MailboxList {
    /// Builds the coroutine from the root path containing per-mailbox
    /// Maildir directories.
    pub fn new(root: impl AsRef<Path>) -> Self {
        trace!("prepare Maildir mailbox listing");
        Self {
            inner: MaildirList::new(root),
        }
    }

    /// Advances the coroutine.
    pub fn resume(&mut self, arg: Option<MailboxListArg>) -> MailboxListResult {
        let inner_arg =
            arg.map(|MailboxListArg::DirRead(entries)| MaildirListArg::DirRead(entries));

        match self.inner.resume(inner_arg) {
            MaildirListResult::WantsDirRead(paths) => MailboxListResult::WantsDirRead(paths),
            MaildirListResult::Ok(maildirs) => {
                let mut mailboxes: Vec<Mailbox> = maildirs.into_iter().map(Mailbox::from).collect();
                mailboxes.sort_by(|a, b| a.name.cmp(&b.name));
                MailboxListResult::Ok(mailboxes)
            }
            MaildirListResult::Err(err) => MailboxListResult::Err(err),
        }
    }
}

/// Result returned by [`MailboxList::resume`].
#[derive(Debug)]
pub enum MailboxListResult {
    Ok(Vec<Mailbox>),
    WantsDirRead(BTreeSet<String>),
    Err(MaildirListError),
}

/// Argument fed back to [`MailboxList::resume`] after the caller
/// performed the requested filesystem operation.
#[derive(Debug)]
pub enum MailboxListArg {
    /// Response to [`MailboxListResult::WantsDirRead`]: each requested
    /// directory path mapped to the set of entry paths found inside.
    DirRead(BTreeMap<String, BTreeSet<String>>),
}

impl From<Maildir> for Mailbox {
    fn from(maildir: Maildir) -> Self {
        let name = maildir.name().unwrap_or("").to_string();

        Self {
            id: name.clone(),
            name,
            total: None,
            unread: None,
        }
    }
}
