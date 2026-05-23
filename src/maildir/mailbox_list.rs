//! Maildir mailbox listing, wrapping
//! [`io_maildir::coroutines::maildir_list::MaildirList`].
//!
//! Counts are not populated; counting requires enumerating each
//! maildir's `cur/`+`new/`, which is driven at the [`EmailClientStd`]
//! level.
//!
//! [`EmailClientStd`]: crate::client::EmailClientStd

use alloc::{
    collections::{BTreeMap, BTreeSet},
    string::ToString,
    vec::Vec,
};

use io_maildir::{
    coroutines::maildir_list::{MaildirList as InnerMaildirList, MaildirListArg, MaildirListError},
    maildir::Maildir,
    path::MaildirPath,
};
use log::trace;

use crate::mailbox::Mailbox;

/// Argument fed back to [`MaildirMailboxList::resume`].
#[derive(Debug)]
pub enum MaildirMailboxListArg {
    DirRead(BTreeMap<MaildirPath, BTreeSet<MaildirPath>>),
    DirExists(BTreeMap<MaildirPath, bool>),
}

/// Result returned by [`MaildirMailboxList::resume`].
#[derive(Debug)]
pub enum MaildirMailboxListResult {
    Ok(Vec<Mailbox>),
    WantsDirRead(BTreeSet<MaildirPath>),
    WantsDirExists(BTreeSet<MaildirPath>),
    Err(MaildirListError),
}

/// I/O-free coroutine listing every Maildir under a root directory.
pub struct MaildirMailboxList {
    inner: InnerMaildirList,
}

impl MaildirMailboxList {
    pub fn new(root: impl Into<MaildirPath>) -> Self {
        trace!("prepare Maildir mailbox listing");
        Self {
            inner: InnerMaildirList::new(root),
        }
    }

    pub fn resume(&mut self, arg: Option<MaildirMailboxListArg>) -> MaildirMailboxListResult {
        use io_maildir::coroutines::maildir_list::MaildirListResult;

        let inner_arg = arg.map(|arg| match arg {
            MaildirMailboxListArg::DirRead(entries) => MaildirListArg::DirRead(entries),
            MaildirMailboxListArg::DirExists(probes) => MaildirListArg::DirExists(probes),
        });

        match self.inner.resume(inner_arg) {
            MaildirListResult::WantsDirRead(paths) => MaildirMailboxListResult::WantsDirRead(paths),
            MaildirListResult::WantsDirExists(paths) => {
                MaildirMailboxListResult::WantsDirExists(paths)
            }
            MaildirListResult::Ok(maildirs) => {
                let mut mailboxes: Vec<Mailbox> = maildirs.into_iter().map(Mailbox::from).collect();
                mailboxes.sort_by(|a, b| a.name.cmp(&b.name));
                MaildirMailboxListResult::Ok(mailboxes)
            }
            MaildirListResult::Err(err) => MaildirMailboxListResult::Err(err),
        }
    }
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
