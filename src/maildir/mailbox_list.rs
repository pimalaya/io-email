//! Maildir list-mailboxes coroutine.
//!
//! Wraps [`io_maildir::coroutines::maildir_list::MaildirList`]: scans
//! the root directory and probes each candidate child for the
//! `cur/` + `new/` + `tmp/` triad. With `maildir_plus` set, dotted
//! (`.`-prefixed) siblings and the root itself also count, matching
//! Maildir++ where folders live as siblings of the inbox.
//!
//! `with_counts` is currently a no-op: surfacing totals/unread would
//! need a follow-up directory walk over each maildir's `cur/` + `new/`
//! plus filename-flag parsing. Wire that in once io-email grows a
//! dedicated MaildirMailboxStatus coroutine and add it as a chained
//! stage here.
//!
//! Emits the shared [`Mailbox`] shape directly; Maildir-specific data
//! (root path metadata, subdirectory layout) is dropped on purpose to
//! stay LCD.

use alloc::{string::ToString, vec::Vec};

use io_maildir::{
    coroutine::*,
    coroutines::maildir_list::{MaildirList as InnerMaildirList, MaildirListError},
    maildir::Maildir,
    path::MaildirPath,
};
use log::trace;
use thiserror::Error;

use crate::mailbox::Mailbox;

/// Errors produced by [`MaildirMailboxList`].
#[derive(Debug, Error)]
pub enum MaildirMailboxListError {
    #[error(transparent)]
    List(#[from] MaildirListError),
}

/// I/O-free coroutine listing every Maildir under `root`.
pub struct MaildirMailboxList {
    inner: InnerMaildirList,
}

impl MaildirMailboxList {
    /// `MaildirList` against `root`. `maildir_plus` flips both
    /// `include_dotted` and `include_root` so Maildir++ layouts
    /// surface the inbox alongside its dotted siblings.
    ///
    /// `_with_counts` is accepted for symmetry with the other backends
    /// but currently ignored; see the module doc for the path to
    /// surfacing counts.
    pub fn new(root: impl Into<MaildirPath>, maildir_plus: bool, _with_counts: bool) -> Self {
        trace!("prepare Maildir mailbox listing (maildir_plus={maildir_plus})");
        Self {
            inner: InnerMaildirList::new(root)
                .include_dotted(maildir_plus)
                .include_root(maildir_plus),
        }
    }
}

impl MaildirCoroutine for MaildirMailboxList {
    type Yield = MaildirYield;
    type Return = Result<Vec<Mailbox>, MaildirMailboxListError>;

    fn resume(
        &mut self,
        arg: Option<MaildirReply>,
    ) -> MaildirCoroutineState<Self::Yield, Self::Return> {
        match self.inner.resume(arg) {
            MaildirCoroutineState::Yielded(y) => MaildirCoroutineState::Yielded(y),
            MaildirCoroutineState::Complete(Ok(maildirs)) => {
                let mut mailboxes: Vec<Mailbox> = maildirs.into_iter().map(mailbox_from).collect();
                mailboxes.sort_by(|a, b| a.name.cmp(&b.name));
                MaildirCoroutineState::Complete(Ok(mailboxes))
            }
            MaildirCoroutineState::Complete(Err(err)) => {
                MaildirCoroutineState::Complete(Err(err.into()))
            }
        }
    }
}

/// Converts one [`Maildir`] into the shared [`Mailbox`] shape.
///
/// `id` is the on-disk path so downstream ops can locate the maildir;
/// `name` is the last path segment (Maildir++ dotted names are kept
/// verbatim — decoding is the caller's responsibility for now).
/// Counts default to `None`; populating them needs the follow-up walk
/// described in the module doc.
fn mailbox_from(maildir: Maildir) -> Mailbox {
    let name = maildir.name().unwrap_or("").to_string();
    let id = maildir.path().to_string();
    Mailbox {
        id,
        name,
        total: None,
        unread: None,
    }
}
