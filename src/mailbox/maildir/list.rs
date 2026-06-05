//! Maildir list-mailboxes coroutine wrapping
//! [`io_maildir::maildir::list::MaildirList`].
//!
//! Scans the store root for the cur/new/tmp triad; layout (fs vs
//! Maildir++) is read from the [`MaildirStore`]. `with_counts` is
//! currently a no-op (TODO: chain a MaildirMailboxStatus stage).
//!
//! # Example
//!
//! ```rust,ignore
//! use io_email::mailbox::maildir::list::MaildirMailboxList;
//!
//! let mailboxes = client.run(MaildirMailboxList::new(&client.store, false))?;
//! ```

use alloc::{string::ToString, vec::Vec};

use io_maildir::{
    coroutine::*,
    maildir::{
        list::{MaildirList as InnerMaildirList, MaildirListError},
        types::Maildir,
    },
    store::MaildirStore,
};
use log::trace;
use thiserror::Error;

use crate::mailbox::types::Mailbox;

/// Errors produced by [`MaildirMailboxList`].
#[derive(Debug, Error)]
pub enum MaildirMailboxListError {
    #[error(transparent)]
    List(#[from] MaildirListError),
}

/// I/O-free coroutine listing every Maildir under the store root.
pub struct MaildirMailboxList {
    inner: InnerMaildirList,
}

impl MaildirMailboxList {
    /// `_with_counts` is accepted for shared-API symmetry but ignored.
    pub fn new(store: &MaildirStore, _with_counts: bool) -> Self {
        trace!(
            "prepare Maildir mailbox listing (maildirpp={})",
            store.maildirpp
        );
        Self {
            inner: InnerMaildirList::new(store),
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
/// `id` is the on-disk path; `name` is the last path segment.
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
