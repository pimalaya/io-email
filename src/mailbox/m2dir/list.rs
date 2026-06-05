//! m2dir list-mailboxes coroutine wrapping
//! [`io_m2dir::m2dir::list::M2dirList`]: walks the store depth-first
//! reporting every directory with the .m2dir marker.
//!
//! `with_counts` is currently a no-op (TODO: chain an
//! M2dirMailboxStatus stage).
//!
//! # Example
//!
//! ```rust,ignore
//! use io_email::mailbox::m2dir::list::M2dirMailboxList;
//!
//! let mailboxes = client.run(M2dirMailboxList::new(&client.root, false))?;
//! ```

use alloc::{string::ToString, vec::Vec};
use std::path::PathBuf;

use io_m2dir::{
    coroutine::*,
    m2dir::{
        list::{
            M2dirList as InnerM2dirMailboxList, M2dirListError as InnerM2dirMailboxListError,
            M2dirListOptions as InnerM2dirMailboxListOptions,
        },
        types::M2dir,
    },
    path::M2dirPath,
    store::M2dirStore,
};
use log::trace;
use thiserror::Error;

use crate::mailbox::types::Mailbox;

/// Errors produced by [`M2dirMailboxList`].
#[derive(Debug, Error)]
pub enum M2dirMailboxListError {
    #[error(transparent)]
    List(#[from] InnerM2dirMailboxListError),
}

/// I/O-free coroutine listing every m2dir under a store root.
pub struct M2dirMailboxList {
    inner: InnerM2dirMailboxList,
}

impl M2dirMailboxList {
    /// `_with_counts` is accepted for shared-API symmetry but ignored.
    pub fn new(root: impl Into<PathBuf>, _with_counts: bool) -> Self {
        trace!("prepare m2dir mailbox listing");
        let path: M2dirPath = root.into().into();
        let store = M2dirStore::from_path(path);
        Self {
            inner: InnerM2dirMailboxList::new(&store, InnerM2dirMailboxListOptions::default()),
        }
    }
}

/// Converts one [`M2dir`] into the shared [`Mailbox`] shape.
fn mailbox_from(m2dir: M2dir) -> Mailbox {
    let path = m2dir.path();
    let name = path.file_name().unwrap_or("").to_string();
    let id = path.as_str().to_string();
    Mailbox {
        id,
        name,
        total: None,
        unread: None,
    }
}

impl M2dirCoroutine for M2dirMailboxList {
    type Yield = M2dirYield;
    type Return = Result<Vec<Mailbox>, M2dirMailboxListError>;

    fn resume(&mut self, arg: Option<M2dirArg>) -> M2dirCoroutineState<Self::Yield, Self::Return> {
        match self.inner.resume(arg) {
            M2dirCoroutineState::Yielded(y) => M2dirCoroutineState::Yielded(y),
            M2dirCoroutineState::Complete(Ok(m2dirs)) => {
                let mut mailboxes: Vec<Mailbox> = m2dirs.into_iter().map(mailbox_from).collect();
                mailboxes.sort_by(|a, b| a.name.cmp(&b.name));
                M2dirCoroutineState::Complete(Ok(mailboxes))
            }
            M2dirCoroutineState::Complete(Err(err)) => {
                M2dirCoroutineState::Complete(Err(err.into()))
            }
        }
    }
}
