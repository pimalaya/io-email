//! m2dir list-mailboxes coroutine.
//!
//! Wraps [`io_m2dir::coroutines::mailbox_list::M2dirMailboxList`]:
//! walks the m2store depth-first and reports every directory carrying
//! the `.m2dir` marker as a mailbox. Hidden entries (`.`-prefixed)
//! are skipped; m2dirs can nest, so children are still walked after a
//! match.
//!
//! `with_counts` is currently a no-op for the same reason it's a
//! no-op on Maildir: surfacing totals/unread needs a follow-up walk
//! over each m2dir's entry directory. Wire that in once a
//! dedicated M2dirMailboxStatus coroutine lands.
//!
//! Emits the shared [`Mailbox`] shape directly; m2dir-specific data
//! (marker path, `.meta` directory layout) is dropped on purpose to
//! stay LCD.

use alloc::{string::ToString, vec::Vec};
use std::path::PathBuf;

use io_m2dir::{
    coroutine::*,
    coroutines::mailbox_list::{
        M2dirMailboxList as InnerM2dirMailboxList,
        M2dirMailboxListError as InnerM2dirMailboxListError,
    },
    m2dir::M2dir,
    m2store::M2store,
    path::M2dirPath,
};
use log::trace;
use thiserror::Error;

use crate::mailbox::Mailbox;

/// Errors produced by [`M2dirMailboxList`].
#[derive(Debug, Error)]
pub enum M2dirMailboxListError {
    #[error(transparent)]
    List(#[from] InnerM2dirMailboxListError),
}

/// I/O-free coroutine listing every m2dir under an m2store root.
pub struct M2dirMailboxList {
    inner: InnerM2dirMailboxList,
}

impl M2dirMailboxList {
    /// Builds an [`M2store`] from `root` and constructs the inner
    /// [`InnerM2dirMailboxList`] against it. `_with_counts` is
    /// accepted for symmetry with the other backends but currently
    /// ignored; see the module doc for the path to surfacing counts.
    pub fn new(root: impl Into<PathBuf>, _with_counts: bool) -> Self {
        trace!("prepare m2dir mailbox listing");
        let path: M2dirPath = root.into().into();
        let store = M2store::from_path(path);
        Self {
            inner: InnerM2dirMailboxList::new(&store),
        }
    }
}

/// Converts one [`M2dir`] into the shared [`Mailbox`] shape.
///
/// `id` is the on-disk path so downstream ops can locate the m2dir;
/// `name` is the last path segment. Counts default to `None`;
/// populating them needs the follow-up walk described in the module
/// doc.
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
