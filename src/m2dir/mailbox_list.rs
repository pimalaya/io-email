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

use alloc::{collections::BTreeSet, string::ToString, vec::Vec};
use std::path::PathBuf;

use io_m2dir::{
    coroutine::{M2dirArg, M2dirCoroutine, M2dirCoroutineState, M2dirYield},
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

use crate::{
    coroutine::{
        EmailBackend, EmailCoroutine, EmailCoroutineArg, EmailCoroutineState, FsBatch, FsStep,
    },
    mailbox::Mailbox,
};

/// Errors produced by [`M2dirMailboxList`].
#[derive(Debug, Error)]
pub enum M2dirMailboxListError {
    #[error(transparent)]
    List(#[from] InnerM2dirMailboxListError),
    #[error("coroutine was resumed with the wrong EmailCoroutineArg variant")]
    InvalidArg,
    #[error("coroutine was resumed with an FsBatch variant it did not request")]
    UnexpectedBatch,
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

impl EmailCoroutine for M2dirMailboxList {
    type Yield = FsStep;
    type Return = Result<Vec<Mailbox>, M2dirMailboxListError>;

    const BACKEND: EmailBackend = EmailBackend::M2dir;

    fn resume(
        &mut self,
        arg: EmailCoroutineArg<'_>,
    ) -> EmailCoroutineState<Self::Yield, Self::Return> {
        #[allow(irrefutable_let_patterns)]
        let EmailCoroutineArg::Fs { batch } = arg else {
            return EmailCoroutineState::Complete(Err(M2dirMailboxListError::InvalidArg));
        };

        let inner_arg = match batch {
            None => None,
            Some(FsBatch::DirRead(entries)) => Some(M2dirArg::DirRead(
                entries
                    .into_iter()
                    .map(|(k, v)| (k.into(), v.into_iter().map(M2dirPath::from).collect()))
                    .collect(),
            )),
            Some(FsBatch::FileExists(probes)) => Some(M2dirArg::FileExists(
                probes.into_iter().map(|(k, v)| (k.into(), v)).collect(),
            )),
            // M2dirMailboxList only consumes DirRead / FileExists batches.
            Some(_) => {
                return EmailCoroutineState::Complete(Err(M2dirMailboxListError::UnexpectedBatch));
            }
        };

        match self.inner.resume(inner_arg) {
            M2dirCoroutineState::Complete(Ok(m2dirs)) => {
                let mut mailboxes: Vec<Mailbox> = m2dirs.into_iter().map(mailbox_from).collect();
                mailboxes.sort_by(|a, b| a.name.cmp(&b.name));
                EmailCoroutineState::Complete(Ok(mailboxes))
            }
            M2dirCoroutineState::Yielded(M2dirYield::WantsDirRead(paths)) => {
                EmailCoroutineState::Yielded(FsStep::WantsDirRead(to_pathbuf_set(paths)))
            }
            M2dirCoroutineState::Yielded(M2dirYield::WantsFileExists(paths)) => {
                EmailCoroutineState::Yielded(FsStep::WantsFileExists(to_pathbuf_set(paths)))
            }
            M2dirCoroutineState::Complete(Err(err)) => {
                EmailCoroutineState::Complete(Err(err.into()))
            }
            other => {
                let _ = other;
                unreachable!("M2dirMailboxList only yields DirRead / FileExists / Done / Err");
            }
        }
    }
}

/// Bulk-converts a `BTreeSet<M2dirPath>` to a `BTreeSet<PathBuf>`.
fn to_pathbuf_set(paths: BTreeSet<M2dirPath>) -> BTreeSet<PathBuf> {
    paths.into_iter().map(PathBuf::from).collect()
}

/// Converts one [`M2dir`] into the shared [`Mailbox`] shape.
///
/// `id` is the on-disk path so downstream ops can locate the m2dir;
/// `name` is the last path segment. Counts default to `None` —
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
