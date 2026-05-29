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

use alloc::{collections::BTreeSet, string::ToString, vec::Vec};
use std::path::PathBuf;

use io_maildir::{
    coroutine::{MaildirCoroutine, MaildirCoroutineState, MaildirReply, MaildirYield},
    coroutines::maildir_list::{MaildirList as InnerMaildirList, MaildirListError},
    maildir::Maildir,
    path::MaildirPath,
};
use log::trace;
use thiserror::Error;

use crate::{
    coroutine::{
        EmailBackend, EmailCoroutine, EmailCoroutineArg, EmailCoroutineState, FsBatch, FsStep,
    },
    mailbox::Mailbox,
};

/// Errors produced by [`MaildirMailboxList`].
#[derive(Debug, Error)]
pub enum MaildirMailboxListError {
    #[error(transparent)]
    List(#[from] MaildirListError),
    #[error("coroutine was resumed with the wrong EmailCoroutineArg variant")]
    InvalidArg,
    #[error("coroutine was resumed with an FsBatch variant it did not request")]
    UnexpectedBatch,
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

impl EmailCoroutine for MaildirMailboxList {
    type Yield = FsStep;
    type Return = Result<Vec<Mailbox>, MaildirMailboxListError>;

    const BACKEND: EmailBackend = EmailBackend::Maildir;

    // NOTE: when Maildir is the only enabled backend, EmailCoroutineArg
    // has a single variant so the destructure below is irrefutable
    // and the `else` arm is dead. It comes alive (and the lint goes
    // quiet on its own) as soon as a second backend rejoins.
    fn resume(
        &mut self,
        arg: EmailCoroutineArg<'_>,
    ) -> EmailCoroutineState<Self::Yield, Self::Return> {
        #[allow(irrefutable_let_patterns)]
        let EmailCoroutineArg::Fs { batch } = arg else {
            return EmailCoroutineState::Complete(Err(MaildirMailboxListError::InvalidArg));
        };

        let inner_arg = match batch {
            None => None,
            Some(FsBatch::DirRead(entries)) => Some(MaildirReply::DirRead(
                entries
                    .into_iter()
                    .map(|(k, v)| (k.into(), v.into_iter().map(MaildirPath::from).collect()))
                    .collect(),
            )),
            Some(FsBatch::DirExists(probes)) => Some(MaildirReply::DirExists(
                probes.into_iter().map(|(k, v)| (k.into(), v)).collect(),
            )),
            // MaildirList only consumes DirRead / DirExists batches.
            Some(_) => {
                return EmailCoroutineState::Complete(Err(
                    MaildirMailboxListError::UnexpectedBatch,
                ));
            }
        };

        match self.inner.resume(inner_arg) {
            MaildirCoroutineState::Complete(Ok(maildirs)) => {
                let mut mailboxes: Vec<Mailbox> = maildirs.into_iter().map(mailbox_from).collect();
                mailboxes.sort_by(|a, b| a.name.cmp(&b.name));
                EmailCoroutineState::Complete(Ok(mailboxes))
            }
            MaildirCoroutineState::Yielded(MaildirYield::WantsDirRead(paths)) => {
                EmailCoroutineState::Yielded(FsStep::WantsDirRead(to_pathbuf_set(paths)))
            }
            MaildirCoroutineState::Yielded(MaildirYield::WantsDirExists(paths)) => {
                EmailCoroutineState::Yielded(FsStep::WantsDirExists(to_pathbuf_set(paths)))
            }
            MaildirCoroutineState::Complete(Err(err)) => {
                EmailCoroutineState::Complete(Err(err.into()))
            }
            // MaildirList only yields DirRead / DirExists / Done / Err;
            // all the other Wants* variants belong to write-side
            // coroutines that haven't been ported yet.
            other => {
                let _ = other;
                unreachable!("MaildirList only yields DirRead / DirExists / Done / Err");
            }
        }
    }
}

/// Bulk-converts a `BTreeSet<MaildirPath>` to a `BTreeSet<PathBuf>`.
fn to_pathbuf_set(paths: BTreeSet<MaildirPath>) -> BTreeSet<PathBuf> {
    paths.into_iter().map(PathBuf::from).collect()
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
