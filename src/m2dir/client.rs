//! Std-blocking m2dir client.
//!
//! Wraps an inner [`InnerM2dirClient`] and pumps any standard-shape
//! [`M2dirCoroutine`] (Yield = [`M2dirYield`]) against the local
//! filesystem via [`std::fs`].

use alloc::{
    collections::{BTreeMap, BTreeSet},
    string::String,
    vec::Vec,
};
use std::{
    collections::hash_map::RandomState,
    fs,
    hash::{BuildHasher, Hasher},
    io,
    path::{Path, PathBuf},
    process,
};

use io_m2dir::{client::M2dirClient as InnerM2dirClient, coroutine::*, path::M2dirPath};
use log::trace;
use thiserror::Error;

#[cfg(feature = "search")]
use crate::{
    envelope::m2dir::search::{M2dirEnvelopeSearch, M2dirEnvelopeSearchError},
    search::query::SearchEmailsQuery,
};
use crate::{
    envelope::{
        m2dir::list::{M2dirEnvelopeList, M2dirEnvelopeListError},
        types::Envelope,
    },
    flag::{
        m2dir::store::{M2dirFlagStore, M2dirFlagStoreError},
        types::{Flag, FlagOp},
    },
    mailbox::{
        m2dir::{
            create::{M2dirMailboxCreate, M2dirMailboxCreateError},
            delete::{M2dirMailboxDelete, M2dirMailboxDeleteError},
            list::{M2dirMailboxList, M2dirMailboxListError},
        },
        types::Mailbox,
    },
    message::m2dir::{
        add::{M2dirMessageAdd, M2dirMessageAddError},
        copy::{M2dirMessageCopy, M2dirMessageCopyError},
        delete::{M2dirMessageDelete, M2dirMessageDeleteError},
        get::{M2dirMessageGet, M2dirMessageGetError},
        r#move::{M2dirMessageMove, M2dirMessageMoveError},
    },
};

/// Errors surfaced by [`M2dirClient`] while running a coroutine.
///
/// One variant per shared-API m2dir coroutine.
#[derive(Debug, Error)]
pub enum M2dirClientError {
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    MailboxList(#[from] M2dirMailboxListError),
    #[error(transparent)]
    EnvelopeList(#[from] M2dirEnvelopeListError),
    #[cfg(feature = "search")]
    #[error(transparent)]
    EnvelopeSearch(#[from] M2dirEnvelopeSearchError),
    #[error(transparent)]
    FlagStore(#[from] M2dirFlagStoreError),
    #[error(transparent)]
    MailboxCreate(#[from] M2dirMailboxCreateError),
    #[error(transparent)]
    MailboxDelete(#[from] M2dirMailboxDeleteError),
    #[error(transparent)]
    MessageAdd(#[from] M2dirMessageAddError),
    #[error(transparent)]
    MessageCopy(#[from] M2dirMessageCopyError),
    #[error(transparent)]
    MessageDelete(#[from] M2dirMessageDeleteError),
    #[error(transparent)]
    MessageGet(#[from] M2dirMessageGetError),
    #[error(transparent)]
    MessageMove(#[from] M2dirMessageMoveError),
    #[error(transparent)]
    Inner(#[from] io_m2dir::client::M2dirClientError),
}

/// Light m2dir client wrapping a filesystem root.
///
/// The shared root lives on [`Self::inner`]; m2dir has no extra
/// per-session knobs (no auto_select, no capability list).
pub struct M2dirClient {
    pub inner: InnerM2dirClient,
}

impl M2dirClient {
    /// Wraps a fresh inner client rooted at `root`. No filesystem
    /// check is performed at construction time.
    pub fn new(root: impl Into<M2dirPath>) -> Self {
        Self {
            inner: InnerM2dirClient::new(root),
        }
    }

    /// Pumps any standard-shape m2dir coroutine
    /// (`Yield = M2dirYield`, `Return = Result<T, E>`) against the
    /// local filesystem until it terminates.
    ///
    /// Duplicates the body of [`InnerM2dirClient::run`] so error
    /// variants route through [`M2dirClientError`] directly.
    pub fn run<C, T, E>(&self, mut coroutine: C) -> Result<T, M2dirClientError>
    where
        C: M2dirCoroutine<Yield = M2dirYield, Return = Result<T, E>>,
        M2dirClientError: From<E>,
    {
        let mut arg: Option<M2dirArg> = None;

        loop {
            match coroutine.resume(arg.take()) {
                M2dirCoroutineState::Complete(Ok(out)) => return Ok(out),
                M2dirCoroutineState::Complete(Err(err)) => return Err(err.into()),
                M2dirCoroutineState::Yielded(M2dirYield::WantsPid) => {
                    arg = Some(M2dirArg::Pid(process::id()));
                }
                M2dirCoroutineState::Yielded(M2dirYield::WantsRandom { len }) => {
                    arg = Some(M2dirArg::Random(random_bytes(len)));
                }
                M2dirCoroutineState::Yielded(M2dirYield::WantsFileExists(paths)) => {
                    arg = Some(M2dirArg::FileExists(file_exists(paths)));
                }
                M2dirCoroutineState::Yielded(M2dirYield::WantsDirRead(paths)) => {
                    arg = Some(M2dirArg::DirRead(read_dirs(paths)?));
                }
                M2dirCoroutineState::Yielded(M2dirYield::WantsDirCreate(paths)) => {
                    create_dirs(paths)?;
                    arg = Some(M2dirArg::DirCreate);
                }
                M2dirCoroutineState::Yielded(M2dirYield::WantsDirRemove(paths)) => {
                    remove_dirs(paths)?;
                    arg = Some(M2dirArg::DirRemove);
                }
                M2dirCoroutineState::Yielded(M2dirYield::WantsFileRead(paths)) => {
                    arg = Some(M2dirArg::FileRead(read_files_tolerant(paths)?));
                }
                M2dirCoroutineState::Yielded(M2dirYield::WantsFileCreate(files)) => {
                    write_files(files)?;
                    arg = Some(M2dirArg::FileCreate);
                }
                M2dirCoroutineState::Yielded(M2dirYield::WantsFileRemove(paths)) => {
                    remove_files_tolerant(paths)?;
                    arg = Some(M2dirArg::FileRemove);
                }
                M2dirCoroutineState::Yielded(M2dirYield::WantsRename(pairs)) => {
                    rename_paths(pairs)?;
                    arg = Some(M2dirArg::Rename);
                }
            }
        }
    }

    /// Lists every m2dir under the store root. `with_counts` is
    /// accepted for symmetry with the other backends; surfacing
    /// totals/unread needs a follow-up walk and is currently a no-op.
    pub fn list_mailboxes(&self, with_counts: bool) -> Result<Vec<Mailbox>, M2dirClientError> {
        self.run(M2dirMailboxList::new(
            PathBuf::from(self.inner.root().as_str()),
            with_counts,
        ))
    }

    /// Lists envelopes from `mailbox`. `page = None` and
    /// `page_size = None` fetch the whole mailbox; envelopes are
    /// sorted by date descending.
    pub fn list_envelopes(
        &self,
        mailbox: &str,
        page: Option<u32>,
        page_size: Option<u32>,
        with_attachment: bool,
    ) -> Result<Vec<Envelope>, M2dirClientError> {
        self.run(M2dirEnvelopeList::new(
            PathBuf::from(self.inner.root().as_str()),
            mailbox,
            page,
            page_size,
            with_attachment,
        )?)
    }

    /// Searches envelopes in `mailbox` against the shared query.
    /// Filtering and sorting happen client-side after a full scan.
    #[cfg(feature = "search")]
    pub fn search_envelopes(
        &self,
        mailbox: &str,
        query: Option<&SearchEmailsQuery>,
        page: Option<u32>,
        page_size: Option<u32>,
        with_attachment: bool,
    ) -> Result<Vec<Envelope>, M2dirClientError> {
        self.run(M2dirEnvelopeSearch::new(
            PathBuf::from(self.inner.root().as_str()),
            mailbox,
            query,
            page,
            page_size,
            with_attachment,
        )?)
    }

    /// Adds, sets, or removes `flags` on every id by rewriting each
    /// `.meta/<id>.flags` sidecar.
    pub fn store_flags(
        &self,
        mailbox: &str,
        ids: &[&str],
        flags: &[Flag],
        op: FlagOp,
    ) -> Result<(), M2dirClientError> {
        self.run(M2dirFlagStore::new(
            PathBuf::from(self.inner.root().as_str()),
            mailbox,
            ids,
            flags,
            op,
        )?)
    }

    /// Reads one message's raw bytes by id, validating the checksum
    /// embedded in its filename.
    pub fn get_message(&self, mailbox: &str, id: &str) -> Result<Vec<u8>, M2dirClientError> {
        self.run(M2dirMessageGet::new(
            PathBuf::from(self.inner.root().as_str()),
            mailbox,
            id,
        )?)
    }

    /// Appends `raw` to `mailbox`, then persists `flags` as the
    /// `.meta/<id>.flags` sidecar when non-empty. Returns the minted
    /// entry id.
    pub fn add_message(
        &self,
        mailbox: &str,
        flags: &[Flag],
        raw: Vec<u8>,
    ) -> Result<String, M2dirClientError> {
        self.run(M2dirMessageAdd::new(
            PathBuf::from(self.inner.root().as_str()),
            mailbox,
            flags,
            raw,
        )?)
    }

    /// Creates `name` as a new m2dir mailbox: the folder, the
    /// `.m2dir` marker and the `.meta` sub-directory.
    pub fn create_mailbox(&self, name: &str) -> Result<(), M2dirClientError> {
        self.run(M2dirMailboxCreate::new(
            PathBuf::from(self.inner.root().as_str()),
            name,
        )?)
    }

    /// Recursively removes the m2dir at `name`.
    pub fn delete_mailbox(&self, name: &str) -> Result<(), M2dirClientError> {
        self.run(M2dirMailboxDelete::new(
            PathBuf::from(self.inner.root().as_str()),
            name,
        )?)
    }

    /// Removes the entry `id` and every matching `.meta/<id>*` file.
    pub fn delete_message(&self, mailbox: &str, id: &str) -> Result<(), M2dirClientError> {
        self.run(M2dirMessageDelete::new(
            PathBuf::from(self.inner.root().as_str()),
            mailbox,
            id,
        )?)
    }

    /// Copies every id from `from` to `to`. Flag sidecars are not
    /// propagated; callers add flags explicitly after the copy when
    /// needed.
    pub fn copy_messages(
        &self,
        from: &str,
        to: &str,
        ids: &[&str],
    ) -> Result<(), M2dirClientError> {
        self.run(M2dirMessageCopy::new(
            PathBuf::from(self.inner.root().as_str()),
            from,
            to,
            ids,
        )?)
    }

    /// Moves every id from `from` to `to`. Flag sidecars are not
    /// propagated; callers add flags explicitly after the move when
    /// needed.
    pub fn move_messages(
        &self,
        from: &str,
        to: &str,
        ids: &[&str],
    ) -> Result<(), M2dirClientError> {
        self.run(M2dirMessageMove::new(
            PathBuf::from(self.inner.root().as_str()),
            from,
            to,
            ids,
        )?)
    }
}

// ---- Filesystem helpers (duplicated from io_m2dir::client) ----

fn create_dirs(paths: BTreeSet<M2dirPath>) -> Result<(), io::Error> {
    for path in paths {
        trace!("create_dir_all {path}");
        fs::create_dir_all(path.as_str())?;
    }
    Ok(())
}

fn remove_dirs(paths: BTreeSet<M2dirPath>) -> Result<(), io::Error> {
    for path in paths {
        trace!("remove_dir_all {path}");
        fs::remove_dir_all(path.as_str())?;
    }
    Ok(())
}

fn write_files(files: BTreeMap<M2dirPath, Vec<u8>>) -> Result<(), io::Error> {
    for (path, contents) in files {
        trace!("write {path} ({} bytes)", contents.len());

        if let Some(parent) = Path::new(path.as_str()).parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(path.as_str(), &contents)?;
    }
    Ok(())
}

fn remove_files_tolerant(paths: BTreeSet<M2dirPath>) -> Result<(), io::Error> {
    for path in paths {
        trace!("remove_file (tolerant) {path}");
        match fs::remove_file(path.as_str()) {
            Ok(()) => {}
            Err(err) if err.kind() == io::ErrorKind::NotFound => {}
            Err(err) => return Err(err),
        }
    }
    Ok(())
}

fn read_dirs(
    paths: BTreeSet<M2dirPath>,
) -> Result<BTreeMap<M2dirPath, BTreeSet<M2dirPath>>, io::Error> {
    let mut entries = BTreeMap::new();

    for path in paths {
        trace!("read_dir {path}");

        let mut names = BTreeSet::new();
        match fs::read_dir(path.as_str()) {
            Ok(iter) => {
                for entry in iter {
                    let entry = entry?;
                    names.insert(normalize_path(entry.path()));
                }
            }
            Err(err) if err.kind() == io::ErrorKind::NotFound => {}
            Err(err) if err.kind() == io::ErrorKind::NotADirectory => {}
            Err(err) => return Err(err),
        }

        entries.insert(path, names);
    }

    Ok(entries)
}

fn read_files_tolerant(
    paths: BTreeSet<M2dirPath>,
) -> Result<BTreeMap<M2dirPath, Vec<u8>>, io::Error> {
    let mut contents = BTreeMap::new();

    for path in paths {
        trace!("read_file (tolerant) {path}");
        match fs::read(path.as_str()) {
            Ok(bytes) => {
                contents.insert(path, bytes);
            }
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                contents.insert(path, Vec::new());
            }
            Err(err) => return Err(err),
        }
    }

    Ok(contents)
}

fn rename_paths(pairs: Vec<(M2dirPath, M2dirPath)>) -> Result<(), io::Error> {
    for (from, to) in pairs {
        trace!("rename {from} -> {to}");
        fs::rename(from.as_str(), to.as_str())?;
    }
    Ok(())
}

fn file_exists(paths: BTreeSet<M2dirPath>) -> BTreeMap<M2dirPath, bool> {
    let mut out = BTreeMap::new();
    for path in paths {
        let exists = fs::metadata(path.as_str())
            .map(|m| m.is_file())
            .unwrap_or(false);
        trace!("file_exists {path}: {exists}");
        out.insert(path, exists);
    }
    out
}

fn normalize_path(path: PathBuf) -> M2dirPath {
    let s = path.to_string_lossy().into_owned();
    #[cfg(windows)]
    let s = s.replace('\\', "/");
    M2dirPath::new(s)
}

/// Generates `len` pseudo-random bytes seeded from [`RandomState`],
/// iterated via xorshift64*. Mirrors io-m2dir's own helper.
fn random_bytes(len: usize) -> Vec<u8> {
    let mut state = RandomState::new().build_hasher().finish();
    if state == 0 {
        state = 0xdeadbeef;
    }

    let mut out = Vec::with_capacity(len);
    let mut buf = 0u64;
    let mut i = 8;

    while out.len() < len {
        if i == 8 {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            buf = state;
            i = 0;
        }
        out.push(buf as u8);
        buf >>= 8;
        i += 1;
    }

    out
}
