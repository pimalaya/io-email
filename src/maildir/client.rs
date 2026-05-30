//! Std-blocking Maildir client.
//!
//! Holds an inner [`io_maildir::client::MaildirClient`] wrapping the
//! filesystem root and its per-store knobs (`dovecot_keywords`,
//! `keywords_header`, `strip_headers`, `maildir_plus`,
//! `maildirpp_inbox`, `fs_layout`).
//!
//! [`Self::run`] pumps io-email Maildir coroutines directly against the
//! local filesystem; the inner client's own helpers stay reachable
//! through [`Self::inner`] for ops that the shared API does not cover.

use alloc::{string::String, vec::Vec};
use std::{
    fs, io, process,
    time::{SystemTime, UNIX_EPOCH},
};

use gethostname::gethostname;
use io_maildir::{client::MaildirClient as InnerMaildirClient, coroutine::*, path::MaildirPath};
use log::trace;
use thiserror::Error;

use crate::{
    envelope::Envelope,
    flag::{Flag, FlagOp},
    mailbox::Mailbox,
    maildir::{
        envelope_list::{MaildirEnvelopeList, MaildirEnvelopeListError},
        flag_store::{MaildirFlagStore, MaildirFlagStoreError},
        mailbox_create::{MaildirMailboxCreate, MaildirMailboxCreateError},
        mailbox_delete::{MaildirMailboxDelete, MaildirMailboxDeleteError},
        mailbox_list::{MaildirMailboxList, MaildirMailboxListError},
        message_add::{MaildirMessageAdd, MaildirMessageAddError},
        message_copy::{MaildirMessageCopy, MaildirMessageCopyError},
        message_delete::{MaildirMessageDelete, MaildirMessageDeleteError},
        message_get::{MaildirMessageGet, MaildirMessageGetError},
        message_move::{MaildirMessageMove, MaildirMessageMoveError},
    },
};
#[cfg(feature = "search")]
use crate::{
    maildir::envelope_search::{MaildirEnvelopeSearch, MaildirEnvelopeSearchError},
    search::query::SearchEmailsQuery,
};

/// Errors surfaced by [`MaildirClient`] while running a coroutine.
///
/// One variant per shared-API Maildir coroutine.
#[derive(Debug, Error)]
pub enum MaildirClientError {
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    MailboxList(#[from] MaildirMailboxListError),
    #[error(transparent)]
    EnvelopeList(#[from] MaildirEnvelopeListError),
    #[cfg(feature = "search")]
    #[error(transparent)]
    EnvelopeSearch(#[from] MaildirEnvelopeSearchError),
    #[error(transparent)]
    FlagStore(#[from] MaildirFlagStoreError),
    #[error(transparent)]
    MailboxCreate(#[from] MaildirMailboxCreateError),
    #[error(transparent)]
    MailboxDelete(#[from] MaildirMailboxDeleteError),
    #[error(transparent)]
    MessageAdd(#[from] MaildirMessageAddError),
    #[error(transparent)]
    MessageCopy(#[from] MaildirMessageCopyError),
    #[error(transparent)]
    MessageDelete(#[from] MaildirMessageDeleteError),
    #[error(transparent)]
    MessageGet(#[from] MaildirMessageGetError),
    #[error(transparent)]
    MessageMove(#[from] MaildirMessageMoveError),
    #[error(transparent)]
    Inner(#[from] io_maildir::client::MaildirClientError),
}

/// Std-blocking Maildir client built on a filesystem root.
///
/// All per-store behaviour knobs (`dovecot_keywords`, `keywords_header`,
/// `strip_headers`, `maildir_plus`, `maildirpp_inbox`, `fs_layout`)
/// live on [`Self::inner`] and are read through it on every shared-API
/// call.
pub struct MaildirClient {
    pub inner: InnerMaildirClient,
}

impl MaildirClient {
    /// Wraps a fresh inner client rooted at `root`. All knobs default
    /// to strict-Maildir behaviour; flip them on [`Self::inner`] before
    /// running coroutines.
    pub fn new(root: impl Into<MaildirPath>) -> Self {
        Self {
            inner: InnerMaildirClient::new(root),
        }
    }

    /// Pumps any standard-shape Maildir coroutine
    /// (`Yield = MaildirYield`, `Return = Result<T, E>`) against the
    /// local filesystem until it terminates.
    ///
    /// Reaches into [`Self::inner`] for the root rather than delegating
    /// to [`io_maildir::client::MaildirClient::run`] so error variants
    /// route through [`MaildirClientError`] directly.
    pub fn run<C, T, E>(&self, mut coroutine: C) -> Result<T, MaildirClientError>
    where
        C: MaildirCoroutine<Yield = MaildirYield, Return = Result<T, E>>,
        MaildirClientError: From<E>,
    {
        let mut arg: Option<MaildirReply> = None;

        loop {
            match coroutine.resume(arg.take()) {
                MaildirCoroutineState::Complete(Ok(out)) => return Ok(out),
                MaildirCoroutineState::Complete(Err(err)) => return Err(err.into()),
                MaildirCoroutineState::Yielded(MaildirYield::WantsFileExists(paths)) => {
                    let mut out = alloc::collections::BTreeMap::new();
                    for path in paths {
                        let exists = fs::metadata(path.as_str())
                            .map(|m| m.is_file())
                            .unwrap_or(false);
                        trace!("file_exists {path}: {exists}");
                        out.insert(path, exists);
                    }
                    arg = Some(MaildirReply::FileExists(out));
                }
                MaildirCoroutineState::Yielded(MaildirYield::WantsDirExists(paths)) => {
                    let mut out = alloc::collections::BTreeMap::new();
                    for path in paths {
                        let exists = fs::metadata(path.as_str())
                            .map(|m| m.is_dir())
                            .unwrap_or(false);
                        trace!("dir_exists {path}: {exists}");
                        out.insert(path, exists);
                    }
                    arg = Some(MaildirReply::DirExists(out));
                }
                MaildirCoroutineState::Yielded(MaildirYield::WantsDirRead(paths)) => {
                    let mut entries = alloc::collections::BTreeMap::new();
                    for path in paths {
                        trace!("read_dir {path}");
                        let mut names = alloc::collections::BTreeSet::new();
                        match fs::read_dir(path.as_str()) {
                            Ok(iter) => {
                                for entry in iter {
                                    let entry = entry?;
                                    names.insert(normalize_path(entry.path()));
                                }
                            }
                            Err(err) if err.kind() == io::ErrorKind::NotFound => {}
                            Err(err) => return Err(err.into()),
                        }
                        entries.insert(path, names);
                    }
                    arg = Some(MaildirReply::DirRead(entries));
                }
                MaildirCoroutineState::Yielded(MaildirYield::WantsFileRead(paths)) => {
                    let mut contents = alloc::collections::BTreeMap::new();
                    for path in paths {
                        trace!("read_file {path}");
                        let bytes = fs::read(path.as_str())?;
                        contents.insert(path, bytes);
                    }
                    arg = Some(MaildirReply::FileRead(contents));
                }
                MaildirCoroutineState::Yielded(MaildirYield::WantsFileCreate(files)) => {
                    for (path, bytes) in files {
                        trace!("write {path} ({} bytes)", bytes.len());
                        if let Some(parent) = std::path::Path::new(path.as_str()).parent() {
                            fs::create_dir_all(parent)?;
                        }
                        fs::write(path.as_str(), &bytes)?;
                    }
                    arg = Some(MaildirReply::FileCreate);
                }
                MaildirCoroutineState::Yielded(MaildirYield::WantsDirCreate(paths)) => {
                    for path in paths {
                        trace!("create_dir_all {path}");
                        fs::create_dir_all(path.as_str())?;
                    }
                    arg = Some(MaildirReply::DirCreate);
                }
                MaildirCoroutineState::Yielded(MaildirYield::WantsDirRemove(paths)) => {
                    for path in paths {
                        trace!("remove_dir_all {path}");
                        fs::remove_dir_all(path.as_str())?;
                    }
                    arg = Some(MaildirReply::DirRemove);
                }
                MaildirCoroutineState::Yielded(MaildirYield::WantsRename(pairs)) => {
                    for (from, to) in pairs {
                        trace!("rename {from} -> {to}");
                        fs::rename(from.as_str(), to.as_str())?;
                    }
                    arg = Some(MaildirReply::Rename);
                }
                MaildirCoroutineState::Yielded(MaildirYield::WantsCopy(pairs)) => {
                    for (from, to) in pairs {
                        trace!("copy {from} -> {to}");
                        fs::copy(from.as_str(), to.as_str())?;
                    }
                    arg = Some(MaildirReply::Copy);
                }
                MaildirCoroutineState::Yielded(MaildirYield::WantsTime) => {
                    let ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
                    arg = Some(MaildirReply::Time {
                        secs: ts.as_secs(),
                        nanos: ts.subsec_nanos(),
                    });
                }
                MaildirCoroutineState::Yielded(MaildirYield::WantsPid) => {
                    arg = Some(MaildirReply::Pid(process::id()));
                }
                MaildirCoroutineState::Yielded(MaildirYield::WantsHostname) => {
                    let hostname = gethostname().into_string().unwrap_or_default();
                    arg = Some(MaildirReply::Hostname(hostname));
                }
            }
        }
    }

    /// Lists every Maildir under the configured root. `with_counts`
    /// is currently a no-op; see [`MaildirMailboxList`] for the path
    /// to surfacing per-mailbox totals.
    pub fn list_mailboxes(&self, with_counts: bool) -> Result<Vec<Mailbox>, MaildirClientError> {
        let root = self.inner.root().clone();
        let maildir_plus = self.inner.maildir_plus;
        self.run(MaildirMailboxList::new(
            std::path::PathBuf::from(root),
            maildir_plus,
            with_counts,
        ))
    }

    /// Lists envelopes from `mailbox`. `page = None` and
    /// `page_size = None` return the whole listing. The
    /// `with_attachment` knob is currently ignored on Maildir.
    pub fn list_envelopes(
        &self,
        mailbox: &str,
        page: Option<u32>,
        page_size: Option<u32>,
        _with_attachment: bool,
    ) -> Result<Vec<Envelope>, MaildirClientError> {
        let root = self.inner.root().clone();
        let maildir_plus = self.inner.maildir_plus;
        self.run(MaildirEnvelopeList::new(
            std::path::PathBuf::from(root),
            maildir_plus,
            mailbox,
            page,
            page_size,
        )?)
    }

    /// Searches envelopes in `mailbox` against the shared query.
    /// Filter / sort / paginate are applied client-side.
    #[cfg(feature = "search")]
    pub fn search_envelopes(
        &self,
        mailbox: &str,
        query: Option<&SearchEmailsQuery>,
        page: Option<u32>,
        page_size: Option<u32>,
        _with_attachment: bool,
    ) -> Result<Vec<Envelope>, MaildirClientError> {
        let root = self.inner.root().clone();
        let maildir_plus = self.inner.maildir_plus;
        self.run(MaildirEnvelopeSearch::new(
            std::path::PathBuf::from(root),
            maildir_plus,
            mailbox,
            query,
            page,
            page_size,
        )?)
    }

    /// Adds, sets, or removes `flags` on a Maildir id set.
    pub fn store_flags(
        &self,
        mailbox: &str,
        ids: &[&str],
        flags: &[Flag],
        op: FlagOp,
    ) -> Result<(), MaildirClientError> {
        let root = self.inner.root().clone();
        let maildir_plus = self.inner.maildir_plus;
        self.run(MaildirFlagStore::new(
            std::path::PathBuf::from(root),
            maildir_plus,
            mailbox,
            ids,
            flags,
            op,
        )?)
    }

    /// Reads one message's raw RFC 5322 bytes from `mailbox`.
    pub fn get_message(&self, mailbox: &str, id: &str) -> Result<Vec<u8>, MaildirClientError> {
        let root = self.inner.root().clone();
        let maildir_plus = self.inner.maildir_plus;
        self.run(MaildirMessageGet::new(
            std::path::PathBuf::from(root),
            maildir_plus,
            mailbox,
            id,
        )?)
    }

    /// Appends `raw` to `mailbox` under `cur/` with the given flags.
    /// Returns the Maildir filename minus the `:2,FLAGS` suffix.
    pub fn add_message(
        &self,
        mailbox: &str,
        flags: &[Flag],
        raw: Vec<u8>,
    ) -> Result<String, MaildirClientError> {
        let root = self.inner.root().clone();
        let maildir_plus = self.inner.maildir_plus;
        self.run(MaildirMessageAdd::new(
            std::path::PathBuf::from(root),
            maildir_plus,
            mailbox,
            flags,
            raw,
        )?)
    }

    /// Creates `name` as a new Maildir under the configured root.
    pub fn create_mailbox(&self, name: &str) -> Result<(), MaildirClientError> {
        let root = self.inner.root().clone();
        let maildir_plus = self.inner.maildir_plus;
        self.run(MaildirMailboxCreate::new(
            std::path::PathBuf::from(root),
            maildir_plus,
            name,
        )?)
    }

    /// Recursively removes the Maildir named `name`.
    pub fn delete_mailbox(&self, name: &str) -> Result<(), MaildirClientError> {
        let root = self.inner.root().clone();
        let maildir_plus = self.inner.maildir_plus;
        self.run(MaildirMailboxDelete::new(
            std::path::PathBuf::from(root),
            maildir_plus,
            name,
        )?)
    }

    /// Flags `id` in `mailbox` as Trashed. Maildir has no atomic
    /// "remove" primitive; pair with a periodic expunge to reclaim
    /// space.
    pub fn delete_message(&self, mailbox: &str, id: &str) -> Result<(), MaildirClientError> {
        let root = self.inner.root().clone();
        let maildir_plus = self.inner.maildir_plus;
        self.run(MaildirMessageDelete::new(
            std::path::PathBuf::from(root),
            maildir_plus,
            mailbox,
            id,
        )?)
    }

    /// Copies every id from `from` to `to`.
    pub fn copy_messages(
        &self,
        from: &str,
        to: &str,
        ids: &[&str],
    ) -> Result<(), MaildirClientError> {
        let root = self.inner.root().clone();
        let maildir_plus = self.inner.maildir_plus;
        self.run(MaildirMessageCopy::new(
            std::path::PathBuf::from(root),
            maildir_plus,
            from,
            to,
            ids,
        )?)
    }

    /// Moves every id from `from` to `to`.
    pub fn move_messages(
        &self,
        from: &str,
        to: &str,
        ids: &[&str],
    ) -> Result<(), MaildirClientError> {
        let root = self.inner.root().clone();
        let maildir_plus = self.inner.maildir_plus;
        self.run(MaildirMessageMove::new(
            std::path::PathBuf::from(root),
            maildir_plus,
            from,
            to,
            ids,
        )?)
    }
}

/// Bulk-normalises a [`std::path::PathBuf`] into the Unix-styled
/// [`MaildirPath`] used by the io-maildir core.
fn normalize_path(path: std::path::PathBuf) -> MaildirPath {
    let s = path.to_string_lossy().into_owned();
    #[cfg(windows)]
    let s = s.replace('\\', "/");
    MaildirPath::new(s)
}
