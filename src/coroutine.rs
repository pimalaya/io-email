//! Generator-shape io-email coroutine.
//!
//! Mirrors the shape of `core::ops::Coroutine` (still unstable in std
//! at the time of writing, but the API is well understood): a
//! [`Yield`](EmailCoroutine::Yield) associated type for intermediate
//! progress, a [`Return`](EmailCoroutine::Return) associated type for
//! terminal output, and a two-variant [`CoroutineState`]
//! (`Yielded` / `Complete`).
//!
//! ## One-shot vs streaming
//!
//! For one-shot operations (`mailbox_list`, `envelope_list`,
//! `message_get`, …) the Yield type is one of the per-backend step
//! enums ([`ImapStep`], [`JmapStep`], [`SmtpStep`], [`FsStep`]) and
//! the Return type is `Result<Output, Error>`. The matching driver
//! loop ([`crate::client::EmailClientStd`]) pumps I/O until
//! [`CoroutineState::Complete`] is reached.
//!
//! For streaming operations (`watch_mailbox`) the Yield type is a
//! per-coroutine enum mixing I/O step requests with domain events
//! (e.g. [`crate::event::WatchEvent`]); the Return type is
//! `Result<(), Error>` and `Complete` only fires when shutdown is
//! requested or the protocol errors out.
//!
//! ## Resume args
//!
//! [`EmailCoroutine::resume`] takes [`EmailCoroutineArg`]. The
//! driver's job is to ferry socket bytes / filesystem batches into
//! the coroutine based on the previous yield. Each backend reuses the
//! same Arg variant; the per-coroutine Yield type picks the exact set
//! of `Wants*` variants the coroutine needs.
//!
//! ## Why not `Pin<&mut Self>`?
//!
//! Std's `Coroutine` uses `Pin<&mut Self>` because compiler-generated
//! generators can hold self-referential borrows across `yield`
//! points. Our state machines are hand-rolled and drop every borrow
//! before returning from `resume`; pinning adds caller friction with
//! no payoff. A pin-respecting adapter trait is trivial to bolt on
//! later if interop with `gen` blocks ever matters.

#[cfg(any(feature = "maildir", feature = "m2dir"))]
use alloc::collections::{BTreeMap, BTreeSet};
#[cfg(feature = "maildir")]
use alloc::string::String;
#[cfg(any(
    feature = "imap",
    feature = "jmap",
    feature = "maildir",
    feature = "m2dir",
    feature = "smtp"
))]
use alloc::vec::Vec;
#[cfg(not(any(feature = "imap", feature = "jmap", feature = "smtp")))]
use core::marker::PhantomData;
#[cfg(any(feature = "maildir", feature = "m2dir"))]
use std::path::PathBuf;

#[cfg(feature = "imap")]
use io_imap::codec::fragmentizer::Fragmentizer;

/// Tag identifying which backend a coroutine drives.
///
/// Each [`EmailCoroutine`] implementor publishes its tag via
/// [`EmailCoroutine::BACKEND`]; the std client matches on it to pick
/// the right per-backend context for the driver loop.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EmailBackend {
    #[cfg(feature = "imap")]
    Imap,
    #[cfg(feature = "jmap")]
    Jmap,
    #[cfg(feature = "maildir")]
    Maildir,
    #[cfg(feature = "m2dir")]
    M2dir,
    #[cfg(feature = "smtp")]
    Smtp,
    // NOTE: mbox / notmuch variants land here once those backends
    // rejoin.
}

/// Input handed to [`EmailCoroutine::resume`] on each step.
///
/// Each variant bundles a backend family's per-step state: the
/// persistent context the driver owns across resumes (e.g. an IMAP
/// [`Fragmentizer`]) plus the fresh input from the last completed I/O
/// step. Socket-based variants carry the bytes the socket just yielded
/// (`None` on the very first resume or right after a `WantsWrite`
/// step); the [`Self::Fs`] variant carries a filesystem batch shared
/// by every filesystem backend.
pub enum EmailCoroutineArg<'a> {
    #[cfg(feature = "imap")]
    Imap {
        fragmentizer: &'a mut Fragmentizer,
        bytes: Option<&'a [u8]>,
    },
    /// JMAP runs over plain HTTP; the HTTP parser state lives inside
    /// the coroutine itself (via `io_jmap::rfc8620::send::JmapSend`),
    /// so the per-step context reduces to the socket bytes.
    #[cfg(feature = "jmap")]
    Jmap { bytes: Option<&'a [u8]> },
    /// SMTP runs an RFC 5321 mail transaction; the bytes are the
    /// server replies that the SmtpMessageSend state machine
    /// consumes.
    #[cfg(feature = "smtp")]
    Smtp { bytes: Option<&'a [u8]> },
    /// Filesystem-based backends (Maildir, m2dir) share this variant.
    /// `batch` is the reply to the previous filesystem-flavored step,
    /// or `None` on the very first resume.
    #[cfg(any(feature = "maildir", feature = "m2dir"))]
    Fs { batch: Option<FsBatch> },
    /// Anchors the `'a` parameter when no enabled backend uses it
    /// (e.g. maildir-only builds). Never constructed; never matched.
    #[doc(hidden)]
    #[cfg(not(any(feature = "imap", feature = "jmap", feature = "smtp")))]
    _Phantom(PhantomData<&'a ()>),
}

/// Filesystem batch handed back to any filesystem-backed coroutine
/// on resume, paired with the [`FsStep`] variant that requested it.
///
/// Paths are typed [`PathBuf`] so the Step / Arg pair stays shared
/// across Maildir / m2dir / future filesystem backends; each
/// coroutine converts to its own internal path type (e.g.
/// `MaildirPath`, `M2dirPath`) at the boundary via the
/// `From<PathBuf>` impls on those types.
///
/// Side-effecting batches (create, remove, rename, copy) carry no
/// payload back: the driver either applied them or surfaced an
/// [`std::io::Error`] before resume was ever called.
#[cfg(any(feature = "maildir", feature = "m2dir"))]
#[derive(Clone, Debug)]
pub enum FsBatch {
    /// Reply to [`FsStep::WantsDirRead`]: for each probed directory,
    /// the set of paths it contained.
    DirRead(BTreeMap<PathBuf, BTreeSet<PathBuf>>),
    /// Reply to [`FsStep::WantsDirExists`]: for each probed path,
    /// whether a directory exists at that location.
    DirExists(BTreeMap<PathBuf, bool>),
    /// Reply to [`FsStep::WantsFileExists`]: for each probed path,
    /// whether a regular file exists at that location.
    FileExists(BTreeMap<PathBuf, bool>),
    /// Reply to [`FsStep::WantsFileRead`]: for each probed path, the
    /// file's bytes. Missing files are surfaced as an empty buffer so
    /// the coroutine can still progress.
    FileRead(BTreeMap<PathBuf, Vec<u8>>),
    /// Acknowledges [`FsStep::WantsFileCreate`]: the driver wrote
    /// every requested file successfully.
    FileCreate,
    /// Acknowledges [`FsStep::WantsFileRemove`]: the driver removed
    /// every requested file successfully.
    FileRemove,
    /// Acknowledges [`FsStep::WantsDirCreate`].
    DirCreate,
    /// Acknowledges [`FsStep::WantsDirRemove`].
    DirRemove,
    /// Acknowledges [`FsStep::WantsRename`].
    Rename,
    /// Acknowledges [`FsStep::WantsCopy`].
    #[cfg(feature = "maildir")]
    Copy,
    /// Reply to [`FsStep::WantsTime`]: the driver's current Unix
    /// time, used to mint Maildir message identifiers.
    #[cfg(feature = "maildir")]
    Time { secs: u64, nanos: u32 },
    /// Reply to [`FsStep::WantsPid`]: the driver's process id, used
    /// to mint Maildir / m2dir message identifiers.
    Pid(u32),
    /// Reply to [`FsStep::WantsHostname`]: the driver's host name,
    /// used to mint Maildir message identifiers.
    #[cfg(feature = "maildir")]
    Hostname(String),
    /// Reply to [`FsStep::WantsRandom`]: `len` random bytes from the
    /// driver, used to mint m2dir entry identifiers.
    #[cfg(feature = "m2dir")]
    Random(Vec<u8>),
}

/// State yielded by an [`EmailCoroutine::resume`] step.
///
/// Two-variant by design (matches std's `core::ops::CoroutineState`):
/// any further variation lives inside the per-coroutine `Yield` type.
#[derive(Debug)]
pub enum EmailCoroutineState<Y, R> {
    /// Intermediate yield. The driver should react to `Y` (run I/O,
    /// deliver an event to the caller, …) and resume the coroutine
    /// again.
    Yielded(Y),
    /// Terminal yield. No further resume is valid; resuming anyway is
    /// implementation-defined per coroutine but typically panics or
    /// returns a "resumed after completion" error variant on the next
    /// `Return`.
    Complete(R),
}

/// Standard-shape io-email coroutine.
///
/// Implementors own their backend's internal state machine and
/// advertise their target backend through [`Self::BACKEND`] so a
/// generic driver can route them to the matching per-protocol
/// context.
pub trait EmailCoroutine {
    /// Intermediate value handed back on every step. One-shot
    /// coroutines use the matching per-backend step enum
    /// ([`ImapStep`], [`JmapStep`], [`SmtpStep`], [`FsStep`]);
    /// streaming coroutines use a custom enum mixing step requests
    /// with domain events.
    type Yield;
    /// Terminal value. By convention `Result<T, E>`; the `Ok` arm
    /// carries the operation's final output (often `()` for
    /// long-running streams), the `Err` arm carries the cause.
    type Return;

    /// Backend tag used by drivers to pick the right per-backend
    /// context for the resume arg.
    const BACKEND: EmailBackend;

    /// Advances the coroutine one step.
    fn resume(
        &mut self,
        arg: EmailCoroutineArg<'_>,
    ) -> EmailCoroutineState<Self::Yield, Self::Return>;
}

/// I/O step requested by a one-shot IMAP coroutine.
#[cfg(feature = "imap")]
#[derive(Debug)]
pub enum ImapStep {
    /// Driver should read more bytes from the socket and feed them
    /// back via [`EmailCoroutineArg::Imap::bytes`] on the next
    /// resume.
    WantsRead,
    /// Driver should write these bytes to the socket; the next
    /// resume typically takes `bytes: None`.
    WantsWrite(Vec<u8>),
}

/// I/O step requested by a one-shot JMAP coroutine.
#[cfg(feature = "jmap")]
#[derive(Debug)]
pub enum JmapStep {
    WantsRead,
    WantsWrite(Vec<u8>),
}

/// I/O step requested by a one-shot SMTP coroutine.
#[cfg(feature = "smtp")]
#[derive(Debug)]
pub enum SmtpStep {
    WantsRead,
    WantsWrite(Vec<u8>),
}

/// Filesystem step requested by a one-shot Maildir / m2dir
/// coroutine. The driver matches each variant against
/// `std::fs::*` (or the platform equivalent) and feeds the result
/// back as the matching [`FsBatch`] variant on the next resume.
#[cfg(any(feature = "maildir", feature = "m2dir"))]
#[derive(Debug)]
pub enum FsStep {
    /// Driver should `read_dir` each path and feed the result back
    /// as [`FsBatch::DirRead`].
    WantsDirRead(BTreeSet<PathBuf>),
    /// Driver should probe each path with `metadata().is_dir()` and
    /// feed the result back as [`FsBatch::DirExists`].
    WantsDirExists(BTreeSet<PathBuf>),
    /// Driver should probe each path with `metadata().is_file()` and
    /// feed the result back as [`FsBatch::FileExists`].
    WantsFileExists(BTreeSet<PathBuf>),
    /// Driver should read each file's bytes and feed the result back
    /// as [`FsBatch::FileRead`].
    WantsFileRead(BTreeSet<PathBuf>),
    /// Driver should write each `(path, bytes)` pair (creating
    /// parent dirs as needed) and ack via [`FsBatch::FileCreate`].
    WantsFileCreate(BTreeMap<PathBuf, Vec<u8>>),
    /// Driver should remove each file and ack via
    /// [`FsBatch::FileRemove`].
    WantsFileRemove(BTreeSet<PathBuf>),
    /// Driver should create each directory (parents included) and
    /// ack via [`FsBatch::DirCreate`].
    WantsDirCreate(BTreeSet<PathBuf>),
    /// Driver should recursively remove each directory and ack via
    /// [`FsBatch::DirRemove`].
    WantsDirRemove(BTreeSet<PathBuf>),
    /// Driver should rename each `(from, to)` pair and ack via
    /// [`FsBatch::Rename`].
    WantsRename(Vec<(PathBuf, PathBuf)>),
    /// Driver should copy each `(from, to)` pair and ack via
    /// [`FsBatch::Copy`]. Maildir only.
    #[cfg(feature = "maildir")]
    WantsCopy(Vec<(PathBuf, PathBuf)>),
    /// Driver should supply the current Unix time and feed back
    /// [`FsBatch::Time`]. Maildir only.
    #[cfg(feature = "maildir")]
    WantsTime,
    /// Driver should supply the current process id and feed back
    /// [`FsBatch::Pid`].
    WantsPid,
    /// Driver should supply the host name and feed back
    /// [`FsBatch::Hostname`]. Maildir only.
    #[cfg(feature = "maildir")]
    WantsHostname,
    /// Driver should supply `len` random bytes and feed back
    /// [`FsBatch::Random`]. m2dir only.
    #[cfg(feature = "m2dir")]
    WantsRandom { len: usize },
}
