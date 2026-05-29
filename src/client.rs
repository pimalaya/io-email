//! Std blocking unified email client.
//!
//! Holds one slot per supported backend; each shared op constructs
//! the matching per-protocol coroutine (e.g.
//! [`crate::imap::mailbox_list::ImapMailboxList`] /
//! [`crate::jmap::mailbox_list::JmapMailboxList`]) and drives it via
//! [`EmailClientStd::run`], the io-email twin of
//! [`io_imap::client::ImapClientStd::run`].
//!
//! Dispatch is static: each coroutine publishes its target backend
//! through the [`EmailCoroutine::BACKEND`] const, and `run` matches on
//! that to pick the per-protocol driver loop (`run_imap`, `run_jmap`,
//! …). The convenience methods (`list_mailboxes`, …) walk the
//! registered slots in priority order and call `run` against the
//! first one that fits.

#[cfg(any(feature = "maildir", feature = "m2dir"))]
use alloc::collections::{BTreeMap, BTreeSet};
#[cfg(feature = "maildir")]
use alloc::string::String;
use alloc::vec::Vec;

#[cfg(any(feature = "imap", feature = "jmap", feature = "smtp"))]
use std::io::{Read, Write};
#[cfg(any(feature = "maildir", feature = "m2dir"))]
use std::{fs, io, path::PathBuf, process};
#[cfg(feature = "maildir")]
use std::{
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};

#[cfg(feature = "imap")]
use io_imap::codec::fragmentizer::Fragmentizer;
#[cfg(feature = "jmap")]
use io_jmap::rfc8620::session::JmapSession;
#[cfg(any(feature = "imap", feature = "jmap", feature = "smtp"))]
use pimalaya_stream::std::stream::StreamStd;
#[cfg(feature = "jmap")]
use secrecy::SecretString;
use thiserror::Error;

#[cfg(any(feature = "maildir", feature = "m2dir"))]
use crate::coroutine::FsBatch;
#[cfg(any(feature = "maildir", feature = "m2dir"))]
use crate::coroutine::FsStep;
#[cfg(feature = "imap")]
use crate::coroutine::ImapStep;
#[cfg(feature = "jmap")]
use crate::coroutine::JmapStep;
#[cfg(feature = "smtp")]
use crate::coroutine::SmtpStep;
#[cfg(all(feature = "imap", feature = "search"))]
use crate::imap::envelope_search::{ImapEnvelopeSearch, ImapEnvelopeSearchError};
#[cfg(feature = "imap")]
use crate::imap::{
    envelope_list::{ImapEnvelopeList, ImapEnvelopeListError},
    flag_store::{ImapFlagStore, ImapFlagStoreError},
    mailbox_create::{ImapMailboxCreate, ImapMailboxCreateError},
    mailbox_delete::{ImapMailboxDelete, ImapMailboxDeleteError},
    mailbox_list::{ImapMailboxList, ImapMailboxListError},
    message_add::{ImapMessageAdd, ImapMessageAddError},
    message_copy::{ImapMessageCopy, ImapMessageCopyError},
    message_delete::{ImapMessageDelete, ImapMessageDeleteError},
    message_get::{ImapMessageGet, ImapMessageGetError},
    message_move::{ImapMessageMove, ImapMessageMoveError},
};
#[cfg(all(feature = "jmap", feature = "search"))]
use crate::jmap::envelope_search::{JmapEnvelopeSearch, JmapEnvelopeSearchError};
#[cfg(feature = "jmap")]
use crate::jmap::{
    envelope_list::{JmapEnvelopeList, JmapEnvelopeListError},
    flag_store::{JmapFlagStore, JmapFlagStoreError},
    mailbox_create::{JmapMailboxCreate, JmapMailboxCreateError},
    mailbox_delete::{JmapMailboxDelete, JmapMailboxDeleteError},
    mailbox_list::{JmapMailboxList, JmapMailboxListError},
    message_add::{JmapMessageAdd, JmapMessageAddError},
    message_copy::{JmapMessageCopy, JmapMessageCopyError},
    message_delete::{JmapMessageDelete, JmapMessageDeleteError},
    message_get::{JmapMessageGet, JmapMessageGetError},
    message_move::{JmapMessageMove, JmapMessageMoveError},
    message_send::{JmapMessageSend, JmapMessageSendError},
};
#[cfg(all(feature = "m2dir", feature = "search"))]
use crate::m2dir::envelope_search::{M2dirEnvelopeSearch, M2dirEnvelopeSearchError};
#[cfg(feature = "m2dir")]
use crate::m2dir::{
    envelope_list::{M2dirEnvelopeList, M2dirEnvelopeListError},
    flag_store::{M2dirFlagStore, M2dirFlagStoreError},
    mailbox_create::{M2dirMailboxCreate, M2dirMailboxCreateError},
    mailbox_delete::{M2dirMailboxDelete, M2dirMailboxDeleteError},
    mailbox_list::{M2dirMailboxList, M2dirMailboxListError},
    message_add::{M2dirMessageAdd, M2dirMessageAddError},
    message_copy::{M2dirMessageCopy, M2dirMessageCopyError},
    message_delete::{M2dirMessageDelete, M2dirMessageDeleteError},
    message_get::{M2dirMessageGet, M2dirMessageGetError},
    message_move::{M2dirMessageMove, M2dirMessageMoveError},
};
#[cfg(all(feature = "maildir", feature = "search"))]
use crate::maildir::envelope_search::{MaildirEnvelopeSearch, MaildirEnvelopeSearchError};
#[cfg(feature = "maildir")]
use crate::maildir::{
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
};
#[cfg(feature = "smtp")]
use crate::smtp::message_send::{SmtpMessageSend, SmtpMessageSendError};
use crate::{
    coroutine::{EmailCoroutine, EmailCoroutineArg, EmailCoroutineState},
    envelope::Envelope,
    flag::{Flag, FlagOp},
    mailbox::Mailbox,
};

#[cfg(any(feature = "imap", feature = "jmap", feature = "smtp"))]
const READ_BUFFER_SIZE: usize = 16 * 1024;
#[cfg(feature = "imap")]
const FRAGMENTIZER_MAX_MESSAGE_SIZE: u32 = 100 * 1024 * 1024;

/// Errors returned by [`EmailClientStd`].
#[derive(Debug, Error)]
pub enum EmailClientStdError {
    #[cfg(feature = "imap")]
    #[error(transparent)]
    ImapMailboxList(#[from] ImapMailboxListError),
    #[cfg(feature = "imap")]
    #[error(transparent)]
    ImapMailboxCreate(#[from] ImapMailboxCreateError),
    #[cfg(feature = "imap")]
    #[error(transparent)]
    ImapMailboxDelete(#[from] ImapMailboxDeleteError),
    #[cfg(feature = "imap")]
    #[error(transparent)]
    ImapEnvelopeList(#[from] ImapEnvelopeListError),
    #[cfg(feature = "imap")]
    #[error(transparent)]
    ImapFlagStore(#[from] ImapFlagStoreError),
    #[cfg(feature = "imap")]
    #[error(transparent)]
    ImapMessageGet(#[from] ImapMessageGetError),
    #[cfg(feature = "imap")]
    #[error(transparent)]
    ImapMessageAdd(#[from] ImapMessageAddError),
    #[cfg(feature = "imap")]
    #[error(transparent)]
    ImapMessageDelete(#[from] ImapMessageDeleteError),
    #[cfg(feature = "imap")]
    #[error(transparent)]
    ImapMessageCopy(#[from] ImapMessageCopyError),
    #[cfg(feature = "imap")]
    #[error(transparent)]
    ImapMessageMove(#[from] ImapMessageMoveError),
    #[cfg(feature = "imap")]
    #[error(transparent)]
    ImapWatchMailbox(#[from] crate::imap::watch_mailbox::ImapWatchMailboxError),
    #[cfg(all(feature = "imap", feature = "search"))]
    #[error(transparent)]
    ImapEnvelopeSearch(#[from] ImapEnvelopeSearchError),

    #[cfg(feature = "jmap")]
    #[error(transparent)]
    JmapMailboxList(#[from] JmapMailboxListError),
    #[cfg(feature = "jmap")]
    #[error(transparent)]
    JmapMailboxCreate(#[from] JmapMailboxCreateError),
    #[cfg(feature = "jmap")]
    #[error(transparent)]
    JmapMailboxDelete(#[from] JmapMailboxDeleteError),
    #[cfg(feature = "jmap")]
    #[error(transparent)]
    JmapEnvelopeList(#[from] JmapEnvelopeListError),
    #[cfg(feature = "jmap")]
    #[error(transparent)]
    JmapFlagStore(#[from] JmapFlagStoreError),
    #[cfg(feature = "jmap")]
    #[error(transparent)]
    JmapMessageGet(#[from] JmapMessageGetError),
    #[cfg(feature = "jmap")]
    #[error(transparent)]
    JmapMessageAdd(#[from] JmapMessageAddError),
    #[cfg(feature = "jmap")]
    #[error(transparent)]
    JmapMessageDelete(#[from] JmapMessageDeleteError),
    #[cfg(feature = "jmap")]
    #[error(transparent)]
    JmapMessageCopy(#[from] JmapMessageCopyError),
    #[cfg(feature = "jmap")]
    #[error(transparent)]
    JmapMessageMove(#[from] JmapMessageMoveError),
    #[cfg(feature = "jmap")]
    #[error(transparent)]
    JmapMessageSend(#[from] JmapMessageSendError),
    #[cfg(feature = "jmap")]
    #[error(transparent)]
    JmapWatchMailbox(#[from] crate::jmap::watch_mailbox::JmapWatchMailboxError),
    #[cfg(all(feature = "jmap", feature = "search"))]
    #[error(transparent)]
    JmapEnvelopeSearch(#[from] JmapEnvelopeSearchError),

    #[cfg(feature = "smtp")]
    #[error(transparent)]
    SmtpMessageSend(#[from] SmtpMessageSendError),

    /// JMAP backend is registered but no identity / drafts ids were
    /// configured on the [`JmapContext`], so send_message cannot
    /// dispatch.
    #[cfg(feature = "jmap")]
    #[error("JMAP context is missing the identity_id or drafts_mailbox_id required for send")]
    JmapMissingSendConfig,

    #[cfg(feature = "maildir")]
    #[error(transparent)]
    MaildirMailboxList(#[from] MaildirMailboxListError),
    #[cfg(feature = "maildir")]
    #[error(transparent)]
    MaildirMailboxCreate(#[from] MaildirMailboxCreateError),
    #[cfg(feature = "maildir")]
    #[error(transparent)]
    MaildirMailboxDelete(#[from] MaildirMailboxDeleteError),
    #[cfg(feature = "maildir")]
    #[error(transparent)]
    MaildirEnvelopeList(#[from] MaildirEnvelopeListError),
    #[cfg(feature = "maildir")]
    #[error(transparent)]
    MaildirFlagStore(#[from] MaildirFlagStoreError),
    #[cfg(feature = "maildir")]
    #[error(transparent)]
    MaildirMessageGet(#[from] MaildirMessageGetError),
    #[cfg(feature = "maildir")]
    #[error(transparent)]
    MaildirMessageAdd(#[from] MaildirMessageAddError),
    #[cfg(feature = "maildir")]
    #[error(transparent)]
    MaildirMessageDelete(#[from] MaildirMessageDeleteError),
    #[cfg(feature = "maildir")]
    #[error(transparent)]
    MaildirMessageCopy(#[from] MaildirMessageCopyError),
    #[cfg(feature = "maildir")]
    #[error(transparent)]
    MaildirMessageMove(#[from] MaildirMessageMoveError),
    #[cfg(all(feature = "maildir", feature = "search"))]
    #[error(transparent)]
    MaildirEnvelopeSearch(#[from] MaildirEnvelopeSearchError),

    #[cfg(feature = "m2dir")]
    #[error(transparent)]
    M2dirMailboxList(#[from] M2dirMailboxListError),
    #[cfg(feature = "m2dir")]
    #[error(transparent)]
    M2dirMailboxCreate(#[from] M2dirMailboxCreateError),
    #[cfg(feature = "m2dir")]
    #[error(transparent)]
    M2dirMailboxDelete(#[from] M2dirMailboxDeleteError),
    #[cfg(feature = "m2dir")]
    #[error(transparent)]
    M2dirEnvelopeList(#[from] M2dirEnvelopeListError),
    #[cfg(feature = "m2dir")]
    #[error(transparent)]
    M2dirFlagStore(#[from] M2dirFlagStoreError),
    #[cfg(feature = "m2dir")]
    #[error(transparent)]
    M2dirMessageGet(#[from] M2dirMessageGetError),
    #[cfg(feature = "m2dir")]
    #[error(transparent)]
    M2dirMessageAdd(#[from] M2dirMessageAddError),
    #[cfg(feature = "m2dir")]
    #[error(transparent)]
    M2dirMessageDelete(#[from] M2dirMessageDeleteError),
    #[cfg(feature = "m2dir")]
    #[error(transparent)]
    M2dirMessageCopy(#[from] M2dirMessageCopyError),
    #[cfg(feature = "m2dir")]
    #[error(transparent)]
    M2dirMessageMove(#[from] M2dirMessageMoveError),
    #[cfg(all(feature = "m2dir", feature = "search"))]
    #[error(transparent)]
    M2dirEnvelopeSearch(#[from] M2dirEnvelopeSearchError),

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error("no registered backend supports this operation")]
    UnsupportedOperation,
}

/// Per-connection IMAP state owned by the std client.
///
/// `auto_select` is a policy flag the per-message coroutines read at
/// construction time: when set (default), they self-SELECT the target
/// mailbox before issuing STORE / FETCH / COPY / MOVE / EXPUNGE.
/// Sync engines flip it off and pre-select once per mailbox batch to
/// avoid a SELECT per hunk.
///
/// `capabilities` is the live capability list discovered at login
/// (`CAPABILITY` / `LOGIN` / `AUTHENTICATE` with
/// `ensure_capabilities`). Required for
/// [`EmailClientStd::watch_mailbox`] (the IMAP watcher needs
/// `QRESYNC`); other ops ignore it for now.
#[cfg(feature = "imap")]
pub struct ImapContext {
    pub stream: StreamStd,
    pub fragmentizer: Fragmentizer,
    pub auto_select: bool,
    pub capabilities: alloc::vec::Vec<io_imap::types::response::Capability<'static>>,
}

#[cfg(feature = "imap")]
impl ImapContext {
    /// Wraps an already-connected stream with a fresh fragmentizer,
    /// the default `auto_select = true` policy, and an empty
    /// capability list. Populate `capabilities` after login when
    /// `watch_envelopes` is in scope.
    pub fn new(stream: StreamStd) -> Self {
        Self {
            stream,
            fragmentizer: Fragmentizer::new(FRAGMENTIZER_MAX_MESSAGE_SIZE),
            auto_select: true,
            capabilities: alloc::vec::Vec::new(),
        }
    }
}

/// Per-connection JMAP state owned by the std client: an
/// already-connected HTTP stream, the bearer / basic credential, and
/// the discovered session object.
///
/// `identity_id` and `drafts_mailbox_id` are required for
/// [`EmailClientStd::send_message`] only; populate them via JMAP
/// `Identity/get` (`type=role:identity`) and `Mailbox/query`
/// (`role: drafts`) at session bootstrap when sending is in scope.
#[cfg(feature = "jmap")]
pub struct JmapContext {
    pub stream: StreamStd,
    pub http_auth: SecretString,
    pub session: JmapSession,
    pub identity_id: Option<alloc::string::String>,
    pub drafts_mailbox_id: Option<alloc::string::String>,
}

#[cfg(feature = "jmap")]
impl JmapContext {
    /// Wraps an already-connected stream paired with the bearer /
    /// basic credential and the discovered [`JmapSession`]. Identity
    /// and drafts ids default to `None`; callers that intend to send
    /// must populate them before calling
    /// [`EmailClientStd::send_message`].
    pub fn new(stream: StreamStd, http_auth: SecretString, session: JmapSession) -> Self {
        Self {
            stream,
            http_auth,
            session,
            identity_id: None,
            drafts_mailbox_id: None,
        }
    }
}

/// Per-connection SMTP state owned by the std client: an already
/// authenticated stream.
///
/// `default_reverse_path` is an optional override for the SMTP
/// `MAIL FROM` envelope: when set, [`EmailClientStd::send_message`]
/// ignores the message's `From:` header and uses this address
/// instead. Required when the configured account uses an alias whose
/// header sender differs from the SMTP envelope sender (DKIM-aligned
/// gateways, bounce-address rewriting).
#[cfg(feature = "smtp")]
pub struct SmtpContext {
    pub stream: StreamStd,
    pub default_reverse_path: Option<alloc::string::String>,
}

#[cfg(feature = "smtp")]
impl SmtpContext {
    /// Wraps an already-authenticated SMTP stream with no envelope
    /// override.
    pub fn new(stream: StreamStd) -> Self {
        Self {
            stream,
            default_reverse_path: None,
        }
    }
}

/// Per-mailbox-tree Maildir state owned by the std client: the root
/// directory plus the Maildir++ flag (when set, dotted siblings and
/// the root itself count as mailboxes).
///
/// `root` is a [`PathBuf`] so the std client speaks the standard path
/// type; the coroutine converts to [`io_maildir::path::MaildirPath`]
/// at construction time.
#[cfg(feature = "maildir")]
pub struct MaildirContext {
    pub root: PathBuf,
    pub maildir_plus: bool,
}

#[cfg(feature = "maildir")]
impl MaildirContext {
    /// Wraps a Maildir root. `maildir_plus = false` is the classic
    /// "children only, no dotted entries" layout; flip it on for
    /// Maildir++ where the root itself is the inbox and folders are
    /// `.Dotted.Sibling` directories.
    pub fn new(root: impl Into<PathBuf>, maildir_plus: bool) -> Self {
        Self {
            root: root.into(),
            maildir_plus,
        }
    }
}

/// Per-m2store m2dir state owned by the std client: the m2store root
/// directory.
///
/// `root` is a [`PathBuf`] so the std client speaks the standard path
/// type; the coroutine converts to [`io_m2dir::path::M2dirPath`] at
/// construction time.
#[cfg(feature = "m2dir")]
pub struct M2dirContext {
    pub root: PathBuf,
}

#[cfg(feature = "m2dir")]
impl M2dirContext {
    /// Wraps an m2store root.
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }
}

/// Std-blocking unified email client.
#[derive(Default)]
pub struct EmailClientStd {
    #[cfg(feature = "imap")]
    imap: Option<ImapContext>,
    #[cfg(feature = "jmap")]
    jmap: Option<JmapContext>,
    #[cfg(feature = "maildir")]
    maildir: Option<MaildirContext>,
    #[cfg(feature = "m2dir")]
    m2dir: Option<M2dirContext>,
    #[cfg(feature = "smtp")]
    smtp: Option<SmtpContext>,
}

impl EmailClientStd {
    /// Creates an empty client with no backend registered.
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers the IMAP backend.
    #[cfg(feature = "imap")]
    pub fn with_imap(mut self, ctx: ImapContext) -> Self {
        self.imap = Some(ctx);
        self
    }

    /// Registers the JMAP backend.
    #[cfg(feature = "jmap")]
    pub fn with_jmap(mut self, ctx: JmapContext) -> Self {
        self.jmap = Some(ctx);
        self
    }

    /// Registers the Maildir backend.
    #[cfg(feature = "maildir")]
    pub fn with_maildir(mut self, ctx: MaildirContext) -> Self {
        self.maildir = Some(ctx);
        self
    }

    /// Registers the m2dir backend.
    #[cfg(feature = "m2dir")]
    pub fn with_m2dir(mut self, ctx: M2dirContext) -> Self {
        self.m2dir = Some(ctx);
        self
    }

    /// Registers the SMTP backend. The stream must already be
    /// authenticated (EHLO + STARTTLS + AUTH) before this point;
    /// `send_message` only runs the mail transaction (MAIL FROM →
    /// RCPT TO → DATA).
    #[cfg(feature = "smtp")]
    pub fn with_smtp(mut self, ctx: SmtpContext) -> Self {
        self.smtp = Some(ctx);
        self
    }

    /// Drives an IMAP coroutine against the registered
    /// [`ImapContext`] until it terminates.
    ///
    /// Generic over the coroutine's `Return = Result<O, E>` so any
    /// per-coroutine output type round-trips cleanly into
    /// [`EmailClientStdError`] via the matching `#[from]` arm. The
    /// missing-context case short-circuits to
    /// [`EmailClientStdError::UnsupportedOperation`] instead of
    /// panicking.
    #[cfg(feature = "imap")]
    pub fn run_imap<C, O, E>(&mut self, coroutine: C) -> Result<O, EmailClientStdError>
    where
        C: EmailCoroutine<Yield = ImapStep, Return = Result<O, E>>,
        EmailClientStdError: From<E>,
    {
        let Some(ctx) = self.imap.as_mut() else {
            return Err(EmailClientStdError::UnsupportedOperation);
        };
        run_imap(ctx, coroutine)
    }

    /// Drives a JMAP coroutine against the registered [`JmapContext`].
    #[cfg(feature = "jmap")]
    pub fn run_jmap<C, O, E>(&mut self, coroutine: C) -> Result<O, EmailClientStdError>
    where
        C: EmailCoroutine<Yield = JmapStep, Return = Result<O, E>>,
        EmailClientStdError: From<E>,
    {
        let Some(ctx) = self.jmap.as_mut() else {
            return Err(EmailClientStdError::UnsupportedOperation);
        };
        run_jmap(ctx, coroutine)
    }

    /// Drives a filesystem-backed coroutine (Maildir or m2dir). The
    /// driver owns the `std::fs::*` calls; the coroutine carries its
    /// own root path internally so no per-backend context state is
    /// needed here.
    #[cfg(any(feature = "maildir", feature = "m2dir"))]
    pub fn run_fs<C, O, E>(&mut self, coroutine: C) -> Result<O, EmailClientStdError>
    where
        C: EmailCoroutine<Yield = FsStep, Return = Result<O, E>>,
        EmailClientStdError: From<E>,
    {
        let _ = self;
        run_fs(coroutine)
    }

    /// Drives an SMTP coroutine against the registered
    /// [`SmtpContext`].
    #[cfg(feature = "smtp")]
    pub fn run_smtp<C, O, E>(&mut self, coroutine: C) -> Result<O, EmailClientStdError>
    where
        C: EmailCoroutine<Yield = SmtpStep, Return = Result<O, E>>,
        EmailClientStdError: From<E>,
    {
        let Some(ctx) = self.smtp.as_mut() else {
            return Err(EmailClientStdError::UnsupportedOperation);
        };
        run_smtp(ctx, coroutine)
    }

    /// Lists every mailbox available to the active account.
    ///
    /// Priority order is fixed for now: Maildir, then JMAP, then IMAP.
    /// Direct callers who need a non-default backend can build the
    /// coroutine themselves and call [`Self::run`].
    #[allow(unused_variables)]
    pub fn list_mailboxes(
        &mut self,
        with_counts: bool,
    ) -> Result<Vec<Mailbox>, EmailClientStdError> {
        #[cfg(feature = "maildir")]
        if let Some(maildir) = self.maildir.as_ref() {
            let root = maildir.root.clone();
            let maildir_plus = maildir.maildir_plus;
            return self.run_fs(MaildirMailboxList::new(root, maildir_plus, with_counts));
        }

        #[cfg(feature = "m2dir")]
        if let Some(m2dir) = self.m2dir.as_ref() {
            return self.run_fs(M2dirMailboxList::new(m2dir.root.clone(), with_counts));
        }

        #[cfg(feature = "jmap")]
        if let Some(jmap) = self.jmap.as_ref() {
            return self.run_jmap(JmapMailboxList::new(
                &jmap.session,
                &jmap.http_auth,
                with_counts,
            )?);
        }

        #[cfg(feature = "imap")]
        if self.imap.is_some() {
            return self.run_imap(ImapMailboxList::new(with_counts));
        }

        Err(EmailClientStdError::UnsupportedOperation)
    }

    /// Lists envelopes from `mailbox`.
    ///
    /// `page` is 1-indexed (defaults to 1); `page_size = None` returns
    /// the whole mailbox; `with_attachment` populates
    /// [`Envelope::has_attachment`] when the backend can answer
    /// cheaply. JMAP and Maildir ignore `with_attachment` today:
    /// the former returns the property unconditionally, the latter
    /// always parses bodies on Maildir.
    ///
    /// Priority order: Maildir → m2dir → JMAP → IMAP (same as
    /// [`Self::list_mailboxes`]).
    #[allow(unused_variables)]
    pub fn list_envelopes(
        &mut self,
        mailbox: &str,
        page: Option<u32>,
        page_size: Option<u32>,
        with_attachment: bool,
    ) -> Result<Vec<Envelope>, EmailClientStdError> {
        #[cfg(feature = "maildir")]
        if let Some(maildir) = self.maildir.as_ref() {
            let root = maildir.root.clone();
            let maildir_plus = maildir.maildir_plus;
            return self.run_fs(MaildirEnvelopeList::new(
                root,
                maildir_plus,
                mailbox,
                page,
                page_size,
            )?);
        }
        #[cfg(feature = "m2dir")]
        if let Some(m2dir) = self.m2dir.as_ref() {
            let root = m2dir.root.clone();
            return self.run_fs(M2dirEnvelopeList::new(
                root,
                mailbox,
                page,
                page_size,
                with_attachment,
            )?);
        }
        #[cfg(feature = "jmap")]
        if let Some(jmap) = self.jmap.as_ref() {
            return self.run_jmap(JmapEnvelopeList::new(
                &jmap.session,
                &jmap.http_auth,
                mailbox,
                page,
                page_size,
            )?);
        }
        #[cfg(feature = "imap")]
        if self.imap.is_some() {
            return self.run_imap(ImapEnvelopeList::new(
                mailbox,
                page,
                page_size,
                with_attachment,
            )?);
        }
        Err(EmailClientStdError::UnsupportedOperation)
    }

    /// Searches envelopes in `mailbox` matching the parsed
    /// [`SearchEmailsQuery`].
    ///
    /// `page` is 1-indexed (defaults to 1); `page_size = None` returns
    /// the whole result set. `query = None` is equivalent to
    /// "all envelopes, date descending" and behaves like
    /// [`Self::list_envelopes`] without `with_attachment`.
    ///
    /// Priority order: Maildir → m2dir → JMAP → IMAP (same as
    /// [`Self::list_envelopes`]). Per-backend nuances:
    ///
    /// - IMAP requires the `SORT` capability (RFC 5256); without it,
    ///   the server returns `BAD` and the error surfaces as
    ///   `ImapEnvelopeSearchError::Sort`.
    /// - JMAP's `before` / `after` filter primitives are anchored to
    ///   `receivedAt`, while the DSL targets `sentAt`; the JMAP
    ///   coroutine over-approximates server-side then re-applies the
    ///   exact `sentAt` predicate client-side.
    /// - Maildir / m2dir evaluate the filter entirely client-side
    ///   against parsed envelopes; `body <pattern>` reuses the same
    ///   in-memory bytes loaded for header parsing.
    #[cfg(feature = "search")]
    #[cfg_attr(
        not(any(
            feature = "imap",
            feature = "jmap",
            feature = "maildir",
            feature = "m2dir"
        )),
        allow(unused_variables)
    )]
    pub fn search_envelopes(
        &mut self,
        mailbox: &str,
        query: Option<&crate::search::query::SearchEmailsQuery>,
        page: Option<u32>,
        page_size: Option<u32>,
    ) -> Result<Vec<Envelope>, EmailClientStdError> {
        #[cfg(feature = "maildir")]
        if let Some(maildir) = self.maildir.as_ref() {
            let root = maildir.root.clone();
            let maildir_plus = maildir.maildir_plus;
            return self.run_fs(MaildirEnvelopeSearch::new(
                root,
                maildir_plus,
                mailbox,
                query,
                page,
                page_size,
            )?);
        }
        #[cfg(feature = "m2dir")]
        if let Some(m2dir) = self.m2dir.as_ref() {
            let root = m2dir.root.clone();
            return self.run_fs(M2dirEnvelopeSearch::new(
                root, mailbox, query, page, page_size, false,
            )?);
        }
        #[cfg(feature = "jmap")]
        if let Some(jmap) = self.jmap.as_ref() {
            return self.run_jmap(JmapEnvelopeSearch::new(
                &jmap.session,
                &jmap.http_auth,
                mailbox,
                query,
                page,
                page_size,
            )?);
        }
        #[cfg(feature = "imap")]
        if self.imap.is_some() {
            return self.run_imap(ImapEnvelopeSearch::new(
                mailbox, query, page, page_size, false,
            )?);
        }
        Err(EmailClientStdError::UnsupportedOperation)
    }

    /// Adds the given flags to every message in `ids` inside `mailbox`,
    /// preserving any flag already set.
    pub fn add_flags(
        &mut self,
        mailbox: &str,
        ids: &[&str],
        flags: &[Flag],
    ) -> Result<(), EmailClientStdError> {
        self.store_flags(mailbox, ids, flags, FlagOp::Add)
    }

    /// Replaces the flag set of every message in `ids` inside `mailbox`
    /// with `flags` exactly.
    pub fn set_flags(
        &mut self,
        mailbox: &str,
        ids: &[&str],
        flags: &[Flag],
    ) -> Result<(), EmailClientStdError> {
        self.store_flags(mailbox, ids, flags, FlagOp::Set)
    }

    /// Removes the given flags from every message in `ids` inside
    /// `mailbox`.
    pub fn delete_flags(
        &mut self,
        mailbox: &str,
        ids: &[&str],
        flags: &[Flag],
    ) -> Result<(), EmailClientStdError> {
        self.store_flags(mailbox, ids, flags, FlagOp::Remove)
    }

    #[cfg_attr(
        not(any(
            feature = "imap",
            feature = "jmap",
            feature = "maildir",
            feature = "m2dir"
        )),
        allow(unused_variables)
    )]
    fn store_flags(
        &mut self,
        mailbox: &str,
        ids: &[&str],
        flags: &[Flag],
        op: FlagOp,
    ) -> Result<(), EmailClientStdError> {
        #[cfg(feature = "maildir")]
        if let Some(maildir) = self.maildir.as_ref() {
            let root = maildir.root.clone();
            let maildir_plus = maildir.maildir_plus;
            return self.run_fs(MaildirFlagStore::new(
                root,
                maildir_plus,
                mailbox,
                ids,
                flags,
                op,
            )?);
        }
        #[cfg(feature = "m2dir")]
        if let Some(m2dir) = self.m2dir.as_ref() {
            let root = m2dir.root.clone();
            return self.run_fs(M2dirFlagStore::new(root, mailbox, ids, flags, op)?);
        }
        #[cfg(feature = "jmap")]
        if let Some(jmap) = self.jmap.as_ref() {
            return self.run_jmap(JmapFlagStore::new(
                &jmap.session,
                &jmap.http_auth,
                mailbox,
                ids,
                flags,
                op,
            )?);
        }
        #[cfg(feature = "imap")]
        if let Some(auto_select) = self.imap.as_ref().map(|c| c.auto_select) {
            return self.run_imap(ImapFlagStore::new(mailbox, ids, flags, op, auto_select)?);
        }
        Err(EmailClientStdError::UnsupportedOperation)
    }

    /// Fetches the raw RFC 5322 bytes of message `id` from `mailbox`,
    /// leaving the \Seen flag untouched.
    #[cfg_attr(
        not(any(
            feature = "imap",
            feature = "jmap",
            feature = "maildir",
            feature = "m2dir"
        )),
        allow(unused_variables)
    )]
    pub fn get_message(&mut self, mailbox: &str, id: &str) -> Result<Vec<u8>, EmailClientStdError> {
        #[cfg(feature = "maildir")]
        if let Some(maildir) = self.maildir.as_ref() {
            let root = maildir.root.clone();
            let maildir_plus = maildir.maildir_plus;
            return self.run_fs(MaildirMessageGet::new(root, maildir_plus, mailbox, id)?);
        }
        #[cfg(feature = "m2dir")]
        if let Some(m2dir) = self.m2dir.as_ref() {
            let root = m2dir.root.clone();
            return self.run_fs(M2dirMessageGet::new(root, mailbox, id)?);
        }
        #[cfg(feature = "jmap")]
        if let Some(jmap) = self.jmap.as_ref() {
            return self.run_jmap(JmapMessageGet::new(
                &jmap.session,
                &jmap.http_auth,
                mailbox,
                id,
            )?);
        }
        #[cfg(feature = "imap")]
        if let Some(auto_select) = self.imap.as_ref().map(|c| c.auto_select) {
            return self.run_imap(ImapMessageGet::new(mailbox, id, auto_select)?);
        }
        Err(EmailClientStdError::UnsupportedOperation)
    }

    /// Appends a raw RFC 5322 message to `mailbox`, tagged with the
    /// given `flags`. Returns the backend-assigned id.
    #[cfg_attr(
        not(any(
            feature = "imap",
            feature = "jmap",
            feature = "maildir",
            feature = "m2dir"
        )),
        allow(unused_variables)
    )]
    pub fn add_message(
        &mut self,
        mailbox: &str,
        flags: &[Flag],
        raw: Vec<u8>,
    ) -> Result<alloc::string::String, EmailClientStdError> {
        #[cfg(feature = "maildir")]
        if let Some(maildir) = self.maildir.as_ref() {
            let root = maildir.root.clone();
            let maildir_plus = maildir.maildir_plus;
            return self.run_fs(MaildirMessageAdd::new(
                root,
                maildir_plus,
                mailbox,
                flags,
                raw,
            )?);
        }
        #[cfg(feature = "m2dir")]
        if let Some(m2dir) = self.m2dir.as_ref() {
            let root = m2dir.root.clone();
            return self.run_fs(M2dirMessageAdd::new(root, mailbox, flags, raw)?);
        }
        #[cfg(feature = "jmap")]
        if let Some(jmap) = self.jmap.as_ref() {
            return self.run_jmap(JmapMessageAdd::new(
                &jmap.session,
                &jmap.http_auth,
                mailbox,
                flags,
                raw,
            )?);
        }
        #[cfg(feature = "imap")]
        if self.imap.is_some() {
            return self.run_imap(ImapMessageAdd::new(mailbox, flags, raw)?);
        }
        Err(EmailClientStdError::UnsupportedOperation)
    }

    /// Creates a mailbox named `name` (backend-specific layout).
    #[cfg_attr(
        not(any(
            feature = "imap",
            feature = "jmap",
            feature = "maildir",
            feature = "m2dir"
        )),
        allow(unused_variables)
    )]
    pub fn create_mailbox(&mut self, name: &str) -> Result<(), EmailClientStdError> {
        #[cfg(feature = "maildir")]
        if let Some(maildir) = self.maildir.as_ref() {
            let root = maildir.root.clone();
            let maildir_plus = maildir.maildir_plus;
            return self.run_fs(MaildirMailboxCreate::new(root, maildir_plus, name)?);
        }
        #[cfg(feature = "m2dir")]
        if let Some(m2dir) = self.m2dir.as_ref() {
            let root = m2dir.root.clone();
            return self.run_fs(M2dirMailboxCreate::new(root, name)?);
        }
        #[cfg(feature = "jmap")]
        if let Some(jmap) = self.jmap.as_ref() {
            return self.run_jmap(JmapMailboxCreate::new(
                &jmap.session,
                &jmap.http_auth,
                name,
            )?);
        }
        #[cfg(feature = "imap")]
        if self.imap.is_some() {
            return self.run_imap(ImapMailboxCreate::new(name)?);
        }
        Err(EmailClientStdError::UnsupportedOperation)
    }

    /// Deletes the mailbox named `name` and every message inside it.
    #[cfg_attr(
        not(any(
            feature = "imap",
            feature = "jmap",
            feature = "maildir",
            feature = "m2dir"
        )),
        allow(unused_variables)
    )]
    pub fn delete_mailbox(&mut self, name: &str) -> Result<(), EmailClientStdError> {
        #[cfg(feature = "maildir")]
        if let Some(maildir) = self.maildir.as_ref() {
            let root = maildir.root.clone();
            let maildir_plus = maildir.maildir_plus;
            return self.run_fs(MaildirMailboxDelete::new(root, maildir_plus, name)?);
        }
        #[cfg(feature = "m2dir")]
        if let Some(m2dir) = self.m2dir.as_ref() {
            let root = m2dir.root.clone();
            return self.run_fs(M2dirMailboxDelete::new(root, name)?);
        }
        #[cfg(feature = "jmap")]
        if let Some(jmap) = self.jmap.as_ref() {
            return self.run_jmap(JmapMailboxDelete::new(
                &jmap.session,
                &jmap.http_auth,
                name,
            )?);
        }
        #[cfg(feature = "imap")]
        if self.imap.is_some() {
            return self.run_imap(ImapMailboxDelete::new(name)?);
        }
        Err(EmailClientStdError::UnsupportedOperation)
    }

    /// Permanently deletes message `id` from `mailbox`.
    #[cfg_attr(
        not(any(
            feature = "imap",
            feature = "jmap",
            feature = "maildir",
            feature = "m2dir"
        )),
        allow(unused_variables)
    )]
    pub fn delete_message(&mut self, mailbox: &str, id: &str) -> Result<(), EmailClientStdError> {
        #[cfg(feature = "maildir")]
        if let Some(maildir) = self.maildir.as_ref() {
            let root = maildir.root.clone();
            let maildir_plus = maildir.maildir_plus;
            return self.run_fs(MaildirMessageDelete::new(root, maildir_plus, mailbox, id)?);
        }
        #[cfg(feature = "m2dir")]
        if let Some(m2dir) = self.m2dir.as_ref() {
            let root = m2dir.root.clone();
            return self.run_fs(M2dirMessageDelete::new(root, mailbox, id)?);
        }
        #[cfg(feature = "jmap")]
        if let Some(jmap) = self.jmap.as_ref() {
            return self.run_jmap(JmapMessageDelete::new(
                &jmap.session,
                &jmap.http_auth,
                mailbox,
                id,
            )?);
        }
        #[cfg(feature = "imap")]
        if let Some(auto_select) = self.imap.as_ref().map(|c| c.auto_select) {
            return self.run_imap(ImapMessageDelete::new(mailbox, id, auto_select)?);
        }
        Err(EmailClientStdError::UnsupportedOperation)
    }

    /// Copies every message in `ids` from `from` to `to`, leaving the
    /// originals in place.
    #[cfg_attr(
        not(any(
            feature = "imap",
            feature = "jmap",
            feature = "maildir",
            feature = "m2dir"
        )),
        allow(unused_variables)
    )]
    pub fn copy_messages(
        &mut self,
        from: &str,
        to: &str,
        ids: &[&str],
    ) -> Result<(), EmailClientStdError> {
        #[cfg(feature = "maildir")]
        if let Some(maildir) = self.maildir.as_ref() {
            let root = maildir.root.clone();
            let maildir_plus = maildir.maildir_plus;
            return self.run_fs(MaildirMessageCopy::new(root, maildir_plus, from, to, ids)?);
        }
        #[cfg(feature = "m2dir")]
        if let Some(m2dir) = self.m2dir.as_ref() {
            let root = m2dir.root.clone();
            return self.run_fs(M2dirMessageCopy::new(root, from, to, ids)?);
        }
        #[cfg(feature = "jmap")]
        if let Some(jmap) = self.jmap.as_ref() {
            return self.run_jmap(JmapMessageCopy::new(
                &jmap.session,
                &jmap.http_auth,
                from,
                to,
                ids,
            )?);
        }
        #[cfg(feature = "imap")]
        if let Some(auto_select) = self.imap.as_ref().map(|c| c.auto_select) {
            return self.run_imap(ImapMessageCopy::new(from, to, ids, auto_select)?);
        }
        Err(EmailClientStdError::UnsupportedOperation)
    }

    /// Moves every message in `ids` from `from` to `to`; the originals
    /// are removed from `from`.
    #[cfg_attr(
        not(any(
            feature = "imap",
            feature = "jmap",
            feature = "maildir",
            feature = "m2dir"
        )),
        allow(unused_variables)
    )]
    pub fn move_messages(
        &mut self,
        from: &str,
        to: &str,
        ids: &[&str],
    ) -> Result<(), EmailClientStdError> {
        #[cfg(feature = "maildir")]
        if let Some(maildir) = self.maildir.as_ref() {
            let root = maildir.root.clone();
            let maildir_plus = maildir.maildir_plus;
            return self.run_fs(MaildirMessageMove::new(root, maildir_plus, from, to, ids)?);
        }
        #[cfg(feature = "m2dir")]
        if let Some(m2dir) = self.m2dir.as_ref() {
            let root = m2dir.root.clone();
            return self.run_fs(M2dirMessageMove::new(root, from, to, ids)?);
        }
        #[cfg(feature = "jmap")]
        if let Some(jmap) = self.jmap.as_ref() {
            return self.run_jmap(JmapMessageMove::new(
                &jmap.session,
                &jmap.http_auth,
                from,
                to,
                ids,
            )?);
        }
        #[cfg(feature = "imap")]
        if let Some(auto_select) = self.imap.as_ref().map(|c| c.auto_select) {
            return self.run_imap(ImapMessageMove::new(from, to, ids, auto_select)?);
        }
        Err(EmailClientStdError::UnsupportedOperation)
    }

    /// Sends a raw RFC 5322 message.
    ///
    /// Priority order: SMTP → JMAP. SMTP runs the mail transaction
    /// on the registered [`SmtpContext`] stream (envelope sender
    /// derived from `From:` or [`SmtpContext::default_reverse_path`];
    /// recipients from `To:` + `Cc:` + `Bcc:`). JMAP uploads the
    /// blob, imports it into the drafts mailbox, and submits it
    /// under the configured identity — both
    /// [`JmapContext::identity_id`] and
    /// [`JmapContext::drafts_mailbox_id`] must be set.
    #[cfg_attr(not(any(feature = "smtp", feature = "jmap")), allow(unused_variables))]
    pub fn send_message(&mut self, raw: Vec<u8>) -> Result<(), EmailClientStdError> {
        #[cfg(feature = "smtp")]
        if let Some(smtp) = self.smtp.as_ref() {
            let override_path = smtp.default_reverse_path.clone();
            return self.run_smtp(SmtpMessageSend::new(raw, override_path.as_deref())?);
        }
        #[cfg(feature = "jmap")]
        if let Some(jmap) = self.jmap.as_ref() {
            let (Some(identity_id), Some(drafts_id)) =
                (jmap.identity_id.clone(), jmap.drafts_mailbox_id.clone())
            else {
                return Err(EmailClientStdError::JmapMissingSendConfig);
            };
            return self.run_jmap(JmapMessageSend::new(
                &jmap.session,
                &jmap.http_auth,
                &identity_id,
                &drafts_id,
                raw,
            )?);
        }
        Err(EmailClientStdError::UnsupportedOperation)
    }

    /// Watches `mailbox` for envelope-level deltas, forwarding every
    /// event through the caller-supplied [`Sender`].
    ///
    /// This call **blocks** the current thread: it drives the inner
    /// generator-shape coroutine ([`crate::coroutine`]) in a loop,
    /// fans IMAP socket reads / writes onto the registered
    /// [`ImapContext`] stream, and pushes each yielded
    /// [`crate::event::WatchEvent`] into `tx`. Returns when either
    /// `shutdown` flips (cooperative; the inner watcher winds the
    /// IDLE down at the next loop tick), the receiver behind `tx` is
    /// dropped (the caller signalled stop), or the protocol layer
    /// errors out.
    ///
    /// The threading model is the caller's choice: spawn a dedicated
    /// `std::thread` if you want async-like consumption, or block
    /// in-place from a main loop. Pair `tx` / `rx` from
    /// [`std::sync::mpsc::channel`] (or [`sync_channel`]) for
    /// backpressure control.
    ///
    /// Priority order: IMAP → JMAP. IMAP requires
    /// [`ImapContext::capabilities`](Self) to advertise `QRESYNC`;
    /// JMAP uses an HTTP/1.1 single-connection cycle (subscribe via
    /// EventSource `closeafter=state`, drain `Email/changes` +
    /// `Email/get`, resubscribe — the JMAP analog of IMAP IDLE).
    /// Maildir / m2dir watchers land later under the same signature.
    ///
    /// [`Sender`]: std::sync::mpsc::Sender
    /// [`sync_channel`]: std::sync::mpsc::sync_channel
    #[cfg(any(feature = "imap", feature = "jmap"))]
    pub fn watch_mailbox(
        &mut self,
        mailbox: &str,
        shutdown: alloc::sync::Arc<core::sync::atomic::AtomicBool>,
        tx: std::sync::mpsc::Sender<crate::event::WatchEvent>,
    ) -> Result<(), EmailClientStdError> {
        #[cfg(feature = "imap")]
        if self.imap.is_some() {
            return self.watch_mailbox_imap(mailbox, shutdown, tx);
        }
        #[cfg(feature = "jmap")]
        if self.jmap.is_some() {
            return self.watch_mailbox_jmap(mailbox, shutdown, tx);
        }
        let _ = (mailbox, shutdown, tx);
        Err(EmailClientStdError::UnsupportedOperation)
    }

    #[cfg(feature = "imap")]
    fn watch_mailbox_imap(
        &mut self,
        mailbox: &str,
        shutdown: alloc::sync::Arc<core::sync::atomic::AtomicBool>,
        tx: std::sync::mpsc::Sender<crate::event::WatchEvent>,
    ) -> Result<(), EmailClientStdError> {
        use core::time::Duration;

        use std::io::ErrorKind;

        use crate::{
            coroutine::{EmailCoroutine as _, EmailCoroutineState},
            imap::watch_mailbox::{ImapWatchMailbox, ImapWatchMailboxYield},
        };

        // SAFETY by construction: only called when self.imap is Some.
        let ctx = self
            .imap
            .as_mut()
            .expect("watch_mailbox_imap called without an IMAP context");

        ctx.stream.set_read_timeout(Some(Duration::from_secs(5)))?;

        let mut coro = ImapWatchMailbox::new(mailbox, &ctx.capabilities, shutdown)?;

        let mut buf = [0u8; READ_BUFFER_SIZE];
        let mut bytes: Option<&[u8]> = None;
        loop {
            let arg = EmailCoroutineArg::Imap {
                fragmentizer: &mut ctx.fragmentizer,
                bytes,
            };

            match coro.resume(arg) {
                EmailCoroutineState::Yielded(ImapWatchMailboxYield::WantsRead) => {
                    match ctx.stream.read(&mut buf) {
                        Ok(n) => bytes = Some(&buf[..n]),
                        Err(err) if err.kind() == ErrorKind::WouldBlock => bytes = None,
                        Err(err) if err.kind() == ErrorKind::TimedOut => bytes = None,
                        Err(err) => return Err(err.into()),
                    }
                }
                EmailCoroutineState::Yielded(ImapWatchMailboxYield::WantsWrite(out)) => {
                    ctx.stream.write_all(&out)?;
                    bytes = None;
                }
                EmailCoroutineState::Yielded(ImapWatchMailboxYield::Event(evt)) => {
                    if tx.send(evt).is_err() {
                        return Ok(());
                    }
                    bytes = None;
                }
                EmailCoroutineState::Complete(result) => return Ok(result?),
            }
        }
    }

    #[cfg(feature = "jmap")]
    fn watch_mailbox_jmap(
        &mut self,
        mailbox: &str,
        shutdown: alloc::sync::Arc<core::sync::atomic::AtomicBool>,
        tx: std::sync::mpsc::Sender<crate::event::WatchEvent>,
    ) -> Result<(), EmailClientStdError> {
        use core::time::Duration;

        use std::io::ErrorKind;

        use crate::coroutine::{EmailCoroutine as _, EmailCoroutineState};
        use crate::jmap::watch_mailbox::{JmapWatchMailbox, JmapWatchMailboxYield};

        // SAFETY by construction: only called when self.jmap is Some.
        let ctx = self
            .jmap
            .as_mut()
            .expect("watch_mailbox_jmap called without a JMAP context");

        ctx.stream.set_read_timeout(Some(Duration::from_secs(5)))?;

        let mut coro = JmapWatchMailbox::new(&ctx.session, &ctx.http_auth, mailbox, shutdown)?;

        let mut buf = [0u8; READ_BUFFER_SIZE];
        let mut bytes: Option<&[u8]> = None;
        loop {
            let arg = EmailCoroutineArg::Jmap { bytes };

            match coro.resume(arg) {
                EmailCoroutineState::Yielded(JmapWatchMailboxYield::WantsRead) => {
                    match ctx.stream.read(&mut buf) {
                        Ok(n) => bytes = Some(&buf[..n]),
                        Err(err) if err.kind() == ErrorKind::WouldBlock => bytes = None,
                        Err(err) if err.kind() == ErrorKind::TimedOut => bytes = None,
                        Err(err) => return Err(err.into()),
                    }
                }
                EmailCoroutineState::Yielded(JmapWatchMailboxYield::WantsWrite(out)) => {
                    ctx.stream.write_all(&out)?;
                    bytes = None;
                }
                EmailCoroutineState::Yielded(JmapWatchMailboxYield::Event(evt)) => {
                    if tx.send(evt).is_err() {
                        return Ok(());
                    }
                    bytes = None;
                }
                EmailCoroutineState::Complete(result) => return Ok(result?),
            }
        }
    }
}

// ---- Per-backend driver loops (free fns) ------------------------------
//
// Each driver matches on its backend's step enum. The per-method
// `EmailClientStd::run_<backend>` wrappers grab the matching context
// before delegating here.

#[cfg(feature = "imap")]
fn run_imap<C, O, E>(ctx: &mut ImapContext, mut coroutine: C) -> Result<O, EmailClientStdError>
where
    C: EmailCoroutine<Yield = ImapStep, Return = Result<O, E>>,
    EmailClientStdError: From<E>,
{
    let mut buf = [0u8; READ_BUFFER_SIZE];
    let mut bytes: Option<&[u8]> = None;

    loop {
        let arg = EmailCoroutineArg::Imap {
            fragmentizer: &mut ctx.fragmentizer,
            bytes,
        };
        match coroutine.resume(arg) {
            EmailCoroutineState::Yielded(ImapStep::WantsRead) => {
                let n = ctx.stream.read(&mut buf)?;
                bytes = Some(&buf[..n]);
            }
            EmailCoroutineState::Yielded(ImapStep::WantsWrite(out)) => {
                ctx.stream.write_all(&out)?;
                bytes = None;
            }
            EmailCoroutineState::Complete(Ok(out)) => return Ok(out),
            EmailCoroutineState::Complete(Err(err)) => return Err(err.into()),
        }
    }
}

#[cfg(feature = "jmap")]
fn run_jmap<C, O, E>(ctx: &mut JmapContext, mut coroutine: C) -> Result<O, EmailClientStdError>
where
    C: EmailCoroutine<Yield = JmapStep, Return = Result<O, E>>,
    EmailClientStdError: From<E>,
{
    let mut buf = [0u8; READ_BUFFER_SIZE];
    let mut bytes: Option<&[u8]> = None;

    loop {
        let arg = EmailCoroutineArg::Jmap { bytes };
        match coroutine.resume(arg) {
            EmailCoroutineState::Yielded(JmapStep::WantsRead) => {
                let n = ctx.stream.read(&mut buf)?;
                bytes = Some(&buf[..n]);
            }
            EmailCoroutineState::Yielded(JmapStep::WantsWrite(out)) => {
                ctx.stream.write_all(&out)?;
                bytes = None;
            }
            EmailCoroutineState::Complete(Ok(out)) => return Ok(out),
            EmailCoroutineState::Complete(Err(err)) => return Err(err.into()),
        }
    }
}

/// SMTP driver loop: same shape as [`run_imap`] / [`run_jmap`], but
/// the `EmailCoroutineArg::Smtp` variant carries only the bytes
/// pumped through the socket (no fragmentizer / no HTTP parser).
#[cfg(feature = "smtp")]
fn run_smtp<C, O, E>(ctx: &mut SmtpContext, mut coroutine: C) -> Result<O, EmailClientStdError>
where
    C: EmailCoroutine<Yield = SmtpStep, Return = Result<O, E>>,
    EmailClientStdError: From<E>,
{
    let mut buf = [0u8; READ_BUFFER_SIZE];
    let mut bytes: Option<&[u8]> = None;

    loop {
        let arg = EmailCoroutineArg::Smtp { bytes };
        match coroutine.resume(arg) {
            EmailCoroutineState::Yielded(SmtpStep::WantsRead) => {
                let n = ctx.stream.read(&mut buf)?;
                bytes = Some(&buf[..n]);
            }
            EmailCoroutineState::Yielded(SmtpStep::WantsWrite(out)) => {
                ctx.stream.write_all(&out)?;
                bytes = None;
            }
            EmailCoroutineState::Complete(Ok(out)) => return Ok(out),
            EmailCoroutineState::Complete(Err(err)) => return Err(err.into()),
        }
    }
}

/// Shared driver loop for every filesystem-backed coroutine
/// (Maildir, m2dir, future mbox / notmuch). The coroutine carries
/// its own root path internally; the driver just routes filesystem
/// batches keyed on [`PathBuf`].
#[cfg(any(feature = "maildir", feature = "m2dir"))]
fn run_fs<C, O, E>(mut coroutine: C) -> Result<O, EmailClientStdError>
where
    C: EmailCoroutine<Yield = FsStep, Return = Result<O, E>>,
    EmailClientStdError: From<E>,
{
    let mut batch: Option<FsBatch> = None;

    loop {
        let arg = EmailCoroutineArg::Fs { batch };
        match coroutine.resume(arg) {
            EmailCoroutineState::Yielded(FsStep::WantsDirRead(paths)) => {
                batch = Some(FsBatch::DirRead(read_dirs(paths)?));
            }
            EmailCoroutineState::Yielded(FsStep::WantsDirExists(paths)) => {
                batch = Some(FsBatch::DirExists(probe(paths, |m| m.is_dir())));
            }
            EmailCoroutineState::Yielded(FsStep::WantsFileExists(paths)) => {
                batch = Some(FsBatch::FileExists(probe(paths, |m| m.is_file())));
            }
            EmailCoroutineState::Yielded(FsStep::WantsFileRead(paths)) => {
                batch = Some(FsBatch::FileRead(read_files(paths)?));
            }
            EmailCoroutineState::Yielded(FsStep::WantsFileCreate(files)) => {
                write_files(files)?;
                batch = Some(FsBatch::FileCreate);
            }
            EmailCoroutineState::Yielded(FsStep::WantsFileRemove(paths)) => {
                remove_files(paths)?;
                batch = Some(FsBatch::FileRemove);
            }
            EmailCoroutineState::Yielded(FsStep::WantsDirCreate(paths)) => {
                create_dirs(paths)?;
                batch = Some(FsBatch::DirCreate);
            }
            EmailCoroutineState::Yielded(FsStep::WantsDirRemove(paths)) => {
                remove_dirs(paths)?;
                batch = Some(FsBatch::DirRemove);
            }
            EmailCoroutineState::Yielded(FsStep::WantsRename(pairs)) => {
                rename_pairs(pairs)?;
                batch = Some(FsBatch::Rename);
            }
            #[cfg(feature = "maildir")]
            EmailCoroutineState::Yielded(FsStep::WantsCopy(pairs)) => {
                copy_pairs(pairs)?;
                batch = Some(FsBatch::Copy);
            }
            #[cfg(feature = "maildir")]
            EmailCoroutineState::Yielded(FsStep::WantsTime) => {
                let elapsed = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default();
                batch = Some(FsBatch::Time {
                    secs: elapsed.as_secs(),
                    nanos: elapsed.subsec_nanos(),
                });
            }
            EmailCoroutineState::Yielded(FsStep::WantsPid) => {
                batch = Some(FsBatch::Pid(process::id()));
            }
            #[cfg(feature = "maildir")]
            EmailCoroutineState::Yielded(FsStep::WantsHostname) => {
                batch = Some(FsBatch::Hostname(hostname()));
            }
            #[cfg(feature = "m2dir")]
            EmailCoroutineState::Yielded(FsStep::WantsRandom { len }) => {
                batch = Some(FsBatch::Random(random_bytes(len)));
            }
            EmailCoroutineState::Complete(Ok(out)) => return Ok(out),
            EmailCoroutineState::Complete(Err(err)) => return Err(err.into()),
        }
    }
}

/// Reads the contents of each requested directory; missing directories
/// surface as empty entries so the coroutine can move past them.
#[cfg(any(feature = "maildir", feature = "m2dir"))]
fn read_dirs(paths: BTreeSet<PathBuf>) -> Result<BTreeMap<PathBuf, BTreeSet<PathBuf>>, io::Error> {
    let mut entries = BTreeMap::new();
    for path in paths {
        let mut names = BTreeSet::new();
        match fs::read_dir(&path) {
            Ok(iter) => {
                for entry in iter {
                    let entry = entry?;
                    names.insert(entry.path());
                }
            }
            Err(err) if err.kind() == io::ErrorKind::NotFound => {}
            Err(err) => return Err(err),
        }
        entries.insert(path, names);
    }
    Ok(entries)
}

/// Probes each requested path with `metadata()` and the given predicate
/// (`is_dir` or `is_file`). Missing paths report as `false` rather than
/// erroring.
#[cfg(any(feature = "maildir", feature = "m2dir"))]
fn probe(paths: BTreeSet<PathBuf>, pred: fn(&fs::Metadata) -> bool) -> BTreeMap<PathBuf, bool> {
    let mut out = BTreeMap::new();
    for path in paths {
        let exists = fs::metadata(&path).map(|m| pred(&m)).unwrap_or(false);
        out.insert(path, exists);
    }
    out
}

/// Reads every requested file; missing files surface as empty buffers
/// so the coroutine can carry on.
#[cfg(any(feature = "maildir", feature = "m2dir"))]
fn read_files(paths: BTreeSet<PathBuf>) -> Result<BTreeMap<PathBuf, Vec<u8>>, io::Error> {
    let mut out = BTreeMap::new();
    for path in paths {
        let bytes = match fs::read(&path) {
            Ok(bytes) => bytes,
            Err(err) if err.kind() == io::ErrorKind::NotFound => Vec::new(),
            Err(err) => return Err(err),
        };
        out.insert(path, bytes);
    }
    Ok(out)
}

/// Writes every `(path, bytes)` pair, creating parent directories as
/// needed. Coroutines that yield [`FsStep::WantsFileCreate`] expect
/// at-rest persistence before the next resume.
#[cfg(any(feature = "maildir", feature = "m2dir"))]
fn write_files(files: BTreeMap<PathBuf, Vec<u8>>) -> Result<(), io::Error> {
    for (path, bytes) in files {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(path, bytes)?;
    }
    Ok(())
}

/// Removes every requested file. Missing files are silently ignored so
/// coroutines can issue idempotent cleanup batches.
#[cfg(any(feature = "maildir", feature = "m2dir"))]
fn remove_files(paths: BTreeSet<PathBuf>) -> Result<(), io::Error> {
    for path in paths {
        match fs::remove_file(&path) {
            Ok(()) => {}
            Err(err) if err.kind() == io::ErrorKind::NotFound => {}
            Err(err) => return Err(err),
        }
    }
    Ok(())
}

#[cfg(any(feature = "maildir", feature = "m2dir"))]
fn create_dirs(paths: BTreeSet<PathBuf>) -> Result<(), io::Error> {
    for path in paths {
        fs::create_dir_all(path)?;
    }
    Ok(())
}

#[cfg(any(feature = "maildir", feature = "m2dir"))]
fn remove_dirs(paths: BTreeSet<PathBuf>) -> Result<(), io::Error> {
    for path in paths {
        match fs::remove_dir_all(&path) {
            Ok(()) => {}
            Err(err) if err.kind() == io::ErrorKind::NotFound => {}
            Err(err) => return Err(err),
        }
    }
    Ok(())
}

#[cfg(any(feature = "maildir", feature = "m2dir"))]
fn rename_pairs(pairs: Vec<(PathBuf, PathBuf)>) -> Result<(), io::Error> {
    for (from, to) in pairs {
        if let Some(parent) = to.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::rename(from, to)?;
    }
    Ok(())
}

#[cfg(feature = "maildir")]
fn copy_pairs(pairs: Vec<(PathBuf, PathBuf)>) -> Result<(), io::Error> {
    for (from, to) in pairs {
        if let Some(parent) = to.parent() {
            fs::create_dir_all(parent)?;
        }
        copy_file(&from, &to)?;
    }
    Ok(())
}

/// Best-effort hardlink-then-copy fallback so concurrent readers never
/// observe a partial destination.
#[cfg(feature = "maildir")]
fn copy_file(from: &Path, to: &Path) -> Result<(), io::Error> {
    match fs::hard_link(from, to) {
        Ok(()) => Ok(()),
        Err(_) => fs::copy(from, to).map(|_| ()),
    }
}

/// Best-effort hostname; falls back to "localhost" when the system
/// call fails or the value is not valid UTF-8. Maildir filenames only
/// use it for uniqueness, so any deterministic string works.
#[cfg(feature = "maildir")]
fn hostname() -> String {
    use std::ffi::CStr;
    let mut buf = [0u8; 256];
    // SAFETY: gethostname writes up to buf.len() bytes and NUL-terminates;
    // we treat anything past the first NUL as garbage.
    let rc = unsafe { libc_gethostname(buf.as_mut_ptr() as *mut _, buf.len()) };
    if rc != 0 {
        return "localhost".into();
    }
    let nul = buf.iter().position(|&b| b == 0).unwrap_or(buf.len() - 1);
    buf[nul] = 0;
    CStr::from_bytes_with_nul(&buf[..=nul])
        .ok()
        .and_then(|c| c.to_str().ok())
        .map(String::from)
        .unwrap_or_else(|| "localhost".into())
}

#[cfg(feature = "maildir")]
unsafe extern "C" {
    #[link_name = "gethostname"]
    fn libc_gethostname(name: *mut core::ffi::c_char, len: usize) -> core::ffi::c_int;
}

#[cfg(feature = "m2dir")]
fn random_bytes(len: usize) -> Vec<u8> {
    use std::io::Read;
    let mut buf = alloc::vec![0u8; len];
    // /dev/urandom is good enough for m2dir filename nonces; an error
    // here would just degrade uniqueness, so fall back to zeros.
    if let Ok(mut f) = fs::File::open("/dev/urandom") {
        let _ = f.read_exact(&mut buf);
    }
    buf
}
