//! # Standard, blocking unified email client
//!
//! Sum type over the per-backend std clients exposed by [`io-imap`],
//! [`io-jmap`], [`io-maildir`] and [`io-smtp`]. After construction,
//! callers see one type and one method set; backend dispatch happens
//! once inside each method.
//!
//! Construction stays asymmetric on purpose — every backend has its
//! own `connect` / `new` story (URLs, TLS, sessions, paths) — so
//! [`EmailClient`] is built from a fully-initialised per-backend
//! client via the [`From`] impls below. For JMAP this means the
//! caller must have already driven [`JmapClient::session_get`].
//!
//! The shared method surface is intentionally narrow: only
//! operations that have a meaningful translation across every
//! enabled backend are exposed here, returning shared types from
//! [`crate`] (e.g. [`Mailbox`], [`Envelope`]). Backend-specific
//! operations stay on the inner client and remain reachable through
//! the `as_<backend>_mut` escape hatches.
//!
//! [`io-imap`]: io_imap
//! [`io-jmap`]: io_jmap
//! [`io-maildir`]: io_maildir
//! [`io-smtp`]: io_smtp
//! [`JmapClient::session_get`]: io_jmap::client::JmapClient::session_get

use alloc::vec::Vec;

#[cfg(feature = "imap")]
use alloc::collections::BTreeMap;
#[cfg(any(feature = "imap", feature = "jmap", feature = "maildir"))]
use alloc::string::String;
#[cfg(any(feature = "imap", feature = "jmap"))]
use alloc::string::ToString;
#[cfg(feature = "imap")]
use core::num::NonZeroU32;

#[cfg(feature = "imap")]
use io_imap::{
    client::{ImapClient, ImapClientError},
    types::{
        fetch::{MacroOrMessageDataItemNames, MessageDataItem, MessageDataItemName},
        flag::{Flag as ImapFlag, StoreType},
        mailbox::{ListMailbox, Mailbox as ImapMailbox},
        sequence::SequenceSet,
        status::{StatusDataItem, StatusDataItemName},
    },
};
#[cfg(feature = "jmap")]
use io_jmap::{
    client::{JmapClient, JmapClientError},
    rfc8621::{capabilities, email::EmailFilter, email_set::JmapEmailSetArgs},
};
#[cfg(feature = "maildir")]
use io_maildir::{
    client::{MaildirClient, MaildirClientError},
    flag::{Flag as MdFlag, Flags as MdFlags},
    maildir::Maildir,
};
#[cfg(feature = "smtp")]
use io_smtp::client::{SmtpClient, SmtpClientError};
#[cfg(any(feature = "imap", feature = "jmap", feature = "maildir"))]
use log::trace;
use thiserror::Error;
#[cfg(feature = "jmap")]
use url::Url;

#[cfg(feature = "imap")]
use crate::imap::envelope_list::{build_item_names, compute_window, envelope_from};
#[cfg(feature = "jmap")]
use crate::jmap::envelope_list::envelope_properties;
#[cfg(feature = "jmap")]
use crate::jmap::message_get::resolve_download_url;
use crate::{envelope::Envelope, flag::Flag, mailbox::Mailbox};

/// Errors returned by [`EmailClient`].
#[derive(Debug, Error)]
pub enum EmailClientError {
    #[cfg(feature = "imap")]
    #[error(transparent)]
    Imap(#[from] ImapClientError),
    #[cfg(feature = "jmap")]
    #[error(transparent)]
    Jmap(#[from] JmapClientError),
    #[cfg(feature = "maildir")]
    #[error(transparent)]
    Maildir(#[from] MaildirClientError),
    #[cfg(feature = "smtp")]
    #[error(transparent)]
    Smtp(#[from] SmtpClientError),

    #[error("operation not supported by the active backend")]
    UnsupportedOperation,

    #[cfg(feature = "imap")]
    #[error("invalid IMAP mailbox name `{0}`")]
    InvalidImapMailbox(String),
    #[cfg(feature = "imap")]
    #[error("invalid IMAP UID `{0}` (expected non-zero u32)")]
    InvalidImapUid(String),
    #[cfg(feature = "imap")]
    #[error("empty UID list — at least one id is required")]
    EmptyImapUidList,
    #[cfg(feature = "imap")]
    #[error("invalid IMAP sequence-set window `{0}`")]
    InvalidImapWindow(String),
    #[cfg(feature = "imap")]
    #[error("FETCH did not return any body for the requested message")]
    ImapEmptyBody,

    #[cfg(feature = "jmap")]
    #[error("Email/get returned no email for the requested id")]
    JmapEmailNotFound,
    #[cfg(feature = "jmap")]
    #[error("Email/get response did not include a blobId")]
    JmapMissingBlobId,
    #[cfg(feature = "jmap")]
    #[error("resolved JMAP download URL is invalid: {0}")]
    InvalidJmapDownloadUrl(String),
    #[cfg(feature = "jmap")]
    #[error("JMAP blob download was redirected; not yet supported")]
    JmapUnsupportedRedirect,

    #[cfg(feature = "maildir")]
    #[error("invalid maildir at path `{0}`")]
    InvalidMaildir(String),
}

/// Std-blocking unified email client wrapping any one of the
/// per-backend clients.
#[allow(clippy::large_enum_variant)]
pub enum EmailClient {
    #[cfg(feature = "imap")]
    Imap(ImapClient),
    #[cfg(feature = "jmap")]
    Jmap(JmapClient),
    #[cfg(feature = "maildir")]
    Maildir(MaildirClient),
    #[cfg(feature = "smtp")]
    Smtp(SmtpClient),
}

impl EmailClient {
    /// Returns the inner [`ImapClient`] when the active variant is
    /// [`EmailClient::Imap`], for backend-specific operations not
    /// exposed on the unified surface.
    #[cfg(feature = "imap")]
    pub fn as_imap_mut(&mut self) -> Option<&mut ImapClient> {
        match self {
            Self::Imap(c) => Some(c),
            #[allow(unreachable_patterns)]
            _ => None,
        }
    }

    /// Returns the inner [`JmapClient`] when the active variant is
    /// [`EmailClient::Jmap`].
    #[cfg(feature = "jmap")]
    pub fn as_jmap_mut(&mut self) -> Option<&mut JmapClient> {
        match self {
            Self::Jmap(c) => Some(c),
            #[allow(unreachable_patterns)]
            _ => None,
        }
    }

    /// Returns the inner [`MaildirClient`] when the active variant is
    /// [`EmailClient::Maildir`].
    #[cfg(feature = "maildir")]
    pub fn as_maildir_mut(&mut self) -> Option<&mut MaildirClient> {
        match self {
            Self::Maildir(c) => Some(c),
            #[allow(unreachable_patterns)]
            _ => None,
        }
    }

    /// Returns the inner [`SmtpClient`] when the active variant is
    /// [`EmailClient::Smtp`].
    #[cfg(feature = "smtp")]
    pub fn as_smtp_mut(&mut self) -> Option<&mut SmtpClient> {
        match self {
            Self::Smtp(c) => Some(c),
            #[allow(unreachable_patterns)]
            _ => None,
        }
    }

    /// Lists every mailbox visible to the active backend, projected
    /// into the shared [`Mailbox`] type.
    ///
    /// When `with_counts` is `true`, [`Mailbox::total`] and
    /// [`Mailbox::unread`] are populated for backends that support
    /// counts. JMAP populates them unconditionally (free in the same
    /// `Mailbox/get` response). IMAP issues an extra `STATUS` per
    /// mailbox. Maildir does not implement counts yet and leaves both
    /// as `None`.
    ///
    /// - **IMAP**: issues `LIST "" "*"`, then `STATUS <mbox>
    ///   (MESSAGES UNSEEN)` per mailbox when `with_counts` is set.
    /// - **JMAP**: issues batched `Mailbox/query` + `Mailbox/get`
    ///   with no filter, sort or paging. The caller must have
    ///   already driven [`JmapClient::session_get`].
    /// - **Maildir**: enumerates every valid Maildir under the
    ///   client's root path. Counts are not implemented.
    /// - **SMTP**: returns
    ///   [`EmailClientError::UnsupportedOperation`].
    ///
    /// [`JmapClient::session_get`]: io_jmap::client::JmapClient::session_get
    #[cfg_attr(
        not(any(feature = "imap", feature = "jmap", feature = "maildir")),
        allow(unused_variables)
    )]
    pub fn list_mailboxes(&mut self, with_counts: bool) -> Result<Vec<Mailbox>, EmailClientError> {
        match self {
            #[cfg(not(any(
                feature = "imap",
                feature = "jmap",
                feature = "maildir",
                feature = "smtp"
            )))]
            _ => Err(EmailClientError::UnsupportedOperation),
            #[cfg(feature = "imap")]
            Self::Imap(client) => {
                trace!("EmailClient::list_mailboxes via IMAP (counts={with_counts})");
                // SAFETY: "" and "*" are always valid IMAP mailbox
                // tokens.
                let reference: ImapMailbox<'static> = "".try_into().unwrap();
                let pattern: ListMailbox<'static> = "*".try_into().unwrap();
                let listing = client.list(reference, pattern)?;
                let mut mailboxes: Vec<Mailbox> = listing.into_iter().map(Mailbox::from).collect();

                if with_counts {
                    let items: Vec<StatusDataItemName> =
                        vec![StatusDataItemName::Messages, StatusDataItemName::Unseen];
                    for mailbox in &mut mailboxes {
                        let mbox: ImapMailbox<'static> =
                            mailbox.id.clone().try_into().map_err(|_| {
                                EmailClientError::InvalidImapMailbox(mailbox.id.clone())
                            })?;
                        let data = client.status(mbox, items.clone())?;
                        for item in data {
                            match item {
                                StatusDataItem::Messages(n) => {
                                    mailbox.total = Some(u64::from(n));
                                }
                                StatusDataItem::Unseen(n) => {
                                    mailbox.unread = Some(u64::from(n));
                                }
                                _ => {}
                            }
                        }
                    }
                }

                Ok(mailboxes)
            }
            #[cfg(feature = "jmap")]
            Self::Jmap(client) => {
                trace!("EmailClient::list_mailboxes via JMAP");
                // JMAP populates total/unread unconditionally — they
                // ride on the same Mailbox/get response, so
                // with_counts is irrelevant here.
                let _ = with_counts;
                let output = client.mailbox_query(None, None, None, None, None)?;
                Ok(output.mailboxes.into_iter().map(Mailbox::from).collect())
            }
            #[cfg(feature = "maildir")]
            Self::Maildir(client) => {
                trace!("EmailClient::list_mailboxes via Maildir");
                // Maildir counts would require enumerating each
                // maildir's cur/+new/ entries and parsing flag
                // suffixes; not implemented yet.
                let _ = with_counts;
                let maildirs = client.list_maildirs()?;
                let mut mailboxes: Vec<Mailbox> = maildirs.into_iter().map(Mailbox::from).collect();
                mailboxes.sort_by(|a, b| a.name.cmp(&b.name));
                Ok(mailboxes)
            }
            #[cfg(feature = "smtp")]
            Self::Smtp(_) => Err(EmailClientError::UnsupportedOperation),
        }
    }

    /// Lists envelopes from the given mailbox, projected into the
    /// shared [`Envelope`] type. Pagination is 1-indexed; page 1 is
    /// the most recent window.
    ///
    /// When `with_attachment` is set, each envelope's
    /// [`Envelope::has_attachment`] is populated. JMAP returns the
    /// flag for free (it always rides on `Email/get`); IMAP issues an
    /// additional `BODYSTRUCTURE` fetch item; Maildir already parses
    /// the message body for subject/from/to so the toggle is a no-op
    /// there.
    ///
    /// - **IMAP**: `SELECT <mailbox>` + `FETCH UID FLAGS ENVELOPE
    ///   RFC822.SIZE [BODYSTRUCTURE]` over a sequence-set window
    ///   computed from `EXISTS`.
    /// - **JMAP**: batched `Email/query` + `Email/get`. The
    ///   `mailbox` argument is the JMAP mailbox id; pass an empty
    ///   string to query the whole account.
    /// - **Maildir**: lists every message in `<root>/<mailbox>`,
    ///   sorts by date descending, then slices the requested page.
    /// - **SMTP**: returns
    ///   [`EmailClientError::UnsupportedOperation`].
    #[cfg_attr(
        not(any(feature = "imap", feature = "jmap", feature = "maildir")),
        allow(unused_variables)
    )]
    pub fn list_envelopes(
        &mut self,
        mailbox: &str,
        page: Option<u32>,
        page_size: Option<u32>,
        with_attachment: bool,
    ) -> Result<Vec<Envelope>, EmailClientError> {
        match self {
            #[cfg(not(any(
                feature = "imap",
                feature = "jmap",
                feature = "maildir",
                feature = "smtp"
            )))]
            _ => Err(EmailClientError::UnsupportedOperation),
            #[cfg(feature = "imap")]
            Self::Imap(client) => {
                trace!("EmailClient::list_envelopes via IMAP (attachments={with_attachment})");
                let mbox = parse_imap_mailbox(mailbox)?;
                let select = client.select(mbox)?;
                let exists = select.exists.unwrap_or(0);

                let Some(window) = compute_window(exists, page, page_size) else {
                    return Ok(Vec::new());
                };

                let sequence_set: SequenceSet = window
                    .as_str()
                    .try_into()
                    .map_err(|_| EmailClientError::InvalidImapWindow(window.clone()))?;

                let item_names = build_item_names(with_attachment);

                let data = client.fetch(sequence_set, item_names, false)?;
                Ok(envelopes_from_fetch(data))
            }
            #[cfg(feature = "jmap")]
            Self::Jmap(client) => {
                trace!("EmailClient::list_envelopes via JMAP");
                let _ = with_attachment; // JMAP populates always.
                let (position, limit) = compute_jmap_position_limit(page, page_size);
                let filter = mailbox_filter(mailbox);
                let output = client.email_query(
                    filter,
                    None,
                    position,
                    limit,
                    Some(envelope_properties()),
                )?;
                Ok(output.emails.into_iter().map(Envelope::from).collect())
            }
            #[cfg(feature = "maildir")]
            Self::Maildir(client) => {
                trace!("EmailClient::list_envelopes via Maildir");
                let _ = with_attachment; // Maildir parses unconditionally.
                let maildir = open_maildir(client, mailbox)?;
                let messages = client.list_messages(maildir)?;
                let mut envelopes: Vec<Envelope> =
                    messages.into_iter().map(Envelope::from).collect();
                envelopes.sort_by(|a, b| b.date.cmp(&a.date));
                Ok(paginate(envelopes, page, page_size))
            }
            #[cfg(feature = "smtp")]
            Self::Smtp(_) => Err(EmailClientError::UnsupportedOperation),
        }
    }

    /// Adds the given flags to the listed messages.
    ///
    /// - **IMAP**: `SELECT <mailbox>` + `UID STORE +FLAGS`. Ids are
    ///   parsed as IMAP UIDs.
    /// - **JMAP**: `Email/set` with `keywords/<keyword>: true`
    ///   patches. The `mailbox` argument is unused.
    /// - **Maildir**: drives `MaildirFlagsAdd` once per id.
    /// - **SMTP**: returns
    ///   [`EmailClientError::UnsupportedOperation`].
    pub fn add_flags(
        &mut self,
        mailbox: &str,
        ids: &[&str],
        flags: &[Flag],
    ) -> Result<(), EmailClientError> {
        self.store_flags(mailbox, ids, flags, FlagOp::Add)
    }

    /// Replaces the flags of the listed messages with the given set.
    /// Same backend semantics as [`add_flags`](Self::add_flags) but
    /// uses the protocol's set/replace primitive.
    pub fn set_flags(
        &mut self,
        mailbox: &str,
        ids: &[&str],
        flags: &[Flag],
    ) -> Result<(), EmailClientError> {
        self.store_flags(mailbox, ids, flags, FlagOp::Set)
    }

    /// Removes the given flags from the listed messages.
    /// Same backend semantics as [`add_flags`](Self::add_flags) but
    /// uses the protocol's remove primitive.
    pub fn delete_flags(
        &mut self,
        mailbox: &str,
        ids: &[&str],
        flags: &[Flag],
    ) -> Result<(), EmailClientError> {
        self.store_flags(mailbox, ids, flags, FlagOp::Remove)
    }

    #[cfg_attr(
        not(any(feature = "imap", feature = "jmap", feature = "maildir")),
        allow(unused_variables)
    )]
    fn store_flags(
        &mut self,
        mailbox: &str,
        ids: &[&str],
        flags: &[Flag],
        op: FlagOp,
    ) -> Result<(), EmailClientError> {
        match self {
            #[cfg(not(any(
                feature = "imap",
                feature = "jmap",
                feature = "maildir",
                feature = "smtp"
            )))]
            _ => Err(EmailClientError::UnsupportedOperation),
            #[cfg(feature = "imap")]
            Self::Imap(client) => {
                trace!("EmailClient::store_flags via IMAP ({op:?})");
                let mbox = parse_imap_mailbox(mailbox)?;
                let _ = client.select(mbox)?;
                let sequence_set = parse_imap_uids(ids)?;
                let imap_flags: Vec<ImapFlag<'static>> = flags.iter().map(imap_flag_from).collect();
                let kind = match op {
                    FlagOp::Add => StoreType::Add,
                    FlagOp::Set => StoreType::Replace,
                    FlagOp::Remove => StoreType::Remove,
                };
                let _ = client.store(sequence_set, kind, imap_flags, true)?;
                Ok(())
            }
            #[cfg(feature = "jmap")]
            Self::Jmap(client) => {
                trace!("EmailClient::store_flags via JMAP ({op:?})");
                let _ = mailbox; // JMAP keywords are global per email.
                let mut args = JmapEmailSetArgs::default();
                for id in ids {
                    for flag in flags {
                        let keyword = jmap_keyword_from(flag);
                        match op {
                            FlagOp::Add | FlagOp::Set => {
                                args.set_keyword(id.to_string(), keyword);
                            }
                            FlagOp::Remove => {
                                args.unset_keyword(id.to_string(), keyword);
                            }
                        }
                    }
                }
                let _ = client.email_set(args)?;
                Ok(())
            }
            #[cfg(feature = "maildir")]
            Self::Maildir(client) => {
                trace!("EmailClient::store_flags via Maildir ({op:?})");
                let maildir = open_maildir(client, mailbox)?;
                let md_flags: MdFlags = flags.iter().map(maildir_flag_from).collect();
                for id in ids {
                    match op {
                        FlagOp::Add => {
                            client.add_flags(maildir.clone(), *id, md_flags.clone())?;
                        }
                        FlagOp::Set => {
                            client.set_flags(maildir.clone(), *id, md_flags.clone())?;
                        }
                        FlagOp::Remove => {
                            client.remove_flags(maildir.clone(), *id, md_flags.clone())?;
                        }
                    }
                }
                Ok(())
            }
            #[cfg(feature = "smtp")]
            Self::Smtp(_) => Err(EmailClientError::UnsupportedOperation),
        }
    }

    /// Fetches the raw RFC 5322 bytes of a single message.
    ///
    /// - **IMAP**: `SELECT <mailbox>` + `UID FETCH <id> BODY.PEEK[]`.
    /// - **JMAP**: `Email/get` (asking for `blobId`) +
    ///   `Blob/download`. The `mailbox` argument is unused.
    /// - **Maildir**: reads the on-disk message file.
    /// - **SMTP**: returns
    ///   [`EmailClientError::UnsupportedOperation`].
    #[cfg_attr(
        not(any(feature = "imap", feature = "jmap", feature = "maildir")),
        allow(unused_variables)
    )]
    pub fn get_message(&mut self, mailbox: &str, id: &str) -> Result<Vec<u8>, EmailClientError> {
        match self {
            #[cfg(not(any(
                feature = "imap",
                feature = "jmap",
                feature = "maildir",
                feature = "smtp"
            )))]
            _ => Err(EmailClientError::UnsupportedOperation),
            #[cfg(feature = "imap")]
            Self::Imap(client) => {
                trace!("EmailClient::get_message via IMAP");
                let mbox = parse_imap_mailbox(mailbox)?;
                let _ = client.select(mbox)?;
                let sequence_set = parse_imap_uids(&[id])?;
                let item_names = MacroOrMessageDataItemNames::MessageDataItemNames(vec![
                    MessageDataItemName::BodyExt {
                        section: None,
                        partial: None,
                        peek: true,
                    },
                ]);
                let data = client.fetch(sequence_set, item_names, true)?;
                let bytes = data
                    .into_values()
                    .flat_map(|items| items.into_inner().into_iter())
                    .find_map(|item| match item {
                        MessageDataItem::BodyExt { data, .. } => {
                            data.0.map(|d| d.as_ref().to_vec())
                        }
                        _ => None,
                    })
                    .ok_or(EmailClientError::ImapEmptyBody)?;
                Ok(bytes)
            }
            #[cfg(feature = "jmap")]
            Self::Jmap(client) => {
                trace!("EmailClient::get_message via JMAP");
                let _ = mailbox;
                let session = client
                    .session()
                    .ok_or(JmapClientError::MissingSession)?
                    .clone();
                let output = client.email_get(
                    vec![id.to_string()],
                    Some(vec!["blobId".into()]),
                    false,
                    false,
                    0,
                )?;
                let email = output
                    .emails
                    .into_iter()
                    .next()
                    .ok_or(EmailClientError::JmapEmailNotFound)?;
                let blob_id = email.blob_id.ok_or(EmailClientError::JmapMissingBlobId)?;
                let account_id = session
                    .primary_accounts
                    .get(capabilities::MAIL)
                    .cloned()
                    .unwrap_or_default();
                let url_str = resolve_download_url(&session.download_url, &account_id, &blob_id);
                let url = Url::parse(&url_str)
                    .map_err(|_| EmailClientError::InvalidJmapDownloadUrl(url_str))?;
                Ok(client.blob_download(&url)?)
            }
            #[cfg(feature = "maildir")]
            Self::Maildir(client) => {
                trace!("EmailClient::get_message via Maildir");
                let maildir = open_maildir(client, mailbox)?;
                let message = client.get(maildir, id)?;
                Ok(message.into())
            }
            #[cfg(feature = "smtp")]
            Self::Smtp(_) => Err(EmailClientError::UnsupportedOperation),
        }
    }
}

#[derive(Clone, Copy, Debug)]
#[allow(dead_code)]
enum FlagOp {
    Add,
    Set,
    Remove,
}

#[cfg(feature = "imap")]
fn parse_imap_mailbox(name: &str) -> Result<ImapMailbox<'static>, EmailClientError> {
    String::from(name)
        .try_into()
        .map_err(|_| EmailClientError::InvalidImapMailbox(name.to_string()))
}

#[cfg(feature = "imap")]
fn parse_imap_uids(ids: &[&str]) -> Result<SequenceSet, EmailClientError> {
    if ids.is_empty() {
        return Err(EmailClientError::EmptyImapUidList);
    }

    let uids: Vec<NonZeroU32> = ids
        .iter()
        .map(|s| {
            s.parse::<NonZeroU32>()
                .map_err(|_| EmailClientError::InvalidImapUid((*s).to_string()))
        })
        .collect::<Result<_, _>>()?;

    SequenceSet::try_from(uids).map_err(|_| EmailClientError::EmptyImapUidList)
}

#[cfg(feature = "imap")]
fn imap_flag_from(flag: &Flag) -> ImapFlag<'static> {
    match flag {
        Flag::Seen => ImapFlag::Seen,
        Flag::Answered => ImapFlag::Answered,
        Flag::Flagged => ImapFlag::Flagged,
        Flag::Draft => ImapFlag::Draft,
    }
}

#[cfg(feature = "imap")]
fn envelopes_from_fetch(
    data: BTreeMap<NonZeroU32, io_imap::types::core::Vec1<MessageDataItem<'static>>>,
) -> Vec<Envelope> {
    data.into_iter()
        .rev()
        .map(|(seq, items)| envelope_from(seq.get(), items.into_inner()))
        .collect()
}

#[cfg(feature = "jmap")]
fn jmap_keyword_from(flag: &Flag) -> String {
    match flag {
        Flag::Seen => "$seen".into(),
        Flag::Answered => "$answered".into(),
        Flag::Flagged => "$flagged".into(),
        Flag::Draft => "$draft".into(),
    }
}

#[cfg(feature = "jmap")]
fn mailbox_filter(mailbox: &str) -> Option<EmailFilter> {
    if mailbox.is_empty() {
        return None;
    }
    Some(EmailFilter {
        in_mailbox: Some(mailbox.to_string()),
        ..EmailFilter::default()
    })
}

#[cfg(feature = "jmap")]
fn compute_jmap_position_limit(
    page: Option<u32>,
    page_size: Option<u32>,
) -> (Option<u64>, Option<u64>) {
    let Some(size) = page_size else {
        return (None, None);
    };

    let page = page.unwrap_or(1).max(1);
    let position = ((page - 1) as u64).saturating_mul(size as u64);

    (Some(position), Some(size as u64))
}

#[cfg(feature = "maildir")]
fn maildir_flag_from(flag: &Flag) -> MdFlag {
    match flag {
        Flag::Seen => MdFlag::Seen,
        Flag::Answered => MdFlag::Replied,
        Flag::Flagged => MdFlag::Flagged,
        Flag::Draft => MdFlag::Draft,
    }
}

#[cfg(feature = "maildir")]
fn open_maildir(client: &MaildirClient, name: &str) -> Result<Maildir, EmailClientError> {
    let path = client.root().join(name);
    Maildir::try_from(path.as_path())
        .map_err(|_| EmailClientError::InvalidMaildir(path.to_string_lossy().into_owned()))
}

#[cfg(feature = "maildir")]
fn paginate(envelopes: Vec<Envelope>, page: Option<u32>, page_size: Option<u32>) -> Vec<Envelope> {
    let Some(size) = page_size else {
        return envelopes;
    };

    if size == 0 {
        return Vec::new();
    }

    let page = page.unwrap_or(1).max(1);
    let skip = ((page - 1) as usize).saturating_mul(size as usize);

    if skip >= envelopes.len() {
        return Vec::new();
    }

    envelopes
        .into_iter()
        .skip(skip)
        .take(size as usize)
        .collect()
}

#[cfg(feature = "imap")]
impl From<ImapClient> for EmailClient {
    fn from(client: ImapClient) -> Self {
        Self::Imap(client)
    }
}

#[cfg(feature = "jmap")]
impl From<JmapClient> for EmailClient {
    fn from(client: JmapClient) -> Self {
        Self::Jmap(client)
    }
}

#[cfg(feature = "maildir")]
impl From<MaildirClient> for EmailClient {
    fn from(client: MaildirClient) -> Self {
        Self::Maildir(client)
    }
}

#[cfg(feature = "smtp")]
impl From<SmtpClient> for EmailClient {
    fn from(client: SmtpClient) -> Self {
        Self::Smtp(client)
    }
}
