//! Std blocking unified email client: sum type over the per-backend
//! std clients. Built from an already-initialised per-backend client
//! via `From` impls; backend-specific operations stay reachable by
//! pattern-matching the enum.

use alloc::{string::String, vec::Vec};

use log::trace;
#[cfg(any(feature = "imap", feature = "smtp"))]
use pimalaya_stream::std::stream::StreamStd;
use thiserror::Error;

#[cfg(feature = "search")]
use crate::search::query::SearchEmailsQuery;
use crate::{
    envelope::Envelope,
    flag::{Flag, FlagOp},
    mailbox::Mailbox,
};

/// Errors returned by [`EmailClientStd`].
///
/// Backend-specific errors propagate transparently through the per-backend
/// variants ([`Imap`](Self::Imap), [`Jmap`](Self::Jmap),
/// [`Maildir`](Self::Maildir), [`Smtp`](Self::Smtp)). Everything else is a
/// least-common-denominator reason any backend may surface, named without
/// protocol prefix to match the shared input/output API.
#[derive(Debug, Error)]
pub enum EmailClientStdError {
    #[cfg(feature = "imap")]
    #[error(transparent)]
    Imap(#[from] io_imap::client::ImapClientStdError),
    #[cfg(feature = "jmap")]
    #[error(transparent)]
    Jmap(#[from] io_jmap::client::JmapClientStdError),
    #[cfg(feature = "maildir")]
    #[error(transparent)]
    Maildir(#[from] io_maildir::client::MaildirClientError),
    #[cfg(feature = "smtp")]
    #[error(transparent)]
    Smtp(#[from] io_smtp::client::SmtpClientStdError),

    #[error("operation not supported by the active backend")]
    UnsupportedOperation,

    #[error("invalid mailbox `{0}`")]
    InvalidMailbox(String),
    #[error("invalid message id `{0}`")]
    InvalidId(String),
    #[error("invalid email address `{0}`")]
    InvalidAddress(String),
    #[error("invalid URL `{0}`")]
    InvalidUrl(String),
    #[error("invalid message content: {0}")]
    InvalidMessageContent(String),

    #[error("mailbox `{0}` not found")]
    MailboxNotFound(String),
    #[error("message `{0}` not found")]
    MessageNotFound(String),
    #[error("no identity configured for `{0}`")]
    IdentityNotFound(String),
    #[error("empty message body")]
    EmptyMessageBody,
    #[error("missing required input `{0}`")]
    MissingInput(&'static str),
    #[error("operation `{0}` failed")]
    OperationFailed(&'static str),
}

/// Std-blocking unified email client wrapping any one of the per-backend
/// clients.
#[derive(Debug)]
pub enum EmailClientStd {
    #[cfg(feature = "imap")]
    Imap(io_imap::client::ImapClientStd<StreamStd>),
    #[cfg(feature = "jmap")]
    Jmap(io_jmap::client::JmapClientStd),
    #[cfg(feature = "maildir")]
    Maildir(io_maildir::client::MaildirClient),
    #[cfg(feature = "smtp")]
    Smtp(io_smtp::client::SmtpClientStd<StreamStd>),
}

impl EmailClientStd {
    /// Lists every mailbox available to the active account.
    ///
    /// When `with_counts` is `true`, [`Mailbox::total`] and
    /// [`Mailbox::unread`] are populated when the backend supports them;
    /// otherwise they are left as `None`. Backends that surface counts
    /// for free always populate the fields regardless of the flag.
    pub fn list_mailboxes(
        &mut self,
        with_counts: bool,
    ) -> Result<Vec<Mailbox>, EmailClientStdError> {
        trace!("list mailboxes with {self:?}");

        match self {
            #[cfg(feature = "imap")]
            Self::Imap(client) => {
                use io_imap::types::{
                    mailbox::Mailbox as ImapMailbox,
                    status::{StatusDataItem, StatusDataItemName},
                };

                let mut mailboxes: Vec<_> = client
                    .list("".try_into().unwrap(), "*".try_into().unwrap())?
                    .into_iter()
                    .map(Mailbox::from)
                    .collect();

                if with_counts {
                    let items: Vec<StatusDataItemName> =
                        vec![StatusDataItemName::Messages, StatusDataItemName::Unseen];

                    for mailbox in &mut mailboxes {
                        let mbox: ImapMailbox<'static> =
                            mailbox.id.clone().try_into().map_err(|_| {
                                EmailClientStdError::InvalidMailbox(mailbox.id.clone())
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
                let output = client.mailbox_query(None, None, None, None, None)?;
                Ok(output.mailboxes.into_iter().map(Mailbox::from).collect())
            }
            #[cfg(feature = "maildir")]
            Self::Maildir(client) => {
                let maildirs = client.list_maildirs()?;
                let mut mailboxes: Vec<_> = maildirs.into_iter().map(Mailbox::from).collect();
                mailboxes.sort_by(|a, b| a.name.cmp(&b.name));
                Ok(mailboxes)
            }
            #[cfg(feature = "smtp")]
            Self::Smtp(_) => Err(EmailClientStdError::UnsupportedOperation),
        }
    }

    /// Lists envelopes from the given mailbox.
    ///
    /// `mailbox` is the backend-specific mailbox identifier (name or
    /// id). `page` is 1-indexed; pass `None` to default to page 1.
    /// `page_size = None` returns the full window. When
    /// `with_attachment` is set, [`Envelope::has_attachment`] is
    /// populated when the backend reports it (otherwise left as
    /// `None`).
    ///
    /// Default ordering is date descending (most recent first). Use
    /// [`Self::search_envelopes`] to filter and/or sort with the shared
    /// search DSL.
    pub fn list_envelopes(
        &mut self,
        mailbox: &str,
        page: Option<u32>,
        page_size: Option<u32>,
        with_attachment: bool,
    ) -> Result<Vec<Envelope>, EmailClientStdError> {
        trace!("list envelopes with {self:?}");

        match self {
            #[cfg(feature = "imap")]
            Self::Imap(client) => {
                use io_imap::types::{core::Vec1, fetch::MessageDataItem, sequence::SequenceSet};

                use crate::imap::{
                    convert::parse_mailbox,
                    envelope_list::{build_item_names, compute_window, envelope_from},
                };

                let mbox = parse_mailbox(mailbox)?;
                let select = client.select(mbox)?;
                let exists = select.exists.unwrap_or(0);
                if exists == 0 {
                    return Ok(Vec::new());
                }

                let Some(window) = compute_window(exists, page, page_size) else {
                    return Ok(Vec::new());
                };

                let sequence_set: SequenceSet = window
                    .as_str()
                    .try_into()
                    .expect("compute_window produced a valid sequence set");

                let item_names = build_item_names(with_attachment);
                let data = client.fetch(sequence_set, item_names, false)?;

                let envelopes: Vec<_> = data
                    .into_iter()
                    .rev()
                    .map(|(seq, items): (_, Vec1<MessageDataItem<'static>>)| {
                        envelope_from(seq.get(), items.into_inner())
                    })
                    .collect();

                Ok(envelopes)
            }
            #[cfg(feature = "jmap")]
            Self::Jmap(client) => {
                use crate::jmap::{
                    convert::{compute_position_limit, mailbox_filter},
                    envelope_list::envelope_properties,
                };

                let (position, limit) = compute_position_limit(page, page_size);
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
                use crate::maildir::convert::{open_maildir, paginate};

                let maildir = open_maildir(client, mailbox)?;
                let messages = client.list_messages(maildir)?;

                let mut envelopes: Vec<_> = messages.into_iter().map(Envelope::from).collect();
                envelopes.sort_by(|a, b| b.date.cmp(&a.date));

                Ok(paginate(envelopes, page, page_size))
            }
            #[cfg(feature = "smtp")]
            Self::Smtp(_) => Err(EmailClientStdError::UnsupportedOperation),
        }
    }

    /// Searches envelopes in the given mailbox using the shared search
    /// DSL (requires the `search` cargo feature).
    ///
    /// `query` carries an optional filter and/or sort. When the filter
    /// is `None`, every envelope in `mailbox` matches; when the sort is
    /// `None`, the default is date descending (most recent first).
    /// Pagination follows the same rules as [`Self::list_envelopes`].
    ///
    /// Per-protocol translation lives in the matching backend module:
    /// [`crate::imap::envelope_search`] (full grammar, `SEARCH` +
    /// `SORT`), [`crate::jmap::envelope_search`] (conjunctive only;
    /// `or`/`not` are rejected; dates over-approximate `receivedAt`
    /// then re-check `sentAt` client-side), and
    /// [`crate::maildir::envelope_search`] (full grammar except
    /// `body`, evaluated client-side).
    #[cfg(feature = "search")]
    pub fn search_envelopes(
        &mut self,
        mailbox: &str,
        query: Option<&SearchEmailsQuery>,
        page: Option<u32>,
        page_size: Option<u32>,
        with_attachment: bool,
    ) -> Result<Vec<Envelope>, EmailClientStdError> {
        trace!("search envelopes with {self:?}");

        match self {
            #[cfg(feature = "imap")]
            Self::Imap(client) => {
                use alloc::{collections::BTreeMap, string::ToString};

                use io_imap::types::{core::Vec1, fetch::MessageDataItem, sequence::SequenceSet};

                use crate::imap::{
                    convert::parse_mailbox,
                    envelope_list::{build_item_names, envelope_from},
                    envelope_search::{paginate_uids, search_keys, sort_criteria},
                };

                let mbox = parse_mailbox(mailbox)?;
                let select = client.select(mbox)?;
                if select.exists.unwrap_or(0) == 0 {
                    return Ok(Vec::new());
                }

                let search_criteria = search_keys(query.and_then(|q| q.filter.as_ref()))?;
                let sort_criteria = sort_criteria(query.and_then(|q| q.sort.as_deref()));

                let uids = client.sort(sort_criteria, search_criteria, true)?;
                if uids.is_empty() {
                    return Ok(Vec::new());
                }

                let page_uids = paginate_uids(&uids, page, page_size);
                if page_uids.is_empty() {
                    return Ok(Vec::new());
                }

                let uid_str = page_uids
                    .iter()
                    .map(u32::to_string)
                    .collect::<Vec<_>>()
                    .join(",");
                let sequence_set: SequenceSet = uid_str
                    .as_str()
                    .try_into()
                    .map_err(|_| EmailClientStdError::OperationFailed("invalid IMAP UID set"))?;

                let item_names = build_item_names(with_attachment);
                let data = client.fetch(sequence_set, item_names, true)?;

                let by_uid: BTreeMap<u32, Envelope> = data
                    .into_iter()
                    .map(|(_, items): (_, Vec1<MessageDataItem<'static>>)| {
                        let items = items.into_inner();
                        let uid = items.iter().find_map(|item| match item {
                            MessageDataItem::Uid(u) => Some(u.get()),
                            _ => None,
                        });
                        let env = envelope_from(0, items);
                        (uid.unwrap_or(0), env)
                    })
                    .collect();

                let envelopes: Vec<_> = page_uids
                    .iter()
                    .filter_map(|u| by_uid.get(u).cloned())
                    .collect();

                Ok(envelopes)
            }
            #[cfg(feature = "jmap")]
            Self::Jmap(client) => {
                use crate::jmap::{
                    convert::{compute_position_limit, mailbox_filter},
                    envelope_list::envelope_properties,
                    envelope_search::{build, post_filter},
                };

                let base = mailbox_filter(mailbox).unwrap_or_default();
                let converted = build(query, base)?;

                let (position, limit) = if converted.post_filters.is_empty() {
                    compute_position_limit(page, page_size)
                } else {
                    (None, None)
                };

                let output = client.email_query(
                    Some(converted.filter),
                    Some(converted.sort),
                    position,
                    limit,
                    Some(envelope_properties()),
                )?;

                let mut envelopes: Vec<_> = output.emails.into_iter().map(Envelope::from).collect();

                if !converted.post_filters.is_empty() {
                    envelopes.retain(|env| post_filter(env, &converted.post_filters));

                    let total = envelopes.len();
                    let size = page_size.map(|n| n as usize);
                    let start =
                        ((page.unwrap_or(1).max(1) - 1) as usize).saturating_mul(size.unwrap_or(0));
                    let end = match size {
                        Some(n) => start.saturating_add(n).min(total),
                        None => total,
                    };
                    if start >= total {
                        envelopes.clear();
                    } else {
                        envelopes = envelopes[start..end].to_vec();
                    }
                }

                Ok(envelopes)
            }
            #[cfg(feature = "maildir")]
            Self::Maildir(client) => {
                use crate::maildir::{
                    convert::{open_maildir, paginate},
                    envelope_search::{compare, filter_references_body, matches},
                };

                let maildir = open_maildir(client, mailbox)?;
                let messages = client.list_messages(maildir)?;

                let mut envelopes: Vec<_> = messages.into_iter().map(Envelope::from).collect();

                if let Some(query) = query {
                    if let Some(filter) = &query.filter {
                        if filter_references_body(filter) {
                            return Err(EmailClientStdError::OperationFailed(
                                "envelopes search `body` filter is not yet supported on Maildir",
                            ));
                        }
                        envelopes.retain(|env| matches(env, filter));
                    }

                    match query.sort.as_deref() {
                        Some(sort) if !sort.is_empty() => {
                            envelopes.sort_by(|a, b| compare(a, b, sort));
                        }
                        _ => envelopes.sort_by(|a, b| b.date.cmp(&a.date)),
                    }
                } else {
                    envelopes.sort_by(|a, b| b.date.cmp(&a.date));
                }

                Ok(paginate(envelopes, page, page_size))
            }
            #[cfg(feature = "smtp")]
            Self::Smtp(_) => Err(EmailClientStdError::UnsupportedOperation),
        }
    }

    /// Adds the given flags to every message in `ids` inside
    /// `mailbox`, preserving any flags already set. `ids` is a slice
    /// of backend-specific message identifiers.
    pub fn add_flags(
        &mut self,
        mailbox: &str,
        ids: &[&str],
        flags: &[Flag],
    ) -> Result<(), EmailClientStdError> {
        self.store_flags(mailbox, ids, flags, FlagOp::Add)
    }

    /// Replaces the flag set of every message in `ids` inside
    /// `mailbox` with `flags` exactly. Any prior flag not in `flags`
    /// is removed.
    pub fn set_flags(
        &mut self,
        mailbox: &str,
        ids: &[&str],
        flags: &[Flag],
    ) -> Result<(), EmailClientStdError> {
        self.store_flags(mailbox, ids, flags, FlagOp::Set)
    }

    /// Removes the given flags from every message in `ids` inside
    /// `mailbox`. Flags not present on a message are silently skipped.
    pub fn delete_flags(
        &mut self,
        mailbox: &str,
        ids: &[&str],
        flags: &[Flag],
    ) -> Result<(), EmailClientStdError> {
        self.store_flags(mailbox, ids, flags, FlagOp::Remove)
    }

    fn store_flags(
        &mut self,
        mailbox: &str,
        ids: &[&str],
        flags: &[Flag],
        op: FlagOp,
    ) -> Result<(), EmailClientStdError> {
        trace!("store flags ({op:?}) with {self:?}");

        match self {
            #[cfg(feature = "imap")]
            Self::Imap(client) => {
                use io_imap::types::flag::StoreType;

                use crate::imap::convert::{flag_from, parse_mailbox, parse_uids};

                let mbox = parse_mailbox(mailbox)?;
                let _ = client.select(mbox)?;

                let sequence_set = parse_uids(ids)?;
                let imap_flags: Vec<_> = flags.iter().map(flag_from).collect();
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
                use alloc::string::ToString;

                use io_jmap::rfc8621::email_set::JmapEmailSetArgs;

                use crate::jmap::convert::keyword_from;

                let mut args = JmapEmailSetArgs::default();

                for id in ids {
                    for flag in flags {
                        let keyword = keyword_from(flag);
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
                use io_maildir::flag::Flags as MdFlags;

                use crate::maildir::convert::{flag_from, open_maildir};

                let maildir = open_maildir(client, mailbox)?;
                let md_flags: MdFlags = flags.iter().map(flag_from).collect();

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
            Self::Smtp(_) => Err(EmailClientStdError::UnsupportedOperation),
        }
    }

    /// Fetches the raw RFC 5322 bytes of message `id` from `mailbox`.
    ///
    /// `mailbox` may be ignored by backends whose ids are globally
    /// scoped. Returns the message body as-is, with no modification
    /// to the seen/read state.
    pub fn get_message(&mut self, mailbox: &str, id: &str) -> Result<Vec<u8>, EmailClientStdError> {
        trace!("get message with {self:?}");

        match self {
            #[cfg(feature = "imap")]
            Self::Imap(client) => {
                use io_imap::types::fetch::{
                    MacroOrMessageDataItemNames, MessageDataItem, MessageDataItemName,
                };

                use crate::imap::convert::{parse_mailbox, parse_uids};

                let mbox = parse_mailbox(mailbox)?;
                let _ = client.select(mbox)?;

                let sequence_set = parse_uids(&[id])?;
                let item_names = MacroOrMessageDataItemNames::MessageDataItemNames(vec![
                    MessageDataItemName::BodyExt {
                        section: None,
                        partial: None,
                        peek: true,
                    },
                ]);

                let data = client.fetch(sequence_set, item_names, true)?;

                data.into_values()
                    .flat_map(|items| items.into_inner().into_iter())
                    .find_map(|item| match item {
                        MessageDataItem::BodyExt { data, .. } => {
                            data.0.map(|d| d.as_ref().to_vec())
                        }
                        _ => None,
                    })
                    .ok_or(EmailClientStdError::EmptyMessageBody)
            }
            #[cfg(feature = "jmap")]
            Self::Jmap(client) => {
                use alloc::string::ToString;

                use io_jmap::{client::JmapClientStdError, rfc8621::capabilities};
                use url::Url;

                use crate::jmap::message_get::resolve_download_url;

                let session = client
                    .session()
                    .ok_or(JmapClientStdError::MissingSession)?
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
                    .ok_or_else(|| EmailClientStdError::MessageNotFound(id.to_string()))?;
                let blob_id = email
                    .blob_id
                    .ok_or(EmailClientStdError::OperationFailed("retrieve blob id"))?;

                let account_id = session
                    .primary_accounts
                    .get(capabilities::MAIL)
                    .cloned()
                    .unwrap_or_default();
                let url_str = resolve_download_url(&session.download_url, &account_id, &blob_id);
                let url =
                    Url::parse(&url_str).map_err(|_| EmailClientStdError::InvalidUrl(url_str))?;

                Ok(client.blob_download(&url)?)
            }
            #[cfg(feature = "maildir")]
            Self::Maildir(client) => {
                use crate::maildir::convert::open_maildir;

                let maildir = open_maildir(client, mailbox)?;
                let message = client.get(maildir, id)?;

                Ok(message.into())
            }
            #[cfg(feature = "smtp")]
            Self::Smtp(_) => Err(EmailClientStdError::UnsupportedOperation),
        }
    }

    /// Appends a raw RFC 5322 message to `mailbox`, tagged with the
    /// given `flags`. `raw` must be a syntactically valid RFC 5322
    /// message; framing-level escaping is handled by the backend.
    pub fn add_message(
        &mut self,
        mailbox: &str,
        flags: &[Flag],
        raw: Vec<u8>,
    ) -> Result<(), EmailClientStdError> {
        trace!("add message with {self:?}");

        match self {
            #[cfg(feature = "imap")]
            Self::Imap(client) => {
                use alloc::string::ToString;

                use io_imap::types::{core::Literal, extensions::binary::LiteralOrLiteral8};

                use crate::imap::convert::{flag_from, parse_mailbox};

                let mbox = parse_mailbox(mailbox)?;
                let imap_flags: Vec<_> = flags.iter().map(flag_from).collect();
                let literal = Literal::try_from(raw)
                    .map_err(|e| EmailClientStdError::InvalidMessageContent(e.to_string()))?;
                let message = LiteralOrLiteral8::Literal(literal);

                let _ = client.append(mbox, imap_flags, None, message)?;

                Ok(())
            }
            #[cfg(feature = "jmap")]
            Self::Jmap(client) => {
                use alloc::{collections::BTreeMap, string::ToString};

                use io_jmap::{
                    client::JmapClientStdError,
                    rfc8621::{capabilities, email::EmailImport},
                };
                use url::Url;

                use crate::jmap::convert::keyword_from;

                let session = client
                    .session()
                    .ok_or(JmapClientStdError::MissingSession)?
                    .clone();
                let account_id = session
                    .primary_accounts
                    .get(capabilities::MAIL)
                    .cloned()
                    .unwrap_or_default();
                let upload_url_str = session.upload_url.replace("{accountId}", &account_id);
                let upload_url = Url::parse(&upload_url_str)
                    .map_err(|_| EmailClientStdError::InvalidUrl(upload_url_str))?;
                let blob = client.blob_upload(&upload_url, "message/rfc822", raw)?;

                let mq = client.mailbox_query(None, None, None, None, None)?;
                let mailbox_id = mq
                    .mailboxes
                    .iter()
                    .find(|m| m.name.as_deref() == Some(mailbox))
                    .and_then(|m| m.id.clone())
                    .ok_or_else(|| EmailClientStdError::MailboxNotFound(mailbox.to_string()))?;

                let mut mailbox_ids = BTreeMap::new();
                mailbox_ids.insert(mailbox_id, true);

                let keywords = if flags.is_empty() {
                    None
                } else {
                    Some(flags.iter().map(|f| (keyword_from(f), true)).collect())
                };

                let mut emails = BTreeMap::new();
                emails.insert(
                    "new".to_string(),
                    EmailImport {
                        blob_id: blob.blob_id,
                        mailbox_ids,
                        keywords,
                        received_at: None,
                    },
                );

                let output = client.email_import(emails)?;
                if !output.not_created.is_empty() || output.created.is_empty() {
                    return Err(EmailClientStdError::OperationFailed("import"));
                }

                Ok(())
            }
            #[cfg(feature = "maildir")]
            Self::Maildir(client) => {
                use io_maildir::{flag::Flags as MdFlags, maildir::MaildirSubdir};

                use crate::maildir::convert::{flag_from, open_maildir};

                let maildir = open_maildir(client, mailbox)?;
                let md_flags: MdFlags = flags.iter().map(flag_from).collect();

                let _ = client.store(maildir, MaildirSubdir::Cur, md_flags, raw)?;

                Ok(())
            }
            #[cfg(feature = "smtp")]
            Self::Smtp(_) => Err(EmailClientStdError::UnsupportedOperation),
        }
    }

    /// Copies every message in `ids` from mailbox `from` to mailbox
    /// `to`, leaving the originals in place.
    pub fn copy_messages(
        &mut self,
        from: &str,
        to: &str,
        ids: &[&str],
    ) -> Result<(), EmailClientStdError> {
        trace!("copy messages with {self:?}");

        match self {
            #[cfg(feature = "imap")]
            Self::Imap(client) => {
                use crate::imap::convert::{parse_mailbox, parse_uids};

                let src = parse_mailbox(from)?;
                let dst = parse_mailbox(to)?;
                let _ = client.select(src)?;

                let sequence_set = parse_uids(ids)?;
                let _ = client.copy(sequence_set, dst, true)?;

                Ok(())
            }
            #[cfg(feature = "jmap")]
            Self::Jmap(client) => {
                use alloc::string::ToString;

                use io_jmap::rfc8621::email_set::JmapEmailSetArgs;

                let mq = client.mailbox_query(None, None, None, None, None)?;
                let dst_id = mq
                    .mailboxes
                    .iter()
                    .find(|m| m.name.as_deref() == Some(to))
                    .and_then(|m| m.id.clone())
                    .ok_or_else(|| EmailClientStdError::MailboxNotFound(to.to_string()))?;

                let mut args = JmapEmailSetArgs::default();
                for id in ids {
                    args.add_to_mailbox(id.to_string(), dst_id.clone());
                }

                let _ = client.email_set(args)?;

                Ok(())
            }
            #[cfg(feature = "maildir")]
            Self::Maildir(client) => {
                use crate::maildir::convert::open_maildir;

                let src = open_maildir(client, from)?;
                let dst = open_maildir(client, to)?;

                for id in ids {
                    client.copy(*id, src.clone(), dst.clone(), None)?;
                }

                Ok(())
            }
            #[cfg(feature = "smtp")]
            Self::Smtp(_) => Err(EmailClientStdError::UnsupportedOperation),
        }
    }

    /// Moves every message in `ids` from mailbox `from` to mailbox
    /// `to`. The originals are removed from `from`.
    pub fn move_messages(
        &mut self,
        from: &str,
        to: &str,
        ids: &[&str],
    ) -> Result<(), EmailClientStdError> {
        trace!("move messages with {self:?}");

        match self {
            #[cfg(feature = "imap")]
            Self::Imap(client) => {
                use crate::imap::convert::{parse_mailbox, parse_uids};

                let src = parse_mailbox(from)?;
                let dst = parse_mailbox(to)?;
                let _ = client.select(src)?;

                let sequence_set = parse_uids(ids)?;
                let _ = client.r#move(sequence_set, dst, true)?;

                Ok(())
            }
            #[cfg(feature = "jmap")]
            Self::Jmap(client) => {
                use alloc::string::ToString;

                use io_jmap::rfc8621::email_set::JmapEmailSetArgs;

                let mq = client.mailbox_query(None, None, None, None, None)?;
                let src_id = mq
                    .mailboxes
                    .iter()
                    .find(|m| m.name.as_deref() == Some(from))
                    .and_then(|m| m.id.clone())
                    .ok_or_else(|| EmailClientStdError::MailboxNotFound(from.to_string()))?;
                let dst_id = mq
                    .mailboxes
                    .iter()
                    .find(|m| m.name.as_deref() == Some(to))
                    .and_then(|m| m.id.clone())
                    .ok_or_else(|| EmailClientStdError::MailboxNotFound(to.to_string()))?;

                let mut args = JmapEmailSetArgs::default();
                for id in ids {
                    args.remove_from_mailbox(id.to_string(), src_id.clone());
                    args.add_to_mailbox(id.to_string(), dst_id.clone());
                }

                let _ = client.email_set(args)?;

                Ok(())
            }
            #[cfg(feature = "maildir")]
            Self::Maildir(client) => {
                use crate::maildir::convert::open_maildir;

                let src = open_maildir(client, from)?;
                let dst = open_maildir(client, to)?;

                for id in ids {
                    client.r#move(*id, src.clone(), dst.clone(), None)?;
                }

                Ok(())
            }
            #[cfg(feature = "smtp")]
            Self::Smtp(_) => Err(EmailClientStdError::UnsupportedOperation),
        }
    }

    /// Sends a raw RFC 5322 message.
    ///
    /// `from` is the envelope sender (bare `local@domain`, no angle
    /// brackets); `to` is the non-empty list of envelope recipients.
    /// Envelope addresses are independent of the `From:` / `To:` /
    /// `Cc:` / `Bcc:` headers inside `raw` and govern actual routing.
    pub fn send_message(
        &mut self,
        raw: Vec<u8>,
        from: &str,
        to: &[&str],
    ) -> Result<(), EmailClientStdError> {
        trace!("send message with {self:?}");

        match self {
            #[cfg(feature = "imap")]
            Self::Imap(_) => Err(EmailClientStdError::UnsupportedOperation),
            #[cfg(feature = "maildir")]
            Self::Maildir(_) => Err(EmailClientStdError::UnsupportedOperation),
            #[cfg(feature = "smtp")]
            Self::Smtp(client) => {
                use io_smtp::rfc5321::types::{
                    forward_path::ForwardPath, reverse_path::ReversePath,
                };

                use crate::smtp::convert::parse_mailbox;

                if to.is_empty() {
                    return Err(EmailClientStdError::MissingInput("to"));
                }

                let reverse_path = ReversePath::Mailbox(parse_mailbox(from)?);
                let forward_paths: Vec<ForwardPath<'static>> = to
                    .iter()
                    .map(|addr| parse_mailbox(addr).map(ForwardPath))
                    .collect::<Result<_, _>>()?;

                client.send(reverse_path, forward_paths, raw)?;

                Ok(())
            }
            #[cfg(feature = "jmap")]
            Self::Jmap(client) => {
                use alloc::{collections::BTreeMap, string::ToString};

                use io_jmap::{
                    client::JmapClientStdError,
                    rfc8621::{
                        capabilities,
                        email::EmailImport,
                        email_submission::{
                            EmailAddressWithParameters, EmailSubmissionCreate, Envelope,
                        },
                        mailbox::{MailboxFilter, MailboxRole},
                    },
                };
                use url::Url;

                if to.is_empty() {
                    return Err(EmailClientStdError::MissingInput("to"));
                }

                let identity_id = client
                    .identity_get(None)?
                    .identities
                    .into_iter()
                    .find(|i| i.email == from)
                    .map(|i| i.id)
                    .ok_or_else(|| EmailClientStdError::IdentityNotFound(from.to_string()))?;

                let drafts_id = client
                    .mailbox_query(
                        Some(MailboxFilter {
                            role: Some(MailboxRole::Drafts),
                            ..MailboxFilter::default()
                        }),
                        None,
                        None,
                        None,
                        None,
                    )?
                    .mailboxes
                    .into_iter()
                    .next()
                    .and_then(|m| m.id)
                    .ok_or_else(|| EmailClientStdError::MailboxNotFound("drafts".to_string()))?;

                let session = client
                    .session()
                    .ok_or(JmapClientStdError::MissingSession)?
                    .clone();
                let account_id = session
                    .primary_accounts
                    .get(capabilities::MAIL)
                    .cloned()
                    .unwrap_or_default();
                let upload_url_str = session.upload_url.replace("{accountId}", &account_id);
                let upload_url = Url::parse(&upload_url_str)
                    .map_err(|_| EmailClientStdError::InvalidUrl(upload_url_str))?;
                let blob = client.blob_upload(&upload_url, "message/rfc822", raw)?;

                let mut mailbox_ids = BTreeMap::new();
                mailbox_ids.insert(drafts_id, true);

                let mut imports = BTreeMap::new();
                imports.insert(
                    "new".to_string(),
                    EmailImport {
                        blob_id: blob.blob_id,
                        mailbox_ids,
                        keywords: None,
                        received_at: None,
                    },
                );

                let import = client.email_import(imports)?;
                if !import.not_created.is_empty() || import.created.is_empty() {
                    return Err(EmailClientStdError::OperationFailed("import"));
                }
                let email_id = import
                    .created
                    .values()
                    .next()
                    .and_then(|e| e.id.clone())
                    .ok_or(EmailClientStdError::OperationFailed("import"))?;

                let envelope = Envelope {
                    mail_from: EmailAddressWithParameters {
                        email: from.to_string(),
                        parameters: None,
                    },
                    rcpt_to: to
                        .iter()
                        .map(|addr| EmailAddressWithParameters {
                            email: (*addr).to_string(),
                            parameters: None,
                        })
                        .collect(),
                };

                let mut submissions = BTreeMap::new();
                submissions.insert(
                    "send".to_string(),
                    EmailSubmissionCreate {
                        identity_id,
                        email_id,
                        envelope: Some(envelope),
                    },
                );

                let submission = client.email_submission_set(submissions)?;
                if !submission.not_created.is_empty() || submission.created.is_empty() {
                    return Err(EmailClientStdError::OperationFailed("submission"));
                }

                Ok(())
            }
        }
    }
}
