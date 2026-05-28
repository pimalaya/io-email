//! IMAP backend implementations of the [`EmailClientStd`] private
//! dispatch methods. The public surface lives in [`crate::client`];
//! each `<op>_imap` method here is `pub(crate)` so that dispatcher
//! can route the call.

use core::num::NonZeroU32;

#[cfg(feature = "search")]
use alloc::collections::BTreeMap;
use alloc::{
    format,
    string::{String, ToString},
    vec,
    vec::Vec,
};

use io_imap::types::{
    core::{AString, Literal, Vec1},
    extensions::binary::LiteralOrLiteral8,
    fetch::{MacroOrMessageDataItemNames, MessageDataItem, MessageDataItemName},
    flag::StoreType,
    mailbox::Mailbox as ImapMailbox,
    response::Capability,
    search::SearchKey,
    sequence::SequenceSet,
    status::{StatusDataItem, StatusDataItemName},
};
use log::trace;
use mail_parser::MessageParser;
use pimalaya_stream::std::stream::StreamStd;
use uuid::Uuid;

#[cfg(feature = "search")]
use crate::search::query::SearchEmailsQuery;
use crate::{
    client::{EmailClientStd, EmailClientStdError},
    envelope::{Envelope, EnvelopeDiff, FlagUpdate},
    flag::{Flag, FlagOp},
    imap::{
        convert::{flag_from, parse_mailbox, parse_uids},
        envelope_diff::{
            ImapState, envelope_from_items, flag_update_from_items, new_message_item_names,
            new_message_window,
        },
        envelope_list::{build_item_names, compute_window, envelope_from},
    },
    mailbox::Mailbox,
};

#[cfg(feature = "search")]
use crate::imap::envelope_search::{paginate_uids, search_keys, sort_criteria};

impl EmailClientStd {
    /// Registers the IMAP backend. First `with_*` call wins for any
    /// op IMAP supports; reassigning leaves the priority position
    /// unchanged.
    pub fn with_imap(mut self, client: io_imap::client::ImapClientStd<StreamStd>) -> Self {
        self.imap = Some(client);
        if !self.order.contains(&crate::client::BackendKind::Imap) {
            self.order.push(crate::client::BackendKind::Imap);
        }
        self
    }

    /// Borrows the underlying IMAP client when registered. Reach for
    /// this only when the LCD dispatcher cannot express what you need
    /// (CONDSTORE / QRESYNC fast-paths, IDLE, etc.); code that
    /// consumes the accessor is by construction not portable across
    /// backends.
    pub fn as_imap(&self) -> Option<&io_imap::client::ImapClientStd<StreamStd>> {
        self.imap.as_ref()
    }

    /// Mutable variant of [`Self::as_imap`].
    pub fn as_imap_mut(&mut self) -> Option<&mut io_imap::client::ImapClientStd<StreamStd>> {
        self.imap.as_mut()
    }

    pub(crate) fn imap_list_mailboxes(
        &mut self,
        with_counts: bool,
    ) -> Result<Vec<Mailbox>, EmailClientStdError> {
        let client = self.imap.as_mut().expect("imap slot registered");

        let mut mailboxes: Vec<_> = client
            .list("".try_into().unwrap(), "*".try_into().unwrap())?
            .into_iter()
            .map(Mailbox::from)
            .collect();

        if with_counts {
            let items: Vec<StatusDataItemName> =
                vec![StatusDataItemName::Messages, StatusDataItemName::Unseen];

            for mailbox in &mut mailboxes {
                let mbox: ImapMailbox<'static> = mailbox
                    .id
                    .clone()
                    .try_into()
                    .map_err(|_| EmailClientStdError::InvalidMailbox(mailbox.id.clone()))?;

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

    pub(crate) fn imap_list_envelopes(
        &mut self,
        mailbox: &str,
        page: Option<u32>,
        page_size: Option<u32>,
        with_attachment: bool,
    ) -> Result<Vec<Envelope>, EmailClientStdError> {
        let client = self.imap.as_mut().expect("imap slot registered");

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

    #[cfg(feature = "search")]
    pub(crate) fn imap_search_envelopes(
        &mut self,
        mailbox: &str,
        query: Option<&SearchEmailsQuery>,
        page: Option<u32>,
        page_size: Option<u32>,
        with_attachment: bool,
    ) -> Result<Vec<Envelope>, EmailClientStdError> {
        let client = self.imap.as_mut().expect("imap slot registered");

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

    pub(crate) fn imap_store_flags(
        &mut self,
        mailbox: &str,
        ids: &[&str],
        flags: &[Flag],
        op: FlagOp,
    ) -> Result<(), EmailClientStdError> {
        let client = self.imap.as_mut().expect("imap slot registered");

        if client.auto_select {
            let mbox = parse_mailbox(mailbox)?;
            client.select(mbox)?;
        }

        let sequence_set = parse_uids(ids)?;
        let imap_flags: Vec<_> = flags.iter().map(flag_from).collect();
        let kind = match op {
            FlagOp::Add => StoreType::Add,
            FlagOp::Set => StoreType::Replace,
            FlagOp::Remove => StoreType::Remove,
        };

        client.store(sequence_set, kind, imap_flags, true)?;

        Ok(())
    }

    pub(crate) fn imap_get_message(
        &mut self,
        mailbox: &str,
        id: &str,
    ) -> Result<Vec<u8>, EmailClientStdError> {
        let client = self.imap.as_mut().expect("imap slot registered");

        if client.auto_select {
            let mbox = parse_mailbox(mailbox)?;
            client.select(mbox)?;
        }

        let sequence_set = parse_uids(&[id])?;
        let item_names =
            MacroOrMessageDataItemNames::MessageDataItemNames(vec![MessageDataItemName::BodyExt {
                section: None,
                partial: None,
                peek: true,
            }]);

        let data = client.fetch(sequence_set, item_names, true)?;

        data.into_values()
            .flat_map(|items| items.into_inner().into_iter())
            .find_map(|item| match item {
                MessageDataItem::BodyExt { data, .. } => data.0.map(|d| d.as_ref().to_vec()),
                _ => None,
            })
            .ok_or(EmailClientStdError::EmptyMessageBody)
    }

    pub(crate) fn imap_add_message(
        &mut self,
        mailbox: &str,
        flags: &[Flag],
        mut raw: Vec<u8>,
    ) -> Result<String, EmailClientStdError> {
        let client = self.imap.as_mut().expect("imap slot registered");

        let imap_flags: Vec<_> = flags.iter().map(flag_from).collect();

        // Extract or synthesize a Message-ID up front: needed as a
        // fallback to recover the UID on servers that don't advertise
        // UIDPLUS (no APPENDUID response code, RFC 4315).
        let parsed_id = MessageParser::default()
            .parse_headers(&raw)
            .and_then(|m| m.message_id().map(ToString::to_string))
            .filter(|s| !s.is_empty());

        let message_id = match parsed_id {
            Some(id) => id,
            None => {
                let generated = format!("{}@io-email.invalid", Uuid::new_v4());
                trace!("appended message had no Message-ID; injected <{generated}>");
                let header = format!("Message-ID: <{generated}>\r\n");
                raw.splice(0..0, header.bytes());
                generated
            }
        };

        let literal = Literal::try_from(raw)
            .map_err(|e| EmailClientStdError::InvalidMessageContent(e.to_string()))?;
        let message = LiteralOrLiteral8::Literal(literal);

        let mbox = parse_mailbox(mailbox)?;
        let (_, appenduid) = client.append(mbox, imap_flags, None, message)?;

        if let Some((_, uid)) = appenduid {
            return Ok(uid.to_string());
        }

        // No UIDPLUS: select the mailbox and recover the UID via
        // `UID SEARCH HEADER Message-ID`. If duplicates exist (rare),
        // take the highest UID (newest append).
        let mbox = parse_mailbox(mailbox)?;
        client.select(mbox)?;

        let field = AString::try_from("Message-ID")
            .map_err(|_| EmailClientStdError::OperationFailed("imap header field"))?;
        let value = AString::try_from(message_id)
            .map_err(|_| EmailClientStdError::OperationFailed("imap header value"))?;
        let criteria = Vec1::from(SearchKey::Header(field, value));
        let uids = client.search(criteria, true)?;

        let uid = uids
            .into_iter()
            .max()
            .ok_or(EmailClientStdError::OperationFailed("append uid lookup"))?;

        Ok(uid.to_string())
    }

    pub(crate) fn imap_create_mailbox(&mut self, name: &str) -> Result<(), EmailClientStdError> {
        let client = self.imap.as_mut().expect("imap slot registered");
        let mbox = parse_mailbox(name)?;
        client.create(mbox)?;
        Ok(())
    }

    pub(crate) fn imap_delete_mailbox(&mut self, name: &str) -> Result<(), EmailClientStdError> {
        let client = self.imap.as_mut().expect("imap slot registered");
        let mbox = parse_mailbox(name)?;
        client.delete(mbox)?;
        Ok(())
    }

    pub(crate) fn imap_delete_message(
        &mut self,
        mailbox: &str,
        id: &str,
    ) -> Result<(), EmailClientStdError> {
        let client = self.imap.as_mut().expect("imap slot registered");

        if client.auto_select {
            let mbox = parse_mailbox(mailbox)?;
            client.select(mbox)?;
        }

        let sequence_set = parse_uids(&[id])?;
        let imap_flags = vec![flag_from(&Flag::from_iana(crate::flag::IanaFlag::Deleted))];
        client.store(sequence_set, StoreType::Add, imap_flags, true)?;
        client.expunge()?;
        Ok(())
    }

    pub(crate) fn imap_copy_messages(
        &mut self,
        from: &str,
        to: &str,
        ids: &[&str],
    ) -> Result<(), EmailClientStdError> {
        let client = self.imap.as_mut().expect("imap slot registered");

        let dst = parse_mailbox(to)?;
        if client.auto_select {
            let src = parse_mailbox(from)?;
            client.select(src)?;
        }

        let sequence_set = parse_uids(ids)?;
        client.copy(sequence_set, dst, true)?;

        Ok(())
    }

    pub(crate) fn imap_move_messages(
        &mut self,
        from: &str,
        to: &str,
        ids: &[&str],
    ) -> Result<(), EmailClientStdError> {
        let client = self.imap.as_mut().expect("imap slot registered");

        let dst = parse_mailbox(to)?;
        if client.auto_select {
            let src = parse_mailbox(from)?;
            client.select(src)?;
        }

        let sequence_set = parse_uids(ids)?;
        client.r#move(sequence_set, dst, true)?;

        Ok(())
    }

    pub(crate) fn imap_watch_envelopes(
        self,
        mailbox: String,
    ) -> Result<crate::watch::WatchStream, EmailClientStdError> {
        let client = self
            .imap
            .expect("imap slot registered (checked by dispatcher)");
        crate::imap::watch::watch_envelopes(client, mailbox)
    }

    pub(crate) fn imap_diff_envelopes(
        &mut self,
        mailbox: &str,
        state: Option<&[u8]>,
    ) -> Result<EnvelopeDiff, EmailClientStdError> {
        let client = self.imap.as_mut().expect("imap slot registered");
        let mbox = parse_mailbox(mailbox)?;

        let cached = state.and_then(ImapState::decode);

        // Without QRESYNC there is no CONDSTORE-based delta path on
        // this client. Surface a hint to fall back to a full list, but
        // skip the baseline-capture roundtrip (we cannot encode a
        // meaningful checkpoint without HIGHESTMODSEQ).
        let supports_qresync = client
            .capabilities()
            .map(|caps| caps.contains(&Capability::QResync))
            .unwrap_or(false);
        if !supports_qresync {
            return Ok(EnvelopeDiff::FullListRequired { new_state: None });
        }

        // First sync or unusable cached state: SELECT, fetch the
        // highest UID via UID FETCH `*`, and capture the new
        // checkpoint for next time.
        let Some(cached) = cached else {
            return imap_baseline(client, mbox);
        };

        let Some(uid_validity_nz) = NonZeroU32::new(cached.uid_validity) else {
            return imap_baseline(client, mbox);
        };

        let select_data =
            match client.select_qresync(mbox.clone(), uid_validity_nz, cached.highest_mod_seq) {
                Ok(data) => data,
                Err(_) => return imap_baseline(client, mbox),
            };

        let server_uid_validity = select_data
            .uid_validity
            .map(NonZeroU32::get)
            .unwrap_or(cached.uid_validity);
        if server_uid_validity != cached.uid_validity {
            return imap_baseline(client, mbox);
        }

        let flag_updates: Vec<FlagUpdate> = select_data
            .changed
            .iter()
            .filter_map(|fetch| flag_update_from_items(fetch.items.as_ref()))
            .collect();

        let vanished_ids: Vec<String> = select_data
            .vanished_earlier
            .iter()
            .map(|uid| uid.get().to_string())
            .collect();

        let mut new_envelopes: Vec<Envelope> = Vec::new();
        if let Some(window) = new_message_window(cached.highest_uid) {
            if let Ok(sequence_set) = SequenceSet::try_from(window.as_str()) {
                let data = client.fetch(sequence_set, new_message_item_names(), true)?;
                new_envelopes = data
                    .into_iter()
                    .map(|(_, items)| envelope_from_items(items.into_inner()))
                    .collect();
            }
        }

        let highest_uid = new_envelopes
            .iter()
            .filter_map(|e| e.id.parse::<u32>().ok())
            .max()
            .unwrap_or(cached.highest_uid);

        let new_highest_mod_seq = select_data
            .highest_mod_seq
            .unwrap_or(cached.highest_mod_seq);

        let new_state = ImapState {
            uid_validity: server_uid_validity,
            highest_mod_seq: new_highest_mod_seq,
            highest_uid,
        }
        .encode();

        Ok(EnvelopeDiff::Incremental {
            new_state,
            flag_updates,
            new_envelopes,
            vanished_ids,
        })
    }
}

/// Captures a fresh IMAP checkpoint from a plain SELECT plus a
/// `UID FETCH *` to read the highest UID. Used on the first sync, when
/// the stored state is unusable, or when UIDVALIDITY bumped.
fn imap_baseline(
    client: &mut io_imap::client::ImapClientStd<pimalaya_stream::std::stream::StreamStd>,
    mbox: ImapMailbox<'static>,
) -> Result<EnvelopeDiff, EmailClientStdError> {
    let select = client.select(mbox)?;
    let Some(uid_validity) = select.uid_validity.map(NonZeroU32::get) else {
        return Ok(EnvelopeDiff::FullListRequired { new_state: None });
    };

    let exists = select.exists.unwrap_or(0);
    let mut highest_uid: u32 = 0;
    if exists > 0 {
        let sequence_set: SequenceSet = "*"
            .try_into()
            .expect("`*` is a valid sequence set spelling");
        let item_names =
            MacroOrMessageDataItemNames::MessageDataItemNames(vec![MessageDataItemName::Uid]);
        let data = client.fetch(sequence_set, item_names, false)?;
        highest_uid = data
            .into_values()
            .flat_map(|items| items.into_inner().into_iter())
            .filter_map(|item| match item {
                MessageDataItem::Uid(u) => Some(u.get()),
                _ => None,
            })
            .max()
            .unwrap_or(0);
    }

    let highest_mod_seq = select.highest_mod_seq.unwrap_or(0);

    let new_state = ImapState {
        uid_validity,
        highest_mod_seq,
        highest_uid,
    }
    .encode();

    Ok(EnvelopeDiff::FullListRequired {
        new_state: Some(new_state),
    })
}
