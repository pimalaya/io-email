//! Helpers for incremental envelope sync (CONDSTORE / QRESYNC) used
//! by [`crate::client::EmailClientStd::diff_envelopes`].

use core::num::NonZeroU32;

use alloc::{
    format,
    string::{String, ToString},
    vec,
    vec::Vec,
};

use io_imap::types::{
    fetch::{MacroOrMessageDataItemNames, MessageDataItem, MessageDataItemName},
    flag::FlagFetch,
};

use crate::{
    envelope::{Envelope, FlagUpdate},
    flag::Flag,
    imap::envelope_list::envelope_from,
};

/// Wire size of an [`ImapState`]: uid_validity (u32) + highest_mod_seq
/// (u64) + highest_uid (u32), little-endian.
const STATE_BYTES: usize = 4 + 8 + 4;

/// IMAP sync checkpoint: uid_validity + highest_mod_seq feed SELECT
/// (QRESYNC ...); highest_uid scopes the follow-up UID FETCH
/// high+1:*.
#[derive(Clone, Copy, Debug, Default)]
pub struct ImapState {
    pub uid_validity: u32,
    pub highest_mod_seq: u64,
    pub highest_uid: u32,
}

impl ImapState {
    /// Decodes a byte blob produced by [`Self::encode`]; `None` on
    /// length mismatch (treat as no usable state).
    pub fn decode(bytes: &[u8]) -> Option<Self> {
        if bytes.len() != STATE_BYTES {
            return None;
        }

        let uid_validity = u32::from_le_bytes(bytes[0..4].try_into().ok()?);
        let highest_mod_seq = u64::from_le_bytes(bytes[4..12].try_into().ok()?);
        let highest_uid = u32::from_le_bytes(bytes[12..16].try_into().ok()?);

        Some(Self {
            uid_validity,
            highest_mod_seq,
            highest_uid,
        })
    }

    /// Encodes the checkpoint into a 16-byte little-endian blob.
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(STATE_BYTES);
        out.extend_from_slice(&self.uid_validity.to_le_bytes());
        out.extend_from_slice(&self.highest_mod_seq.to_le_bytes());
        out.extend_from_slice(&self.highest_uid.to_le_bytes());
        out
    }
}

/// FETCH items for the follow-up UID FETCH high+1:*; same as
/// [`crate::imap::envelope_list::build_item_names`] without
/// BodyStructure.
pub fn new_message_item_names() -> MacroOrMessageDataItemNames<'static> {
    MacroOrMessageDataItemNames::MessageDataItemNames(vec![
        MessageDataItemName::Uid,
        MessageDataItemName::Flags,
        MessageDataItemName::Envelope,
        MessageDataItemName::Rfc822Size,
    ])
}

/// UID sequence-set high+1:*; `None` on overflow.
pub fn new_message_window(high: u32) -> Option<String> {
    let start = high.checked_add(1)?;
    Some(format!("{start}:*"))
}

/// Translates one QRESYNC implicit `* FETCH` payload into a
/// [`FlagUpdate`]; `None` when neither UID nor FLAGS were surfaced.
pub fn flag_update_from_items(items: &[MessageDataItem<'static>]) -> Option<FlagUpdate> {
    let mut uid: Option<NonZeroU32> = None;
    let mut flags: Option<alloc::collections::BTreeSet<Flag>> = None;

    for item in items {
        match item {
            MessageDataItem::Uid(u) => uid = Some(*u),
            MessageDataItem::Flags(fs) => {
                flags = Some(
                    fs.iter()
                        .filter_map(|f| match f {
                            FlagFetch::Flag(flag) => Some(Flag::from_raw(flag.to_string())),
                            _ => None,
                        })
                        .collect(),
                );
            }
            _ => {}
        }
    }

    Some(FlagUpdate {
        id: uid?.get().to_string(),
        flags: flags?,
    })
}

/// Builds an [`crate::envelope::Envelope`] from a FETCH item list;
/// thin wrapper over [`envelope_from`].
pub fn envelope_from_items(items: Vec<MessageDataItem<'static>>) -> Envelope {
    envelope_from(0, items)
}
