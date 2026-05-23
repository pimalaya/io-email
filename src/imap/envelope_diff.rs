//! IMAP incremental envelope fetch (CONDSTORE / QRESYNC).
//!
//! Helpers used by [`crate::client::EmailClientStd::diff_envelopes`]
//! to encode the opaque state blob, build the QRESYNC SELECT
//! parameter, scope the follow-up UID FETCH against the highest known
//! UID, and translate QRESYNC's implicit FETCH payload into
//! [`crate::envelope::FlagUpdate`].

use core::num::NonZeroU32;

use alloc::{
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

/// Wire size of [`ImapState::encode`] / [`ImapState::decode`]:
/// `uid_validity` (u32 LE) + `highest_mod_seq` (u64 LE) +
/// `highest_uid` (u32 LE).
const STATE_BYTES: usize = 4 + 8 + 4;

/// Decoded IMAP checkpoint. `uid_validity` and `highest_mod_seq` feed
/// `SELECT (QRESYNC ...)`; `highest_uid` scopes the follow-up
/// `UID FETCH <high+1>:*` for newly added messages.
#[derive(Clone, Copy, Debug, Default)]
pub struct ImapState {
    pub uid_validity: u32,
    pub highest_mod_seq: u64,
    pub highest_uid: u32,
}

impl ImapState {
    /// Decodes an opaque byte blob produced by [`Self::encode`].
    /// Returns `None` when the length is wrong; callers should treat
    /// that as "no usable state" and fall through to a full list.
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

    /// Encodes the checkpoint into a 16-byte little-endian blob. The
    /// engine stores this opaquely; only this module reads it.
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(STATE_BYTES);
        out.extend_from_slice(&self.uid_validity.to_le_bytes());
        out.extend_from_slice(&self.highest_mod_seq.to_le_bytes());
        out.extend_from_slice(&self.highest_uid.to_le_bytes());
        out
    }
}

/// FETCH items for the follow-up `UID FETCH <high+1>:*`, the same set
/// used by [`crate::imap::envelope_list::build_item_names`] minus
/// `BodyStructure` (incremental sync does not care about attachment
/// detection).
pub fn new_message_item_names() -> MacroOrMessageDataItemNames<'static> {
    MacroOrMessageDataItemNames::MessageDataItemNames(vec![
        MessageDataItemName::Uid,
        MessageDataItemName::Flags,
        MessageDataItemName::Envelope,
        MessageDataItemName::Rfc822Size,
    ])
}

/// IMAP UID sequence-set spelling `(high+1):*`. Returns `None` when
/// `high.checked_add(1)` overflows (a 4-billion-UID mailbox is not
/// our problem).
pub fn new_message_window(high: u32) -> Option<String> {
    let start = high.checked_add(1)?;
    Some(format!("{start}:*"))
}

/// Translates one QRESYNC implicit `* FETCH` payload into a
/// [`FlagUpdate`]. Returns `None` when neither UID nor FLAGS were
/// surfaced (an unsupported response shape, not a fatal error).
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

/// Builds an [`crate::envelope::Envelope`] from a raw FETCH item list.
/// Thin wrapper around [`envelope_from`] so the changes path does not
/// need to import private symbols.
pub fn envelope_from_items(items: Vec<MessageDataItem<'static>>) -> Envelope {
    envelope_from(0, items)
}
