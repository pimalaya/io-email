//! Conversions between IMAP wire types and the shared types used by
//! [`EmailClientStd`], plus the `From` impl that wraps an
//! already-connected [`ImapClientStd<StreamStd>`] into a fresh unified
//! client with IMAP as the only registered backend.

use core::num::NonZeroU32;

use alloc::{
    string::{String, ToString},
    vec::Vec,
};

use io_imap::{
    client::ImapClientStd,
    types::{flag::Flag as ImapFlag, mailbox::Mailbox as ImapMailbox, sequence::SequenceSet},
};
use pimalaya_stream::std::stream::StreamStd;

use crate::{
    client::{EmailClientStd, EmailClientStdError},
    flag::Flag,
};

impl From<ImapClientStd<StreamStd>> for EmailClientStd {
    fn from(client: ImapClientStd<StreamStd>) -> Self {
        Self::new().with_imap(client)
    }
}

/// Parses a shared mailbox name into an IMAP [`Mailbox`].
pub(crate) fn parse_mailbox(name: &str) -> Result<ImapMailbox<'static>, EmailClientStdError> {
    String::from(name)
        .try_into()
        .map_err(|_| EmailClientStdError::InvalidMailbox(name.to_string()))
}

/// Parses a list of stringified UIDs into an IMAP [`SequenceSet`].
/// Returns [`EmailClientStdError::MissingInput`] if `ids` is empty or
/// every entry parses to zero.
pub(crate) fn parse_uids(ids: &[&str]) -> Result<SequenceSet, EmailClientStdError> {
    if ids.is_empty() {
        return Err(EmailClientStdError::MissingInput("ids"));
    }

    let uids: Vec<NonZeroU32> = ids
        .iter()
        .map(|s| {
            s.parse::<NonZeroU32>()
                .map_err(|_| EmailClientStdError::InvalidId((*s).to_string()))
        })
        .collect::<Result<_, _>>()?;

    SequenceSet::try_from(uids).map_err(|_| EmailClientStdError::MissingInput("ids"))
}

/// Maps a shared [`Flag`] to its IMAP system-flag counterpart.
pub(crate) fn flag_from(flag: &Flag) -> ImapFlag<'static> {
    match flag {
        Flag::Seen => ImapFlag::Seen,
        Flag::Answered => ImapFlag::Answered,
        Flag::Flagged => ImapFlag::Flagged,
        Flag::Draft => ImapFlag::Draft,
    }
}
