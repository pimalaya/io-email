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
    types::{
        core::Atom, flag::Flag as ImapFlag, mailbox::Mailbox as ImapMailbox, sequence::SequenceSet,
    },
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

/// Maps a shared [`Flag`] to its IMAP wire counterpart.
///
/// IANA-classified flags become the matching system flag (or
/// `\Forwarded`-style extension) at their canonical wire spelling;
/// custom user keywords pass through as IMAP `Keyword` atoms when the
/// raw bytes are atom-valid, falling back to a stripped lowercase
/// reconstruction otherwise.
pub(crate) fn flag_from(flag: &Flag) -> ImapFlag<'static> {
    use crate::flag::IanaFlag;

    match flag.iana() {
        Some(IanaFlag::Seen) => ImapFlag::Seen,
        Some(IanaFlag::Answered) => ImapFlag::Answered,
        Some(IanaFlag::Flagged) => ImapFlag::Flagged,
        Some(IanaFlag::Draft) => ImapFlag::Draft,
        Some(IanaFlag::Deleted) => ImapFlag::Deleted,
        Some(_) => ImapFlag::keyword(
            Atom::try_from(String::from(flag.raw()))
                .expect("canonical IANA keyword is a valid IMAP atom"),
        ),
        None => match Atom::try_from(String::from(flag.raw())) {
            Ok(atom) => ImapFlag::keyword(atom),
            Err(_) => ImapFlag::keyword(
                Atom::try_from(sanitise_atom(flag.raw()))
                    .expect("sanitised atom contains only atom-safe ASCII"),
            ),
        },
    }
}

/// Replaces every non-atom-safe byte with `_` so a custom keyword that
/// contains spaces, control bytes or `()<>{}` survives round-tripping
/// through `IMAP STORE`. Lossy by design; callers are expected to
/// preserve the original spelling on the round-trip side (m2dir
/// sidecar, JMAP wire keyword) when fidelity matters.
fn sanitise_atom(raw: &str) -> String {
    raw.chars()
        .map(|c| {
            if c.is_ascii()
                && !c.is_control()
                && !matches!(
                    c,
                    ' ' | '(' | ')' | '{' | '%' | '*' | '"' | '\\' | ']' | '\x7f'
                )
            {
                c
            } else {
                '_'
            }
        })
        .collect()
}
