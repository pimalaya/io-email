//! Conversions between IMAP wire types and the shared LCD types.
//!
//! Each helper returns a typed "invalid input" marker so each
//! coroutine can fold it into its own error enum.

use alloc::{
    string::{String, ToString},
    vec::Vec,
};
use core::num::NonZeroU32;

use io_imap::types::{
    core::Atom, flag::Flag as ImapFlag, mailbox::Mailbox as ImapMailbox, sequence::SequenceSet,
};

use crate::flag::types::{Flag, IanaFlag};

/// `name` could not be encoded as an IMAP mailbox token.
#[derive(Debug)]
pub struct InvalidMailboxName(pub String);

/// `ids` was empty, contained a non-`u32`, or could not assemble into
/// a [`SequenceSet`].
#[derive(Debug)]
pub enum InvalidUidSet {
    Empty,
    Invalid(String),
}

/// Parses a shared mailbox name into an IMAP Mailbox token.
pub fn parse_mailbox(name: &str) -> Result<ImapMailbox<'static>, InvalidMailboxName> {
    String::from(name)
        .try_into()
        .map_err(|_| InvalidMailboxName(name.to_string()))
}

/// Parses a list of stringified UIDs into an IMAP [`SequenceSet`].
pub fn parse_uids(ids: &[&str]) -> Result<SequenceSet, InvalidUidSet> {
    if ids.is_empty() {
        return Err(InvalidUidSet::Empty);
    }

    let uids: Vec<NonZeroU32> = ids
        .iter()
        .map(|s| {
            s.parse::<NonZeroU32>()
                .map_err(|_| InvalidUidSet::Invalid((*s).to_string()))
        })
        .collect::<Result<_, _>>()?;

    SequenceSet::try_from(uids).map_err(|_| InvalidUidSet::Empty)
}

/// Maps a shared [`Flag`] to its IMAP wire counterpart.
///
/// IANA flags become the matching system flag; custom keywords pass
/// through as Keyword atoms, with a sanitised fallback when the raw
/// spelling is not atom-safe.
pub fn flag_from(flag: &Flag) -> ImapFlag<'static> {
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

/// Replaces every non-atom-safe byte with `_` so a keyword with
/// spaces, controls or `()<>{}` survives IMAP STORE.
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
