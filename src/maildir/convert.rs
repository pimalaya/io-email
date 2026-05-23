//! Conversions between Maildir filesystem types and the shared types
//! used by [`EmailClientStd`], plus the `From` impl that wraps an
//! already-built [`MaildirClient`] into a fresh unified client with
//! Maildir as the only registered backend.

use alloc::vec::Vec;

use io_maildir::{client::MaildirClient, flag::Flag as MdFlag, maildir::Maildir};

use crate::{
    client::{EmailClientStd, EmailClientStdError},
    envelope::Envelope,
    flag::Flag,
};

impl From<MaildirClient> for EmailClientStd {
    fn from(client: MaildirClient) -> Self {
        Self::new().with_maildir(client)
    }
}

/// Maps a shared [`Flag`] to its Maildir info-section letter.
///
/// Returns `None` for IANA keywords that have no Maildir letter
/// (`Forwarded`, `Junk`, custom) and for user-defined custom
/// keywords. Callers drop unmapped flags or persist them through a
/// sidecar.
pub(crate) fn flag_to_maildir(flag: &Flag) -> Option<MdFlag> {
    use crate::flag::IanaFlag;

    match flag.iana()? {
        IanaFlag::Seen => Some(MdFlag::Seen),
        IanaFlag::Answered => Some(MdFlag::Replied),
        IanaFlag::Flagged => Some(MdFlag::Flagged),
        IanaFlag::Draft => Some(MdFlag::Draft),
        IanaFlag::Deleted => Some(MdFlag::Trashed),
        _ => None,
    }
}

/// Builds a shared [`Flag`] from a Maildir info-section letter.
///
/// Maildir letters have no wire casing, so the canonical IANA
/// spelling is synthesised via [`Flag::iana`]. Returns `None` for
/// letters outside the standard six.
pub(crate) fn flag_from_char(c: char) -> Option<Flag> {
    use crate::flag::IanaFlag;

    match c {
        'S' => Some(Flag::from_iana(IanaFlag::Seen)),
        'R' => Some(Flag::from_iana(IanaFlag::Answered)),
        'F' => Some(Flag::from_iana(IanaFlag::Flagged)),
        'D' => Some(Flag::from_iana(IanaFlag::Draft)),
        'T' => Some(Flag::from_iana(IanaFlag::Deleted)),
        'P' => Some(Flag::from_iana(IanaFlag::Forwarded)),
        _ => None,
    }
}

/// Opens the Maildir at `<client.root()>/<name>`, returning
/// [`EmailClientStdError::InvalidMailbox`] when the path does not
/// point at a valid Maildir layout.
pub(crate) fn open_maildir(
    client: &MaildirClient,
    name: &str,
) -> Result<Maildir, EmailClientStdError> {
    let path = client.root().join(name);
    client
        .load_maildir(path.clone())
        .map_err(|_| EmailClientStdError::InvalidMailbox(path.into_string()))
}

/// 1-indexed pagination on an in-memory list. `page_size = None`
/// returns the full slice; `page_size = 0` or a page past the end
/// returns an empty vector.
pub(crate) fn paginate(
    envelopes: Vec<Envelope>,
    page: Option<u32>,
    page_size: Option<u32>,
) -> Vec<Envelope> {
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
