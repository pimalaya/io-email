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

/// Maps a shared [`Flag`] to its Maildir flag counterpart.
pub(crate) fn flag_from(flag: &Flag) -> MdFlag {
    match flag {
        Flag::Seen => MdFlag::Seen,
        Flag::Answered => MdFlag::Replied,
        Flag::Flagged => MdFlag::Flagged,
        Flag::Draft => MdFlag::Draft,
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
    Maildir::try_from(path.as_path())
        .map_err(|_| EmailClientStdError::InvalidMailbox(path.to_string_lossy().into_owned()))
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
