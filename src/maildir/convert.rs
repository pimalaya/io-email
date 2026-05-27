//! Conversions between Maildir filesystem types and the shared types
//! used by [`EmailClientStd`], plus the `From` impl that wraps an
//! already-built [`MaildirClient`] into a fresh unified client with
//! Maildir as the only registered backend.

use alloc::{
    string::{String, ToString},
    vec::Vec,
};

use io_maildir::{client::MaildirClient, flag::MaildirFlag as MdFlag, maildir::Maildir};

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

/// Maps a shared [`Flag`] to a [`MdFlag`].
///
/// IANA-registered keywords without a Maildir info-section letter
/// (`Forwarded`, `Junk`, custom) become [`MdFlag::Keyword`] carrying
/// the wire spelling; the MaildirClient then ferries them through the
/// dovecot-keywords file or a configured header. Returns `None` only
/// when the flag has no usable representation at all.
pub(crate) fn flag_to_maildir(flag: &Flag) -> Option<MdFlag> {
    use crate::flag::IanaFlag;

    match flag.iana() {
        Some(IanaFlag::Seen) => Some(MdFlag::Seen),
        Some(IanaFlag::Answered) => Some(MdFlag::Replied),
        Some(IanaFlag::Flagged) => Some(MdFlag::Flagged),
        Some(IanaFlag::Draft) => Some(MdFlag::Draft),
        Some(IanaFlag::Deleted) => Some(MdFlag::Trashed),
        Some(IanaFlag::Forwarded) => Some(MdFlag::Passed),
        Some(_) | None => Some(MdFlag::Keyword(flag.raw().to_string())),
    }
}

/// Builds a shared [`Flag`] from a Maildir info-section letter.
///
/// Maildir letters have no wire casing, so the canonical IANA
/// spelling is synthesised via [`Flag::from_iana`]. Returns `None`
/// for letters outside the standard six.
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

/// Builds a shared [`Flag`] from a [`MdFlag`]. Keyword variants
/// preserve the wire spelling verbatim through [`Flag::from_raw`].
pub(crate) fn flag_from_maildir(flag: &MdFlag) -> Option<Flag> {
    use crate::flag::IanaFlag;

    match flag {
        MdFlag::Seen => Some(Flag::from_iana(IanaFlag::Seen)),
        MdFlag::Replied => Some(Flag::from_iana(IanaFlag::Answered)),
        MdFlag::Flagged => Some(Flag::from_iana(IanaFlag::Flagged)),
        MdFlag::Draft => Some(Flag::from_iana(IanaFlag::Draft)),
        MdFlag::Trashed => Some(Flag::from_iana(IanaFlag::Deleted)),
        MdFlag::Passed => Some(Flag::from_iana(IanaFlag::Forwarded)),
        MdFlag::Keyword(raw) => Some(Flag::from_raw(raw.clone())),
    }
}

/// Opens the Maildir at `<client.root()>/<physical_name>`, where the
/// physical name follows the configured layout:
///
/// - default Maildir: identity (`Work` → `Work/`);
/// - Maildir++ (`client.maildir_plus = true`): logical `/`-separated
///   names are translated to dotted siblings with a leading dot
///   (`Work/Foo` → `.Work.Foo/`).
///
/// Returns [`EmailClientStdError::InvalidMailbox`] when the path does
/// not point at a valid Maildir layout.
pub(crate) fn open_maildir(
    client: &MaildirClient,
    name: &str,
) -> Result<Maildir, EmailClientStdError> {
    let physical = physical_mailbox_name(client, name)?;
    let path = if physical.is_empty() {
        client.root().clone()
    } else {
        client.root().join(&physical)
    };
    client
        .load_maildir(path.clone())
        .map_err(|_| EmailClientStdError::InvalidMailbox(path.into_string()))
}

/// Translates a shared mailbox name into the physical directory name
/// honoured by the underlying filesystem. Returns the empty string
/// when `name` denotes the root Maildir (Maildir++ inbox alias).
pub(crate) fn physical_mailbox_name(
    client: &MaildirClient,
    name: &str,
) -> Result<String, EmailClientStdError> {
    if client.maildir_plus && name == client.maildirpp_inbox {
        return Ok(String::new());
    }

    if client.maildir_plus {
        let trimmed = name.trim_matches('/');
        if trimmed.is_empty() {
            return Err(EmailClientStdError::InvalidMailbox(name.into()));
        }

        let mut physical = String::from(".");
        physical.push_str(&trimmed.replace('/', "."));
        return Ok(physical);
    }

    if client.fs_layout {
        let trimmed = name.trim_matches('/');
        if trimmed.is_empty() || trimmed.contains("..") {
            return Err(EmailClientStdError::InvalidMailbox(name.into()));
        }
        return Ok(trimmed.to_string());
    }

    if name.contains('/') {
        return Err(EmailClientStdError::InvalidMailbox(name.into()));
    }
    Ok(name.to_string())
}

/// Reverse of [`physical_mailbox_name`]: derives the logical mailbox
/// name from a physical directory name reported by `list_maildirs`.
/// `physical` empty (or matching the root path) maps to the Maildir++
/// inbox alias.
pub(crate) fn logical_mailbox_name(client: &MaildirClient, physical: &str) -> String {
    if client.maildir_plus && physical.is_empty() {
        return client.maildirpp_inbox.clone();
    }

    if !client.maildir_plus {
        return physical.to_string();
    }

    let stripped = physical.strip_prefix('.').unwrap_or(physical);
    stripped.replace('.', "/")
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
