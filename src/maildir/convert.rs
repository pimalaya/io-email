//! Shared helpers for the Maildir coroutines: flag conversions,
//! mailbox-name to on-disk-path resolution, and `paginate` shared with
//! the m2dir backend.

use alloc::{
    string::{String, ToString},
    vec::Vec,
};
use std::path::{Path, PathBuf};

use io_maildir::flag::{MaildirFlag, MaildirFlags};
use thiserror::Error;

use crate::flag::{Flag, IanaFlag};

/// Errors produced by [`resolve_mailbox`].
#[derive(Clone, Debug, Error)]
#[error("invalid Maildir mailbox `{0}`")]
pub struct InvalidMailboxName(pub String);

/// Translates a logical mailbox name to its on-disk Maildir directory,
/// following the layout configured on the [`MaildirContext`]:
///
/// - classic Maildir (`maildir_plus = false`): identity (`Work` →
///   `<root>/Work/`); slashes are rejected;
/// - Maildir++ (`maildir_plus = true`): `/`-separated names become
///   dotted siblings under the root (`Work/Foo` → `<root>/.Work.Foo/`),
///   and the empty string (or `INBOX`-like default handled by the
///   caller) maps to the root itself.
///
/// [`MaildirContext`]: crate::client::MaildirContext
pub(crate) fn resolve_mailbox(
    root: &Path,
    maildir_plus: bool,
    name: &str,
) -> Result<PathBuf, InvalidMailboxName> {
    if maildir_plus {
        let trimmed = name.trim_matches('/');
        if trimmed.is_empty() {
            return Ok(root.to_path_buf());
        }
        let mut physical = String::from(".");
        physical.push_str(&trimmed.replace('/', "."));
        return Ok(root.join(physical));
    }

    if name.contains('/') || name.contains("..") {
        return Err(InvalidMailboxName(name.to_string()));
    }
    if name.is_empty() {
        return Err(InvalidMailboxName(name.to_string()));
    }
    Ok(root.join(name))
}

/// Maps a shared [`Flag`] onto a [`MaildirFlag`]. Non-IANA keywords
/// flow through as [`MaildirFlag::Keyword`], preserving the wire
/// spelling for the dovecot-keywords sidecar.
pub(crate) fn flag_to_maildir(flag: &Flag) -> MaildirFlag {
    match flag.iana() {
        Some(IanaFlag::Seen) => MaildirFlag::Seen,
        Some(IanaFlag::Answered) => MaildirFlag::Replied,
        Some(IanaFlag::Flagged) => MaildirFlag::Flagged,
        Some(IanaFlag::Draft) => MaildirFlag::Draft,
        Some(IanaFlag::Deleted) => MaildirFlag::Trashed,
        Some(IanaFlag::Forwarded) => MaildirFlag::Passed,
        Some(_) | None => MaildirFlag::Keyword(flag.raw().to_string()),
    }
}

/// Builds a [`MaildirFlags`] set from the shared flag slice for the
/// inner io-maildir coroutines.
pub(crate) fn flags_to_maildir(flags: &[Flag]) -> MaildirFlags {
    flags.iter().map(flag_to_maildir).collect()
}

/// Builds a shared [`Flag`] from a Maildir info-section letter
/// (`S`, `R`, `F`, `D`, `T`, `P`). Returns `None` for letters outside
/// the standard six (dovecot custom-keyword slots `a..z` etc.).
pub(crate) fn flag_from_char(c: char) -> Option<Flag> {
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

/// 1-indexed pagination on an in-memory list. `page_size = None`
/// returns the full slice; `page_size = 0` or a page past the end
/// returns an empty vector. Shared between Maildir and m2dir
/// envelope listings whose backends don't paginate at the filesystem
/// level.
pub(crate) fn paginate<T>(items: Vec<T>, page: Option<u32>, page_size: Option<u32>) -> Vec<T> {
    let Some(size) = page_size else {
        return items;
    };
    if size == 0 {
        return Vec::new();
    }
    let page = page.unwrap_or(1).max(1);
    let skip = ((page - 1) as usize).saturating_mul(size as usize);
    if skip >= items.len() {
        return Vec::new();
    }
    items.into_iter().skip(skip).take(size as usize).collect()
}
