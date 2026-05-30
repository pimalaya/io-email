//! Shared helpers for the m2dir coroutines: mailbox-name to on-disk
//! path resolution, flag / address conversions used by `envelope_list`,
//! and `paginate` shared with the maildir backend.

use alloc::{
    string::{String, ToString},
    vec::Vec,
};
use std::path::PathBuf;

use chrono::DateTime;
use io_m2dir::{
    entry::M2dirEntry,
    flag::M2dirFlags,
    m2dir::M2dir,
    m2store::{M2store, NewFolderError},
    path::M2dirPath,
};
use mail_parser::{Address as MailParserAddress, Message as ParsedMessage};
use thiserror::Error;

use crate::{
    address::Address,
    envelope::{Envelope, normalize_message_id},
    flag::Flag,
};

/// Errors produced by [`resolve_mailbox`].
#[derive(Debug, Error)]
pub enum InvalidMailboxName {
    #[error(transparent)]
    NewFolder(#[from] NewFolderError),
}

/// Builds an [`M2store`] handle from the shared root.
pub(crate) fn store_from_root(root: impl Into<PathBuf>) -> M2store {
    let path: M2dirPath = root.into().into();
    M2store::from_path(path)
}

/// Resolves a mailbox name to its on-disk m2dir directory under the
/// configured root. Honours the same `/`-separated convention as
/// io-m2dir's `M2store::resolve_folder_path`.
pub(crate) fn resolve_mailbox(
    root: impl Into<PathBuf>,
    name: &str,
) -> Result<M2dir, InvalidMailboxName> {
    let store = store_from_root(root);
    let path = store.resolve_folder_path(name)?;
    Ok(M2dir::from_path(path))
}

/// Parses a `.meta/<id>.flags` line into a shared [`Flag`]. Whitespace
/// is trimmed so stray editor newlines do not break IANA recognition.
pub(crate) fn flag_from_meta_line(line: &str) -> Flag {
    Flag::from_raw(line.trim())
}

/// Inverse of [`flag_from_meta_line`]: each flag is serialised on its
/// own line in canonical wire form.
pub(crate) fn flag_to_meta_line(flag: &Flag) -> String {
    flag.raw().to_string()
}

/// Builds an [`M2dirFlags`] payload from the shared flag slice.
pub(crate) fn flags_to_m2dir(flags: &[Flag]) -> M2dirFlags {
    flags.iter().map(flag_to_meta_line).collect()
}

/// Folds an entry + meta flags + parsed message into a shared
/// [`Envelope`]. `has_attachment` is left `None`; the caller can
/// overwrite it after a full body parse.
pub(crate) fn envelope_from(
    entry: &M2dirEntry,
    meta: &M2dirFlags,
    parsed: &ParsedMessage<'_>,
) -> Envelope {
    let id = entry.id().to_string();
    let flags = meta.iter().map(flag_from_meta_line).collect();
    let subject = parsed.subject().unwrap_or_default().to_string();
    let from = parsed.from().map(addresses_from).unwrap_or_default();
    let to = parsed.to().map(addresses_from).unwrap_or_default();
    let date = parsed
        .date()
        .and_then(|d| DateTime::parse_from_rfc3339(&d.to_rfc3339()).ok());
    let size = parsed.raw_message().len() as u64;
    let message_id = parsed.message_id().and_then(normalize_message_id);
    Envelope {
        id,
        message_id,
        flags,
        subject,
        from,
        to,
        date,
        size,
        has_attachment: None,
    }
}

/// 1-indexed pagination on an in-memory list. `page_size = None`
/// returns the full slice; `page_size = 0` or a page past the end
/// returns an empty vector.
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

fn addresses_from(addrs: &MailParserAddress<'_>) -> Vec<Address> {
    addrs
        .clone()
        .into_list()
        .into_iter()
        .filter_map(|a| {
            let email = a.address?.into_owned();
            if email.is_empty() {
                return None;
            }
            let name = a.name.map(|s| s.into_owned());
            Some(Address { name, email })
        })
        .collect()
}
