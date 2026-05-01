//! Maildir envelope listing, wrapping
//! [`io_maildir::coroutines::message_list::MaildirMessagesList`] and
//! producing the shared [`Envelope`](crate::envelope::Envelope) type
//! on completion.

use alloc::{
    collections::{BTreeMap, BTreeSet},
    string::{String, ToString},
    vec::Vec,
};
use std::path::Path;

use chrono::DateTime;
use io_maildir::{
    coroutines::message_list::{
        MaildirMessagesList, MaildirMessagesListArg, MaildirMessagesListError,
        MaildirMessagesListResult,
    },
    maildir::Maildir,
    message::Message as MaildirMessage,
};
use log::trace;

use crate::{address::Address, envelope::Envelope, flag::Flag};

/// I/O-free coroutine listing every message inside a single Maildir.
///
/// Maildir has no inherent ordering, so envelopes are sorted by date
/// descending (most recent first) before pagination is applied. Page
/// numbers are 1-indexed; `page_size = None` returns the full sorted
/// list.
pub struct EnvelopeList {
    inner: MaildirMessagesList,
    page: Option<u32>,
    page_size: Option<u32>,
}

impl EnvelopeList {
    /// Builds the coroutine from the target Maildir.
    pub fn new(maildir: Maildir, page: Option<u32>, page_size: Option<u32>) -> Self {
        trace!("prepare Maildir envelope listing");
        Self {
            inner: MaildirMessagesList::new(maildir),
            page,
            page_size,
        }
    }

    /// Advances the coroutine.
    pub fn resume(&mut self, arg: Option<EnvelopeListArg>) -> EnvelopeListResult {
        let inner_arg = arg.map(|arg| match arg {
            EnvelopeListArg::DirRead(entries) => MaildirMessagesListArg::DirRead(entries),
            EnvelopeListArg::FileRead(contents) => MaildirMessagesListArg::FileRead(contents),
        });

        match self.inner.resume(inner_arg) {
            MaildirMessagesListResult::WantsDirRead(paths) => {
                EnvelopeListResult::WantsDirRead(paths)
            }
            MaildirMessagesListResult::WantsFileRead(paths) => {
                EnvelopeListResult::WantsFileRead(paths)
            }
            MaildirMessagesListResult::Ok(messages) => {
                let mut envelopes: Vec<Envelope> =
                    messages.into_iter().map(Envelope::from).collect();
                envelopes.sort_by(|a, b| b.date.cmp(&a.date));
                let envelopes = paginate(envelopes, self.page, self.page_size);
                EnvelopeListResult::Ok(envelopes)
            }
            MaildirMessagesListResult::Err(err) => EnvelopeListResult::Err(err),
        }
    }
}

/// Slices a sorted envelope list to the requested 1-indexed page.
/// `page_size = None` returns the input unchanged.
fn paginate(envelopes: Vec<Envelope>, page: Option<u32>, page_size: Option<u32>) -> Vec<Envelope> {
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

/// Result returned by [`EnvelopeList::resume`].
#[derive(Debug)]
pub enum EnvelopeListResult {
    Ok(Vec<Envelope>),
    WantsDirRead(BTreeSet<String>),
    WantsFileRead(BTreeSet<String>),
    Err(MaildirMessagesListError),
}

/// Argument fed back to [`EnvelopeList::resume`] after the caller
/// performed the requested filesystem operation.
#[derive(Debug)]
pub enum EnvelopeListArg {
    /// Response to [`EnvelopeListResult::WantsDirRead`]: each requested
    /// directory path mapped to the set of entry paths found inside.
    DirRead(BTreeMap<String, BTreeSet<String>>),

    /// Response to [`EnvelopeListResult::WantsFileRead`]: each requested
    /// file path mapped to its raw contents.
    FileRead(BTreeMap<String, Vec<u8>>),
}

impl From<MaildirMessage> for Envelope {
    fn from(message: MaildirMessage) -> Self {
        let id = message.id().unwrap_or_default().to_string();
        let flags = parse_filename_flags(message.path());
        let size = message.contents().len() as u64;

        let parsed = message.parsed();

        let subject = parsed
            .as_ref()
            .and_then(|m| m.subject())
            .unwrap_or_default()
            .to_string();

        let from = parsed
            .as_ref()
            .and_then(|m| m.from())
            .map(addresses_from)
            .unwrap_or_default();

        let to = parsed
            .as_ref()
            .and_then(|m| m.to())
            .map(addresses_from)
            .unwrap_or_default();

        let date = parsed
            .as_ref()
            .and_then(|m| m.date())
            .and_then(|d| DateTime::parse_from_rfc3339(&d.to_rfc3339()).ok());

        // We already paid for the parse above (subject/from/to/date),
        // so populate has_attachment unconditionally — there's no
        // additional cost.
        let has_attachment = parsed.as_ref().map(|m| m.attachment_count() > 0);

        Self {
            id,
            flags,
            subject,
            from,
            to,
            date,
            size,
            has_attachment,
        }
    }
}

/// Parses the Maildir filename info section (`<id>:2,<letters>` on
/// Unix, `<id>;2,<letters>` on Windows) and returns the flags it
/// encodes, dropping any letter the shared LCD does not recognise
/// (`T` Trashed, `P` Passed).
fn parse_filename_flags(path: &Path) -> BTreeSet<Flag> {
    let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
        return BTreeSet::new();
    };

    let Some((_, letters)) = name.rsplit_once(',') else {
        return BTreeSet::new();
    };

    letters
        .chars()
        .filter_map(|c| {
            let mut buf = [0u8; 4];
            Flag::parse(c.encode_utf8(&mut buf))
        })
        .collect()
}

fn addresses_from(addrs: &io_maildir::types::Address<'_>) -> Vec<Address> {
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
