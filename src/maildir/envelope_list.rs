//! Maildir envelope listing, wrapping
//! [`io_maildir::coroutines::message_list::MaildirMessagesList`].
//!
//! Maildir has no inherent ordering; envelopes are sorted by date
//! descending and then paginated. `page` is 1-indexed.

use alloc::{
    collections::{BTreeMap, BTreeSet},
    string::ToString,
    vec::Vec,
};
use core::mem;

use chrono::DateTime;
use io_maildir::{
    coroutines::message_list::{
        MaildirMessagesList as InnerMaildirMessagesList, MaildirMessagesListArg,
        MaildirMessagesListError, MaildirMessagesListResult,
    },
    entry::MaildirEntry,
    maildir::Maildir,
    message::MaildirMessage,
    path::MaildirPath,
};
use log::trace;
use mail_parser::Address as MailParserAddress;

use crate::{
    address::Address,
    envelope::{Envelope, normalize_message_id},
    flag::Flag,
};

/// Argument fed back to [`MaildirEnvelopeList::resume`].
#[derive(Debug)]
pub enum MaildirEnvelopeListArg {
    DirRead(BTreeMap<MaildirPath, BTreeSet<MaildirPath>>),
    FileExists(BTreeMap<MaildirPath, bool>),
    FileRead(BTreeMap<MaildirPath, Vec<u8>>),
}

/// Result returned by [`MaildirEnvelopeList::resume`].
#[derive(Debug)]
pub enum MaildirEnvelopeListResult {
    Ok(Vec<Envelope>),
    WantsDirRead(BTreeSet<MaildirPath>),
    WantsFileExists(BTreeSet<MaildirPath>),
    WantsFileRead(BTreeSet<MaildirPath>),
    Err(MaildirMessagesListError),
}

/// Internal state of [`MaildirEnvelopeList`].
#[derive(Default)]
enum State {
    Listing(InnerMaildirMessagesList),
    Reading(BTreeSet<MaildirEntry>),
    #[default]
    Done,
}

/// I/O-free coroutine listing every message inside a single Maildir,
/// sorted by date descending then paginated.
pub struct MaildirEnvelopeList {
    state: State,
    page: Option<u32>,
    page_size: Option<u32>,
}

impl MaildirEnvelopeList {
    pub fn new(maildir: Maildir, page: Option<u32>, page_size: Option<u32>) -> Self {
        trace!("prepare Maildir envelope listing");
        Self {
            state: State::Listing(InnerMaildirMessagesList::new(maildir)),
            page,
            page_size,
        }
    }

    pub fn resume(&mut self, arg: Option<MaildirEnvelopeListArg>) -> MaildirEnvelopeListResult {
        match (mem::take(&mut self.state), arg) {
            (State::Listing(mut inner), arg) => {
                let inner_arg = match arg {
                    None => None,
                    Some(MaildirEnvelopeListArg::DirRead(entries)) => {
                        Some(MaildirMessagesListArg::DirRead(entries))
                    }
                    Some(MaildirEnvelopeListArg::FileExists(probes)) => {
                        Some(MaildirMessagesListArg::FileExists(probes))
                    }
                    Some(MaildirEnvelopeListArg::FileRead(_)) => {
                        // FileRead is consumed in State::Reading; if the
                        // caller feeds it back during listing, surface
                        // an invalid-arg error through the inner state.
                        return MaildirEnvelopeListResult::Err(MaildirMessagesListError::Invalid(
                            Some(MaildirMessagesListArg::FileExists(BTreeMap::new())),
                            io_maildir::coroutines::message_list::State::Invalid,
                        ));
                    }
                };

                match inner.resume(inner_arg) {
                    MaildirMessagesListResult::WantsDirRead(paths) => {
                        self.state = State::Listing(inner);
                        MaildirEnvelopeListResult::WantsDirRead(paths)
                    }
                    MaildirMessagesListResult::WantsFileExists(paths) => {
                        self.state = State::Listing(inner);
                        MaildirEnvelopeListResult::WantsFileExists(paths)
                    }
                    MaildirMessagesListResult::Ok(entries) => {
                        if entries.is_empty() {
                            return MaildirEnvelopeListResult::Ok(Vec::new());
                        }
                        let paths: BTreeSet<MaildirPath> =
                            entries.iter().map(|e| e.path().clone()).collect();
                        self.state = State::Reading(entries);
                        MaildirEnvelopeListResult::WantsFileRead(paths)
                    }
                    MaildirMessagesListResult::Err(err) => MaildirEnvelopeListResult::Err(err),
                }
            }
            (State::Reading(entries), Some(MaildirEnvelopeListArg::FileRead(mut contents))) => {
                let mut envelopes: Vec<Envelope> = entries
                    .into_iter()
                    .filter_map(|entry| {
                        let bytes = contents.remove(entry.path())?;
                        let message = MaildirMessage::from((entry.path().clone(), bytes));
                        Some(Envelope::from(message))
                    })
                    .collect();
                envelopes.sort_by(|a, b| b.date.cmp(&a.date));
                MaildirEnvelopeListResult::Ok(paginate(envelopes, self.page, self.page_size))
            }
            (state, _) => {
                self.state = state;
                MaildirEnvelopeListResult::Err(MaildirMessagesListError::Invalid(
                    None,
                    io_maildir::coroutines::message_list::State::Invalid,
                ))
            }
        }
    }
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

        let has_attachment = parsed.as_ref().map(|m| m.attachment_count() > 0);

        let message_id = parsed
            .as_ref()
            .and_then(|m| m.message_id())
            .and_then(normalize_message_id);

        Self {
            id,
            message_id,
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

fn parse_filename_flags(path: &MaildirPath) -> BTreeSet<Flag> {
    let Some(name) = path.file_name() else {
        return BTreeSet::new();
    };

    let Some((_, letters)) = name.rsplit_once(',') else {
        return BTreeSet::new();
    };

    letters
        .chars()
        .filter_map(crate::maildir::convert::flag_from_char)
        .collect()
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
