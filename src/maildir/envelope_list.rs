//! Maildir envelope-list coroutine: MaildirEntryList over cur/new
//! then a batched WantsFileRead pass parses RFC 5322 headers.
//!
//! Sorted by Date: header descending, paginated 1-indexed.
//!
//! # Example
//!
//! ```rust,ignore
//! use io_email::maildir::envelope_list::MaildirEnvelopeList;
//!
//! let envs = client.run(MaildirEnvelopeList::new(&client.store, "INBOX", Some(1), Some(50))?)?;
//! ```

use alloc::{collections::BTreeSet, string::ToString, vec::Vec};
use core::mem;

use chrono::DateTime;
use io_maildir::{
    coroutine::*,
    entry::{
        list::{MaildirEntryList as InnerList, MaildirEntryListError as InnerErr},
        types::{MaildirEntry, MaildirFullEntry},
    },
    maildir::types::Maildir,
    path::FsPath,
    store::MaildirStore,
};
use log::trace;
use mail_parser::Address as MailParserAddress;
use thiserror::Error;

use crate::{
    address::Address,
    envelope::{Envelope, normalize_message_id},
    flag::Flag,
    maildir::convert::{InvalidMailboxName, flag_from_char, mailbox_path, paginate},
};

/// Errors produced by [`MaildirEnvelopeList`].
#[derive(Debug, Error)]
pub enum MaildirEnvelopeListError {
    #[error(transparent)]
    List(#[from] InnerErr),
    #[error(transparent)]
    InvalidMailbox(#[from] InvalidMailboxName),
    #[error("coroutine was resumed with a MaildirReply variant it did not request")]
    UnexpectedReply,
    #[error("coroutine was resumed after completion")]
    ResumedAfterDone,
}

/// I/O-free coroutine listing every message in a Maildir, sorted by
/// Date: descending then paginated.
pub struct MaildirEnvelopeList {
    state: State,
    page: Option<u32>,
    page_size: Option<u32>,
}

impl MaildirEnvelopeList {
    pub fn new(
        store: &MaildirStore,
        mailbox: &str,
        page: Option<u32>,
        page_size: Option<u32>,
    ) -> Result<Self, MaildirEnvelopeListError> {
        trace!("prepare Maildir envelope listing");
        let path = mailbox_path(mailbox)?;
        let maildir = Maildir::from_path(store.resolve(&path));
        Ok(Self {
            state: State::Listing(InnerList::new(maildir)),
            page,
            page_size,
        })
    }
}

impl MaildirCoroutine for MaildirEnvelopeList {
    type Yield = MaildirYield;
    type Return = Result<Vec<Envelope>, MaildirEnvelopeListError>;

    fn resume(
        &mut self,
        arg: Option<MaildirReply>,
    ) -> MaildirCoroutineState<Self::Yield, Self::Return> {
        match mem::replace(&mut self.state, State::Done) {
            State::Listing(mut inner) => match inner.resume(arg) {
                MaildirCoroutineState::Yielded(y) => {
                    self.state = State::Listing(inner);
                    MaildirCoroutineState::Yielded(y)
                }
                MaildirCoroutineState::Complete(Ok(entries)) => {
                    if entries.is_empty() {
                        return MaildirCoroutineState::Complete(Ok(Vec::new()));
                    }
                    let paths: BTreeSet<FsPath> =
                        entries.iter().map(|e| e.path().clone()).collect();
                    self.state = State::Reading(entries);
                    MaildirCoroutineState::Yielded(MaildirYield::WantsFileRead(paths))
                }
                MaildirCoroutineState::Complete(Err(err)) => {
                    MaildirCoroutineState::Complete(Err(err.into()))
                }
            },
            State::Reading(entries) => {
                let Some(MaildirReply::FileRead(mut contents)) = arg else {
                    self.state = State::Reading(entries);
                    return MaildirCoroutineState::Complete(Err(
                        MaildirEnvelopeListError::UnexpectedReply,
                    ));
                };
                let mut envelopes: Vec<Envelope> = entries
                    .into_iter()
                    .filter_map(|entry| {
                        let bytes = contents.remove(entry.path())?;
                        Some(envelope_from_entry(&MaildirFullEntry::from((
                            entry.path().clone(),
                            bytes,
                        ))))
                    })
                    .collect();
                envelopes.sort_by(|a, b| b.date.cmp(&a.date));
                MaildirCoroutineState::Complete(Ok(paginate(envelopes, self.page, self.page_size)))
            }
            State::Done => {
                MaildirCoroutineState::Complete(Err(MaildirEnvelopeListError::ResumedAfterDone))
            }
        }
    }
}

enum State {
    Listing(InnerList),
    Reading(BTreeSet<MaildirEntry>),
    Done,
}

fn envelope_from_entry(entry: &MaildirFullEntry) -> Envelope {
    let id = entry.id().unwrap_or_default().to_string();
    let flags = parse_filename_flags(entry.path());
    let size = entry.contents().len() as u64;
    let parsed = entry.parsed();

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

    Envelope {
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

/// IANA flags from a Maildir filename's info section.
fn parse_filename_flags(path: &FsPath) -> BTreeSet<Flag> {
    let Some(name) = path.file_name() else {
        return BTreeSet::new();
    };
    let Some((_, letters)) = name.rsplit_once(',') else {
        return BTreeSet::new();
    };
    letters.chars().filter_map(flag_from_char).collect()
}

/// mail-parser address group to shared [`Address`] list.
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
