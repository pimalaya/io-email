//! IMAP message get (`SELECT` + `FETCH BODY[]`), wrapping a private
//! orchestrator that selects the mailbox, then fetches the raw RFC
//! 5322 bytes for the requested message.

use alloc::vec::Vec;
use core::{mem, num::NonZeroU32};

use io_imap::{
    context::ImapContext,
    rfc3501::{
        fetch::{
            ImapMessageFetchError as ImapFetchError, ImapMessageFetchFirst,
            ImapMessageFetchFirstResult,
        },
        select::{
            ImapMailboxSelect, ImapMailboxSelectError as ImapSelectError, ImapMailboxSelectResult,
        },
    },
    types::{
        fetch::{MacroOrMessageDataItemNames, MessageDataItem, MessageDataItemName},
        mailbox::Mailbox as ImapMailbox,
    },
};
use log::trace;
use thiserror::Error;

/// Errors produced while orchestrating SELECT + FETCH for IMAP message
/// retrieval.
#[derive(Debug, Error)]
pub enum MessageGetError {
    #[error(transparent)]
    Select(#[from] ImapSelectError),
    #[error(transparent)]
    Fetch(#[from] ImapFetchError),
    #[error("FETCH did not return any body for the requested message")]
    Empty,
    #[error("IMAP message get was resumed after completion")]
    AlreadyDone,
}

/// Result returned by [`MessageGet::resume`].
#[derive(Debug)]
pub enum MessageGetResult {
    Ok(Vec<u8>),
    WantsRead,
    WantsWrite(Vec<u8>),
    Err(MessageGetError),
}

/// I/O-free coroutine wrapping `SELECT <mailbox>` followed by `FETCH
/// <id> BODY.PEEK[]`. Returns the raw RFC 5322 bytes on completion.
pub struct MessageGet {
    inner: Inner,
    pending: Option<PendingFetch>,
}

struct PendingFetch {
    id: NonZeroU32,
    uid: bool,
}

enum Inner {
    Selecting(ImapMailboxSelect),
    Fetching(ImapMessageFetchFirst),
    Done,
}

impl MessageGet {
    /// Selects the mailbox read-write, then fetches the message body
    /// (peek) for the given id.
    pub fn new(
        context: ImapContext,
        mailbox: ImapMailbox<'static>,
        id: NonZeroU32,
        uid: bool,
    ) -> Self {
        trace!("prepare IMAP message get");
        Self {
            inner: Inner::Selecting(ImapMailboxSelect::new(context, mailbox)),
            pending: Some(PendingFetch { id, uid }),
        }
    }

    /// Advances the orchestrator. Drives SELECT first, then FETCH.
    pub fn resume(&mut self, arg: Option<&[u8]>) -> MessageGetResult {
        let mut input = arg;

        loop {
            let inner = mem::replace(&mut self.inner, Inner::Done);

            match inner {
                Inner::Selecting(mut select) => match select.resume(input.take()) {
                    ImapMailboxSelectResult::WantsRead => {
                        self.inner = Inner::Selecting(select);
                        return MessageGetResult::WantsRead;
                    }
                    ImapMailboxSelectResult::WantsWrite(bytes) => {
                        self.inner = Inner::Selecting(select);
                        return MessageGetResult::WantsWrite(bytes);
                    }
                    ImapMailboxSelectResult::Err { err, .. } => {
                        return MessageGetResult::Err(err.into());
                    }
                    ImapMailboxSelectResult::Ok { context, .. } => {
                        let pending = self.pending.take().expect("pending fetch set on construct");
                        let item_names = MacroOrMessageDataItemNames::MessageDataItemNames(vec![
                            MessageDataItemName::BodyExt {
                                section: None,
                                partial: None,
                                peek: true,
                            },
                        ]);
                        let fetch = ImapMessageFetchFirst::new(
                            context,
                            pending.id,
                            item_names,
                            pending.uid,
                        );
                        self.inner = Inner::Fetching(fetch);
                    }
                },
                Inner::Fetching(mut fetch) => match fetch.resume(input.take()) {
                    ImapMessageFetchFirstResult::WantsRead => {
                        self.inner = Inner::Fetching(fetch);
                        return MessageGetResult::WantsRead;
                    }
                    ImapMessageFetchFirstResult::WantsWrite(bytes) => {
                        self.inner = Inner::Fetching(fetch);
                        return MessageGetResult::WantsWrite(bytes);
                    }
                    ImapMessageFetchFirstResult::Err { err, .. } => {
                        return MessageGetResult::Err(err.into());
                    }
                    ImapMessageFetchFirstResult::Ok { items, .. } => {
                        let raw = items.into_inner().into_iter().find_map(|item| match item {
                            MessageDataItem::BodyExt { data, .. } => {
                                data.0.map(|d| d.as_ref().to_vec())
                            }
                            _ => None,
                        });

                        let Some(raw) = raw else {
                            return MessageGetResult::Err(MessageGetError::Empty);
                        };

                        return MessageGetResult::Ok(raw);
                    }
                },
                Inner::Done => {
                    return MessageGetResult::Err(MessageGetError::AlreadyDone);
                }
            }
        }
    }
}
