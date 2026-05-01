//! IMAP flag delete (`SELECT` + `STORE -FLAGS`), wrapping a private
//! orchestrator that selects the mailbox, then issues a STORE command
//! to remove the requested flags.

use alloc::vec::Vec;
use core::mem;

use io_imap::{
    context::ImapContext,
    rfc3501::{
        select::{
            ImapMailboxSelect, ImapMailboxSelectError as ImapSelectError, ImapMailboxSelectResult,
        },
        store::{ImapMessageStore, ImapMessageStoreError, ImapMessageStoreResult},
    },
    types::{
        flag::{Flag as ImapFlag, StoreType},
        mailbox::Mailbox as ImapMailbox,
        sequence::SequenceSet,
    },
};
use log::trace;
use thiserror::Error;

/// Errors produced while orchestrating SELECT + STORE for IMAP flag
/// delete.
#[derive(Debug, Error)]
pub enum FlagDeleteError {
    #[error(transparent)]
    Select(#[from] ImapSelectError),
    #[error(transparent)]
    Store(#[from] ImapMessageStoreError),
    #[error("IMAP flag delete was resumed after completion")]
    AlreadyDone,
}

/// Result returned by [`FlagDelete::resume`].
#[derive(Debug)]
pub enum FlagDeleteResult {
    Ok,
    WantsRead,
    WantsWrite(Vec<u8>),
    Err(FlagDeleteError),
}

/// I/O-free coroutine wrapping `SELECT <mailbox>` followed by `STORE
/// <sequence-set> -FLAGS <flags>`.
pub struct FlagDelete {
    inner: Inner,
    pending: Option<PendingStore>,
}

struct PendingStore {
    sequence_set: SequenceSet,
    flags: Vec<ImapFlag<'static>>,
    uid: bool,
}

enum Inner {
    Selecting(ImapMailboxSelect),
    Storing(ImapMessageStore),
    Done,
}

impl FlagDelete {
    /// Selects the mailbox read-write, then issues `STORE
    /// <sequence-set> -FLAGS <flags>`.
    pub fn new(
        context: ImapContext,
        mailbox: ImapMailbox<'static>,
        sequence_set: SequenceSet,
        flags: Vec<ImapFlag<'static>>,
        uid: bool,
    ) -> Self {
        trace!("prepare IMAP flag delete");
        Self {
            inner: Inner::Selecting(ImapMailboxSelect::new(context, mailbox)),
            pending: Some(PendingStore {
                sequence_set,
                flags,
                uid,
            }),
        }
    }

    /// Advances the orchestrator. Drives SELECT first, then STORE.
    pub fn resume(&mut self, arg: Option<&[u8]>) -> FlagDeleteResult {
        let mut input = arg;

        loop {
            let inner = mem::replace(&mut self.inner, Inner::Done);

            match inner {
                Inner::Selecting(mut select) => match select.resume(input.take()) {
                    ImapMailboxSelectResult::WantsRead => {
                        self.inner = Inner::Selecting(select);
                        return FlagDeleteResult::WantsRead;
                    }
                    ImapMailboxSelectResult::WantsWrite(bytes) => {
                        self.inner = Inner::Selecting(select);
                        return FlagDeleteResult::WantsWrite(bytes);
                    }
                    ImapMailboxSelectResult::Err { err, .. } => {
                        return FlagDeleteResult::Err(err.into());
                    }
                    ImapMailboxSelectResult::Ok { context, .. } => {
                        let pending = self.pending.take().expect("pending store set on construct");
                        let store = ImapMessageStore::new(
                            context,
                            pending.sequence_set,
                            StoreType::Remove,
                            pending.flags,
                            pending.uid,
                        );
                        self.inner = Inner::Storing(store);
                    }
                },
                Inner::Storing(mut store) => match store.resume(input.take()) {
                    ImapMessageStoreResult::WantsRead => {
                        self.inner = Inner::Storing(store);
                        return FlagDeleteResult::WantsRead;
                    }
                    ImapMessageStoreResult::WantsWrite(bytes) => {
                        self.inner = Inner::Storing(store);
                        return FlagDeleteResult::WantsWrite(bytes);
                    }
                    ImapMessageStoreResult::Err { err, .. } => {
                        return FlagDeleteResult::Err(err.into());
                    }
                    ImapMessageStoreResult::Ok { .. } => return FlagDeleteResult::Ok,
                },
                Inner::Done => {
                    return FlagDeleteResult::Err(FlagDeleteError::AlreadyDone);
                }
            }
        }
    }
}
