//! IMAP flag add (`SELECT` + `STORE +FLAGS`), wrapping a private
//! orchestrator that selects the mailbox, then issues a STORE command
//! to add the requested flags.

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
/// add.
#[derive(Debug, Error)]
pub enum FlagAddError {
    #[error(transparent)]
    Select(#[from] ImapSelectError),
    #[error(transparent)]
    Store(#[from] ImapMessageStoreError),
    #[error("IMAP flag add was resumed after completion")]
    AlreadyDone,
}

/// Result returned by [`FlagAdd::resume`].
#[derive(Debug)]
pub enum FlagAddResult {
    Ok,
    WantsRead,
    WantsWrite(Vec<u8>),
    Err(FlagAddError),
}

/// I/O-free coroutine wrapping `SELECT <mailbox>` followed by `STORE
/// <sequence-set> +FLAGS <flags>`.
pub struct FlagAdd {
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

impl FlagAdd {
    /// Selects the mailbox read-write, then issues `STORE
    /// <sequence-set> +FLAGS <flags>`.
    pub fn new(
        context: ImapContext,
        mailbox: ImapMailbox<'static>,
        sequence_set: SequenceSet,
        flags: Vec<ImapFlag<'static>>,
        uid: bool,
    ) -> Self {
        trace!("prepare IMAP flag add");
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
    pub fn resume(&mut self, arg: Option<&[u8]>) -> FlagAddResult {
        let mut input = arg;

        loop {
            let inner = mem::replace(&mut self.inner, Inner::Done);

            match inner {
                Inner::Selecting(mut select) => match select.resume(input.take()) {
                    ImapMailboxSelectResult::WantsRead => {
                        self.inner = Inner::Selecting(select);
                        return FlagAddResult::WantsRead;
                    }
                    ImapMailboxSelectResult::WantsWrite(bytes) => {
                        self.inner = Inner::Selecting(select);
                        return FlagAddResult::WantsWrite(bytes);
                    }
                    ImapMailboxSelectResult::Err { err, .. } => {
                        return FlagAddResult::Err(err.into());
                    }
                    ImapMailboxSelectResult::Ok { context, .. } => {
                        let pending = self.pending.take().expect("pending store set on construct");
                        let store = ImapMessageStore::new(
                            context,
                            pending.sequence_set,
                            StoreType::Add,
                            pending.flags,
                            pending.uid,
                        );
                        self.inner = Inner::Storing(store);
                    }
                },
                Inner::Storing(mut store) => match store.resume(input.take()) {
                    ImapMessageStoreResult::WantsRead => {
                        self.inner = Inner::Storing(store);
                        return FlagAddResult::WantsRead;
                    }
                    ImapMessageStoreResult::WantsWrite(bytes) => {
                        self.inner = Inner::Storing(store);
                        return FlagAddResult::WantsWrite(bytes);
                    }
                    ImapMessageStoreResult::Err { err, .. } => {
                        return FlagAddResult::Err(err.into());
                    }
                    ImapMessageStoreResult::Ok { .. } => return FlagAddResult::Ok,
                },
                Inner::Done => {
                    return FlagAddResult::Err(FlagAddError::AlreadyDone);
                }
            }
        }
    }
}
