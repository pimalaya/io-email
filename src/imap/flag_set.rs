//! IMAP flag set (`SELECT` + `STORE FLAGS`), wrapping a private
//! orchestrator that selects the mailbox, then issues a STORE command
//! to replace the flags.

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
/// set.
#[derive(Debug, Error)]
pub enum FlagSetError {
    #[error(transparent)]
    Select(#[from] ImapSelectError),
    #[error(transparent)]
    Store(#[from] ImapMessageStoreError),
    #[error("IMAP flag set was resumed after completion")]
    AlreadyDone,
}

/// Result returned by [`FlagSet::resume`].
#[derive(Debug)]
pub enum FlagSetResult {
    Ok,
    WantsRead,
    WantsWrite(Vec<u8>),
    Err(FlagSetError),
}

/// I/O-free coroutine wrapping `SELECT <mailbox>` followed by `STORE
/// <sequence-set> FLAGS <flags>`.
pub struct FlagSet {
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

impl FlagSet {
    /// Selects the mailbox read-write, then issues `STORE
    /// <sequence-set> FLAGS <flags>`.
    pub fn new(
        context: ImapContext,
        mailbox: ImapMailbox<'static>,
        sequence_set: SequenceSet,
        flags: Vec<ImapFlag<'static>>,
        uid: bool,
    ) -> Self {
        trace!("prepare IMAP flag set");
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
    pub fn resume(&mut self, arg: Option<&[u8]>) -> FlagSetResult {
        let mut input = arg;

        loop {
            let inner = mem::replace(&mut self.inner, Inner::Done);

            match inner {
                Inner::Selecting(mut select) => match select.resume(input.take()) {
                    ImapMailboxSelectResult::WantsRead => {
                        self.inner = Inner::Selecting(select);
                        return FlagSetResult::WantsRead;
                    }
                    ImapMailboxSelectResult::WantsWrite(bytes) => {
                        self.inner = Inner::Selecting(select);
                        return FlagSetResult::WantsWrite(bytes);
                    }
                    ImapMailboxSelectResult::Err { err, .. } => {
                        return FlagSetResult::Err(err.into());
                    }
                    ImapMailboxSelectResult::Ok { context, .. } => {
                        let pending = self.pending.take().expect("pending store set on construct");
                        let store = ImapMessageStore::new(
                            context,
                            pending.sequence_set,
                            StoreType::Replace,
                            pending.flags,
                            pending.uid,
                        );
                        self.inner = Inner::Storing(store);
                    }
                },
                Inner::Storing(mut store) => match store.resume(input.take()) {
                    ImapMessageStoreResult::WantsRead => {
                        self.inner = Inner::Storing(store);
                        return FlagSetResult::WantsRead;
                    }
                    ImapMessageStoreResult::WantsWrite(bytes) => {
                        self.inner = Inner::Storing(store);
                        return FlagSetResult::WantsWrite(bytes);
                    }
                    ImapMessageStoreResult::Err { err, .. } => {
                        return FlagSetResult::Err(err.into());
                    }
                    ImapMessageStoreResult::Ok { .. } => return FlagSetResult::Ok,
                },
                Inner::Done => {
                    return FlagSetResult::Err(FlagSetError::AlreadyDone);
                }
            }
        }
    }
}
