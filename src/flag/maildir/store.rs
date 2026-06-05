//! Maildir flag-store coroutine: walks every id and drives one
//! per-id inner coroutine ([`MaildirFlagsAdd`] / [`MaildirFlagsSet`] /
//! [`MaildirFlagsRemove`]) per pass.
//!
//! Each inner coroutine relocates the message file in cur/new with
//! the updated info-section letters.
//!
//! # Example
//!
//! ```rust,ignore
//! use io_email::{flag::FlagOp, flag::maildir::store::MaildirFlagStore};
//!
//! client.run(MaildirFlagStore::new(&client.store, "INBOX", &["msg-id"], &flags, FlagOp::Add)?)?;
//! ```
//!
//! [`MaildirFlagsAdd`]: io_maildir::flag::add::MaildirFlagsAdd
//! [`MaildirFlagsSet`]: io_maildir::flag::set::MaildirFlagsSet
//! [`MaildirFlagsRemove`]: io_maildir::flag::remove::MaildirFlagsRemove

use alloc::{collections::VecDeque, string::String};

use io_maildir::{
    coroutine::*,
    flag::{
        add::{MaildirFlagsAdd as InnerAdd, MaildirFlagsAddError as AddErr},
        remove::{MaildirFlagsRemove as InnerRemove, MaildirFlagsRemoveError as RemoveErr},
        set::{MaildirFlagsSet as InnerSet, MaildirFlagsSetError as SetErr},
        types::MaildirFlags,
    },
    maildir::types::Maildir,
    store::MaildirStore,
};
use log::trace;
use thiserror::Error;

use crate::{
    flag::types::{Flag, FlagOp},
    maildir::convert::{InvalidMailboxName, flags_to_maildir, mailbox_path},
};

/// Errors produced by [`MaildirFlagStore`].
#[derive(Debug, Error)]
pub enum MaildirFlagStoreError {
    #[error(transparent)]
    Add(#[from] AddErr),
    #[error(transparent)]
    Set(#[from] SetErr),
    #[error(transparent)]
    Remove(#[from] RemoveErr),
    #[error(transparent)]
    InvalidMailbox(#[from] InvalidMailboxName),
}

/// I/O-free coroutine applying a flag store across every id in turn.
pub struct MaildirFlagStore {
    maildir: Maildir,
    flags: MaildirFlags,
    op: FlagOp,
    pending: VecDeque<String>,
    current: Option<Stage>,
}

impl MaildirFlagStore {
    pub fn new(
        store: &MaildirStore,
        mailbox: &str,
        ids: &[&str],
        flags: &[Flag],
        op: FlagOp,
    ) -> Result<Self, MaildirFlagStoreError> {
        trace!("prepare Maildir flag store ({op:?})");
        let path = mailbox_path(mailbox)?;
        let maildir = Maildir::from_path(store.resolve(&path));
        let flags = flags_to_maildir(flags);
        let pending = ids.iter().map(|s| (*s).into()).collect();
        Ok(Self {
            maildir,
            flags,
            op,
            pending,
            current: None,
        })
    }
}

enum Stage {
    Add(InnerAdd),
    Set(InnerSet),
    Remove(InnerRemove),
}

impl Stage {
    fn start(maildir: &Maildir, id: String, flags: MaildirFlags, op: FlagOp) -> Self {
        match op {
            FlagOp::Add => Stage::Add(InnerAdd::new(maildir.clone(), id, flags)),
            FlagOp::Set => Stage::Set(InnerSet::new(maildir.clone(), id, flags)),
            FlagOp::Remove => Stage::Remove(InnerRemove::new(maildir.clone(), id, flags)),
        }
    }
}

impl MaildirCoroutine for MaildirFlagStore {
    type Yield = MaildirYield;
    type Return = Result<(), MaildirFlagStoreError>;

    fn resume(
        &mut self,
        arg: Option<MaildirReply>,
    ) -> MaildirCoroutineState<Self::Yield, Self::Return> {
        let mut arg = arg;
        loop {
            if self.current.is_none() {
                let Some(id) = self.pending.pop_front() else {
                    return MaildirCoroutineState::Complete(Ok(()));
                };
                self.current = Some(Stage::start(&self.maildir, id, self.flags.clone(), self.op));
            }

            // SAFETY: `current` was just set above if it was None.
            let stage = self.current.as_mut().unwrap();
            match stage.resume_passthrough(arg.take()) {
                MaildirCoroutineState::Complete(Ok(())) => {
                    self.current = None;
                }
                MaildirCoroutineState::Yielded(y) => return MaildirCoroutineState::Yielded(y),
                MaildirCoroutineState::Complete(Err(err)) => {
                    return MaildirCoroutineState::Complete(Err(err));
                }
            }
        }
    }
}

impl Stage {
    fn resume_passthrough(
        &mut self,
        arg: Option<MaildirReply>,
    ) -> MaildirCoroutineState<MaildirYield, Result<(), MaildirFlagStoreError>> {
        match self {
            Stage::Add(inner) => match inner.resume(arg) {
                MaildirCoroutineState::Yielded(y) => MaildirCoroutineState::Yielded(y),
                MaildirCoroutineState::Complete(r) => {
                    MaildirCoroutineState::Complete(r.map_err(Into::into))
                }
            },
            Stage::Set(inner) => match inner.resume(arg) {
                MaildirCoroutineState::Yielded(y) => MaildirCoroutineState::Yielded(y),
                MaildirCoroutineState::Complete(r) => {
                    MaildirCoroutineState::Complete(r.map_err(Into::into))
                }
            },
            Stage::Remove(inner) => match inner.resume(arg) {
                MaildirCoroutineState::Yielded(y) => MaildirCoroutineState::Yielded(y),
                MaildirCoroutineState::Complete(r) => {
                    MaildirCoroutineState::Complete(r.map_err(Into::into))
                }
            },
        }
    }
}
