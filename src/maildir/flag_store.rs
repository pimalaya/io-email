//! Maildir flag-store coroutine.
//!
//! Drives the io-maildir
//! [`MaildirFlagsAdd`] / [`MaildirFlagsSet`] / [`MaildirFlagsRemove`]
//! triad in a single state machine that walks every `id` in turn.
//! Each per-id inner coroutine locates the message file (cur, new),
//! computes the new info-section letter set, and yields a single
//! [`MaildirYield::WantsRename`].
//!
//! [`MaildirFlagsAdd`]: io_maildir::coroutines::flags_add::MaildirFlagsAdd
//! [`MaildirFlagsSet`]: io_maildir::coroutines::flags_set::MaildirFlagsSet
//! [`MaildirFlagsRemove`]: io_maildir::coroutines::flags_remove::MaildirFlagsRemove

use alloc::{collections::VecDeque, string::String};
use std::path::PathBuf;

use io_maildir::{
    coroutine::*,
    coroutines::{
        flags_add::{MaildirFlagsAdd as InnerAdd, MaildirFlagsAddError as AddErr},
        flags_remove::{MaildirFlagsRemove as InnerRemove, MaildirFlagsRemoveError as RemoveErr},
        flags_set::{MaildirFlagsSet as InnerSet, MaildirFlagsSetError as SetErr},
    },
    flag::MaildirFlags,
    maildir::Maildir,
};
use log::trace;
use thiserror::Error;

use crate::{
    flag::{Flag, FlagOp},
    maildir::convert::{InvalidMailboxName, flags_to_maildir, resolve_mailbox},
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
        root: impl Into<PathBuf>,
        maildir_plus: bool,
        mailbox: &str,
        ids: &[&str],
        flags: &[Flag],
        op: FlagOp,
    ) -> Result<Self, MaildirFlagStoreError> {
        trace!("prepare Maildir flag store ({op:?})");
        let path = resolve_mailbox(&root.into(), maildir_plus, mailbox)?;
        let maildir = Maildir::from_path(path);
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

            // SAFETY by construction: `current` was just set if it was None.
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
