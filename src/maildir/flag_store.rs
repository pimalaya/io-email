//! Maildir flag-store coroutine.
//!
//! Drives the io-maildir
//! [`MaildirFlagsAdd`] / [`MaildirFlagsSet`] / [`MaildirFlagsRemove`]
//! triad in a single state machine that walks every `id` in turn.
//! Each per-id inner coroutine locates the message file (cur → new),
//! computes the new info-section letter set, and yields a single
//! [`FsStep::WantsRename`].
//!
//! [`MaildirFlagsAdd`]: io_maildir::coroutines::flags_add::MaildirFlagsAdd
//! [`MaildirFlagsSet`]: io_maildir::coroutines::flags_set::MaildirFlagsSet
//! [`MaildirFlagsRemove`]: io_maildir::coroutines::flags_remove::MaildirFlagsRemove

use alloc::{collections::VecDeque, string::String};
use std::path::PathBuf;

use io_maildir::{
    coroutine::{MaildirCoroutine, MaildirCoroutineState, MaildirReply, MaildirYield},
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
    coroutine::{
        EmailBackend, EmailCoroutine, EmailCoroutineArg, EmailCoroutineState, FsBatch, FsStep,
    },
    flag::{Flag, FlagOp},
    maildir::convert::{
        InvalidMailboxName, dirread_in, flags_to_maildir, pairs_out, paths_out, probes_in,
        resolve_mailbox,
    },
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
    #[error("coroutine was resumed with the wrong EmailCoroutineArg variant")]
    InvalidArg,
    #[error("coroutine was resumed with an FsBatch variant it did not request")]
    UnexpectedBatch,
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

impl EmailCoroutine for MaildirFlagStore {
    type Yield = FsStep;
    type Return = Result<(), MaildirFlagStoreError>;

    const BACKEND: EmailBackend = EmailBackend::Maildir;

    fn resume(
        &mut self,
        arg: EmailCoroutineArg<'_>,
    ) -> EmailCoroutineState<Self::Yield, Self::Return> {
        #[allow(irrefutable_let_patterns)]
        let EmailCoroutineArg::Fs { batch } = arg else {
            return EmailCoroutineState::Complete(Err(MaildirFlagStoreError::InvalidArg));
        };

        let mut batch = batch;
        loop {
            if self.current.is_none() {
                let Some(id) = self.pending.pop_front() else {
                    return EmailCoroutineState::Complete(Ok(()));
                };
                self.current = Some(Stage::start(&self.maildir, id, self.flags.clone(), self.op));
            }

            // SAFETY by construction: `current` was just set if it was None.
            let stage = self.current.as_mut().unwrap();
            match stage.step(batch.take()) {
                StepOutcome::Done => {
                    self.current = None;
                }
                StepOutcome::Yield(state) => return state,
                StepOutcome::Err(err) => return EmailCoroutineState::Complete(Err(err)),
            }
        }
    }
}

enum Stage {
    Add(InnerAdd),
    Set(InnerSet),
    Remove(InnerRemove),
}

enum StepOutcome {
    Done,
    Yield(EmailCoroutineState<FsStep, Result<(), MaildirFlagStoreError>>),
    Err(MaildirFlagStoreError),
}

impl Stage {
    fn start(maildir: &Maildir, id: String, flags: MaildirFlags, op: FlagOp) -> Self {
        match op {
            FlagOp::Add => Stage::Add(InnerAdd::new(maildir.clone(), id, flags)),
            FlagOp::Set => Stage::Set(InnerSet::new(maildir.clone(), id, flags)),
            FlagOp::Remove => Stage::Remove(InnerRemove::new(maildir.clone(), id, flags)),
        }
    }

    fn step(&mut self, batch: Option<FsBatch>) -> StepOutcome {
        match self {
            Stage::Add(inner) => {
                let arg = match batch {
                    None => None,
                    Some(FsBatch::FileExists(probes)) => {
                        Some(MaildirReply::FileExists(probes_in(probes)))
                    }
                    Some(FsBatch::DirRead(entries)) => {
                        Some(MaildirReply::DirRead(dirread_in(entries)))
                    }
                    Some(FsBatch::Rename) => Some(MaildirReply::Rename),
                    Some(_) => return StepOutcome::Err(MaildirFlagStoreError::UnexpectedBatch),
                };
                match inner.resume(arg) {
                    MaildirCoroutineState::Complete(Ok(())) => StepOutcome::Done,
                    MaildirCoroutineState::Yielded(MaildirYield::WantsFileExists(p)) => {
                        StepOutcome::Yield(EmailCoroutineState::Yielded(FsStep::WantsFileExists(
                            paths_out(p),
                        )))
                    }
                    MaildirCoroutineState::Yielded(MaildirYield::WantsDirRead(p)) => {
                        StepOutcome::Yield(EmailCoroutineState::Yielded(FsStep::WantsDirRead(
                            paths_out(p),
                        )))
                    }
                    MaildirCoroutineState::Yielded(MaildirYield::WantsRename(pairs)) => {
                        StepOutcome::Yield(EmailCoroutineState::Yielded(FsStep::WantsRename(
                            pairs_out(pairs),
                        )))
                    }
                    MaildirCoroutineState::Complete(Err(err)) => StepOutcome::Err(err.into()),
                    _ => unreachable!("MaildirFlagsAdd never yields this state"),
                }
            }
            Stage::Set(inner) => {
                let arg = match batch {
                    None => None,
                    Some(FsBatch::FileExists(probes)) => {
                        Some(MaildirReply::FileExists(probes_in(probes)))
                    }
                    Some(FsBatch::DirRead(entries)) => {
                        Some(MaildirReply::DirRead(dirread_in(entries)))
                    }
                    Some(FsBatch::Rename) => Some(MaildirReply::Rename),
                    Some(_) => return StepOutcome::Err(MaildirFlagStoreError::UnexpectedBatch),
                };
                match inner.resume(arg) {
                    MaildirCoroutineState::Complete(Ok(())) => StepOutcome::Done,
                    MaildirCoroutineState::Yielded(MaildirYield::WantsFileExists(p)) => {
                        StepOutcome::Yield(EmailCoroutineState::Yielded(FsStep::WantsFileExists(
                            paths_out(p),
                        )))
                    }
                    MaildirCoroutineState::Yielded(MaildirYield::WantsDirRead(p)) => {
                        StepOutcome::Yield(EmailCoroutineState::Yielded(FsStep::WantsDirRead(
                            paths_out(p),
                        )))
                    }
                    MaildirCoroutineState::Yielded(MaildirYield::WantsRename(pairs)) => {
                        StepOutcome::Yield(EmailCoroutineState::Yielded(FsStep::WantsRename(
                            pairs_out(pairs),
                        )))
                    }
                    MaildirCoroutineState::Complete(Err(err)) => StepOutcome::Err(err.into()),
                    _ => unreachable!("MaildirFlagsSet never yields this state"),
                }
            }
            Stage::Remove(inner) => {
                let arg = match batch {
                    None => None,
                    Some(FsBatch::FileExists(probes)) => {
                        Some(MaildirReply::FileExists(probes_in(probes)))
                    }
                    Some(FsBatch::DirRead(entries)) => {
                        Some(MaildirReply::DirRead(dirread_in(entries)))
                    }
                    Some(FsBatch::Rename) => Some(MaildirReply::Rename),
                    Some(_) => return StepOutcome::Err(MaildirFlagStoreError::UnexpectedBatch),
                };
                match inner.resume(arg) {
                    MaildirCoroutineState::Complete(Ok(())) => StepOutcome::Done,
                    MaildirCoroutineState::Yielded(MaildirYield::WantsFileExists(p)) => {
                        StepOutcome::Yield(EmailCoroutineState::Yielded(FsStep::WantsFileExists(
                            paths_out(p),
                        )))
                    }
                    MaildirCoroutineState::Yielded(MaildirYield::WantsDirRead(p)) => {
                        StepOutcome::Yield(EmailCoroutineState::Yielded(FsStep::WantsDirRead(
                            paths_out(p),
                        )))
                    }
                    MaildirCoroutineState::Yielded(MaildirYield::WantsRename(pairs)) => {
                        StepOutcome::Yield(EmailCoroutineState::Yielded(FsStep::WantsRename(
                            pairs_out(pairs),
                        )))
                    }
                    MaildirCoroutineState::Complete(Err(err)) => StepOutcome::Err(err.into()),
                    _ => unreachable!("MaildirFlagsRemove never yields this state"),
                }
            }
        }
    }
}
