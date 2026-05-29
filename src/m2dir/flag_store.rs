//! m2dir flag-store coroutine.
//!
//! Drives the io-m2dir
//! [`M2dirFlagAdd`] / [`M2dirFlagSet`] / [`M2dirFlagRemove`] triad
//! per id. Each inner coroutine reads (and/or writes) the
//! `.meta/<id>.flags` sidecar file.
//!
//! [`M2dirFlagAdd`]: io_m2dir::coroutines::flag_add::M2dirFlagAdd
//! [`M2dirFlagSet`]: io_m2dir::coroutines::flag_set::M2dirFlagSet
//! [`M2dirFlagRemove`]: io_m2dir::coroutines::flag_remove::M2dirFlagRemove

use alloc::{collections::VecDeque, string::String};
use std::path::PathBuf;

use io_m2dir::{
    coroutine::{M2dirArg, M2dirCoroutine, M2dirCoroutineState, M2dirYield},
    coroutines::{
        flag_add::{M2dirFlagAdd as InnerAdd, M2dirFlagAddError as AddErr},
        flag_remove::{M2dirFlagRemove as InnerRemove, M2dirFlagRemoveError as RemoveErr},
        flag_set::{M2dirFlagSet as InnerSet, M2dirFlagSetError as SetErr},
    },
    flag::M2dirFlags,
    m2dir::M2dir,
};
use log::trace;
use thiserror::Error;

use crate::{
    coroutine::{
        EmailBackend, EmailCoroutine, EmailCoroutineArg, EmailCoroutineState, FsBatch, FsStep,
    },
    flag::{Flag, FlagOp},
    m2dir::convert::{InvalidMailboxName, files_out, flags_to_m2dir, paths_out, resolve_mailbox},
};

/// Errors produced by [`M2dirFlagStore`].
#[derive(Debug, Error)]
pub enum M2dirFlagStoreError {
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
pub struct M2dirFlagStore {
    m2dir: M2dir,
    flags: M2dirFlags,
    op: FlagOp,
    pending: VecDeque<String>,
    current: Option<Stage>,
}

impl M2dirFlagStore {
    pub fn new(
        root: impl Into<PathBuf>,
        mailbox: &str,
        ids: &[&str],
        flags: &[Flag],
        op: FlagOp,
    ) -> Result<Self, M2dirFlagStoreError> {
        trace!("prepare m2dir flag store ({op:?})");
        let m2dir = resolve_mailbox(root, mailbox)?;
        Ok(Self {
            m2dir,
            flags: flags_to_m2dir(flags),
            op,
            pending: ids.iter().map(|s| (*s).into()).collect(),
            current: None,
        })
    }
}

impl EmailCoroutine for M2dirFlagStore {
    type Yield = FsStep;
    type Return = Result<(), M2dirFlagStoreError>;

    const BACKEND: EmailBackend = EmailBackend::M2dir;

    fn resume(
        &mut self,
        arg: EmailCoroutineArg<'_>,
    ) -> EmailCoroutineState<Self::Yield, Self::Return> {
        #[allow(irrefutable_let_patterns)]
        let EmailCoroutineArg::Fs { batch } = arg else {
            return EmailCoroutineState::Complete(Err(M2dirFlagStoreError::InvalidArg));
        };

        let mut batch = batch;
        loop {
            if self.current.is_none() {
                let Some(id) = self.pending.pop_front() else {
                    return EmailCoroutineState::Complete(Ok(()));
                };
                self.current = Some(Stage::start(&self.m2dir, id, self.flags.clone(), self.op));
            }
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
    Yield(EmailCoroutineState<FsStep, Result<(), M2dirFlagStoreError>>),
    Err(M2dirFlagStoreError),
}

impl Stage {
    fn start(m2dir: &M2dir, id: String, flags: M2dirFlags, op: FlagOp) -> Self {
        match op {
            FlagOp::Add => Stage::Add(InnerAdd::new(m2dir, id, flags)),
            FlagOp::Set => Stage::Set(InnerSet::new(m2dir, id, flags)),
            FlagOp::Remove => Stage::Remove(InnerRemove::new(m2dir, id, flags)),
        }
    }

    fn step(&mut self, batch: Option<FsBatch>) -> StepOutcome {
        match self {
            Stage::Add(inner) => {
                let arg = match batch {
                    None => None,
                    Some(FsBatch::FileRead(files)) => Some(M2dirArg::FileRead(
                        files.into_iter().map(|(k, v)| (k.into(), v)).collect(),
                    )),
                    Some(FsBatch::FileCreate) => Some(M2dirArg::FileCreate),
                    Some(_) => return StepOutcome::Err(M2dirFlagStoreError::UnexpectedBatch),
                };
                match inner.resume(arg) {
                    M2dirCoroutineState::Complete(Ok(())) => StepOutcome::Done,
                    M2dirCoroutineState::Yielded(M2dirYield::WantsFileRead(p)) => {
                        StepOutcome::Yield(EmailCoroutineState::Yielded(FsStep::WantsFileRead(
                            paths_out(p),
                        )))
                    }
                    M2dirCoroutineState::Yielded(M2dirYield::WantsFileCreate(f)) => {
                        StepOutcome::Yield(EmailCoroutineState::Yielded(FsStep::WantsFileCreate(
                            files_out(f),
                        )))
                    }
                    M2dirCoroutineState::Complete(Err(err)) => StepOutcome::Err(err.into()),
                    _ => unreachable!("M2dirFlagAdd never yields this state"),
                }
            }
            Stage::Set(inner) => {
                let arg = match batch {
                    None => None,
                    Some(FsBatch::FileCreate) => Some(M2dirArg::FileCreate),
                    Some(FsBatch::FileRemove) => Some(M2dirArg::FileRemove),
                    Some(_) => return StepOutcome::Err(M2dirFlagStoreError::UnexpectedBatch),
                };
                match inner.resume(arg) {
                    M2dirCoroutineState::Complete(Ok(())) => StepOutcome::Done,
                    M2dirCoroutineState::Yielded(M2dirYield::WantsFileCreate(f)) => {
                        StepOutcome::Yield(EmailCoroutineState::Yielded(FsStep::WantsFileCreate(
                            files_out(f),
                        )))
                    }
                    M2dirCoroutineState::Yielded(M2dirYield::WantsFileRemove(p)) => {
                        StepOutcome::Yield(EmailCoroutineState::Yielded(FsStep::WantsFileRemove(
                            paths_out(p),
                        )))
                    }
                    M2dirCoroutineState::Complete(Err(err)) => StepOutcome::Err(err.into()),
                    _ => unreachable!("M2dirFlagSet never yields this state"),
                }
            }
            Stage::Remove(inner) => {
                let arg = match batch {
                    None => None,
                    Some(FsBatch::FileRead(files)) => Some(M2dirArg::FileRead(
                        files.into_iter().map(|(k, v)| (k.into(), v)).collect(),
                    )),
                    Some(FsBatch::FileCreate) => Some(M2dirArg::FileCreate),
                    Some(FsBatch::FileRemove) => Some(M2dirArg::FileRemove),
                    Some(_) => return StepOutcome::Err(M2dirFlagStoreError::UnexpectedBatch),
                };
                match inner.resume(arg) {
                    M2dirCoroutineState::Complete(Ok(())) => StepOutcome::Done,
                    M2dirCoroutineState::Yielded(M2dirYield::WantsFileRead(p)) => {
                        StepOutcome::Yield(EmailCoroutineState::Yielded(FsStep::WantsFileRead(
                            paths_out(p),
                        )))
                    }
                    M2dirCoroutineState::Yielded(M2dirYield::WantsFileCreate(f)) => {
                        StepOutcome::Yield(EmailCoroutineState::Yielded(FsStep::WantsFileCreate(
                            files_out(f),
                        )))
                    }
                    M2dirCoroutineState::Yielded(M2dirYield::WantsFileRemove(p)) => {
                        StepOutcome::Yield(EmailCoroutineState::Yielded(FsStep::WantsFileRemove(
                            paths_out(p),
                        )))
                    }
                    M2dirCoroutineState::Complete(Err(err)) => StepOutcome::Err(err.into()),
                    _ => unreachable!("M2dirFlagRemove never yields this state"),
                }
            }
        }
    }
}
