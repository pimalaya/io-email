//! m2dir message-add coroutine.
//!
//! Wraps [`io_m2dir::coroutines::message_store::M2dirMessageStore`]
//! and, when `flags` is non-empty, chains a follow-up
//! [`M2dirFlagSet`] to persist them as
//! `.meta/<id>.flags`.
//!
//! The store coroutine probes pid + 4 random bytes to mint the entry
//! id (`<date>,<checksum>.<nonce>` per the m2dir spec).
//!
//! [`M2dirFlagSet`]: io_m2dir::coroutines::flag_set::M2dirFlagSet

use alloc::{string::String, vec::Vec};
use core::mem;
use std::path::PathBuf;

use io_m2dir::{
    coroutine::{M2dirArg, M2dirCoroutine, M2dirCoroutineState, M2dirYield},
    coroutines::{
        flag_set::{M2dirFlagSet as InnerFlagSet, M2dirFlagSetError as FlagSetErr},
        message_store::{M2dirMessageStore as InnerStore, M2dirMessageStoreError as StoreErr},
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
    flag::Flag,
    m2dir::convert::{
        InvalidMailboxName, files_out, flags_to_m2dir, pairs_out, paths_out, resolve_mailbox,
    },
};

/// Errors produced by [`M2dirMessageAdd`].
#[derive(Debug, Error)]
pub enum M2dirMessageAddError {
    #[error(transparent)]
    Store(#[from] StoreErr),
    #[error(transparent)]
    SetFlags(#[from] FlagSetErr),
    #[error(transparent)]
    InvalidMailbox(#[from] InvalidMailboxName),
    #[error("coroutine was resumed with the wrong EmailCoroutineArg variant")]
    InvalidArg,
    #[error("coroutine was resumed with an FsBatch variant it did not request")]
    UnexpectedBatch,
}

/// I/O-free coroutine appending a raw message to an m2dir mailbox.
pub struct M2dirMessageAdd {
    state: State,
    m2dir: M2dir,
    flags: M2dirFlags,
}

impl M2dirMessageAdd {
    pub fn new(
        root: impl Into<PathBuf>,
        mailbox: &str,
        flags: &[Flag],
        bytes: Vec<u8>,
    ) -> Result<Self, M2dirMessageAddError> {
        trace!("prepare m2dir message add");
        let m2dir = resolve_mailbox(root, mailbox)?;
        let store = InnerStore::new(m2dir.clone(), bytes);
        Ok(Self {
            state: State::Storing(store),
            m2dir,
            flags: flags_to_m2dir(flags),
        })
    }
}

impl EmailCoroutine for M2dirMessageAdd {
    type Yield = FsStep;
    type Return = Result<String, M2dirMessageAddError>;

    const BACKEND: EmailBackend = EmailBackend::M2dir;

    fn resume(
        &mut self,
        arg: EmailCoroutineArg<'_>,
    ) -> EmailCoroutineState<Self::Yield, Self::Return> {
        #[allow(irrefutable_let_patterns)]
        let EmailCoroutineArg::Fs { batch } = arg else {
            return EmailCoroutineState::Complete(Err(M2dirMessageAddError::InvalidArg));
        };

        match mem::replace(&mut self.state, State::Done) {
            State::Storing(mut store) => {
                let inner_arg = match batch {
                    None => None,
                    Some(FsBatch::Pid(p)) => Some(M2dirArg::Pid(p)),
                    Some(FsBatch::Random(bytes)) => Some(M2dirArg::Random(bytes)),
                    Some(FsBatch::FileCreate) => Some(M2dirArg::FileCreate),
                    Some(FsBatch::Rename) => Some(M2dirArg::Rename),
                    Some(_) => {
                        return EmailCoroutineState::Complete(Err(
                            M2dirMessageAddError::UnexpectedBatch,
                        ));
                    }
                };
                match store.resume(inner_arg) {
                    M2dirCoroutineState::Complete(Ok(entry)) => {
                        if self.flags.is_empty() {
                            EmailCoroutineState::Complete(Ok(entry.id().into()))
                        } else {
                            let id: String = entry.id().into();
                            let set = InnerFlagSet::new(&self.m2dir, &id, self.flags.clone());
                            self.state = State::SettingFlags { set, id };
                            // Re-run resume with no batch to kick the
                            // flag-set stage immediately. Callers see
                            // the next yielded Wants* / Done as if it
                            // were the next step in the same pipeline.
                            self.resume(EmailCoroutineArg::Fs { batch: None })
                        }
                    }
                    M2dirCoroutineState::Yielded(M2dirYield::WantsPid) => {
                        self.state = State::Storing(store);
                        EmailCoroutineState::Yielded(FsStep::WantsPid)
                    }
                    M2dirCoroutineState::Yielded(M2dirYield::WantsRandom { len }) => {
                        self.state = State::Storing(store);
                        EmailCoroutineState::Yielded(FsStep::WantsRandom { len })
                    }
                    M2dirCoroutineState::Yielded(M2dirYield::WantsFileCreate(files)) => {
                        self.state = State::Storing(store);
                        EmailCoroutineState::Yielded(FsStep::WantsFileCreate(files_out(files)))
                    }
                    M2dirCoroutineState::Yielded(M2dirYield::WantsRename(pairs)) => {
                        self.state = State::Storing(store);
                        EmailCoroutineState::Yielded(FsStep::WantsRename(pairs_out(pairs)))
                    }
                    M2dirCoroutineState::Complete(Err(err)) => {
                        EmailCoroutineState::Complete(Err(err.into()))
                    }
                    other => {
                        let _ = other;
                        unreachable!("M2dirMessageStore never yields this state");
                    }
                }
            }
            State::SettingFlags { mut set, id } => {
                let inner_arg = match batch {
                    None => None,
                    Some(FsBatch::FileCreate) => Some(M2dirArg::FileCreate),
                    Some(FsBatch::FileRemove) => Some(M2dirArg::FileRemove),
                    Some(_) => {
                        return EmailCoroutineState::Complete(Err(
                            M2dirMessageAddError::UnexpectedBatch,
                        ));
                    }
                };
                match set.resume(inner_arg) {
                    M2dirCoroutineState::Complete(Ok(())) => EmailCoroutineState::Complete(Ok(id)),
                    M2dirCoroutineState::Yielded(M2dirYield::WantsFileCreate(files)) => {
                        self.state = State::SettingFlags { set, id };
                        EmailCoroutineState::Yielded(FsStep::WantsFileCreate(files_out(files)))
                    }
                    M2dirCoroutineState::Yielded(M2dirYield::WantsFileRemove(paths)) => {
                        self.state = State::SettingFlags { set, id };
                        EmailCoroutineState::Yielded(FsStep::WantsFileRemove(paths_out(paths)))
                    }
                    M2dirCoroutineState::Complete(Err(err)) => {
                        EmailCoroutineState::Complete(Err(err.into()))
                    }
                    other => {
                        let _ = other;
                        unreachable!("M2dirFlagSet never yields this state");
                    }
                }
            }
            State::Done => unreachable!("M2dirMessageAdd resumed after completion"),
        }
    }
}

enum State {
    Storing(InnerStore),
    SettingFlags { set: InnerFlagSet, id: String },
    Done,
}
