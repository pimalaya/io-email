//! m2dir message-copy coroutine: per id, fetches the source bytes via
//! [`M2dirMessageGet`] then writes them to the target via
//! [`M2dirMessageStore`].
//!
//! Flags are not propagated across the copy: the m2dir flag layout
//! lives in a separate `.meta/<id>.flags` sidecar whose payload is
//! reread under different ids on the target side. Callers needing
//! flag-preserving copies should add them via
//! [`crate::client::EmailClientStd::add_flags`] after the copy
//! completes.
//!
//! [`M2dirMessageGet`]: io_m2dir::coroutines::message_get::M2dirMessageGet
//! [`M2dirMessageStore`]: io_m2dir::coroutines::message_store::M2dirMessageStore

use alloc::{collections::VecDeque, string::String};
use core::mem;
use std::path::PathBuf;

use io_m2dir::{
    coroutine::{M2dirArg, M2dirCoroutine, M2dirCoroutineState, M2dirYield},
    coroutines::{
        message_get::{M2dirMessageGet as InnerGet, M2dirMessageGetError as GetErr},
        message_store::{M2dirMessageStore as InnerStore, M2dirMessageStoreError as StoreErr},
    },
    m2dir::M2dir,
};
use log::trace;
use thiserror::Error;

use crate::{
    coroutine::{
        EmailBackend, EmailCoroutine, EmailCoroutineArg, EmailCoroutineState, FsBatch, FsStep,
    },
    m2dir::convert::{
        InvalidMailboxName, dirread_in, fileread_in, files_out, pairs_out, paths_out, probes_in,
        resolve_mailbox,
    },
};

/// Errors produced by [`M2dirMessageCopy`].
#[derive(Debug, Error)]
pub enum M2dirMessageCopyError {
    #[error(transparent)]
    Get(#[from] GetErr),
    #[error(transparent)]
    Store(#[from] StoreErr),
    #[error(transparent)]
    InvalidMailbox(#[from] InvalidMailboxName),
    #[error("coroutine was resumed with the wrong EmailCoroutineArg variant")]
    InvalidArg,
    #[error("coroutine was resumed with an FsBatch variant it did not request")]
    UnexpectedBatch,
}

/// I/O-free coroutine copying every id from `from` to `to`.
pub struct M2dirMessageCopy {
    source: M2dir,
    target: M2dir,
    pending: VecDeque<String>,
    stage: Stage,
}

impl M2dirMessageCopy {
    pub fn new(
        root: impl Into<PathBuf>,
        from: &str,
        to: &str,
        ids: &[&str],
    ) -> Result<Self, M2dirMessageCopyError> {
        trace!("prepare m2dir message copy");
        let root = root.into();
        let source = resolve_mailbox(&root, from)?;
        let target = resolve_mailbox(&root, to)?;
        Ok(Self {
            source,
            target,
            pending: ids.iter().map(|s| (*s).into()).collect(),
            stage: Stage::Idle,
        })
    }
}

impl EmailCoroutine for M2dirMessageCopy {
    type Yield = FsStep;
    type Return = Result<(), M2dirMessageCopyError>;

    const BACKEND: EmailBackend = EmailBackend::M2dir;

    fn resume(
        &mut self,
        arg: EmailCoroutineArg<'_>,
    ) -> EmailCoroutineState<Self::Yield, Self::Return> {
        #[allow(irrefutable_let_patterns)]
        let EmailCoroutineArg::Fs { batch } = arg else {
            return EmailCoroutineState::Complete(Err(M2dirMessageCopyError::InvalidArg));
        };

        let mut batch = batch;
        loop {
            if matches!(self.stage, Stage::Idle) {
                let Some(id) = self.pending.pop_front() else {
                    return EmailCoroutineState::Complete(Ok(()));
                };
                self.stage = Stage::Getting(InnerGet::new(self.source.clone(), id));
            }
            match mem::replace(&mut self.stage, Stage::Idle) {
                Stage::Idle => unreachable!(),
                Stage::Getting(mut get) => {
                    let inner_arg = match batch.take() {
                        None => None,
                        Some(FsBatch::DirRead(entries)) => {
                            Some(M2dirArg::DirRead(dirread_in(entries)))
                        }
                        Some(FsBatch::FileExists(probes)) => {
                            Some(M2dirArg::FileExists(probes_in(probes)))
                        }
                        Some(FsBatch::FileRead(files)) => {
                            Some(M2dirArg::FileRead(fileread_in(files)))
                        }
                        Some(_) => {
                            return EmailCoroutineState::Complete(Err(
                                M2dirMessageCopyError::UnexpectedBatch,
                            ));
                        }
                    };
                    match get.resume(inner_arg) {
                        M2dirCoroutineState::Complete(Ok(ok)) => {
                            self.stage =
                                Stage::Storing(InnerStore::new(self.target.clone(), ok.contents));
                        }
                        M2dirCoroutineState::Yielded(M2dirYield::WantsDirRead(p)) => {
                            self.stage = Stage::Getting(get);
                            return EmailCoroutineState::Yielded(FsStep::WantsDirRead(paths_out(
                                p,
                            )));
                        }
                        M2dirCoroutineState::Yielded(M2dirYield::WantsFileExists(p)) => {
                            self.stage = Stage::Getting(get);
                            return EmailCoroutineState::Yielded(FsStep::WantsFileExists(
                                paths_out(p),
                            ));
                        }
                        M2dirCoroutineState::Yielded(M2dirYield::WantsFileRead(p)) => {
                            self.stage = Stage::Getting(get);
                            return EmailCoroutineState::Yielded(FsStep::WantsFileRead(paths_out(
                                p,
                            )));
                        }
                        M2dirCoroutineState::Complete(Err(err)) => {
                            return EmailCoroutineState::Complete(Err(err.into()));
                        }
                        _ => unreachable!("M2dirMessageGet never yields this state"),
                    }
                }
                Stage::Storing(mut store) => {
                    let inner_arg = match batch.take() {
                        None => None,
                        Some(FsBatch::Pid(p)) => Some(M2dirArg::Pid(p)),
                        Some(FsBatch::Random(bytes)) => Some(M2dirArg::Random(bytes)),
                        Some(FsBatch::FileCreate) => Some(M2dirArg::FileCreate),
                        Some(FsBatch::Rename) => Some(M2dirArg::Rename),
                        Some(_) => {
                            return EmailCoroutineState::Complete(Err(
                                M2dirMessageCopyError::UnexpectedBatch,
                            ));
                        }
                    };
                    match store.resume(inner_arg) {
                        M2dirCoroutineState::Complete(Ok(_entry)) => {
                            // Next id (Idle) or done if pending is
                            // empty (next loop iter handles both).
                        }
                        M2dirCoroutineState::Yielded(M2dirYield::WantsPid) => {
                            self.stage = Stage::Storing(store);
                            return EmailCoroutineState::Yielded(FsStep::WantsPid);
                        }
                        M2dirCoroutineState::Yielded(M2dirYield::WantsRandom { len }) => {
                            self.stage = Stage::Storing(store);
                            return EmailCoroutineState::Yielded(FsStep::WantsRandom { len });
                        }
                        M2dirCoroutineState::Yielded(M2dirYield::WantsFileCreate(f)) => {
                            self.stage = Stage::Storing(store);
                            return EmailCoroutineState::Yielded(FsStep::WantsFileCreate(
                                files_out(f),
                            ));
                        }
                        M2dirCoroutineState::Yielded(M2dirYield::WantsRename(pairs)) => {
                            self.stage = Stage::Storing(store);
                            return EmailCoroutineState::Yielded(FsStep::WantsRename(pairs_out(
                                pairs,
                            )));
                        }
                        M2dirCoroutineState::Complete(Err(err)) => {
                            return EmailCoroutineState::Complete(Err(err.into()));
                        }
                        _ => unreachable!("M2dirMessageStore never yields this state"),
                    }
                }
            }
        }
    }
}

enum Stage {
    Idle,
    Getting(InnerGet),
    Storing(InnerStore),
}
