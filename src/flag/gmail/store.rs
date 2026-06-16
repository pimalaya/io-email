//! Gmail flag-store coroutine: one `users.messages.modify` per id,
//! translating flags to Gmail `addLabelIds`/`removeLabelIds`.
//!
//! `\Seen` is the absence of `UNREAD`, so its polarity is inverted;
//! flags without a Gmail equivalent are dropped. `mailbox` is part of
//! the shared signature but unused: Gmail labels are global per message.

use alloc::{boxed::Box, string::String, vec::Vec};
use core::mem;

use io_gmail::{
    coroutine::{GmailCoroutine, GmailCoroutineState, GmailYield},
    v1::rest::messages::modify::GmailMessageModify,
    v1::send::GmailSendError,
};
use io_http::rfc6750::bearer::HttpAuthBearer;
use log::trace;
use thiserror::Error;

use crate::{
    flag::types::{Flag, FlagOp},
    gmail::convert::label_patch,
};

/// Errors produced by [`GmailFlagStore`].
#[derive(Debug, Error)]
pub enum GmailFlagStoreError {
    #[error(transparent)]
    Send(#[from] GmailSendError),
    #[error("coroutine was resumed after completion")]
    ResumedAfterDone,
}

/// I/O-free coroutine applying a flag store across every id.
pub struct GmailFlagStore {
    state: State,
    auth: HttpAuthBearer,
    user_id: String,
    ids: Vec<String>,
    add: Vec<String>,
    remove: Vec<String>,
}

impl GmailFlagStore {
    pub fn new(
        auth: &HttpAuthBearer,
        user_id: &str,
        _mailbox: &str,
        ids: &[&str],
        flags: &[Flag],
        op: FlagOp,
    ) -> Result<Self, GmailFlagStoreError> {
        trace!("prepare Gmail flag store ({op:?})");
        let (add, remove) = label_patch(flags, op);
        let ids: Vec<String> = ids.iter().map(|id| (*id).into()).collect();

        let state = if ids.is_empty() || (add.is_empty() && remove.is_empty()) {
            State::Noop
        } else {
            let current = Box::new(GmailMessageModify::new(
                auth, user_id, &ids[0], &add, &remove,
            )?);
            State::Modifying { index: 0, current }
        };

        Ok(Self {
            state,
            auth: auth.clone(),
            user_id: user_id.into(),
            ids,
            add,
            remove,
        })
    }
}

enum State {
    Modifying {
        index: usize,
        current: Box<GmailMessageModify>,
    },
    Noop,
    Done,
}

impl GmailCoroutine for GmailFlagStore {
    type Yield = GmailYield;
    type Return = Result<(), GmailFlagStoreError>;

    fn resume(
        &mut self,
        mut bytes: Option<&[u8]>,
    ) -> GmailCoroutineState<Self::Yield, Self::Return> {
        loop {
            match mem::replace(&mut self.state, State::Done) {
                State::Modifying { index, mut current } => match current.resume(bytes) {
                    GmailCoroutineState::Yielded(y) => {
                        self.state = State::Modifying { index, current };
                        return GmailCoroutineState::Yielded(y);
                    }
                    GmailCoroutineState::Complete(Err(err)) => {
                        return GmailCoroutineState::Complete(Err(err.into()));
                    }
                    GmailCoroutineState::Complete(Ok(_)) => {
                        let index = index + 1;
                        if index >= self.ids.len() {
                            return GmailCoroutineState::Complete(Ok(()));
                        }
                        let current = match GmailMessageModify::new(
                            &self.auth,
                            &self.user_id,
                            &self.ids[index],
                            &self.add,
                            &self.remove,
                        ) {
                            Ok(modify) => Box::new(modify),
                            Err(err) => return GmailCoroutineState::Complete(Err(err.into())),
                        };
                        self.state = State::Modifying { index, current };
                        bytes = None;
                    }
                },
                State::Noop => return GmailCoroutineState::Complete(Ok(())),
                State::Done => {
                    return GmailCoroutineState::Complete(Err(
                        GmailFlagStoreError::ResumedAfterDone,
                    ));
                }
            }
        }
    }
}
