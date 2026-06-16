//! Gmail message-move coroutine: one `users.messages.modify` per id
//! adding the destination label and removing the source label.

use alloc::{
    boxed::Box,
    string::{String, ToString},
    vec::Vec,
};
use core::mem;

use io_gmail::{
    coroutine::{GmailCoroutine, GmailCoroutineState, GmailYield},
    v1::rest::messages::modify::GmailMessageModify,
    v1::send::GmailSendError,
};
use io_http::rfc6750::bearer::HttpAuthBearer;
use log::trace;
use thiserror::Error;

/// Errors produced by [`GmailMessageMove`].
#[derive(Debug, Error)]
pub enum GmailMessageMoveError {
    #[error(transparent)]
    Send(#[from] GmailSendError),
    #[error("coroutine was resumed after completion")]
    ResumedAfterDone,
}

/// I/O-free coroutine swapping `from` for `to` on the labels of every id.
pub struct GmailMessageMove {
    state: State,
    auth: HttpAuthBearer,
    user_id: String,
    ids: Vec<String>,
    add: Vec<String>,
    remove: Vec<String>,
}

impl GmailMessageMove {
    pub fn new(
        auth: &HttpAuthBearer,
        user_id: &str,
        from: &str,
        to: &str,
        ids: &[&str],
    ) -> Result<Self, GmailMessageMoveError> {
        trace!("prepare Gmail message move");
        let ids: Vec<String> = ids.iter().map(|id| (*id).into()).collect();
        let add = alloc::vec![to.to_string()];
        let remove = alloc::vec![from.to_string()];

        let state = if ids.is_empty() {
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

impl GmailCoroutine for GmailMessageMove {
    type Yield = GmailYield;
    type Return = Result<(), GmailMessageMoveError>;

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
                        GmailMessageMoveError::ResumedAfterDone,
                    ));
                }
            }
        }
    }
}
