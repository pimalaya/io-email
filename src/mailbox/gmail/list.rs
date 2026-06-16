//! Gmail list-mailboxes coroutine wrapping `users.labels.list`, then
//! one `users.labels.get` per label when counts are requested.
//!
//! Gmail returns labels without counts; `totalEmails`/`unreadEmails`
//! require a per-label fetch, so `with_counts` turns the single list
//! into a list + N gets.

use alloc::{string::String, vec::Vec};
use core::mem;

use io_gmail::{
    coroutine::{GmailCoroutine, GmailCoroutineState, GmailYield},
    v1::rest::labels::{GmailLabel, get::GmailLabelGet, list::GmailLabelsList},
    v1::send::GmailSendError,
};
use io_http::rfc6750::bearer::HttpAuthBearer;
use log::trace;
use thiserror::Error;

use crate::mailbox::types::Mailbox;

/// Errors produced by [`GmailMailboxList`].
#[derive(Debug, Error)]
pub enum GmailMailboxListError {
    #[error(transparent)]
    Send(#[from] GmailSendError),
    #[error("coroutine was resumed after completion")]
    ResumedAfterDone,
}

/// I/O-free coroutine listing every Gmail label as a [`Mailbox`].
pub struct GmailMailboxList {
    state: State,
    auth: HttpAuthBearer,
    user_id: String,
}

impl GmailMailboxList {
    pub fn new(
        auth: &HttpAuthBearer,
        user_id: &str,
        with_counts: bool,
    ) -> Result<Self, GmailMailboxListError> {
        trace!("prepare Gmail mailbox listing (with_counts={with_counts})");
        let list = GmailLabelsList::new(auth, user_id)?;
        Ok(Self {
            state: State::Listing { with_counts, list },
            auth: auth.clone(),
            user_id: user_id.into(),
        })
    }
}

enum State {
    Listing {
        with_counts: bool,
        list: GmailLabelsList,
    },
    Counting {
        labels: Vec<GmailLabel>,
        index: usize,
        current: GmailLabelGet,
        done: Vec<Mailbox>,
    },
    Done,
}

/// Converts one Gmail label into the shared [`Mailbox`] shape.
fn mailbox_from(label: GmailLabel) -> Mailbox {
    Mailbox {
        id: label.id,
        name: label.name,
        total: label.messages_total,
        unread: label.messages_unread,
    }
}

impl GmailCoroutine for GmailMailboxList {
    type Yield = GmailYield;
    type Return = Result<Vec<Mailbox>, GmailMailboxListError>;

    fn resume(
        &mut self,
        mut bytes: Option<&[u8]>,
    ) -> GmailCoroutineState<Self::Yield, Self::Return> {
        loop {
            match mem::replace(&mut self.state, State::Done) {
                State::Listing {
                    with_counts,
                    mut list,
                } => match list.resume(bytes) {
                    GmailCoroutineState::Yielded(y) => {
                        self.state = State::Listing { with_counts, list };
                        return GmailCoroutineState::Yielded(y);
                    }
                    GmailCoroutineState::Complete(Err(err)) => {
                        return GmailCoroutineState::Complete(Err(err.into()));
                    }
                    GmailCoroutineState::Complete(Ok(out)) => {
                        let labels = out.response.labels;
                        if !with_counts {
                            let mailboxes = labels.into_iter().map(mailbox_from).collect();
                            return GmailCoroutineState::Complete(Ok(mailboxes));
                        }
                        if labels.is_empty() {
                            return GmailCoroutineState::Complete(Ok(Vec::new()));
                        }
                        let current =
                            match GmailLabelGet::new(&self.auth, &self.user_id, &labels[0].id) {
                                Ok(get) => get,
                                Err(err) => {
                                    return GmailCoroutineState::Complete(Err(err.into()));
                                }
                            };
                        self.state = State::Counting {
                            labels,
                            index: 0,
                            current,
                            done: Vec::new(),
                        };
                        bytes = None;
                    }
                },
                State::Counting {
                    labels,
                    index,
                    mut current,
                    mut done,
                } => match current.resume(bytes) {
                    GmailCoroutineState::Yielded(y) => {
                        self.state = State::Counting {
                            labels,
                            index,
                            current,
                            done,
                        };
                        return GmailCoroutineState::Yielded(y);
                    }
                    GmailCoroutineState::Complete(Err(err)) => {
                        return GmailCoroutineState::Complete(Err(err.into()));
                    }
                    GmailCoroutineState::Complete(Ok(out)) => {
                        done.push(mailbox_from(out.response));
                        let index = index + 1;
                        if index >= labels.len() {
                            return GmailCoroutineState::Complete(Ok(done));
                        }
                        let current = match GmailLabelGet::new(
                            &self.auth,
                            &self.user_id,
                            &labels[index].id,
                        ) {
                            Ok(get) => get,
                            Err(err) => {
                                return GmailCoroutineState::Complete(Err(err.into()));
                            }
                        };
                        self.state = State::Counting {
                            labels,
                            index,
                            current,
                            done,
                        };
                        bytes = None;
                    }
                },
                State::Done => {
                    return GmailCoroutineState::Complete(Err(
                        GmailMailboxListError::ResumedAfterDone,
                    ));
                }
            }
        }
    }
}
