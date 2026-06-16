//! Gmail envelope-list coroutine: `users.messages.list` (walking
//! `pageToken` to the requested page) then one
//! `users.messages.get` (metadata) per id.
//!
//! Gmail's list endpoint returns only ids; the per-id metadata fetch
//! fills the envelope. `page` is 1-indexed; pages before it are walked
//! via the opaque `nextPageToken`.

use alloc::{
    string::{String, ToString},
    vec::Vec,
};
use core::mem;

use io_gmail::{
    coroutine::{GmailCoroutine, GmailCoroutineState, GmailYield},
    v1::rest::messages::{
        GmailMessageFormat,
        get::GmailMessageGet,
        list::{GmailMessagesList, GmailMessagesListParams},
    },
    v1::send::GmailSendError,
};
use io_http::rfc6750::bearer::HttpAuthBearer;
use log::trace;
use thiserror::Error;

use crate::{
    envelope::types::Envelope,
    gmail::convert::{envelope_from, include_spam_trash},
};

/// Errors produced by [`GmailEnvelopeList`].
#[derive(Debug, Error)]
pub enum GmailEnvelopeListError {
    #[error(transparent)]
    Send(#[from] GmailSendError),
    #[error("coroutine was resumed after completion")]
    ResumedAfterDone,
}

/// I/O-free coroutine listing envelopes from a Gmail label.
pub struct GmailEnvelopeList {
    state: State,
    auth: HttpAuthBearer,
    user_id: String,
    label_ids: Vec<String>,
    page_size: Option<u32>,
    include_spam_trash: bool,
}

impl GmailEnvelopeList {
    pub fn new(
        auth: &HttpAuthBearer,
        user_id: &str,
        mailbox: &str,
        page: Option<u32>,
        page_size: Option<u32>,
    ) -> Result<Self, GmailEnvelopeListError> {
        trace!("prepare Gmail envelope listing");
        let label_ids = alloc::vec![mailbox.to_string()];
        let include_spam_trash = include_spam_trash(mailbox);
        let skip = page.unwrap_or(1).max(1) - 1;

        let params = GmailMessagesListParams {
            label_ids: &label_ids,
            max_results: page_size,
            include_spam_trash,
            ..Default::default()
        };
        let list = GmailMessagesList::new(auth, user_id, &params)?;

        Ok(Self {
            state: State::Listing {
                skip,
                current: list,
            },
            auth: auth.clone(),
            user_id: user_id.into(),
            label_ids,
            page_size,
            include_spam_trash,
        })
    }

    fn list_page(&self, page_token: Option<&str>) -> Result<GmailMessagesList, GmailSendError> {
        let params = GmailMessagesListParams {
            label_ids: &self.label_ids,
            max_results: self.page_size,
            page_token,
            include_spam_trash: self.include_spam_trash,
            ..Default::default()
        };
        GmailMessagesList::new(&self.auth, &self.user_id, &params)
    }

    fn message(&self, id: &str) -> Result<GmailMessageGet, GmailSendError> {
        GmailMessageGet::new(
            &self.auth,
            &self.user_id,
            id,
            GmailMessageFormat::Metadata,
            &[],
        )
    }
}

enum State {
    Listing {
        skip: u32,
        current: GmailMessagesList,
    },
    Getting {
        ids: Vec<String>,
        index: usize,
        current: GmailMessageGet,
        done: Vec<Envelope>,
    },
    Done,
}

impl GmailCoroutine for GmailEnvelopeList {
    type Yield = GmailYield;
    type Return = Result<Vec<Envelope>, GmailEnvelopeListError>;

    fn resume(
        &mut self,
        mut bytes: Option<&[u8]>,
    ) -> GmailCoroutineState<Self::Yield, Self::Return> {
        loop {
            match mem::replace(&mut self.state, State::Done) {
                State::Listing { skip, mut current } => match current.resume(bytes) {
                    GmailCoroutineState::Yielded(y) => {
                        self.state = State::Listing { skip, current };
                        return GmailCoroutineState::Yielded(y);
                    }
                    GmailCoroutineState::Complete(Err(err)) => {
                        return GmailCoroutineState::Complete(Err(err.into()));
                    }
                    GmailCoroutineState::Complete(Ok(out)) => {
                        let response = out.response;

                        if skip > 0 {
                            let Some(token) = response.next_page_token else {
                                return GmailCoroutineState::Complete(Ok(Vec::new()));
                            };
                            let current = match self.list_page(Some(&token)) {
                                Ok(list) => list,
                                Err(err) => {
                                    return GmailCoroutineState::Complete(Err(err.into()));
                                }
                            };
                            self.state = State::Listing {
                                skip: skip - 1,
                                current,
                            };
                            bytes = None;
                            continue;
                        }

                        let ids: Vec<String> =
                            response.messages.into_iter().map(|m| m.id).collect();
                        if ids.is_empty() {
                            return GmailCoroutineState::Complete(Ok(Vec::new()));
                        }
                        let current = match self.message(&ids[0]) {
                            Ok(get) => get,
                            Err(err) => return GmailCoroutineState::Complete(Err(err.into())),
                        };
                        self.state = State::Getting {
                            ids,
                            index: 0,
                            current,
                            done: Vec::new(),
                        };
                        bytes = None;
                    }
                },
                State::Getting {
                    ids,
                    index,
                    mut current,
                    mut done,
                } => match current.resume(bytes) {
                    GmailCoroutineState::Yielded(y) => {
                        self.state = State::Getting {
                            ids,
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
                        done.push(envelope_from(out.response));
                        let index = index + 1;
                        if index >= ids.len() {
                            return GmailCoroutineState::Complete(Ok(done));
                        }
                        let current = match self.message(&ids[index]) {
                            Ok(get) => get,
                            Err(err) => return GmailCoroutineState::Complete(Err(err.into())),
                        };
                        self.state = State::Getting {
                            ids,
                            index,
                            current,
                            done,
                        };
                        bytes = None;
                    }
                },
                State::Done => {
                    return GmailCoroutineState::Complete(Err(
                        GmailEnvelopeListError::ResumedAfterDone,
                    ));
                }
            }
        }
    }
}
