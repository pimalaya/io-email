//! IMAP message-get coroutine.
//!
//! Optional `SELECT <mailbox>` (gated on `auto_select`) followed by
//! `UID FETCH <id> (BODY.PEEK[])`. Returns the raw RFC 5322 bytes
//! without flipping the \Seen flag.

use alloc::{string::String, vec, vec::Vec};
use core::mem;

use io_imap::{
    codec::fragmentizer::Fragmentizer,
    coroutine::{ImapCoroutine, ImapCoroutineState, ImapYield},
    rfc3501::{
        fetch::{ImapMessageFetch, ImapMessageFetchError},
        select::{ImapMailboxSelect, ImapMailboxSelectError},
    },
    types::fetch::{MacroOrMessageDataItemNames, MessageDataItem, MessageDataItemName},
};
use log::trace;
use thiserror::Error;

use crate::imap::convert::{InvalidMailboxName, InvalidUidSet, parse_mailbox, parse_uids};

/// Errors produced by [`ImapMessageGet`].
#[derive(Debug, Error)]
pub enum ImapMessageGetError {
    #[error(transparent)]
    Select(#[from] ImapMailboxSelectError),
    #[error(transparent)]
    Fetch(#[from] ImapMessageFetchError),
    #[error("invalid IMAP mailbox `{0}`")]
    InvalidMailbox(String),
    #[error("invalid message UID `{0}`")]
    InvalidUid(String),
    #[error("empty UID set")]
    EmptyUidSet,
    #[error("FETCH returned no body for the requested message")]
    EmptyBody,
    #[error("coroutine was resumed after completion")]
    ResumedAfterDone,
}

impl From<InvalidMailboxName> for ImapMessageGetError {
    fn from(err: InvalidMailboxName) -> Self {
        Self::InvalidMailbox(err.0)
    }
}

impl From<InvalidUidSet> for ImapMessageGetError {
    fn from(err: InvalidUidSet) -> Self {
        match err {
            InvalidUidSet::Empty => Self::EmptyUidSet,
            InvalidUidSet::Invalid(s) => Self::InvalidUid(s),
        }
    }
}

/// I/O-free coroutine fetching one message's raw RFC 5322 bytes.
pub struct ImapMessageGet {
    state: State,
}

impl ImapMessageGet {
    pub fn new(mailbox: &str, id: &str, auto_select: bool) -> Result<Self, ImapMessageGetError> {
        trace!("prepare IMAP message get (auto_select={auto_select})");
        let mbox = parse_mailbox(mailbox)?;
        let sequence_set = parse_uids(&[id])?;
        let item_names =
            MacroOrMessageDataItemNames::MessageDataItemNames(vec![MessageDataItemName::BodyExt {
                section: None,
                partial: None,
                peek: true,
            }]);
        let fetch = ImapMessageFetch::new(sequence_set, item_names, true);
        let state = if auto_select {
            State::Selecting {
                select: ImapMailboxSelect::new(mbox),
                fetch,
            }
        } else {
            State::Fetching(fetch)
        };
        Ok(Self { state })
    }
}

#[allow(clippy::large_enum_variant)] // see flag_store.rs for rationale
enum State {
    Selecting {
        select: ImapMailboxSelect,
        fetch: ImapMessageFetch,
    },
    Fetching(ImapMessageFetch),
    Done,
}

impl ImapCoroutine for ImapMessageGet {
    type Yield = ImapYield;
    type Return = Result<Vec<u8>, ImapMessageGetError>;

    fn resume(
        &mut self,
        fragmentizer: &mut Fragmentizer,
        mut bytes: Option<&[u8]>,
    ) -> ImapCoroutineState<Self::Yield, Self::Return> {
        loop {
            match mem::replace(&mut self.state, State::Done) {
                State::Selecting { mut select, fetch } => {
                    match select.resume(fragmentizer, bytes.take()) {
                        ImapCoroutineState::Yielded(yielded) => {
                            self.state = State::Selecting { select, fetch };
                            return ImapCoroutineState::Yielded(yielded);
                        }
                        ImapCoroutineState::Complete(Ok(_)) => {
                            self.state = State::Fetching(fetch);
                        }
                        ImapCoroutineState::Complete(Err(err)) => {
                            return ImapCoroutineState::Complete(Err(err.into()));
                        }
                    }
                }
                State::Fetching(mut fetch) => match fetch.resume(fragmentizer, bytes.take()) {
                    ImapCoroutineState::Yielded(yielded) => {
                        self.state = State::Fetching(fetch);
                        return ImapCoroutineState::Yielded(yielded);
                    }
                    ImapCoroutineState::Complete(Ok(data)) => {
                        let raw = data
                            .into_values()
                            .flat_map(|items| items.into_inner().into_iter())
                            .find_map(|item| match item {
                                MessageDataItem::BodyExt { data, .. } => {
                                    data.0.map(|d| d.as_ref().to_vec())
                                }
                                _ => None,
                            });
                        match raw {
                            Some(raw) => return ImapCoroutineState::Complete(Ok(raw)),
                            None => {
                                return ImapCoroutineState::Complete(Err(
                                    ImapMessageGetError::EmptyBody,
                                ));
                            }
                        }
                    }
                    ImapCoroutineState::Complete(Err(err)) => {
                        return ImapCoroutineState::Complete(Err(err.into()));
                    }
                },
                State::Done => {
                    return ImapCoroutineState::Complete(Err(
                        ImapMessageGetError::ResumedAfterDone,
                    ));
                }
            }
        }
    }
}
