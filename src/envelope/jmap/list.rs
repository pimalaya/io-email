//! JMAP envelope-list coroutine wrapping Email/query + Email/get
//! batched in one HTTP round-trip.
//!
//! `page = 1`-indexed pagination is translated to JMAP's
//! position/limit; sort order comes back from the server.
//!
//! # Example
//!
//! ```rust,ignore
//! use io_email::envelope::jmap::list::JmapEnvelopeList;
//!
//! let envs = client.run(JmapEnvelopeList::new(&session, &auth, "mailbox-id", Some(1), Some(50))?)?;
//! ```

use alloc::vec::Vec;
use core::mem;

use io_jmap::{
    coroutine::{JmapCoroutine, JmapCoroutineState, JmapYield},
    rfc8620::JmapSession,
    rfc8621::email::{
        JmapEmailFilter,
        query::{
            JmapEmailQuery as InnerQuery, JmapEmailQueryError as QueryErr, JmapEmailQueryOptions,
        },
    },
};
use log::trace;
use secrecy::SecretString;
use thiserror::Error;

use crate::{
    envelope::types::Envelope,
    jmap::convert::{compute_position_limit, envelope_from, envelope_properties},
};

/// Errors produced by [`JmapEnvelopeList`].
#[derive(Debug, Error)]
pub enum JmapEnvelopeListError {
    #[error(transparent)]
    EmailQuery(#[from] QueryErr),
    #[error("coroutine was resumed after completion")]
    ResumedAfterDone,
}

/// I/O-free coroutine listing envelopes from a JMAP mailbox.
pub struct JmapEnvelopeList {
    state: State,
}

impl JmapEnvelopeList {
    pub fn new(
        session: &JmapSession,
        http_auth: &SecretString,
        mailbox: &str,
        page: Option<u32>,
        page_size: Option<u32>,
    ) -> Result<Self, JmapEnvelopeListError> {
        trace!("prepare JMAP envelope listing");
        let (position, limit) = compute_position_limit(page, page_size);
        let filter = JmapEmailFilter {
            in_mailbox: Some(mailbox.into()),
            ..JmapEmailFilter::default()
        };
        let opts = JmapEmailQueryOptions {
            filter: Some(filter.into()),
            position,
            limit,
            properties: Some(envelope_properties()),
            ..Default::default()
        };
        let inner = InnerQuery::new(session, http_auth, opts)?;
        Ok(Self {
            state: State::Listing(inner),
        })
    }
}

enum State {
    Listing(InnerQuery),
    Done,
}

impl JmapCoroutine for JmapEnvelopeList {
    type Yield = JmapYield;
    type Return = Result<Vec<Envelope>, JmapEnvelopeListError>;

    fn resume(&mut self, bytes: Option<&[u8]>) -> JmapCoroutineState<Self::Yield, Self::Return> {
        match mem::replace(&mut self.state, State::Done) {
            State::Listing(mut query) => match query.resume(bytes) {
                JmapCoroutineState::Complete(Ok(ok)) => {
                    let envelopes = ok.emails.into_iter().map(envelope_from).collect();
                    JmapCoroutineState::Complete(Ok(envelopes))
                }
                JmapCoroutineState::Yielded(y) => {
                    self.state = State::Listing(query);
                    JmapCoroutineState::Yielded(y)
                }
                JmapCoroutineState::Complete(Err(err)) => {
                    JmapCoroutineState::Complete(Err(err.into()))
                }
            },
            State::Done => {
                JmapCoroutineState::Complete(Err(JmapEnvelopeListError::ResumedAfterDone))
            }
        }
    }
}
