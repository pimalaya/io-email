//! JMAP envelope-listing coroutine.
//!
//! Single-stage state machine:
//! `Email/query + Email/get` (batched in a single HTTP round-trip)
//! fetches the envelope properties for messages inside the mailbox.
//!
//! `page = 1`-indexed pagination is translated into JMAP's `position`
//! / `limit`; sorting is left to the server (the JMAP wire result
//! already comes back position-ordered).

use alloc::vec::Vec;
use core::mem;

use io_jmap::{
    coroutine::{JmapCoroutine, JmapCoroutineState, JmapYield},
    rfc8620::session::JmapSession,
    rfc8621::{
        email::EmailFilter,
        email_query::{JmapEmailQuery as InnerQuery, JmapEmailQueryError as QueryErr},
    },
};
use log::trace;
use secrecy::SecretString;
use thiserror::Error;

use crate::{
    envelope::Envelope,
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
        let filter = EmailFilter {
            in_mailbox: Some(mailbox.into()),
            ..EmailFilter::default()
        };
        let inner = InnerQuery::new(
            session,
            http_auth,
            Some(filter.into()),
            None,
            position,
            limit,
            Some(envelope_properties()),
        )?;
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
