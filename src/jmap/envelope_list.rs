//! JMAP envelope-listing coroutine.
//!
//! Two-stage state machine:
//! 1. `Mailbox/query + Mailbox/get` with a name filter resolves the
//!    shared mailbox name to a JMAP id.
//! 2. `Email/query + Email/get` (batched in a single HTTP round-trip)
//!    fetches the envelope properties for messages inside that
//!    mailbox.
//!
//! `page = 1`-indexed pagination is translated into JMAP's `position`
//! / `limit`; sorting is left to the server (the JMAP wire result
//! already comes back position-ordered).

use alloc::{string::String, vec, vec::Vec};
use core::mem;

use io_jmap::{
    coroutine::{JmapCoroutine, JmapCoroutineState, JmapYield},
    rfc8620::session::JmapSession,
    rfc8621::{
        email::EmailFilter,
        email_query::{JmapEmailQuery as InnerQuery, JmapEmailQueryError as QueryErr},
        mailbox::{MailboxFilter, MailboxProperty},
        mailbox_query::{
            JmapMailboxQuery as InnerMailboxQuery, JmapMailboxQueryError as MailboxQueryErr,
        },
    },
};
use log::trace;
use secrecy::SecretString;
use thiserror::Error;

use crate::{
    envelope::Envelope,
    jmap::convert::{compute_position_limit, envelope_from, envelope_properties, find_mailbox_id},
};

/// Errors produced by [`JmapEnvelopeList`].
#[derive(Debug, Error)]
pub enum JmapEnvelopeListError {
    #[error(transparent)]
    MailboxQuery(#[from] MailboxQueryErr),
    #[error(transparent)]
    EmailQuery(#[from] QueryErr),
    #[error("no JMAP mailbox named `{0}` found")]
    NotFound(String),
    #[error("coroutine was resumed after completion")]
    ResumedAfterDone,
}

/// I/O-free coroutine listing envelopes from a JMAP mailbox.
pub struct JmapEnvelopeList {
    state: State,
    name: String,
    session: JmapSession,
    http_auth: SecretString,
    page: Option<u32>,
    page_size: Option<u32>,
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
        let query = InnerMailboxQuery::new(
            session,
            http_auth,
            Some(MailboxFilter {
                name: Some(mailbox.into()),
                ..MailboxFilter::default()
            }),
            None,
            None,
            None,
            Some(vec![MailboxProperty::Id, MailboxProperty::Name]),
        )?;
        Ok(Self {
            state: State::Resolving(query),
            name: mailbox.into(),
            session: session.clone(),
            http_auth: http_auth.clone(),
            page,
            page_size,
        })
    }
}

enum State {
    Resolving(InnerMailboxQuery),
    Listing(InnerQuery),
    Done,
}

impl JmapCoroutine for JmapEnvelopeList {
    type Yield = JmapYield;
    type Return = Result<Vec<Envelope>, JmapEnvelopeListError>;

    fn resume(&mut self, bytes: Option<&[u8]>) -> JmapCoroutineState<Self::Yield, Self::Return> {
        match mem::replace(&mut self.state, State::Done) {
            State::Resolving(mut query) => match query.resume(bytes) {
                JmapCoroutineState::Complete(Ok(ok)) => {
                    let Some(id) = find_mailbox_id(&ok.mailboxes, &self.name) else {
                        return JmapCoroutineState::Complete(Err(JmapEnvelopeListError::NotFound(
                            self.name.clone(),
                        )));
                    };
                    let (position, limit) = compute_position_limit(self.page, self.page_size);
                    let filter = EmailFilter {
                        in_mailbox: Some(id),
                        ..EmailFilter::default()
                    };
                    let inner = match InnerQuery::new(
                        &self.session,
                        &self.http_auth,
                        Some(filter.into()),
                        None,
                        position,
                        limit,
                        Some(envelope_properties()),
                    ) {
                        Ok(q) => q,
                        Err(err) => return JmapCoroutineState::Complete(Err(err.into())),
                    };
                    self.state = State::Listing(inner);
                    JmapCoroutine::resume(self, None)
                }
                JmapCoroutineState::Yielded(y) => {
                    self.state = State::Resolving(query);
                    JmapCoroutineState::Yielded(y)
                }
                JmapCoroutineState::Complete(Err(err)) => {
                    JmapCoroutineState::Complete(Err(err.into()))
                }
            },
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
