//! JMAP watch-mailbox coroutine (generator shape).
//!
//! Mirrors the IMAP-IDLE pattern over a single HTTP/1.1 connection
//! by driving the JMAP EventSource in `closeafter=state` mode: each
//! subscription cycle delivers exactly one
//! [`StateChange`](io_jmap::rfc8620::event_source::StateChange), the
//! server closes the chunked response, the TCP socket is released
//! (HTTP keep-alive), the coroutine runs the follow-up `Email/changes`
//! + `Email/get` POSTs on the same connection, diffs the response
//! against an in-memory shadow, emits one [`WatchEvent`] per delta,
//! then resubscribes for the next cycle.
//!
//! State machine:
//!
//! ```text
//! Resolving (Mailbox/query, exact-name post-filter)
//!     ↓ mailbox_id resolved
//! Subscribing (JmapEventSource, closeafter=state)
//!     ↓ one StateChange + chunked terminator
//! FetchingChanges (Email/changes since previous Email-type state)
//!     ↓ created/updated/destroyed ids
//! FetchingEmails (Email/get on created+updated with envelope + mailboxIds props)
//!     ↓ shadow diff produces N WatchEvent
//! Emitting → drain one event per `Yielded(Event(_))`
//!     ↺ back to Subscribing for next cycle
//! ```
//!
//! Cooperative shutdown via the caller-owned [`Arc<AtomicBool>`]:
//! polled at every resume; transitions to terminal
//! [`CoroutineState::Complete`] when set. The client driver loop is
//! the one that has to honour the flag during an in-progress
//! blocking socket read.
//!
//! Bootstrap suppression: the very first subscription cycle runs
//! against `sinceState = ""`, which on most JMAP servers returns the
//! mailbox's entire current inventory as `created`. To match the
//! IMAP watcher's contract (seed → emit only deltas afterwards),
//! events produced during the bootstrap cycle are silently consumed
//! to populate the shadow without surfacing them to the caller.

use alloc::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    string::String,
    sync::Arc,
    vec,
    vec::Vec,
};
use core::{
    mem,
    sync::atomic::{AtomicBool, Ordering},
};

use io_jmap::{
    coroutine::{JmapCoroutine, JmapCoroutineState, JmapYield},
    rfc8620::{
        changes::JmapChangesOutput,
        event_source::{
            CloseAfter, JmapEventSource, JmapEventSourceError, JmapEventSourceYield, StateChange,
        },
        session::JmapSession,
    },
    rfc8621::{
        email::{Email, EmailProperty},
        email_changes::{JmapEmailChanges, JmapEmailChangesError},
        email_get::{JmapEmailGet, JmapEmailGetError},
        mailbox::{MailboxFilter, MailboxProperty},
        mailbox_query::{JmapMailboxQuery, JmapMailboxQueryError},
    },
};
use log::trace;
use secrecy::SecretString;
use thiserror::Error;

use crate::{
    coroutine::{EmailBackend, EmailCoroutine, EmailCoroutineArg, EmailCoroutineState},
    event::WatchEvent,
    flag::Flag,
    jmap::convert::{account_id_of, envelope_from, envelope_properties, find_mailbox_id},
};

/// Threads a `Result<State, JmapWatchMailboxError>` through the
/// generator-shape `CoroutineState<Y, Result<(), E>>` return type:
/// on `Err` it terminates the coroutine with `Complete`, on `Ok` it
/// unwraps the state. Hand-rolled `?` analog (the `?` operator
/// doesn't apply because `resume` does not return a `Result`).
macro_rules! try_state {
    ($expr:expr) => {
        match $expr {
            Ok(v) => v,
            Err(err) => return EmailCoroutineState::Complete(Err(err)),
        }
    };
}

/// JMAP type tag we subscribe to and diff against.
const EMAIL_TYPE: &str = "Email";
/// Server-side ping cadence (seconds) for the SSE channel.
const PING_SECONDS: u64 = 30;

/// Errors produced by [`JmapWatchMailbox`].
#[derive(Debug, Error)]
pub enum JmapWatchMailboxError {
    #[error(transparent)]
    MailboxQuery(#[from] JmapMailboxQueryError),
    #[error(transparent)]
    EventSource(#[from] JmapEventSourceError),
    #[error(transparent)]
    EmailChanges(#[from] JmapEmailChangesError),
    #[error(transparent)]
    EmailGet(#[from] JmapEmailGetError),
    #[error("no JMAP mailbox named `{0}` found")]
    NotFound(String),
    #[error("coroutine was resumed with the wrong EmailCoroutineArg variant")]
    InvalidArg,
}

/// Per-coroutine Yield: socket I/O on one axis, [`WatchEvent`]s on
/// the other.
#[derive(Debug)]
pub enum JmapWatchMailboxYield {
    /// Driver should read more bytes and feed them back via the
    /// `EmailCoroutineArg::Jmap` variant on the next resume.
    WantsRead,
    /// Driver should write these bytes to the socket; the next
    /// resume typically takes `bytes: None`.
    WantsWrite(Vec<u8>),
    /// One pre-diffed delta computed against the in-memory shadow.
    Event(WatchEvent),
}

/// I/O-free generator-shape coroutine watching a single JMAP mailbox
/// over a single HTTP/1.1 connection.
pub struct JmapWatchMailbox {
    state: State,
    mailbox: String,
    mailbox_id: Option<String>,
    session: JmapSession,
    http_auth: SecretString,
    account_id: String,
    shutdown: Arc<AtomicBool>,
    /// email_id → keyword bag. Maintained in lockstep with the
    /// server-side view of the watched mailbox.
    shadow: BTreeMap<String, BTreeSet<String>>,
    /// Latest known Email-type state for the watched account; passed
    /// as `sinceState` on the next `Email/changes`. `None` on the
    /// bootstrap cycle.
    email_state: Option<String>,
    /// FIFO of events to emit one per resume between subscription
    /// cycles.
    pending: VecDeque<WatchEvent>,
    /// `true` until the bootstrap cycle has populated the shadow.
    /// Events produced while set are dropped (they would otherwise
    /// fire one EnvelopeAdded per existing email at startup).
    suppress_events: bool,
}

impl JmapWatchMailbox {
    pub fn new(
        session: &JmapSession,
        http_auth: &SecretString,
        mailbox: &str,
        shutdown: Arc<AtomicBool>,
    ) -> Result<Self, JmapWatchMailboxError> {
        trace!("prepare JMAP mailbox watch");
        let query = JmapMailboxQuery::new(
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
            mailbox: mailbox.into(),
            mailbox_id: None,
            session: session.clone(),
            http_auth: http_auth.clone(),
            account_id: account_id_of(session),
            shutdown,
            shadow: BTreeMap::new(),
            email_state: None,
            pending: VecDeque::new(),
            suppress_events: true,
        })
    }
}

impl EmailCoroutine for JmapWatchMailbox {
    type Yield = JmapWatchMailboxYield;
    type Return = Result<(), JmapWatchMailboxError>;

    const BACKEND: EmailBackend = EmailBackend::Jmap;

    fn resume(
        &mut self,
        arg: EmailCoroutineArg<'_>,
    ) -> EmailCoroutineState<Self::Yield, Self::Return> {
        #[allow(irrefutable_let_patterns)]
        let EmailCoroutineArg::Jmap { bytes } = arg else {
            return EmailCoroutineState::Complete(Err(JmapWatchMailboxError::InvalidArg));
        };

        if self.shutdown.load(Ordering::SeqCst) {
            self.state = State::Done;
            return EmailCoroutineState::Complete(Ok(()));
        }

        let mut bytes = bytes;
        loop {
            match mem::replace(&mut self.state, State::Done) {
                State::Resolving(mut query) => match query.resume(bytes.take()) {
                    JmapCoroutineState::Yielded(JmapYield::WantsRead) => {
                        self.state = State::Resolving(query);
                        return EmailCoroutineState::Yielded(JmapWatchMailboxYield::WantsRead);
                    }
                    JmapCoroutineState::Yielded(JmapYield::WantsWrite(out)) => {
                        self.state = State::Resolving(query);
                        return EmailCoroutineState::Yielded(JmapWatchMailboxYield::WantsWrite(
                            out,
                        ));
                    }
                    JmapCoroutineState::Complete(Err(err)) => {
                        return EmailCoroutineState::Complete(Err(err.into()));
                    }
                    JmapCoroutineState::Complete(Ok(ok)) => {
                        let Some(id) = find_mailbox_id(&ok.mailboxes, &self.mailbox) else {
                            return EmailCoroutineState::Complete(Err(
                                JmapWatchMailboxError::NotFound(self.mailbox.clone()),
                            ));
                        };
                        self.mailbox_id = Some(id);
                        self.state = try_state!(self.fresh_subscription_state());
                    }
                },
                State::Subscribing {
                    mut es,
                    mut latest_change,
                } => match es.resume(bytes.take()) {
                    JmapCoroutineState::Yielded(JmapEventSourceYield::WantsRead) => {
                        self.state = State::Subscribing { es, latest_change };
                        return EmailCoroutineState::Yielded(JmapWatchMailboxYield::WantsRead);
                    }
                    JmapCoroutineState::Yielded(JmapEventSourceYield::WantsWrite(out)) => {
                        self.state = State::Subscribing { es, latest_change };
                        return EmailCoroutineState::Yielded(JmapWatchMailboxYield::WantsWrite(
                            out,
                        ));
                    }
                    JmapCoroutineState::Yielded(JmapEventSourceYield::Frame(change)) => {
                        latest_change = Some(change);
                        // Stay in Subscribing; the chunked terminator
                        // still has to be drained before the socket
                        // is free for follow-up POSTs.
                        self.state = State::Subscribing { es, latest_change };
                    }
                    JmapCoroutineState::Complete(Ok(())) => {
                        // Subscription cycle complete; socket is now
                        // available for Email/changes + Email/get.
                        self.state = try_state!(self.handle_cycle_end(latest_change));
                    }
                    JmapCoroutineState::Complete(Err(err)) => {
                        return EmailCoroutineState::Complete(Err(err.into()));
                    }
                },
                State::FetchingChanges(mut changes) => match changes.resume(bytes.take()) {
                    JmapCoroutineState::Yielded(JmapYield::WantsRead) => {
                        self.state = State::FetchingChanges(changes);
                        return EmailCoroutineState::Yielded(JmapWatchMailboxYield::WantsRead);
                    }
                    JmapCoroutineState::Yielded(JmapYield::WantsWrite(out)) => {
                        self.state = State::FetchingChanges(changes);
                        return EmailCoroutineState::Yielded(JmapWatchMailboxYield::WantsWrite(
                            out,
                        ));
                    }
                    JmapCoroutineState::Complete(Err(err)) => {
                        return EmailCoroutineState::Complete(Err(err.into()));
                    }
                    JmapCoroutineState::Complete(Ok(ok)) => {
                        self.state = try_state!(self.dispatch_get(ok));
                    }
                },
                State::FetchingEmails {
                    mut get,
                    destroyed,
                    new_state,
                } => match get.resume(bytes.take()) {
                    JmapCoroutineState::Yielded(JmapYield::WantsRead) => {
                        self.state = State::FetchingEmails {
                            get,
                            destroyed,
                            new_state,
                        };
                        return EmailCoroutineState::Yielded(JmapWatchMailboxYield::WantsRead);
                    }
                    JmapCoroutineState::Yielded(JmapYield::WantsWrite(out)) => {
                        self.state = State::FetchingEmails {
                            get,
                            destroyed,
                            new_state,
                        };
                        return EmailCoroutineState::Yielded(JmapWatchMailboxYield::WantsWrite(
                            out,
                        ));
                    }
                    JmapCoroutineState::Complete(Err(err)) => {
                        return EmailCoroutineState::Complete(Err(err.into()));
                    }
                    JmapCoroutineState::Complete(Ok(ok)) => {
                        self.apply_diff(ok.emails, destroyed);
                        self.email_state = Some(new_state);
                        // After the bootstrap cycle the shadow is
                        // seeded; future cycles surface their
                        // deltas.
                        self.suppress_events = false;
                        self.state = State::Emitting;
                    }
                },
                State::Emitting => {
                    if let Some(evt) = self.pending.pop_front() {
                        self.state = State::Emitting;
                        return EmailCoroutineState::Yielded(JmapWatchMailboxYield::Event(evt));
                    }
                    // No events left; subscribe again for the next
                    // cycle.
                    self.state = try_state!(self.fresh_subscription_state());
                }
                State::Done => return EmailCoroutineState::Complete(Ok(())),
            }
        }
    }
}

impl JmapWatchMailbox {
    /// Builds a fresh [`State::Subscribing`] (a new SSE round) using
    /// the configured shutdown flag.
    fn fresh_subscription_state(&self) -> Result<State, JmapWatchMailboxError> {
        let es = JmapEventSource::new(
            &self.session,
            &self.http_auth,
            &[EMAIL_TYPE],
            PING_SECONDS,
            CloseAfter::State,
            self.shutdown.clone(),
        )?;
        Ok(State::Subscribing {
            es,
            latest_change: None,
        })
    }

    /// Cycle ended: inspect the `StateChange` (if any) and decide
    /// whether to issue `Email/changes`, queue a `KeepAlive`, or
    /// resubscribe immediately.
    fn handle_cycle_end(
        &mut self,
        change: Option<StateChange>,
    ) -> Result<State, JmapWatchMailboxError> {
        let observed_state = change
            .as_ref()
            .and_then(|c| c.changed.get(&self.account_id))
            .and_then(|ts| ts.get(EMAIL_TYPE))
            .cloned();

        let needs_diff = match (&observed_state, &self.email_state) {
            (Some(observed), Some(known)) => observed != known,
            // Bootstrap cycle or first observation: always sync.
            (_, None) => true,
            // Server reported no Email state at all (KeepAlive or
            // non-Email change for our account): nothing to do.
            (None, _) => false,
        };

        if needs_diff {
            let since = self.email_state.clone().unwrap_or_default();
            let changes = JmapEmailChanges::new(&self.session, &self.http_auth, since, None)?;
            return Ok(State::FetchingChanges(changes));
        }

        if !self.suppress_events && change.is_some() {
            self.pending.push_back(WatchEvent::KeepAlive);
        }
        Ok(State::Emitting)
    }

    /// Builds an `Email/get` for the union of created+updated ids
    /// and stashes the destroyed-id list for later diffing.
    fn dispatch_get(&self, ok: JmapChangesOutput) -> Result<State, JmapWatchMailboxError> {
        let JmapChangesOutput {
            new_state,
            has_more_changes,
            mut created,
            updated,
            destroyed,
            ..
        } = ok;

        if has_more_changes {
            trace!("JMAP Email/changes truncated; next subscription cycle will catch up");
        }

        created.extend(updated);
        let mut properties = envelope_properties();
        properties.push(EmailProperty::MailboxIds);

        let get = JmapEmailGet::new(
            &self.session,
            &self.http_auth,
            created,
            Some(properties),
            false,
            false,
            0,
        )?;

        Ok(State::FetchingEmails {
            get,
            destroyed,
            new_state,
        })
    }

    /// Folds the freshly-fetched emails + destroyed ids into the
    /// shadow, queueing one [`WatchEvent`] per delta unless the
    /// bootstrap cycle is in progress.
    fn apply_diff(&mut self, emails: Vec<Email>, destroyed: Vec<String>) {
        let Some(mailbox_id) = self.mailbox_id.as_ref() else {
            return;
        };

        for email in emails {
            let Some(id) = email.id.clone() else {
                continue;
            };
            let in_mailbox = email
                .mailbox_ids
                .as_ref()
                .is_some_and(|map| map.get(mailbox_id).copied().unwrap_or(false));
            let new_keywords: BTreeSet<String> = email
                .keywords
                .clone()
                .unwrap_or_default()
                .into_iter()
                .filter_map(|(k, v)| if v { Some(k) } else { None })
                .collect();
            let was_in_shadow = self.shadow.contains_key(&id);

            if in_mailbox {
                if was_in_shadow {
                    let old = self.shadow.get(&id).cloned().unwrap_or_default();
                    let added: BTreeSet<String> = new_keywords.difference(&old).cloned().collect();
                    let removed: BTreeSet<String> =
                        old.difference(&new_keywords).cloned().collect();
                    if !added.is_empty() && !self.suppress_events {
                        self.pending.push_back(WatchEvent::FlagsAdded {
                            mailbox: self.mailbox.clone(),
                            id: id.clone(),
                            flags: added.into_iter().map(Flag::from_raw).collect(),
                        });
                    }
                    if !removed.is_empty() && !self.suppress_events {
                        self.pending.push_back(WatchEvent::FlagsRemoved {
                            mailbox: self.mailbox.clone(),
                            id: id.clone(),
                            flags: removed.into_iter().map(Flag::from_raw).collect(),
                        });
                    }
                    self.shadow.insert(id, new_keywords);
                } else {
                    let envelope = envelope_from(email);
                    if !self.suppress_events {
                        self.pending.push_back(WatchEvent::EnvelopeAdded {
                            mailbox: self.mailbox.clone(),
                            envelope: envelope.clone(),
                        });
                    }
                    self.shadow.insert(envelope.id, new_keywords);
                }
            } else if was_in_shadow {
                if !self.suppress_events {
                    self.pending.push_back(WatchEvent::EnvelopeRemoved {
                        mailbox: self.mailbox.clone(),
                        id: id.clone(),
                    });
                }
                self.shadow.remove(&id);
            }
        }

        for id in destroyed {
            if self.shadow.remove(&id).is_some() && !self.suppress_events {
                self.pending.push_back(WatchEvent::EnvelopeRemoved {
                    mailbox: self.mailbox.clone(),
                    id,
                });
            }
        }
    }
}

/// Internal progression of [`JmapWatchMailbox`].
enum State {
    /// Resolving the mailbox name to a JMAP id.
    Resolving(JmapMailboxQuery),
    /// Subscribed: one cycle's [`JmapEventSource`] running.
    Subscribing {
        es: JmapEventSource,
        latest_change: Option<StateChange>,
    },
    /// Running `Email/changes` since the last known Email-type
    /// state.
    FetchingChanges(JmapEmailChanges),
    /// Running `Email/get` on the changed ids; carries the
    /// destroyed-id list and the post-changes state to commit on
    /// completion.
    FetchingEmails {
        get: JmapEmailGet,
        destroyed: Vec<String>,
        new_state: String,
    },
    /// Draining the per-cycle event queue one at a time.
    Emitting,
    /// Terminal.
    Done,
}
