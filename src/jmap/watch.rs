//! JMAP push-backed watch driver (RFC 8620 §7.2 EventSource).
//!
//! Consumes the [`JmapClientStd`] passed in, queries the watched
//! mailbox to seed an envelope/keyword shadow, opens an SSE channel
//! to `eventSourceUrl`, then loops on
//! [`io_http::client::SseStream`]: every JMAP `StateChange` frame whose
//! `Email` type-state moved triggers `Email/changes` + `Email/get` to
//! pull the new state. The result is filtered by `mailboxIds` against
//! the watched mailbox, diffed against the shadow, and streamed as
//! [`WatchEvent`]s through the returned [`WatchStream`].
//!
//! Cooperative shutdown: the SSE URL is opened with `ping=10` so the
//! server emits a keep-alive comment frame every ten seconds; on each
//! frame the driver polls the shutdown atomic and exits cleanly
//! within at most ten seconds. Dropping the [`WatchStream`] propagates
//! through and closes the socket.

use alloc::{
    collections::BTreeMap,
    string::{String, ToString},
    vec,
    vec::Vec,
};
use core::sync::atomic::Ordering;
use std::sync::{Arc, atomic::AtomicBool, mpsc::SyncSender};
use std::thread;

use io_http::{
    client::{HttpClientStd, SseStream},
    rfc9110::request::HttpRequest,
};
use io_jmap::{
    client::JmapClientStd,
    rfc8620::event_source::{parse_state_change, subscribe_url},
    rfc8621::email::{Email, EmailFilter, EmailProperty},
};
use log::trace;
use pimalaya_stream::{std::stream::StreamStd, tls::Tls};
use secrecy::ExposeSecret;
use url::Url;

use crate::{
    client::EmailClientStdError,
    envelope::Envelope,
    event::WatchEvent,
    flag::Flag,
    watch::{WatchResult, WatchShutdown, WatchStream, channel},
};

const SSE_PING_SECONDS: u64 = 10;
const EMAIL_TYPE: &str = "Email";

/// Builds and starts a JMAP push-backed watch over `mailbox`.
///
/// `mailbox` is the JMAP mailbox id (not the display name); the
/// shared API uses backend-specific identifiers, matching the rest of
/// the JMAP adapter.
pub(crate) fn watch_envelopes(
    mut client: JmapClientStd,
    mailbox: String,
) -> Result<WatchStream, EmailClientStdError> {
    let (initial_state, initial_shadow) = seed_state(&mut client, &mailbox)?;
    let sse = open_event_source(&client)?;

    let (shutdown, shutdown_flag) = WatchShutdown::new();
    let (tx, rx) = channel();

    let handle = thread::spawn(move || {
        watch_loop(
            client,
            sse,
            mailbox,
            initial_state,
            initial_shadow,
            shutdown_flag,
            tx,
        );
    });

    Ok(WatchStream::new(rx, handle, shutdown))
}

/// Per-message snapshot stored in the watcher's shadow. Only the flag
/// set is needed for diffing: the envelope payload travels with each
/// `EnvelopeAdded` event but is not retained across iterations.
#[derive(Clone, Debug)]
struct ShadowEntry {
    flags: alloc::collections::BTreeSet<Flag>,
}

/// Runs the initial `Email/query` + `Email/get` against the watched
/// mailbox to seed the shadow and capture the current `Email` state.
fn seed_state(
    client: &mut JmapClientStd,
    mailbox: &str,
) -> Result<(String, BTreeMap<String, ShadowEntry>), EmailClientStdError> {
    let filter = mailbox_filter(mailbox);
    let query = client.email_query(filter, None, None, None, Some(envelope_properties()))?;

    let mut shadow = BTreeMap::new();
    for email in query.emails {
        let envelope = Envelope::from(email);
        let flags = envelope.flags.clone();
        shadow.insert(envelope.id.clone(), ShadowEntry { flags });
    }

    let ids: Vec<String> = shadow.keys().cloned().collect();
    let state = if ids.is_empty() {
        // No emails yet: trigger an empty get just to read the current
        // Email state.
        client
            .email_get(Vec::new(), None, false, false, 0)?
            .new_state
    } else {
        client
            .email_get(ids, Some(envelope_properties()), false, false, 0)?
            .new_state
    };

    Ok((state, shadow))
}

/// Opens the SSE channel against the session's eventSourceUrl. Uses
/// system trust roots; bring your own [`Tls`] customisation by
/// upgrading the JMAP client connect path if needed.
fn open_event_source(client: &JmapClientStd) -> Result<SseStream, EmailClientStdError> {
    let session = client
        .session()
        .ok_or(EmailClientStdError::OperationFailed("jmap session missing"))?;

    let raw_url = subscribe_url(session, &[EMAIL_TYPE], SSE_PING_SECONDS);
    let url = Url::parse(&raw_url).map_err(|_| EmailClientStdError::InvalidUrl(raw_url.clone()))?;

    let host = url
        .host_str()
        .ok_or_else(|| EmailClientStdError::InvalidUrl(raw_url.clone()))?;
    let port = url.port_or_known_default().unwrap_or(443);

    let mut tls = Tls::default();
    tls.rustls.alpn = vec!["http/1.1".into()];
    let stream = StreamStd::connect_tls(host.to_string(), port, &tls)
        .map_err(|_| EmailClientStdError::OperationFailed("jmap event-source TLS connect"))?;

    let http = HttpClientStd::new(stream);

    let auth = client.http_auth().expose_secret().to_string();
    let request = HttpRequest::get(url)
        .header("Accept", "text/event-stream")
        .header("Authorization", &auth)
        .header("Cache-Control", "no-cache");

    let sse = http.send_streaming(request)?;
    Ok(sse)
}

fn watch_loop(
    mut client: JmapClientStd,
    mut sse: SseStream,
    mailbox: String,
    mut current_state: String,
    mut shadow: BTreeMap<String, ShadowEntry>,
    shutdown: Arc<AtomicBool>,
    tx: SyncSender<WatchResult>,
) {
    loop {
        if shutdown.load(Ordering::SeqCst) {
            break;
        }

        let frame = match sse.next_frame() {
            Ok(Some(frame)) => frame,
            Ok(None) => break,
            Err(err) => {
                let _ = tx.send(Err(err.into()));
                return;
            }
        };

        let change = match parse_state_change(&frame.data) {
            Ok(c) => c,
            Err(err) => {
                trace!("jmap watch: skip unparseable state change frame: {err}");
                continue;
            }
        };

        if change.changed.is_empty() {
            let _ = tx.send(Ok(WatchEvent::KeepAlive));
            continue;
        }

        let next_state = change
            .changed
            .values()
            .find_map(|states| states.get(EMAIL_TYPE).cloned());

        let Some(next_state) = next_state else {
            continue;
        };

        if next_state == current_state {
            continue;
        }

        let outcome = refresh(&mut client, &mailbox, &mut shadow, &current_state, &tx);

        match outcome {
            Ok(state) => current_state = state,
            Err(err) => {
                let _ = tx.send(Err(err));
                return;
            }
        }
    }

    trace!("jmap watch worker exiting");
}

fn refresh(
    client: &mut JmapClientStd,
    mailbox: &str,
    shadow: &mut BTreeMap<String, ShadowEntry>,
    since_state: &str,
    tx: &SyncSender<WatchResult>,
) -> Result<String, EmailClientStdError> {
    let changes = client.email_changes(since_state.to_string(), None)?;

    let mut touched: Vec<String> = Vec::new();
    touched.extend(changes.created.iter().cloned());
    touched.extend(changes.updated.iter().cloned());

    if !touched.is_empty() {
        let got = client.email_get(
            touched.clone(),
            Some(envelope_properties()),
            false,
            false,
            0,
        )?;

        for email in got.emails {
            if !email_is_in_mailbox(&email, mailbox) {
                shadow.remove(email.id.as_deref().unwrap_or(""));
                continue;
            }

            let envelope = Envelope::from(email);
            let flags = envelope.flags.clone();

            match shadow.get(&envelope.id).cloned() {
                None => {
                    shadow.insert(
                        envelope.id.clone(),
                        ShadowEntry {
                            flags: flags.clone(),
                        },
                    );
                    send(
                        tx,
                        Ok(WatchEvent::EnvelopeAdded {
                            mailbox: mailbox.to_string(),
                            envelope,
                        }),
                    )?;
                }
                Some(old) => {
                    let added: alloc::collections::BTreeSet<Flag> =
                        flags.difference(&old.flags).cloned().collect();
                    let removed: alloc::collections::BTreeSet<Flag> =
                        old.flags.difference(&flags).cloned().collect();

                    shadow.insert(
                        envelope.id.clone(),
                        ShadowEntry {
                            flags: flags.clone(),
                        },
                    );

                    if !added.is_empty() {
                        send(
                            tx,
                            Ok(WatchEvent::FlagsAdded {
                                mailbox: mailbox.to_string(),
                                id: envelope.id.clone(),
                                flags: added,
                            }),
                        )?;
                    }

                    if !removed.is_empty() {
                        send(
                            tx,
                            Ok(WatchEvent::FlagsRemoved {
                                mailbox: mailbox.to_string(),
                                id: envelope.id.clone(),
                                flags: removed,
                            }),
                        )?;
                    }
                }
            }
        }
    }

    for destroyed_id in &changes.destroyed {
        if shadow.remove(destroyed_id).is_some() {
            send(
                tx,
                Ok(WatchEvent::EnvelopeRemoved {
                    mailbox: mailbox.to_string(),
                    id: destroyed_id.clone(),
                }),
            )?;
        }
    }

    Ok(changes.new_state)
}

fn email_is_in_mailbox(email: &Email, mailbox: &str) -> bool {
    email
        .mailbox_ids
        .as_ref()
        .map(|ids| ids.contains_key(mailbox))
        .unwrap_or(false)
}

fn mailbox_filter(mailbox: &str) -> Option<EmailFilter> {
    if mailbox.is_empty() {
        return None;
    }
    Some(EmailFilter {
        in_mailbox: Some(mailbox.to_string()),
        ..EmailFilter::default()
    })
}

fn envelope_properties() -> Vec<EmailProperty> {
    vec![
        EmailProperty::Id,
        EmailProperty::Keywords,
        EmailProperty::Subject,
        EmailProperty::From,
        EmailProperty::To,
        EmailProperty::SentAt,
        EmailProperty::Size,
        EmailProperty::HasAttachment,
        EmailProperty::MessageId,
        EmailProperty::MailboxIds,
    ]
}

fn send(tx: &SyncSender<WatchResult>, msg: WatchResult) -> Result<(), EmailClientStdError> {
    tx.send(msg)
        .map_err(|_| EmailClientStdError::OperationFailed("watch channel closed"))
}
