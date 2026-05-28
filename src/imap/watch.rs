//! IMAP IDLE-backed watch driver (RFC 2177).
//!
//! Consumes the [`ImapClientStd`] passed in, SELECTs the watched
//! mailbox, seeds an in-memory envelope shadow with `FETCH 1:* (UID
//! FLAGS ENVELOPE RFC822.SIZE)`, then loops on
//! [`io_imap::rfc2177::idle::ImapIdle`]. Any untagged
//! `EXISTS`/`EXPUNGE`/`FETCH` response winds down IDLE via a transient
//! `DONE`; after IDLE returns clean, the driver re-fetches the
//! mailbox state, diffs it against the shadow, and emits the resulting
//! [`WatchEvent`]s.
//!
//! Cooperative shutdown: the underlying TCP stream carries a 2-second
//! read timeout for the IDLE phase; on every timeout the driver polls
//! the shutdown atomic and, when set, flips its internal
//! `ImapIdleDone` so the running coroutine sends `DONE` before exiting
//! cleanly. The timeout is removed before the post-change FETCH so
//! that long server responses are not chopped up.

use alloc::{
    collections::{BTreeMap, BTreeSet},
    string::{String, ToString},
    vec::Vec,
};
use core::{sync::atomic::Ordering, time::Duration};
use std::{
    io::{ErrorKind, Read, Write},
    sync::Arc,
    sync::atomic::AtomicBool,
    thread,
};

use io_imap::{
    client::ImapClientStd,
    context::ImapContext,
    rfc2177::idle::{ImapIdle, ImapIdleDone, ImapIdleResult},
    types::sequence::SequenceSet,
};
use log::trace;
use pimalaya_stream::std::stream::StreamStd;

use crate::{
    client::EmailClientStdError,
    envelope::Envelope,
    event::WatchEvent,
    flag::Flag,
    imap::{convert::parse_mailbox, envelope_list::*},
    watch::{WatchResult, WatchShutdown, WatchStream, channel},
};

const READ_BUFFER_SIZE: usize = 16 * 1024;
const IDLE_READ_TIMEOUT: Duration = Duration::from_secs(2);

/// Builds and starts an IMAP IDLE-backed watch over `mailbox`.
///
/// Consumes `client` because the underlying TCP socket is dedicated
/// to the IDLE channel for the lifetime of the stream. Spawns a
/// background thread that owns the client and emits [`WatchEvent`]s
/// through the returned [`WatchStream`].
pub(crate) fn watch_envelopes(
    mut client: ImapClientStd<StreamStd>,
    mailbox: String,
) -> Result<WatchStream, EmailClientStdError> {
    let imap_mailbox = parse_mailbox(&mailbox)?;

    client.select(imap_mailbox.clone())?;

    let initial_shadow = fetch_shadow(&mut client)?;

    let (shutdown, shutdown_flag) = WatchShutdown::new();
    let (tx, rx) = channel();

    let watched_mailbox = mailbox.clone();
    let handle = thread::spawn(move || {
        watch_loop(client, watched_mailbox, initial_shadow, shutdown_flag, tx);
    });

    Ok(WatchStream::new(rx, handle, shutdown))
}

/// Per-message snapshot stored in the watcher's shadow. Equality on
/// `(id, flags)` is enough to spot every change the LCD watch
/// surface cares about; subject/from/etc. are immutable post-arrival.
#[derive(Clone, Debug)]
struct ShadowEntry {
    envelope: Envelope,
    flags: BTreeSet<Flag>,
}

fn fetch_shadow(
    client: &mut ImapClientStd<StreamStd>,
) -> Result<BTreeMap<String, ShadowEntry>, EmailClientStdError> {
    let sequence_set: SequenceSet = "1:*"
        .try_into()
        .expect("`1:*` is a valid sequence set spelling");
    let item_names = build_item_names(false);

    let data = client.fetch(sequence_set, item_names, false)?;

    let shadow = data
        .into_iter()
        .map(|(seq, items)| {
            let envelope = envelope_from(seq.get(), items.into_inner());
            let flags = envelope.flags.clone();
            (envelope.id.clone(), ShadowEntry { envelope, flags })
        })
        .collect();

    Ok(shadow)
}

fn watch_loop(
    mut client: ImapClientStd<StreamStd>,
    mailbox: String,
    mut shadow: BTreeMap<String, ShadowEntry>,
    shutdown: Arc<AtomicBool>,
    tx: std::sync::mpsc::SyncSender<WatchResult>,
) {
    loop {
        if shutdown.load(Ordering::SeqCst) {
            break;
        }

        let had_change = match run_idle(&mut client, &shutdown) {
            Ok(seen) => seen,
            Err(err) => {
                let _ = tx.send(Err(err));
                return;
            }
        };

        if shutdown.load(Ordering::SeqCst) {
            break;
        }

        if !had_change {
            continue;
        }

        let new_shadow = match fetch_shadow(&mut client) {
            Ok(s) => s,
            Err(err) => {
                let _ = tx.send(Err(err));
                return;
            }
        };

        if emit_diff(&mailbox, &shadow, &new_shadow, &tx).is_err() {
            return;
        }

        shadow = new_shadow;
    }

    trace!("imap watch worker exiting");
}

/// Runs one IDLE round. Returns `Ok(true)` when at least one untagged
/// change response was buffered (caller should re-fetch and diff),
/// `Ok(false)` when IDLE returned cleanly via the internal 29-second
/// refresh with nothing seen, or [`Err`] on protocol/socket failure.
fn run_idle(
    client: &mut ImapClientStd<StreamStd>,
    shutdown: &Arc<AtomicBool>,
) -> Result<bool, EmailClientStdError> {
    client
        .stream_mut()
        .set_read_timeout(Some(IDLE_READ_TIMEOUT))?;

    let context = client.take_context()?;
    let result = drive_idle(client.stream_mut(), context, shutdown);

    client.stream_mut().set_read_timeout(None)?;

    let (context, transient) = result?;
    client.put_context(context);

    Ok(transient)
}

fn drive_idle(
    stream: &mut StreamStd,
    context: ImapContext,
    shutdown: &Arc<AtomicBool>,
) -> Result<(ImapContext, bool), EmailClientStdError> {
    let idle_done = ImapIdleDone::new();
    let mut idle = ImapIdle::new(context, idle_done.clone());
    let mut buf = [0u8; READ_BUFFER_SIZE];
    let mut arg: Option<Vec<u8>> = None;
    let mut transient = false;

    let context = loop {
        let result = idle.resume(arg.as_deref());
        arg = None;

        match result {
            ImapIdleResult::Data { data, untagged } => {
                let has_change = !data.is_empty() || !untagged.is_empty();
                if has_change && !transient {
                    transient = true;
                    idle_done.done();
                }
            }
            ImapIdleResult::Ok { context } => break context,
            ImapIdleResult::WantsRead => {
                let bytes = match read_with_shutdown(stream, &mut buf, shutdown) {
                    ReadOutcome::Bytes(n) => n,
                    ReadOutcome::Shutdown => {
                        idle_done.done();
                        continue;
                    }
                    ReadOutcome::Eof => {
                        return Err(EmailClientStdError::OperationFailed(
                            "imap idle: unexpected EOF",
                        ));
                    }
                    ReadOutcome::Err(err) => return Err(err.into()),
                };
                arg = Some(buf[..bytes].to_vec());
            }
            ImapIdleResult::WantsWrite(bytes) => {
                stream.write_all(&bytes)?;
            }
            ImapIdleResult::Err { .. } => {
                return Err(EmailClientStdError::OperationFailed("imap idle failed"));
            }
        }
    };

    Ok((context, transient))
}

enum ReadOutcome {
    Bytes(usize),
    Shutdown,
    Eof,
    Err(std::io::Error),
}

fn read_with_shutdown(
    stream: &mut StreamStd,
    buf: &mut [u8],
    shutdown: &Arc<AtomicBool>,
) -> ReadOutcome {
    loop {
        match stream.read(buf) {
            Ok(0) => return ReadOutcome::Eof,
            Ok(n) => return ReadOutcome::Bytes(n),
            Err(err)
                if err.kind() == ErrorKind::WouldBlock || err.kind() == ErrorKind::TimedOut =>
            {
                if shutdown.load(Ordering::SeqCst) {
                    return ReadOutcome::Shutdown;
                }
            }
            Err(err) => return ReadOutcome::Err(err),
        }
    }
}

fn emit_diff(
    mailbox: &str,
    old: &BTreeMap<String, ShadowEntry>,
    new: &BTreeMap<String, ShadowEntry>,
    tx: &std::sync::mpsc::SyncSender<WatchResult>,
) -> Result<(), ()> {
    for (id, new_entry) in new {
        match old.get(id) {
            None => {
                send(
                    tx,
                    Ok(WatchEvent::EnvelopeAdded {
                        mailbox: mailbox.to_string(),
                        envelope: new_entry.envelope.clone(),
                    }),
                )?;
            }
            Some(old_entry) => {
                let added: BTreeSet<Flag> = new_entry
                    .flags
                    .difference(&old_entry.flags)
                    .cloned()
                    .collect();
                let removed: BTreeSet<Flag> = old_entry
                    .flags
                    .difference(&new_entry.flags)
                    .cloned()
                    .collect();

                if !added.is_empty() {
                    send(
                        tx,
                        Ok(WatchEvent::FlagsAdded {
                            mailbox: mailbox.to_string(),
                            id: id.clone(),
                            flags: added,
                        }),
                    )?;
                }

                if !removed.is_empty() {
                    send(
                        tx,
                        Ok(WatchEvent::FlagsRemoved {
                            mailbox: mailbox.to_string(),
                            id: id.clone(),
                            flags: removed,
                        }),
                    )?;
                }
            }
        }
    }

    for id in old.keys() {
        if !new.contains_key(id) {
            send(
                tx,
                Ok(WatchEvent::EnvelopeRemoved {
                    mailbox: mailbox.to_string(),
                    id: id.clone(),
                }),
            )?;
        }
    }

    Ok(())
}

fn send(tx: &std::sync::mpsc::SyncSender<WatchResult>, msg: WatchResult) -> Result<(), ()> {
    tx.send(msg).map_err(|_| ())
}
