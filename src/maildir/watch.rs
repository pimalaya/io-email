//! Maildir fsnotify-backed watch driver.
//!
//! Consumes the [`MaildirClient`] passed in, seeds an envelope/flag
//! shadow from the watched mailbox, then spawns a thread that drives
//! a [`notify::RecommendedWatcher`] over `cur/` and `new/`. Any
//! filesystem event triggers a (debounced) full re-scan; the resulting
//! envelope set is diffed against the shadow and the deltas are
//! streamed as [`WatchEvent`]s.
//!
//! Re-scanning on every event is simple and correct: Maildir flag
//! changes happen via rename, message arrivals happen via move-into-
//! `cur/`, and expunges happen via remove. A debounce window
//! (default 100ms) coalesces the write-then-rename storms typical of
//! Maildir MDAs so the diff runs once per logical change.

use alloc::{
    collections::{BTreeMap, BTreeSet},
    string::{String, ToString},
};
use core::{sync::atomic::Ordering, time::Duration};
use std::{
    path::PathBuf,
    sync::{Arc, atomic::AtomicBool, mpsc::SyncSender},
    thread,
};

use io_maildir::client::MaildirClient;
use log::trace;
use notify::{Config, EventKind, RecommendedWatcher, RecursiveMode, Watcher};

use crate::{
    client::EmailClientStdError,
    envelope::Envelope,
    event::WatchEvent,
    flag::Flag,
    maildir::{convert::open_maildir, envelope_list::envelope_from_message},
    watch::{WatchResult, WatchShutdown, WatchStream, channel},
};

const DEBOUNCE: Duration = Duration::from_millis(100);
const POLL_INTERVAL: Duration = Duration::from_millis(500);

pub(crate) fn watch_envelopes(
    mut client: MaildirClient,
    mailbox: String,
) -> Result<WatchStream, EmailClientStdError> {
    let maildir = open_maildir(&mut client, &mailbox)?;
    let folder_path: PathBuf = maildir.path().as_str().into();

    let initial_shadow = scan_envelopes(&mut client, &mailbox)?;

    let (shutdown, shutdown_flag) = WatchShutdown::new();
    let (tx, rx) = channel();

    let handle = thread::spawn(move || {
        watch_loop(
            client,
            mailbox,
            folder_path,
            initial_shadow,
            shutdown_flag,
            tx,
        );
    });

    Ok(WatchStream::new(rx, handle, shutdown))
}

/// Per-message snapshot. Flag set is the only mutable part across
/// scans; the envelope payload travels with the `EnvelopeAdded` event
/// and is not retained.
#[derive(Clone, Debug)]
struct ShadowEntry {
    flags: BTreeSet<Flag>,
}

fn scan_envelopes(
    client: &mut MaildirClient,
    mailbox: &str,
) -> Result<BTreeMap<String, ShadowEntry>, EmailClientStdError> {
    let maildir = open_maildir(client, mailbox)?;
    let dovecot_table = if client.dovecot_keywords {
        client.load_dovecot_keywords(&maildir)?
    } else {
        BTreeMap::new()
    };
    let header = client.keywords_header;
    let entries: alloc::vec::Vec<_> = client.list_entries(maildir)?.into_iter().collect();
    let messages = client.read_entries_par(&entries)?;

    let shadow = messages
        .iter()
        .map(|m| {
            let envelope = envelope_from_message(m, &dovecot_table, header);
            let flags = envelope.flags.clone();
            (envelope.id.clone(), ShadowEntry { flags })
        })
        .collect();

    Ok(shadow)
}

fn scan_envelopes_full(
    client: &mut MaildirClient,
    mailbox: &str,
) -> Result<BTreeMap<String, (Envelope, BTreeSet<Flag>)>, EmailClientStdError> {
    let maildir = open_maildir(client, mailbox)?;
    let dovecot_table = if client.dovecot_keywords {
        client.load_dovecot_keywords(&maildir)?
    } else {
        BTreeMap::new()
    };
    let header = client.keywords_header;
    let entries: alloc::vec::Vec<_> = client.list_entries(maildir)?.into_iter().collect();
    let messages = client.read_entries_par(&entries)?;

    let map = messages
        .iter()
        .map(|m| {
            let envelope = envelope_from_message(m, &dovecot_table, header);
            let flags = envelope.flags.clone();
            (envelope.id.clone(), (envelope, flags))
        })
        .collect();

    Ok(map)
}

fn watch_loop(
    mut client: MaildirClient,
    mailbox: String,
    folder_path: PathBuf,
    mut shadow: BTreeMap<String, ShadowEntry>,
    shutdown: Arc<AtomicBool>,
    tx: SyncSender<WatchResult>,
) {
    let (notify_tx, notify_rx) = std::sync::mpsc::channel::<notify::Result<notify::Event>>();
    let mut watcher = match RecommendedWatcher::new(
        move |res| {
            let _ = notify_tx.send(res);
        },
        Config::default(),
    ) {
        Ok(w) => w,
        Err(err) => {
            let _ = tx.send(Err(EmailClientStdError::OperationFailed(notify_err(&err))));
            return;
        }
    };

    for sub in ["cur", "new"] {
        let path = folder_path.join(sub);
        if let Err(err) = watcher.watch(&path, RecursiveMode::NonRecursive) {
            let _ = tx.send(Err(EmailClientStdError::OperationFailed(notify_err(&err))));
            return;
        }
    }

    loop {
        if shutdown.load(Ordering::SeqCst) {
            break;
        }

        let saw_event = match notify_rx.recv_timeout(POLL_INTERVAL) {
            Ok(Ok(event)) => relevant_event(&event),
            Ok(Err(err)) => {
                trace!("maildir notify error: {err}");
                false
            }
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => continue,
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
        };

        if !saw_event {
            continue;
        }

        thread::sleep(DEBOUNCE);
        while let Ok(_drained) = notify_rx.try_recv() {}

        if shutdown.load(Ordering::SeqCst) {
            break;
        }

        let new_state = match scan_envelopes_full(&mut client, &mailbox) {
            Ok(s) => s,
            Err(err) => {
                let _ = tx.send(Err(err));
                return;
            }
        };

        if emit_diff(&mailbox, &shadow, &new_state, &tx).is_err() {
            return;
        }

        shadow = new_state
            .into_iter()
            .map(|(id, (_, flags))| (id, ShadowEntry { flags }))
            .collect();
    }

    trace!("maildir watch worker exiting");
}

fn relevant_event(event: &notify::Event) -> bool {
    matches!(
        event.kind,
        EventKind::Create(_) | EventKind::Modify(_) | EventKind::Remove(_)
    )
}

fn emit_diff(
    mailbox: &str,
    old: &BTreeMap<String, ShadowEntry>,
    new: &BTreeMap<String, (Envelope, BTreeSet<Flag>)>,
    tx: &SyncSender<WatchResult>,
) -> Result<(), ()> {
    for (id, (envelope, flags)) in new {
        match old.get(id) {
            None => {
                send(
                    tx,
                    Ok(WatchEvent::EnvelopeAdded {
                        mailbox: mailbox.to_string(),
                        envelope: envelope.clone(),
                    }),
                )?;
            }
            Some(old_entry) => {
                let added: BTreeSet<Flag> = flags.difference(&old_entry.flags).cloned().collect();
                let removed: BTreeSet<Flag> = old_entry.flags.difference(flags).cloned().collect();

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

fn send(tx: &SyncSender<WatchResult>, msg: WatchResult) -> Result<(), ()> {
    tx.send(msg).map_err(|_| ())
}

fn notify_err(_err: &notify::Error) -> &'static str {
    "maildir notify failed"
}
