//! Long-lived stream of [`WatchEvent`]s produced by
//! [`crate::client::EmailClientStd::watch_envelopes`].
//!
//! A [`WatchStream`] is a thin handle over a background worker thread
//! that owns the protocol connection and an mpsc channel. Each backend
//! driver (IMAP IDLE, JMAP SSE, Maildir fsnotify) pushes pre-diffed
//! events into the same channel shape so consumers stay
//! protocol-agnostic.

use core::time::Duration;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
    mpsc::{Receiver, RecvTimeoutError, SyncSender, TryRecvError, sync_channel},
};
use std::thread::JoinHandle;

use log::trace;

use crate::{client::EmailClientStdError, event::WatchEvent};

/// Default bound for the internal mpsc channel. Backpressure parks the
/// protocol thread once consumers fall behind by this many events;
/// slow consumers won't grow memory unboundedly.
pub const DEFAULT_WATCH_CHANNEL_SIZE: usize = 256;

/// Builds a fresh bounded channel sized for one watch stream. Backends
/// use this so the bound stays consistent across protocols.
pub(crate) fn channel() -> (SyncSender<WatchResult>, Receiver<WatchResult>) {
    sync_channel(DEFAULT_WATCH_CHANNEL_SIZE)
}

/// Carrier for results sent over the watch channel. `Err` surfaces a
/// fatal protocol error that ended the watch; subsequent reads observe
/// channel disconnect.
pub(crate) type WatchResult = Result<WatchEvent, EmailClientStdError>;

/// Shutdown signal passed to the worker thread.
///
/// All drivers poll the same [`AtomicBool`] between protocol-loop
/// iterations; the IMAP driver additionally wraps its own internal
/// `ImapIdleDone` so it can wind down the running IDLE coroutine in
/// the same poll cycle.
#[derive(Clone, Debug)]
pub(crate) struct WatchShutdown {
    flag: Arc<AtomicBool>,
}

impl WatchShutdown {
    pub(crate) fn new() -> (Self, Arc<AtomicBool>) {
        let flag = Arc::new(AtomicBool::new(false));
        (Self { flag: flag.clone() }, flag)
    }

    pub(crate) fn signal(&self) {
        self.flag.store(true, Ordering::SeqCst);
    }
}

/// Long-lived stream of [`WatchEvent`]s.
///
/// Backed by a worker thread that owns the protocol connection. Drop
/// or [`WatchStream::close`] to wind it down; the worker observes the
/// shutdown signal, ends its protocol loop (IDLE DONE for IMAP, socket
/// close for JMAP SSE, watcher drop for Maildir) and exits.
pub struct WatchStream {
    rx: Receiver<WatchResult>,
    handle: Option<JoinHandle<()>>,
    shutdown: WatchShutdown,
}

impl WatchStream {
    pub(crate) fn new(
        rx: Receiver<WatchResult>,
        handle: JoinHandle<()>,
        shutdown: WatchShutdown,
    ) -> Self {
        Self {
            rx,
            handle: Some(handle),
            shutdown,
        }
    }

    /// Non-blocking probe for the next event. Returns
    /// [`TryRecvError::Empty`] when the worker has nothing buffered,
    /// or [`TryRecvError::Disconnected`] once the worker exits.
    pub fn try_recv(&self) -> Result<WatchResult, TryRecvError> {
        self.rx.try_recv()
    }

    /// Waits up to `timeout` for the next event. The bounded form is
    /// useful for cooperative shutdown: the caller can interleave
    /// short waits with [`AtomicBool`]-style cancel checks of its own.
    pub fn recv_timeout(&self, timeout: Duration) -> Result<WatchResult, RecvTimeoutError> {
        self.rx.recv_timeout(timeout)
    }

    /// Signals the worker to stop and joins it. Returns the protocol
    /// error if the worker exited because of one; otherwise `Ok(())`.
    /// Always exits within the worker's read-timeout window (typically
    /// a few seconds).
    pub fn close(mut self) -> Result<(), EmailClientStdError> {
        self.shutdown.signal();
        let Some(handle) = self.handle.take() else {
            return Ok(());
        };
        match handle.join() {
            Ok(()) => Ok(()),
            Err(_) => Err(EmailClientStdError::OperationFailed(
                "watch worker panicked",
            )),
        }
    }
}

impl Iterator for WatchStream {
    type Item = WatchResult;

    fn next(&mut self) -> Option<Self::Item> {
        self.rx.recv().ok()
    }
}

impl Drop for WatchStream {
    /// Best-effort wind-down. Signals the worker and joins it without
    /// panicking on errors; this lets a panicking consumer release the
    /// connection without hanging on a half-dead socket.
    fn drop(&mut self) {
        self.shutdown.signal();
        if let Some(handle) = self.handle.take() {
            if let Err(_err) = handle.join() {
                trace!("watch worker panicked during drop");
            }
        }
    }
}
