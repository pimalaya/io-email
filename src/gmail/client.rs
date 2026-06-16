//! Std-blocking Gmail client.
//!
//! Holds an inner [`GmailClientStd`] (from io-gmail) wrapping the boxed
//! stream, the OAuth2 bearer credential and the target user id
//! (usually `me`). Gmail is label-based and stateless over HTTP: there
//! is no session, no capability list and no account-global change
//! token, so the diff/watch shared-API methods are not implemented.
//!
//! [`GmailClientStd::run`] pumps io-email Gmail coroutines against the
//! inner client's stream; [`GmailClientStd::inner`] stays reachable for
//! protocol-specific paths (profile, raw label/message calls).
//!
//! [`GmailClientStd`]: io_gmail::v1::client::GmailClientStd

use alloc::{
    string::{String, ToString},
    sync::Arc,
    vec::Vec,
};
use core::{
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};
use std::{
    io::{self, Read, Write},
    sync::mpsc::Sender,
    thread,
};

use io_gmail::v1::client::GmailClientStdConnectOptions;
#[cfg(any(
    feature = "rustls-ring",
    feature = "rustls-aws",
    feature = "native-tls"
))]
use io_gmail::{
    coroutine::*,
    v1::client::{
        GmailClientStd as InnerGmailClientStd, GmailClientStdError as InnerGmailClientStdError,
    },
    v1::history_poll::{GmailHistoryPoll, GmailHistoryPollError, GmailHistoryPollYield},
};
#[cfg(any(
    feature = "rustls-ring",
    feature = "rustls-aws",
    feature = "native-tls"
))]
use pimalaya_stream::tls::Tls;
use thiserror::Error;

use crate::{
    envelope::{
        event::WatchEvent,
        gmail::{
            list::{GmailEnvelopeList, GmailEnvelopeListError},
            watch::history_diff_to_events,
        },
        types::Envelope,
    },
    flag::{
        gmail::store::{GmailFlagStore, GmailFlagStoreError},
        types::{Flag, FlagOp},
    },
    mailbox::{
        gmail::{
            create::{GmailMailboxCreate, GmailMailboxCreateError},
            delete::{GmailMailboxDelete, GmailMailboxDeleteError},
            list::{GmailMailboxList, GmailMailboxListError},
        },
        types::Mailbox,
    },
    message::gmail::{
        copy::{GmailMessageCopy, GmailMessageCopyError},
        delete::{GmailMessageDelete, GmailMessageDeleteError},
        get::{GmailMessageGet, GmailMessageGetError},
        r#move::{GmailMessageMove, GmailMessageMoveError},
        send::{GmailMessageSend, GmailMessageSendError},
    },
};

/// Errors surfaced by [`GmailClientStd`] while running a coroutine.
///
/// One variant per shared-API Gmail coroutine.
#[derive(Debug, Error)]
pub enum GmailClientError {
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    MailboxList(#[from] GmailMailboxListError),
    #[error(transparent)]
    EnvelopeList(#[from] GmailEnvelopeListError),
    #[error(transparent)]
    FlagStore(#[from] GmailFlagStoreError),
    #[error(transparent)]
    MailboxCreate(#[from] GmailMailboxCreateError),
    #[error(transparent)]
    MailboxDelete(#[from] GmailMailboxDeleteError),
    #[error(transparent)]
    MessageGet(#[from] GmailMessageGetError),
    #[error(transparent)]
    MessageDelete(#[from] GmailMessageDeleteError),
    #[error(transparent)]
    MessageCopy(#[from] GmailMessageCopyError),
    #[error(transparent)]
    MessageMove(#[from] GmailMessageMoveError),
    #[error(transparent)]
    MessageSend(#[from] GmailMessageSendError),
    #[error(transparent)]
    Watch(#[from] GmailHistoryPollError),
    #[error(transparent)]
    Inner(#[from] InnerGmailClientStdError),
}

const READ_BUFFER_SIZE: usize = 16 * 1024;

/// Light Gmail client built on top of the io-gmail type-erased inner.
pub struct GmailClientStd {
    pub inner: InnerGmailClientStd,
}

impl GmailClientStd {
    /// Wraps an already-connected stream with the bare OAuth 2.0 bearer
    /// access token (the client adds the `Bearer ` prefix itself) and
    /// the Gmail user id (usually `me`).
    pub fn new<S: Read + Write + Send + 'static>(
        stream: S,
        token: impl ToString,
        user_id: impl Into<String>,
    ) -> Self {
        Self {
            inner: InnerGmailClientStd::new(
                stream,
                token,
                GmailClientStdConnectOptions {
                    user_id: user_id.into(),
                    ..Default::default()
                },
            ),
        }
    }

    /// Pumps any standard-shape Gmail coroutine
    /// (`Yield = GmailYield`, `Return = Result<T, E>`) against the
    /// inner client's stream until it terminates.
    pub fn run<C, T, E>(&mut self, mut coroutine: C) -> Result<T, GmailClientError>
    where
        C: GmailCoroutine<Yield = GmailYield, Return = Result<T, E>>,
        GmailClientError: From<E>,
    {
        let mut buf = [0u8; READ_BUFFER_SIZE];
        let mut arg: Option<&[u8]> = None;

        loop {
            match coroutine.resume(arg.take()) {
                GmailCoroutineState::Complete(Ok(out)) => return Ok(out),
                GmailCoroutineState::Complete(Err(err)) => return Err(err.into()),
                GmailCoroutineState::Yielded(GmailYield::WantsRead) => {
                    let n = self.inner.stream.read(&mut buf)?;
                    arg = Some(&buf[..n]);
                }
                GmailCoroutineState::Yielded(GmailYield::WantsWrite(bytes)) => {
                    self.inner.stream.write_all(&bytes)?;
                }
            }
        }
    }

    /// Lists every Gmail label as a [`Mailbox`]. With `with_counts`,
    /// issues one extra `users.labels.get` per label to fill
    /// total/unread counts.
    pub fn list_mailboxes(&mut self, with_counts: bool) -> Result<Vec<Mailbox>, GmailClientError> {
        let coroutine = GmailMailboxList::new(&self.inner.auth, &self.inner.user_id, with_counts)?;
        self.run(coroutine)
    }

    /// Lists envelopes from the `mailbox` label. `page` is 1-indexed;
    /// pages before it are walked via Gmail's opaque page token.
    pub fn list_envelopes(
        &mut self,
        mailbox: &str,
        page: Option<u32>,
        page_size: Option<u32>,
    ) -> Result<Vec<Envelope>, GmailClientError> {
        let coroutine = GmailEnvelopeList::new(
            &self.inner.auth,
            &self.inner.user_id,
            mailbox,
            page,
            page_size,
        )?;
        self.run(coroutine)
    }

    /// Adds, sets, or removes `flags` on a Gmail message id set via
    /// `users.messages.modify`. `mailbox` is unused: Gmail labels are
    /// global per message.
    pub fn store_flags(
        &mut self,
        mailbox: &str,
        ids: &[&str],
        flags: &[Flag],
        op: FlagOp,
    ) -> Result<(), GmailClientError> {
        let coroutine = GmailFlagStore::new(
            &self.inner.auth,
            &self.inner.user_id,
            mailbox,
            ids,
            flags,
            op,
        )?;
        self.run(coroutine)
    }

    /// Fetches one message's raw RFC 5322 bytes via
    /// `users.messages.get` (format=RAW).
    pub fn get_message(&mut self, mailbox: &str, id: &str) -> Result<Vec<u8>, GmailClientError> {
        let coroutine = GmailMessageGet::new(&self.inner.auth, &self.inner.user_id, mailbox, id)?;
        self.run(coroutine)
    }

    /// Creates `name` as a new Gmail label.
    pub fn create_mailbox(&mut self, name: &str) -> Result<(), GmailClientError> {
        let coroutine = GmailMailboxCreate::new(&self.inner.auth, &self.inner.user_id, name)?;
        self.run(coroutine)
    }

    /// Deletes the Gmail label `id`; the label is removed from every
    /// message that carried it.
    pub fn delete_mailbox(&mut self, id: &str) -> Result<(), GmailClientError> {
        let coroutine = GmailMailboxDelete::new(&self.inner.auth, &self.inner.user_id, id)?;
        self.run(coroutine)
    }

    /// Permanently deletes the Gmail message `id`.
    pub fn delete_message(&mut self, mailbox: &str, id: &str) -> Result<(), GmailClientError> {
        let coroutine =
            GmailMessageDelete::new(&self.inner.auth, &self.inner.user_id, mailbox, id)?;
        self.run(coroutine)
    }

    /// Copies a message id set into `to` by adding `to`'s label to each
    /// message. The `from` argument is part of the shared signature but
    /// unused.
    pub fn copy_messages(
        &mut self,
        from: &str,
        to: &str,
        ids: &[&str],
    ) -> Result<(), GmailClientError> {
        let coroutine =
            GmailMessageCopy::new(&self.inner.auth, &self.inner.user_id, from, to, ids)?;
        self.run(coroutine)
    }

    /// Moves a message id set from `from` to `to` by adding `to`'s
    /// label and removing `from`'s label on each message.
    pub fn move_messages(
        &mut self,
        from: &str,
        to: &str,
        ids: &[&str],
    ) -> Result<(), GmailClientError> {
        let coroutine =
            GmailMessageMove::new(&self.inner.auth, &self.inner.user_id, from, to, ids)?;
        self.run(coroutine)
    }

    /// Sends a raw RFC 5322 message via `users.messages.send`.
    pub fn send_message(&mut self, raw: Vec<u8>) -> Result<(), GmailClientError> {
        let coroutine = GmailMessageSend::new(&self.inner.auth, &self.inner.user_id, raw)?;
        self.run(coroutine)
    }

    /// Watches the `mailbox` label by driving io-gmail's infinite
    /// [`GmailHistoryPoll`] poll coroutine, converting each raw history diff
    /// into shared [`WatchEvent`]s and forwarding them through `tx`.
    ///
    /// **Blocks** the current thread; returns `Ok(())` when `shutdown`
    /// flips or the receiver behind `tx` is dropped. This std client
    /// owns the wait: the coroutine yields
    /// [`GmailHistoryPollYield::WantsSleep`] and the driver performs the
    /// `thread::sleep` in shutdown-aware chunks.
    pub fn watch_mailbox(
        &mut self,
        mailbox: &str,
        shutdown: Arc<AtomicBool>,
        tx: Sender<WatchEvent>,
    ) -> Result<(), GmailClientError> {
        let mut coroutine = GmailHistoryPoll::new(&self.inner.auth, &self.inner.user_id, mailbox)?;
        let mut buf = [0u8; READ_BUFFER_SIZE];
        let mut bytes: Option<&[u8]> = None;

        loop {
            match coroutine.resume(bytes) {
                // GmailHistoryPoll never completes successfully (it loops
                // forever); Ok carries the uninhabited `Infallible`.
                GmailCoroutineState::Complete(Ok(never)) => match never {},
                GmailCoroutineState::Complete(Err(err)) => return Err(err.into()),
                GmailCoroutineState::Yielded(GmailHistoryPollYield::WantsRead) => {
                    let n = self.inner.stream.read(&mut buf)?;
                    bytes = Some(&buf[..n]);
                }
                GmailCoroutineState::Yielded(GmailHistoryPollYield::WantsWrite(out)) => {
                    self.inner.stream.write_all(&out)?;
                    bytes = None;
                }
                GmailCoroutineState::Yielded(GmailHistoryPollYield::WantsSleep(interval)) => {
                    let mut slept = Duration::ZERO;
                    while slept < interval {
                        if shutdown.load(Ordering::SeqCst) {
                            return Ok(());
                        }
                        let chunk = Duration::from_secs(1).min(interval - slept);
                        thread::sleep(chunk);
                        slept += chunk;
                    }
                    bytes = None;
                }
                GmailCoroutineState::Yielded(GmailHistoryPollYield::Diff(diff)) => {
                    let events = history_diff_to_events(diff, mailbox);
                    if events.is_empty() {
                        if tx.send(WatchEvent::KeepAlive).is_err() {
                            return Ok(());
                        }
                    } else {
                        for event in events {
                            if tx.send(event).is_err() {
                                return Ok(());
                            }
                        }
                    }
                    bytes = None;
                }
            }
        }
    }
}

#[cfg(any(
    feature = "rustls-ring",
    feature = "rustls-aws",
    feature = "native-tls"
))]
impl GmailClientStd {
    /// Opens a TLS connection to the Gmail REST API and builds the
    /// inner client around it. `user_id` is the mailbox owner (usually
    /// `me`).
    pub fn connect(
        tls: &Tls,
        token: impl ToString,
        user_id: impl Into<String>,
    ) -> Result<Self, GmailClientError> {
        let options = GmailClientStdConnectOptions {
            tls: tls.clone(),
            user_id: user_id.into(),
        };
        let inner = InnerGmailClientStd::connect(token, options)?;
        Ok(Self { inner })
    }
}
