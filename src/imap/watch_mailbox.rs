//! IMAP watch-mailbox coroutine wrapping
//! [`io_imap::watch::ImapMailboxWatch`] (IDLE + QRESYNC).
//!
//! Translates inner deltas into shared [`WatchEvent`]s. The caller
//! owns the shutdown [`Arc<AtomicBool>`](alloc::sync::Arc); flipping
//! it winds the IDLE down at the next loop tick.
//!
//! # Example
//!
//! ```rust,ignore
//! use io_email::imap::watch_mailbox::ImapWatchMailbox;
//!
//! let cor = ImapWatchMailbox::new("INBOX", &capabilities, shutdown)?;
//! // drive via the same client.run as the other IMAP coroutines.
//! ```

use alloc::{
    collections::BTreeSet,
    string::{String, ToString},
    sync::Arc,
    vec::Vec,
};
use core::sync::atomic::AtomicBool;

use io_imap::{
    codec::fragmentizer::Fragmentizer,
    coroutine::{ImapCoroutine, ImapCoroutineState},
    types::{flag::Flag as ImapFlag, response::Capability},
    watch::{
        ImapMailboxWatch as InnerWatch, ImapMailboxWatchError as InnerErr, ImapMailboxWatchEvent,
        ImapMailboxWatchYield,
    },
};
use log::trace;
use thiserror::Error;

use crate::{
    event::WatchEvent,
    flag::Flag,
    imap::{
        convert::{InvalidMailboxName, parse_mailbox},
        envelope_list::envelope_from,
    },
};

/// Errors produced by [`ImapWatchMailbox`].
#[derive(Debug, Error)]
pub enum ImapWatchMailboxError {
    #[error(transparent)]
    Watch(#[from] InnerErr),
    #[error("invalid IMAP mailbox `{0}`")]
    InvalidMailbox(String),
}

impl From<InvalidMailboxName> for ImapWatchMailboxError {
    fn from(err: InvalidMailboxName) -> Self {
        Self::InvalidMailbox(err.0)
    }
}

/// Yield mixing socket I/O requests and pre-diffed domain events.
#[derive(Debug)]
pub enum ImapWatchMailboxYield {
    /// Read more bytes and feed them via `bytes` on the next resume.
    WantsRead,
    /// Write these bytes; the next resume usually takes `bytes: None`.
    WantsWrite(Vec<u8>),
    /// One pre-diffed delta from the inner watcher.
    Event(WatchEvent),
}

/// I/O-free coroutine watching a single IMAP mailbox for
/// envelope-level deltas.
pub struct ImapWatchMailbox {
    inner: InnerWatch,
    mailbox: String,
}

impl ImapWatchMailbox {
    /// `capabilities` must include QRESYNC (RFC 7162). The watcher
    /// polls `shutdown` on every loop iteration.
    pub fn new(
        mailbox: &str,
        capabilities: &[Capability<'static>],
        shutdown: Arc<AtomicBool>,
    ) -> Result<Self, ImapWatchMailboxError> {
        trace!("prepare IMAP mailbox watch");
        let mbox = parse_mailbox(mailbox)?;
        Ok(Self {
            inner: InnerWatch::new(capabilities, mbox, shutdown)?,
            mailbox: mailbox.into(),
        })
    }
}

/// Translates an io-imap delta into a shared [`WatchEvent`].
fn translate_event(mailbox: &str, evt: ImapMailboxWatchEvent) -> WatchEvent {
    match evt {
        ImapMailboxWatchEvent::EnvelopeAdded { uid, items } => WatchEvent::EnvelopeAdded {
            mailbox: mailbox.into(),
            envelope: envelope_from(uid.get(), items),
        },
        ImapMailboxWatchEvent::EnvelopeRemoved { uid } => WatchEvent::EnvelopeRemoved {
            mailbox: mailbox.into(),
            id: uid.get().to_string(),
        },
        ImapMailboxWatchEvent::FlagsAdded { uid, flags } => WatchEvent::FlagsAdded {
            mailbox: mailbox.into(),
            id: uid.get().to_string(),
            flags: flags
                .into_iter()
                .map(flag_from_imap)
                .collect::<BTreeSet<_>>(),
        },
        ImapMailboxWatchEvent::FlagsRemoved { uid, flags } => WatchEvent::FlagsRemoved {
            mailbox: mailbox.into(),
            id: uid.get().to_string(),
            flags: flags
                .into_iter()
                .map(flag_from_imap)
                .collect::<BTreeSet<_>>(),
        },
    }
}

/// Wire IMAP flag to shared [`Flag`] via `to_string` + [`Flag::from_raw`].
fn flag_from_imap(flag: ImapFlag<'_>) -> Flag {
    Flag::from_raw(flag.to_string())
}

impl ImapCoroutine for ImapWatchMailbox {
    type Yield = ImapWatchMailboxYield;
    type Return = Result<(), ImapWatchMailboxError>;

    fn resume(
        &mut self,
        fragmentizer: &mut Fragmentizer,
        bytes: Option<&[u8]>,
    ) -> ImapCoroutineState<Self::Yield, Self::Return> {
        match self.inner.resume(fragmentizer, bytes) {
            ImapCoroutineState::Yielded(ImapMailboxWatchYield::WantsRead) => {
                ImapCoroutineState::Yielded(ImapWatchMailboxYield::WantsRead)
            }
            ImapCoroutineState::Yielded(ImapMailboxWatchYield::WantsWrite(out)) => {
                ImapCoroutineState::Yielded(ImapWatchMailboxYield::WantsWrite(out))
            }
            ImapCoroutineState::Yielded(ImapMailboxWatchYield::Event(evt)) => {
                ImapCoroutineState::Yielded(ImapWatchMailboxYield::Event(translate_event(
                    &self.mailbox,
                    evt,
                )))
            }
            ImapCoroutineState::Complete(Ok(())) => ImapCoroutineState::Complete(Ok(())),
            ImapCoroutineState::Complete(Err(err)) => ImapCoroutineState::Complete(Err(err.into())),
        }
    }
}
