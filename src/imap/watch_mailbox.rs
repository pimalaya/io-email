//! IMAP watch-envelopes coroutine (generator shape).
//!
//! Wraps [`io_imap::watch::ImapMailboxWatch`]: the inner watcher runs
//! the IDLE + QRESYNC dance, maintains its own UID→flag shadow, and
//! emits pre-diffed deltas. This module translates each delta into
//! the shared [`WatchEvent`] surface and threads everything through
//! the [`io_imap::coroutine::ImapCoroutine`] trait so the I/O steps
//! and the domain events ride on the same Yield axis.
//!
//! Cooperative shutdown: the caller owns the
//! [`Arc<AtomicBool>`](alloc::sync::Arc) handed to [`Self::new`].
//! Flipping it asks the running IDLE to wind down at its next loop
//! tick; on the following resume the coroutine returns
//! [`CoroutineState::Complete`] with `Ok(())`.

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

/// Per-coroutine Yield: socket I/O step requests on one axis, domain
/// events on the other. The driver dispatches on the variant: I/O
/// variants pump the IMAP socket, [`Self::Event`] is delivered to the
/// caller (callback / channel / async stream — driver's choice).
#[derive(Debug)]
pub enum ImapWatchMailboxYield {
    /// Socket: read more bytes and feed them back via the `bytes`
    /// argument on the next resume.
    WantsRead,
    /// Socket: write these bytes; the next resume typically takes
    /// `bytes: None`.
    WantsWrite(Vec<u8>),
    /// Domain: one pre-diffed delta computed by the inner watcher.
    Event(WatchEvent),
}

/// I/O-free generator-shape coroutine watching a single IMAP mailbox
/// for envelope-level deltas.
pub struct ImapWatchMailbox {
    inner: InnerWatch,
    mailbox: String,
}

impl ImapWatchMailbox {
    /// Constructs the inner watcher. `capabilities` must include
    /// `QRESYNC` (RFC 7162); the inner constructor checks and errors
    /// otherwise. Run `CAPABILITY` (or
    /// [`io_imap::sasl`]-flavoured login with `ensure_capabilities`)
    /// before reaching here so the slice is non-empty.
    ///
    /// `shutdown` is shared with the caller; the watcher polls it on
    /// every loop iteration and winds the IDLE down cleanly when
    /// flipped.
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

/// Translates an io-imap delta into the shared [`WatchEvent`] shape.
/// `EnvelopeAdded` rebuilds a full [`crate::envelope::Envelope`] via
/// [`envelope_from`]; `FlagsAdded` / `FlagsRemoved` map wire-level
/// flags via [`flag_from_imap`]; `EnvelopeRemoved` carries the UID as
/// the shared id.
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

/// Converts a wire-level IMAP flag into a shared [`Flag`] using the
/// same `to_string` → [`Flag::from_raw`] path the envelope listing
/// uses.
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
