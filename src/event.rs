//! Watch event shared across all protocols.
//!
//! Surfaced by [`crate::client::EmailClientStd::watch_envelopes`] as a
//! continuous stream. Each variant is the pre-diffed delta the backend
//! observed: envelopes added or removed from the watched mailbox, and
//! per-message flag additions or removals computed against the watcher's
//! in-memory shadow.

use alloc::{collections::BTreeSet, string::String};

use crate::{envelope::Envelope, flag::Flag};

/// Delta produced by a [`crate::client::EmailClientStd::watch_envelopes`]
/// stream.
///
/// `mailbox` is the watched mailbox name (always the same value for the
/// lifetime of a single stream). Flag deltas are split into `FlagsAdded`
/// and `FlagsRemoved` so hook configs can target one side of a toggle
/// (e.g. "fire when `\Seen` is added").
#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "kebab-case", tag = "type"))]
pub enum WatchEvent {
    /// A new message landed in the watched mailbox.
    EnvelopeAdded { mailbox: String, envelope: Envelope },
    /// An existing message was expunged from the watched mailbox.
    EnvelopeRemoved { mailbox: String, id: String },
    /// One or more flags were set on an existing message.
    FlagsAdded {
        mailbox: String,
        id: String,
        flags: BTreeSet<Flag>,
    },
    /// One or more flags were unset on an existing message.
    FlagsRemoved {
        mailbox: String,
        id: String,
        flags: BTreeSet<Flag>,
    },
    /// The transport is still alive but had no changes to report.
    /// Useful for callers that want to distinguish "stream healthy" from
    /// "stream stalled" without arming their own timer.
    KeepAlive,
}
