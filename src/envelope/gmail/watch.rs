//! Pure raw-to-shared transform for the Gmail watch.
//!
//! The watch *loop* is the infinite [`GmailHistoryPoll`] coroutine in io-gmail
//! (poll `users.history.list`, sleep, repeat); it emits raw
//! [`GmailHistoryDiff`]s in Gmail-native types. This module turns one
//! such diff into the shared [`WatchEvent`]s himalaya consumes. It does
//! no I/O: the std client drives the coroutine, owns the sleep, and
//! feeds each diff through [`history_diff_to_events`].
//!
//! [`GmailHistoryPoll`]: io_gmail::v1::history_poll::GmailHistoryPoll

use alloc::{collections::BTreeSet, string::String, vec::Vec};

use io_gmail::{v1::history_poll::GmailHistoryDiff, v1::rest::history::GmailHistoryLabel};

use crate::{
    envelope::event::WatchEvent,
    gmail::convert::{envelope_from, flag_of_label},
};

/// Folds one raw [`GmailHistoryDiff`] into the shared [`WatchEvent`]s
/// for `mailbox`. Returns an empty vec when the diff carried no change;
/// the caller emits a [`WatchEvent::KeepAlive`] in that case.
///
/// Label deltas on messages added in the same diff are skipped: the
/// `EnvelopeAdded` already carries the message's flags.
pub(crate) fn history_diff_to_events(diff: GmailHistoryDiff, mailbox: &str) -> Vec<WatchEvent> {
    let added_ids: BTreeSet<String> = diff.added.iter().map(|m| m.id.clone()).collect();
    let mut events = Vec::new();

    for message in diff.added {
        events.push(WatchEvent::EnvelopeAdded {
            mailbox: mailbox.into(),
            envelope: envelope_from(message),
        });
    }

    for id in diff.removed {
        if !added_ids.contains(&id.id) {
            events.push(WatchEvent::EnvelopeRemoved {
                mailbox: mailbox.into(),
                id: id.id,
            });
        }
    }

    for label in diff.labels_added {
        if !added_ids.contains(&label.message.id) {
            push_flag_events(&mut events, mailbox, &label, true);
        }
    }

    for label in diff.labels_removed {
        if !added_ids.contains(&label.message.id) {
            push_flag_events(&mut events, mailbox, &label, false);
        }
    }

    events
}

/// Emits flag deltas for one `labelsAdded`/`labelsRemoved` record,
/// honouring the inverted polarity of `UNREAD` (adding `UNREAD` removes
/// `\Seen`). Non-flag labels are ignored.
fn push_flag_events(
    events: &mut Vec<WatchEvent>,
    mailbox: &str,
    label: &GmailHistoryLabel,
    label_added: bool,
) {
    let mut added = BTreeSet::new();
    let mut removed = BTreeSet::new();

    for id in &label.label_ids {
        if let Some((flag, inverted)) = flag_of_label(id) {
            if label_added ^ inverted {
                added.insert(flag);
            } else {
                removed.insert(flag);
            }
        }
    }

    if !added.is_empty() {
        events.push(WatchEvent::FlagsAdded {
            mailbox: mailbox.into(),
            id: label.message.id.clone(),
            flags: added,
        });
    }
    if !removed.is_empty() {
        events.push(WatchEvent::FlagsRemoved {
            mailbox: mailbox.into(),
            id: label.message.id.clone(),
            flags: removed,
        });
    }
}
