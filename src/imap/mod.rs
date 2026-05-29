pub mod convert;
pub mod envelope_list;
#[cfg(feature = "search")]
pub mod envelope_search;
pub mod flag_store;
pub mod mailbox_create;
pub mod mailbox_delete;
pub mod mailbox_list;
pub mod message_add;
pub mod message_copy;
pub mod message_delete;
pub mod message_get;
pub mod message_move;
pub mod watch_mailbox;

// NOTE: envelope_diff (QRESYNC) stays on disk but out of the build
// until it is ported to the new EmailCoroutine shape.
