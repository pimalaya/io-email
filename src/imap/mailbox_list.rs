//! IMAP mailbox listing (`LIST "" "*"`), wrapping
//! [`io_imap::rfc3501::list::ImapMailboxList`] and producing the shared
//! [`Mailbox`](crate::mailbox::Mailbox) type on completion.
//!
//! Per-mailbox counts are not populated here — IMAP requires a
//! separate STATUS round-trip per mailbox, which is driven at the
//! [`crate::client::EmailClient`] level when the caller opts in.

use alloc::vec::Vec;

use io_imap::{
    context::ImapContext,
    rfc3501::list::{
        ImapMailboxList as InnerImapMailboxList, ImapMailboxListError, ImapMailboxListResult,
    },
    types::{
        core::QuotedChar,
        flag::FlagNameAttribute,
        mailbox::{ListMailbox, Mailbox as ImapMailbox},
    },
};
use log::trace;

use crate::mailbox::Mailbox;

/// I/O-free coroutine listing every IMAP mailbox visible to the
/// session. Issues `LIST "" "*"`.
pub struct MailboxList {
    inner: InnerImapMailboxList,
}

impl MailboxList {
    /// Builds the coroutine from an authenticated [`ImapContext`].
    pub fn new(context: ImapContext) -> Self {
        trace!("prepare IMAP mailbox listing");
        // SAFETY: "" and "*" are always valid IMAP mailbox tokens.
        let reference: ImapMailbox<'static> = "".try_into().unwrap();
        let pattern: ListMailbox<'static> = "*".try_into().unwrap();
        Self {
            inner: InnerImapMailboxList::new(context, reference, pattern),
        }
    }

    /// Advances the coroutine.
    pub fn resume(&mut self, arg: Option<&[u8]>) -> MailboxListResult {
        match self.inner.resume(arg) {
            ImapMailboxListResult::WantsRead => MailboxListResult::WantsRead,
            ImapMailboxListResult::WantsWrite(bytes) => MailboxListResult::WantsWrite(bytes),
            ImapMailboxListResult::Ok { mailboxes, .. } => {
                let mailboxes = mailboxes.into_iter().map(Mailbox::from).collect();
                MailboxListResult::Ok(mailboxes)
            }
            ImapMailboxListResult::Err { err, .. } => MailboxListResult::Err(err),
        }
    }
}

/// Result returned by [`MailboxList::resume`].
#[derive(Debug)]
pub enum MailboxListResult {
    Ok(Vec<Mailbox>),
    WantsRead,
    WantsWrite(Vec<u8>),
    Err(ImapMailboxListError),
}

impl
    From<(
        ImapMailbox<'static>,
        Option<QuotedChar>,
        Vec<FlagNameAttribute<'static>>,
    )> for Mailbox
{
    fn from(
        (mailbox, _delimiter, _attrs): (
            ImapMailbox<'static>,
            Option<QuotedChar>,
            Vec<FlagNameAttribute<'static>>,
        ),
    ) -> Self {
        use alloc::string::{String, ToString};

        let name = match mailbox {
            ImapMailbox::Inbox => "Inbox".to_string(),
            ImapMailbox::Other(other) => {
                String::from_utf8_lossy(other.inner().as_ref()).into_owned()
            }
        };

        Self {
            id: name.clone(),
            name,
            total: None,
            unread: None,
        }
    }
}
