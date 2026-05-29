//! IMAP list-mailboxes coroutine.
//!
//! Composes `LIST "" "*"` with, when counts are requested, one
//! `STATUS <mailbox> (MESSAGES UNSEEN)` per row (RFC 3501 §6.3.10).
//! `LIST` does not carry counts; STATUS is the standard way to surface
//! them per mailbox.
//!
//! Emits the shared [`Mailbox`] shape directly; IMAP-specific data
//! (delimiter and SPECIAL-USE attributes) is dropped on purpose to
//! stay LCD.

use alloc::{
    string::{String, ToString},
    vec,
    vec::Vec,
};
use core::mem;

use io_imap::{
    coroutine::{ImapCoroutine, ImapCoroutineState, ImapYield},
    rfc3501::{
        list::{
            ImapMailboxList as InnerImapMailboxList,
            ImapMailboxListError as InnerImapMailboxListError,
        },
        status::{ImapMailboxStatus, ImapMailboxStatusError},
    },
    types::{
        core::QuotedChar,
        flag::FlagNameAttribute,
        mailbox::{ListMailbox, Mailbox as ImapMailbox},
        status::{StatusDataItem, StatusDataItemName},
    },
};
use log::trace;
use thiserror::Error;

use crate::{
    coroutine::{EmailBackend, EmailCoroutine, EmailCoroutineArg, EmailCoroutineState, ImapStep},
    mailbox::Mailbox,
};

/// Errors produced by [`ImapMailboxList`].
#[derive(Debug, Error)]
pub enum ImapMailboxListError {
    #[error(transparent)]
    List(#[from] InnerImapMailboxListError),
    #[error(transparent)]
    Status(#[from] ImapMailboxStatusError),
    #[error("invalid IMAP mailbox `{0}`")]
    InvalidMailbox(String),
    #[error("coroutine was resumed after completion")]
    ResumedAfterDone,
    #[error("coroutine was resumed with the wrong EmailCoroutineArg variant")]
    InvalidArg,
}

/// I/O-free coroutine listing every IMAP mailbox visible to the
/// session, optionally enriched with per-mailbox total / unread counts.
pub struct ImapMailboxList {
    state: State,
    with_counts: bool,
    /// Filled by the LIST stage, then walked one row at a time by the
    /// optional STATUS stage.
    mailboxes: Vec<Mailbox>,
}

impl ImapMailboxList {
    /// `LIST "" "*"` runs first; when `with_counts` is set, one
    /// `STATUS <mailbox> (MESSAGES UNSEEN)` per row follows and
    /// populates [`Mailbox::total`] / [`Mailbox::unread`].
    pub fn new(with_counts: bool) -> Self {
        trace!("prepare IMAP mailbox listing (with_counts={with_counts})");
        // SAFETY: empty reference and "*" pattern are valid IMAP tokens.
        let reference: ImapMailbox<'static> = "".try_into().unwrap();
        let pattern: ListMailbox<'static> = "*".try_into().unwrap();

        Self {
            state: State::Listing(InnerImapMailboxList::new(reference, pattern)),
            with_counts,
            mailboxes: Vec::new(),
        }
    }
}

impl EmailCoroutine for ImapMailboxList {
    const BACKEND: EmailBackend = EmailBackend::Imap;

    type Yield = ImapStep;
    type Return = Result<Vec<Mailbox>, ImapMailboxListError>;

    // NOTE: when IMAP is the only enabled backend, EmailCoroutineArg
    // has a single variant so the destructure below is irrefutable
    // and the `else` arm is dead. It comes alive (and the lint goes
    // quiet on its own) as soon as a second backend rejoins.
    fn resume(
        &mut self,
        arg: EmailCoroutineArg<'_>,
    ) -> EmailCoroutineState<Self::Yield, Self::Return> {
        #[allow(irrefutable_let_patterns)]
        let EmailCoroutineArg::Imap {
            fragmentizer,
            mut bytes,
        } = arg
        else {
            return EmailCoroutineState::Complete(Err(ImapMailboxListError::InvalidArg));
        };

        loop {
            match mem::replace(&mut self.state, State::Done) {
                State::Listing(mut list) => match list.resume(fragmentizer, bytes.take()) {
                    ImapCoroutineState::Yielded(ImapYield::WantsRead) => {
                        self.state = State::Listing(list);
                        return EmailCoroutineState::Yielded(ImapStep::WantsRead);
                    }
                    ImapCoroutineState::Yielded(ImapYield::WantsWrite(bytes)) => {
                        self.state = State::Listing(list);
                        return EmailCoroutineState::Yielded(ImapStep::WantsWrite(bytes));
                    }
                    ImapCoroutineState::Complete(Ok(rows)) => {
                        self.mailboxes = rows.into_iter().map(mailbox_from).collect();
                        if !self.with_counts || self.mailboxes.is_empty() {
                            return EmailCoroutineState::Complete(Ok(mem::take(
                                &mut self.mailboxes,
                            )));
                        }
                        match start_status(&self.mailboxes[0], 0) {
                            Ok(next) => self.state = next,
                            Err(err) => return EmailCoroutineState::Complete(Err(err)),
                        }
                    }
                    ImapCoroutineState::Complete(Err(err)) => {
                        return EmailCoroutineState::Complete(Err(err.into()));
                    }
                },
                State::StatusOne { mut status, cursor } => {
                    match status.resume(fragmentizer, bytes.take()) {
                        ImapCoroutineState::Yielded(ImapYield::WantsRead) => {
                            self.state = State::StatusOne { status, cursor };
                            return EmailCoroutineState::Yielded(ImapStep::WantsRead);
                        }
                        ImapCoroutineState::Yielded(ImapYield::WantsWrite(bytes)) => {
                            self.state = State::StatusOne { status, cursor };
                            return EmailCoroutineState::Yielded(ImapStep::WantsWrite(bytes));
                        }
                        ImapCoroutineState::Complete(Ok(items)) => {
                            apply_status(&mut self.mailboxes[cursor], items);
                            let next_cursor = cursor + 1;
                            if next_cursor >= self.mailboxes.len() {
                                return EmailCoroutineState::Complete(Ok(mem::take(
                                    &mut self.mailboxes,
                                )));
                            }
                            match start_status(&self.mailboxes[next_cursor], next_cursor) {
                                Ok(next) => self.state = next,
                                Err(err) => return EmailCoroutineState::Complete(Err(err)),
                            }
                        }
                        ImapCoroutineState::Complete(Err(err)) => {
                            return EmailCoroutineState::Complete(Err(err.into()));
                        }
                    }
                }
                State::Done => {
                    return EmailCoroutineState::Complete(Err(
                        ImapMailboxListError::ResumedAfterDone,
                    ));
                }
            }
        }
    }
}

enum State {
    Listing(InnerImapMailboxList),
    StatusOne {
        status: ImapMailboxStatus,
        /// Index into `mailboxes` of the row this STATUS targets.
        cursor: usize,
    },
    Done,
}

/// Converts one IMAP `LIST` row into the shared [`Mailbox`] shape.
///
/// Delimiter and attribute flags are dropped on purpose: they're
/// IMAP-specific and not part of the LCD surface.
fn mailbox_from(
    row: (
        ImapMailbox<'static>,
        Option<QuotedChar>,
        Vec<FlagNameAttribute<'static>>,
    ),
) -> Mailbox {
    let (mailbox, _delimiter, _attrs) = row;
    let name = match mailbox {
        ImapMailbox::Inbox => "Inbox".to_string(),
        ImapMailbox::Other(other) => String::from_utf8_lossy(other.inner().as_ref()).into_owned(),
    };

    Mailbox {
        id: name.clone(),
        name,
        total: None,
        unread: None,
    }
}

/// Spins up the STATUS coroutine for `mailbox` and returns the next
/// [`State`]. Reuses the LIST-derived id as the STATUS target.
fn start_status(mailbox: &Mailbox, cursor: usize) -> Result<State, ImapMailboxListError> {
    let mbox: ImapMailbox<'static> = mailbox
        .id
        .clone()
        .try_into()
        .map_err(|_| ImapMailboxListError::InvalidMailbox(mailbox.id.clone()))?;

    let item_names: Vec<StatusDataItemName> =
        vec![StatusDataItemName::Messages, StatusDataItemName::Unseen];

    Ok(State::StatusOne {
        status: ImapMailboxStatus::new(mbox, item_names),
        cursor,
    })
}

/// Folds a STATUS response into the matching mailbox row.
fn apply_status(mailbox: &mut Mailbox, items: Vec<StatusDataItem>) {
    for item in items {
        match item {
            StatusDataItem::Messages(n) => mailbox.total = Some(u64::from(n)),
            StatusDataItem::Unseen(n) => mailbox.unread = Some(u64::from(n)),
            _ => {}
        }
    }
}
