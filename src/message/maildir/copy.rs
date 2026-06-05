//! Maildir message-copy coroutine: chains one
//! [`io_maildir::entry::copy::MaildirEntryCopy`] per id between
//! Maildirs; target subdir mirrors the source subdir.
//!
//! # Example
//!
//! ```rust,ignore
//! use io_email::message::maildir::copy::MaildirMessageCopy;
//!
//! client.run(MaildirMessageCopy::new(&client.store, "INBOX", "Archive", &["msg-id"])?)?;
//! ```

use alloc::{collections::VecDeque, string::String};

use io_maildir::{
    coroutine::*,
    entry::copy::{MaildirEntryCopy as InnerCopy, MaildirEntryCopyError as InnerErr},
    maildir::types::Maildir,
    store::MaildirStore,
};
use log::trace;
use thiserror::Error;

use crate::maildir::convert::{InvalidMailboxName, mailbox_path};

/// Errors produced by [`MaildirMessageCopy`].
#[derive(Debug, Error)]
pub enum MaildirMessageCopyError {
    #[error(transparent)]
    Copy(#[from] InnerErr),
    #[error(transparent)]
    InvalidMailbox(#[from] InvalidMailboxName),
}

/// I/O-free coroutine copying every id from `from` to `to`.
pub struct MaildirMessageCopy {
    source: Maildir,
    target: Maildir,
    pending: VecDeque<String>,
    current: Option<InnerCopy>,
}

impl MaildirMessageCopy {
    pub fn new(
        store: &MaildirStore,
        from: &str,
        to: &str,
        ids: &[&str],
    ) -> Result<Self, MaildirMessageCopyError> {
        trace!("prepare Maildir message copy");
        let source = Maildir::from_path(store.resolve(&mailbox_path(from)?));
        let target = Maildir::from_path(store.resolve(&mailbox_path(to)?));
        Ok(Self {
            source,
            target,
            pending: ids.iter().map(|s| (*s).into()).collect(),
            current: None,
        })
    }
}

impl MaildirCoroutine for MaildirMessageCopy {
    type Yield = MaildirYield;
    type Return = Result<(), MaildirMessageCopyError>;

    fn resume(
        &mut self,
        arg: Option<MaildirReply>,
    ) -> MaildirCoroutineState<Self::Yield, Self::Return> {
        let mut arg = arg;
        loop {
            if self.current.is_none() {
                let Some(id) = self.pending.pop_front() else {
                    return MaildirCoroutineState::Complete(Ok(()));
                };
                self.current = Some(InnerCopy::new(
                    id,
                    self.source.clone(),
                    self.target.clone(),
                    None,
                ));
            }

            let inner = self.current.as_mut().unwrap();
            match inner.resume(arg.take()) {
                MaildirCoroutineState::Complete(Ok(())) => {
                    self.current = None;
                }
                MaildirCoroutineState::Yielded(y) => return MaildirCoroutineState::Yielded(y),
                MaildirCoroutineState::Complete(Err(err)) => {
                    return MaildirCoroutineState::Complete(Err(err.into()));
                }
            }
        }
    }
}
