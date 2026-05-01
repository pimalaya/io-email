//! Maildir message add, wrapping
//! [`io_maildir::coroutines::message_store::MaildirMessageStore`].

use alloc::{collections::BTreeMap, string::String, vec::Vec};

use io_maildir::{
    coroutines::message_store::{
        MaildirMessageStore, MaildirMessageStoreArg, MaildirMessageStoreError,
        MaildirMessageStoreResult,
    },
    flag::Flags,
    maildir::{Maildir, MaildirSubdir},
};
use log::trace;

/// I/O-free coroutine writing a raw RFC 5322 message into a Maildir
/// under the requested `subdir`. Follows the standard maildir
/// delivery protocol (write to `tmp`, then rename into place).
pub struct MessageAdd {
    inner: MaildirMessageStore,
}

impl MessageAdd {
    /// Builds the coroutine. Pass `subdir = None` to default to
    /// [`MaildirSubdir::Cur`] (visible as a "read" message).
    pub fn new(
        maildir: Maildir,
        subdir: Option<MaildirSubdir>,
        flags: Flags,
        contents: Vec<u8>,
    ) -> Self {
        trace!("prepare Maildir message add");
        let subdir = subdir.unwrap_or(MaildirSubdir::Cur);
        Self {
            inner: MaildirMessageStore::new(maildir, subdir, flags, contents),
        }
    }

    /// Advances the coroutine.
    pub fn resume(&mut self, arg: Option<MessageAddArg>) -> MessageAddResult {
        let inner_arg = arg.map(|arg| match arg {
            MessageAddArg::FileCreate => MaildirMessageStoreArg::FileCreate,
            MessageAddArg::Rename => MaildirMessageStoreArg::Rename,
        });

        match self.inner.resume(inner_arg) {
            MaildirMessageStoreResult::Ok { id, path } => MessageAddResult::Ok {
                id,
                path: path.to_string_lossy().into_owned(),
            },
            MaildirMessageStoreResult::WantsFileCreate(files) => {
                MessageAddResult::WantsFileCreate(files)
            }
            MaildirMessageStoreResult::WantsRename(pairs) => MessageAddResult::WantsRename(pairs),
            MaildirMessageStoreResult::Err(err) => MessageAddResult::Err(err),
        }
    }
}

/// Result returned by [`MessageAdd::resume`].
#[derive(Debug)]
pub enum MessageAddResult {
    Ok {
        /// Maildir filename id of the newly-stored message.
        id: String,
        /// Final on-disk path.
        path: String,
    },
    WantsFileCreate(BTreeMap<String, Vec<u8>>),
    WantsRename(Vec<(String, String)>),
    Err(MaildirMessageStoreError),
}

/// Argument fed back to [`MessageAdd::resume`].
#[derive(Debug)]
pub enum MessageAddArg {
    /// Response to [`MessageAddResult::WantsFileCreate`].
    FileCreate,
    /// Response to [`MessageAddResult::WantsRename`].
    Rename,
}
