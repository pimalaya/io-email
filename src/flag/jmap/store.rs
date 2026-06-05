//! JMAP flag-store coroutine wrapping Email/set with a per-id keyword
//! patch (RFC 8621 §4.7).
//!
//! Add/Remove emit per-keyword patches; Set replaces the full bag.
//! Mailbox is part of the shared signature but unused: JMAP keywords
//! are global per email.
//!
//! # Example
//!
//! ```rust,ignore
//! use io_email::{flag::FlagOp, flag::jmap::store::JmapFlagStore};
//!
//! client.run(JmapFlagStore::new(&session, &auth, "_", &["email-id"], &flags, FlagOp::Add)?)?;
//! ```

use alloc::{collections::BTreeMap, string::String};

use io_jmap::{
    coroutine::{JmapCoroutine, JmapCoroutineState, JmapYield},
    rfc8620::JmapSession,
    rfc8621::email::set::{
        JmapEmailSet as InnerSet, JmapEmailSetArgs, JmapEmailSetError as InnerErr,
    },
};
use log::trace;
use secrecy::SecretString;
use thiserror::Error;

use crate::{
    flag::types::{Flag, FlagOp},
    jmap::convert::keyword_from,
};

/// Errors produced by [`JmapFlagStore`].
#[derive(Debug, Error)]
pub enum JmapFlagStoreError {
    #[error(transparent)]
    Set(#[from] InnerErr),
    #[error("Email/set returned per-id failures: {0:?}")]
    NotUpdated(alloc::vec::Vec<String>),
}

/// I/O-free coroutine applying a flag store across every id.
pub struct JmapFlagStore {
    inner: InnerSet,
}

impl JmapFlagStore {
    pub fn new(
        session: &JmapSession,
        http_auth: &SecretString,
        _mailbox: &str,
        ids: &[&str],
        flags: &[Flag],
        op: FlagOp,
    ) -> Result<Self, JmapFlagStoreError> {
        trace!("prepare JMAP flag store ({op:?})");
        let mut args = JmapEmailSetArgs::default();
        for id in ids {
            match op {
                FlagOp::Add => {
                    for flag in flags {
                        args.set_keyword(*id, keyword_from(flag));
                    }
                }
                FlagOp::Remove => {
                    for flag in flags {
                        args.unset_keyword(*id, keyword_from(flag));
                    }
                }
                FlagOp::Set => {
                    let bag: BTreeMap<String, bool> =
                        flags.iter().map(|f| (keyword_from(f), true)).collect();
                    args.replace_keywords(*id, bag);
                }
            }
        }
        Ok(Self {
            inner: InnerSet::new(session, http_auth, args)?,
        })
    }
}

impl JmapCoroutine for JmapFlagStore {
    type Yield = JmapYield;
    type Return = Result<(), JmapFlagStoreError>;

    fn resume(&mut self, bytes: Option<&[u8]>) -> JmapCoroutineState<Self::Yield, Self::Return> {
        match self.inner.resume(bytes) {
            JmapCoroutineState::Yielded(y) => JmapCoroutineState::Yielded(y),
            JmapCoroutineState::Complete(Ok(ok)) => {
                if ok.not_updated.is_empty() {
                    JmapCoroutineState::Complete(Ok(()))
                } else {
                    JmapCoroutineState::Complete(Err(JmapFlagStoreError::NotUpdated(
                        ok.not_updated.into_keys().collect(),
                    )))
                }
            }
            JmapCoroutineState::Complete(Err(err)) => JmapCoroutineState::Complete(Err(err.into())),
        }
    }
}
