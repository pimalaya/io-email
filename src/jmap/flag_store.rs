//! JMAP flag-store coroutine: `Email/set { update: patch }` (RFC 8621
//! §4.7).
//!
//! Builds a single batched request:
//! - `Add` → `SetKeyword` patch per (id, flag).
//! - `Remove` → `UnsetKeyword` patch per (id, flag).
//! - `Set` → `ReplaceKeywords` patch per id (the full bag is the
//!   requested flags; everything else is dropped).
//!
//! Mailbox name is part of the shared signature but unused: JMAP
//! keywords are global per email, not per mailbox.

use alloc::{collections::BTreeMap, string::String};

use io_jmap::{
    coroutine::{JmapCoroutine, JmapCoroutineState, JmapYield},
    rfc8620::session::JmapSession,
    rfc8621::email_set::{
        JmapEmailSet as InnerSet, JmapEmailSetArgs, JmapEmailSetError as InnerErr,
    },
};
use log::trace;
use secrecy::SecretString;
use thiserror::Error;

use crate::{
    coroutine::{EmailBackend, EmailCoroutine, EmailCoroutineArg, EmailCoroutineState, JmapStep},
    flag::{Flag, FlagOp},
    jmap::convert::keyword_from,
};

/// Errors produced by [`JmapFlagStore`].
#[derive(Debug, Error)]
pub enum JmapFlagStoreError {
    #[error(transparent)]
    Set(#[from] InnerErr),
    #[error("Email/set returned per-id failures: {0:?}")]
    NotUpdated(alloc::vec::Vec<String>),
    #[error("coroutine was resumed with the wrong EmailCoroutineArg variant")]
    InvalidArg,
}

/// I/O-free coroutine applying a flag store across every id in turn.
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

impl EmailCoroutine for JmapFlagStore {
    type Yield = JmapStep;
    type Return = Result<(), JmapFlagStoreError>;

    const BACKEND: EmailBackend = EmailBackend::Jmap;

    fn resume(
        &mut self,
        arg: EmailCoroutineArg<'_>,
    ) -> EmailCoroutineState<Self::Yield, Self::Return> {
        #[allow(irrefutable_let_patterns)]
        let EmailCoroutineArg::Jmap { bytes } = arg else {
            return EmailCoroutineState::Complete(Err(JmapFlagStoreError::InvalidArg));
        };

        match self.inner.resume(bytes) {
            JmapCoroutineState::Complete(Ok(ok)) => {
                if ok.not_updated.is_empty() {
                    EmailCoroutineState::Complete(Ok(()))
                } else {
                    EmailCoroutineState::Complete(Err(JmapFlagStoreError::NotUpdated(
                        ok.not_updated.into_keys().collect(),
                    )))
                }
            }
            JmapCoroutineState::Yielded(JmapYield::WantsRead) => {
                EmailCoroutineState::Yielded(JmapStep::WantsRead)
            }
            JmapCoroutineState::Yielded(JmapYield::WantsWrite(out)) => {
                EmailCoroutineState::Yielded(JmapStep::WantsWrite(out))
            }
            JmapCoroutineState::Complete(Err(err)) => {
                EmailCoroutineState::Complete(Err(err.into()))
            }
        }
    }
}
