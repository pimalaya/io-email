//! JMAP flag add (`Email/set` with `keywords/<name>: true` patches),
//! wrapping [`io_jmap::rfc8621::email_set::JmapEmailSet`].

use alloc::{string::String, vec::Vec};

use io_jmap::{
    rfc8620::session::JmapSession,
    rfc8621::email_set::{JmapEmailSet, JmapEmailSetArgs, JmapEmailSetError, JmapEmailSetResult},
};
use log::trace;
use secrecy::SecretString;

/// I/O-free coroutine adding keywords (flags) to a set of emails.
pub struct FlagAdd {
    inner: JmapEmailSet,
}

impl FlagAdd {
    /// Builds the coroutine from a JMAP session, the bearer/basic HTTP
    /// credential, and the `(email_id, keyword)` pairs to set.
    pub fn new<I, J>(
        session: &JmapSession,
        http_auth: &SecretString,
        ids: I,
        keywords: J,
    ) -> Result<Self, JmapEmailSetError>
    where
        I: IntoIterator<Item = String>,
        J: IntoIterator<Item = String> + Clone,
    {
        trace!("prepare JMAP flag add");

        let mut args = JmapEmailSetArgs::default();
        for id in ids {
            for keyword in keywords.clone() {
                args.set_keyword(id.clone(), keyword);
            }
        }

        let inner = JmapEmailSet::new(session, http_auth, args)?;
        Ok(Self { inner })
    }

    /// Advances the coroutine.
    pub fn resume(&mut self, arg: Option<&[u8]>) -> FlagAddResult {
        match self.inner.resume(arg) {
            JmapEmailSetResult::WantsRead => FlagAddResult::WantsRead,
            JmapEmailSetResult::WantsWrite(bytes) => FlagAddResult::WantsWrite(bytes),
            JmapEmailSetResult::Ok { .. } => FlagAddResult::Ok,
            JmapEmailSetResult::Err(err) => FlagAddResult::Err(err),
        }
    }
}

/// Result returned by [`FlagAdd::resume`].
#[derive(Debug)]
pub enum FlagAddResult {
    Ok,
    WantsRead,
    WantsWrite(Vec<u8>),
    Err(JmapEmailSetError),
}
