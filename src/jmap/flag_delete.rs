//! JMAP flag delete (`Email/set` with `keywords/<name>: null` patches),
//! wrapping [`io_jmap::rfc8621::email_set::JmapEmailSet`].

use alloc::{string::String, vec::Vec};

use io_jmap::{
    rfc8620::session::JmapSession,
    rfc8621::email_set::{JmapEmailSet, JmapEmailSetArgs, JmapEmailSetError, JmapEmailSetResult},
};
use log::trace;
use secrecy::SecretString;

/// I/O-free coroutine removing keywords (flags) from a set of emails.
pub struct FlagDelete {
    inner: JmapEmailSet,
}

impl FlagDelete {
    /// Builds the coroutine from a JMAP session, the bearer/basic HTTP
    /// credential, and the `(email_id, keyword)` pairs to unset.
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
        trace!("prepare JMAP flag delete");

        let mut args = JmapEmailSetArgs::default();
        for id in ids {
            for keyword in keywords.clone() {
                args.unset_keyword(id.clone(), keyword);
            }
        }

        let inner = JmapEmailSet::new(session, http_auth, args)?;
        Ok(Self { inner })
    }

    /// Advances the coroutine.
    pub fn resume(&mut self, arg: Option<&[u8]>) -> FlagDeleteResult {
        match self.inner.resume(arg) {
            JmapEmailSetResult::WantsRead => FlagDeleteResult::WantsRead,
            JmapEmailSetResult::WantsWrite(bytes) => FlagDeleteResult::WantsWrite(bytes),
            JmapEmailSetResult::Ok { .. } => FlagDeleteResult::Ok,
            JmapEmailSetResult::Err(err) => FlagDeleteResult::Err(err),
        }
    }
}

/// Result returned by [`FlagDelete::resume`].
#[derive(Debug)]
pub enum FlagDeleteResult {
    Ok,
    WantsRead,
    WantsWrite(Vec<u8>),
    Err(JmapEmailSetError),
}
