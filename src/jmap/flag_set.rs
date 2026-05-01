//! JMAP flag set (`Email/set` with replace-keywords patches), wrapping
//! [`io_jmap::rfc8621::email_set::JmapEmailSet`].

use alloc::{collections::BTreeMap, string::String, vec::Vec};

use io_jmap::{
    rfc8620::session::JmapSession,
    rfc8621::email_set::{JmapEmailSet, JmapEmailSetArgs, JmapEmailSetError, JmapEmailSetResult},
};
use log::trace;
use secrecy::SecretString;

/// I/O-free coroutine replacing the keyword set on a list of emails.
pub struct FlagSet {
    inner: JmapEmailSet,
}

impl FlagSet {
    /// Builds the coroutine from a JMAP session, the bearer/basic HTTP
    /// credential, the list of email ids and the keywords to set on
    /// each one (any existing keyword that is not in this set is
    /// removed).
    pub fn new<I, J>(
        session: &JmapSession,
        http_auth: &SecretString,
        ids: I,
        keywords: J,
    ) -> Result<Self, JmapEmailSetError>
    where
        I: IntoIterator<Item = String>,
        J: IntoIterator<Item = String>,
    {
        trace!("prepare JMAP flag set");

        let mut keywords_map = BTreeMap::new();
        for keyword in keywords {
            keywords_map.insert(keyword, true);
        }

        let mut args = JmapEmailSetArgs::default();
        for id in ids {
            args.replace_keywords(id, keywords_map.clone());
        }

        let inner = JmapEmailSet::new(session, http_auth, args)?;
        Ok(Self { inner })
    }

    /// Advances the coroutine.
    pub fn resume(&mut self, arg: Option<&[u8]>) -> FlagSetResult {
        match self.inner.resume(arg) {
            JmapEmailSetResult::WantsRead => FlagSetResult::WantsRead,
            JmapEmailSetResult::WantsWrite(bytes) => FlagSetResult::WantsWrite(bytes),
            JmapEmailSetResult::Ok { .. } => FlagSetResult::Ok,
            JmapEmailSetResult::Err(err) => FlagSetResult::Err(err),
        }
    }
}

/// Result returned by [`FlagSet::resume`].
#[derive(Debug)]
pub enum FlagSetResult {
    Ok,
    WantsRead,
    WantsWrite(Vec<u8>),
    Err(JmapEmailSetError),
}
