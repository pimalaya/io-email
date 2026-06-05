//! Helpers for incremental envelope sync via Email/changes (RFC 8620
//! §5.2), used by [`crate::client::EmailClientStd::diff_envelopes`].

use alloc::{string::String, vec::Vec};

use io_jmap::{
    client::JmapClientStdError,
    rfc8620::{JmapMethodError, changes::JmapChangesError},
    rfc8621::email::changes::JmapEmailChangesError,
};

/// Email/state is opaque; the encode/decode pair mirrors the IMAP
/// side for symmetry.
pub fn encode(state: &str) -> Vec<u8> {
    state.as_bytes().to_vec()
}

/// Reverse of [`encode`]; `None` on invalid UTF-8.
pub fn decode(bytes: &[u8]) -> Option<String> {
    core::str::from_utf8(bytes).ok().map(String::from)
}

/// True for the spec-defined cannotCalculateChanges signal (RFC 8620
/// §5.2); other errors propagate.
pub fn is_cannot_calculate_changes(err: &JmapClientStdError) -> bool {
    let JmapClientStdError::EmailChanges(JmapEmailChangesError::Changes(JmapChangesError::Method(
        method,
    ))) = err
    else {
        return false;
    };
    matches!(method, JmapMethodError::CannotCalculateChanges { .. })
}
