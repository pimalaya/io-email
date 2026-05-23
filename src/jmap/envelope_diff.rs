//! JMAP incremental envelope fetch (`Email/changes`).
//!
//! Helpers used by [`crate::client::EmailClientStd::diff_envelopes`]
//! to thread the opaque `Email/state` checkpoint through repeated
//! `Email/changes` rounds (RFC 8620 §5.2's `hasMoreChanges` loop) and
//! to detect the `cannotCalculateChanges` error so the dispatcher can
//! degrade to `FullListRequired`.

use alloc::{string::String, vec::Vec};

use io_jmap::{
    client::JmapClientStdError,
    rfc8620::{changes::JmapChangesError, error::JmapMethodError},
    rfc8621::email_changes::JmapEmailChangesError,
};

/// `Email/state` is opaque on the wire; we just pass its UTF-8 bytes
/// through. The encode/decode pair is here for symmetry with the
/// IMAP side (which packs a fixed-size struct).
pub fn encode(state: &str) -> Vec<u8> {
    state.as_bytes().to_vec()
}

/// Decodes the opaque blob back into the JMAP state string. Returns
/// `None` when the bytes are not valid UTF-8; callers treat that as
/// "no usable state".
pub fn decode(bytes: &[u8]) -> Option<String> {
    core::str::from_utf8(bytes).ok().map(String::from)
}

/// True when the [`Email/changes`] error is the spec-defined
/// `cannotCalculateChanges` signal (RFC 8620 §5.2). Everything else
/// propagates as a real error.
pub fn is_cannot_calculate_changes(err: &JmapClientStdError) -> bool {
    let JmapClientStdError::EmailChanges(JmapEmailChangesError::Changes(JmapChangesError::Method(
        method,
    ))) = err
    else {
        return false;
    };
    matches!(method, JmapMethodError::CannotCalculateChanges { .. })
}
