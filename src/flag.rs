//! Email flag (a.k.a. keyword) shared across all protocols.
//!
//! This is the strict least-common-denominator across IMAP system
//! flags (RFC 3501 §2.3.2), JMAP keywords (RFC 8621 §4.1.1) and
//! Maildir filename info-section letters: only the four flags that
//! every backend natively supports are exposed. Backend-specific
//! extras (`\Deleted`, `Trashed`, `Passed`, custom keywords) live on
//! the protocol-specific commands.

/// A flag attached to an envelope or message.
///
/// Variant order matches the JMAP RFC 8621 §4.1.1 declaration order
/// (`$seen`, `$flagged`, `$answered`, `$draft` at the time of writing
/// — kept here as `Seen`, `Answered`, `Flagged`, `Draft` to align with
/// IMAP's display order). The derived `Ord` lets callers store flags
/// in a `BTreeSet<Flag>` with deterministic iteration.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "kebab-case"))]
pub enum Flag {
    Seen,
    Answered,
    Flagged,
    Draft,
}

impl Flag {
    /// Parses a wire flag string (IMAP `\Seen`, JMAP `$seen`, Maildir
    /// letter — already lowercased by the caller is fine).
    ///
    /// Returns `None` for anything outside the four LCD variants.
    /// Callers typically chain `.filter_map(Flag::parse)` to drop
    /// unrecognised flags silently.
    pub fn parse(flag: &str) -> Option<Self> {
        let normalized = flag.trim_start_matches(['\\', '$']).to_ascii_lowercase();
        match normalized.as_str() {
            "seen" | "s" => Some(Self::Seen),
            "answered" | "r" => Some(Self::Answered),
            "flagged" | "f" => Some(Self::Flagged),
            "draft" | "d" => Some(Self::Draft),
            _ => None,
        }
    }
}
