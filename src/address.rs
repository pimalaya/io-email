//! Email address shared across all protocols.

use alloc::string::String;

/// A single email address with an optional display name.
///
/// Common shape used by every protocol-specific envelope and message
/// representation in this crate.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "kebab-case"))]
pub struct Address {
    /// Display name (e.g. `Alice`), if any.
    pub name: Option<String>,

    /// Email address (e.g. `alice@example.org`).
    pub email: String,
}

impl Address {
    pub fn new(email: impl Into<String>) -> Self {
        Self {
            name: None,
            email: email.into(),
        }
    }

    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }
}
