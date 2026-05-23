//! Conversions between m2dir filesystem types and the shared types
//! used by [`EmailClientStd`], plus the `From` impl that wraps an
//! already-built [`M2dirClient`] into a fresh unified client with
//! m2dir as the only registered backend.
//!
//! The dispatcher in [`crate::client`] owns the I/O.

use alloc::{
    string::{String, ToString},
    vec::Vec,
};

use chrono::DateTime;
use io_m2dir::{
    client::{M2dirClient, M2dirClientError},
    entry::Entry,
    flag::Flags as M2Flags,
    m2dir::M2dir,
    m2store::M2store,
};
use mail_parser::Message as ParsedMessage;

use crate::{
    address::Address,
    client::{EmailClientStd, EmailClientStdError},
    envelope::{Envelope, normalize_message_id},
    flag::Flag,
    mailbox::Mailbox,
};

impl From<M2dirClient> for EmailClientStd {
    fn from(client: M2dirClient) -> Self {
        Self::new().with_m2dir(client)
    }
}

/// Builds a shared [`Mailbox`] from an m2dir handle.
///
/// The name is the m2dir's directory basename. The same value is
/// reused as the id since m2dir has no opaque identifier separate
/// from the on-disk folder. Counts are left as `None`: counting
/// requires enumerating the m2dir's entries and is driven at the
/// [`EmailClientStd`] level when callers request it.
pub fn mailbox_from(m2dir: &M2dir) -> Mailbox {
    let name = m2dir.path().file_name().unwrap_or("").to_string();

    Mailbox {
        id: name.clone(),
        name,
        total: None,
        unread: None,
    }
}

/// Builds a shared [`Envelope`] from an m2dir entry and its flags
/// metadata. Only fields available from RFC 5322 headers are populated;
/// [`Envelope::has_attachment`] stays `None`. Callers that need
/// attachment detection must parse the full body separately (e.g.
/// `parsed.attachment_count() > 0`) and overwrite the field
/// post-hoc.
///
/// `parsed` may come from either [`MessageParser::parse`] or
/// [`MessageParser::parse_headers`]; with the latter, body-decoding
/// is skipped and per-message work drops by an order of magnitude.
/// `size` is the raw byte length (no decoding required).
///
/// [`MessageParser::parse`]: mail_parser::MessageParser::parse
/// [`MessageParser::parse_headers`]: mail_parser::MessageParser::parse_headers
pub fn envelope_from(entry: &Entry, meta: &M2Flags, parsed: &ParsedMessage<'_>) -> Envelope {
    let id = entry.id().to_string();

    let flags = meta.iter().map(flag_from_meta_line).collect();

    let subject = parsed.subject().unwrap_or_default().to_string();

    let from = parsed.from().map(addresses_from).unwrap_or_default();

    let to = parsed.to().map(addresses_from).unwrap_or_default();

    let date = parsed
        .date()
        .and_then(|d| DateTime::parse_from_rfc3339(&d.to_rfc3339()).ok());

    let size = parsed.raw_message().len() as u64;

    let message_id = parsed.message_id().and_then(normalize_message_id);

    Envelope {
        id,
        message_id,
        flags,
        subject,
        from,
        to,
        date,
        size,
        has_attachment: None,
    }
}

/// Parses a single line from a `.meta/<id>.flags` file into a shared
/// [`Flag`]. The line is trimmed before classification so that stray
/// whitespace from sloppy editors does not break IANA recognition.
pub fn flag_from_meta_line(line: &str) -> Flag {
    Flag::from_raw(line.trim())
}

/// Serialises a shared [`Flag`] to its `.meta/<id>.flags`
/// representation. The raw spelling already carries the canonical
/// IANA form when classified, or the user keyword as-is otherwise.
pub fn flag_to_meta_line(flag: &Flag) -> String {
    flag.raw().to_string()
}

/// Opens the m2dir folder named `name` under the client's m2store
/// root, returning [`EmailClientStdError::InvalidMailbox`] when the
/// folder does not exist or is missing its `.m2dir` marker.
pub fn open_m2dir(client: &M2dirClient, name: &str) -> Result<M2dir, EmailClientStdError> {
    let store = M2store::from_path(client.root().clone());
    let path = store
        .resolve_folder_path(name)
        .map_err(M2dirClientError::from)?;
    client
        .open_m2dir(path)
        .map_err(|_| EmailClientStdError::InvalidMailbox(name.to_string()))
}

/// 1-indexed pagination on an in-memory list. `page_size = None`
/// returns the full slice; `page_size = 0` or a page past the end
/// returns an empty vector.
pub(crate) fn paginate(
    envelopes: Vec<Envelope>,
    page: Option<u32>,
    page_size: Option<u32>,
) -> Vec<Envelope> {
    let Some(size) = page_size else {
        return envelopes;
    };

    if size == 0 {
        return Vec::new();
    }

    let page = page.unwrap_or(1).max(1);
    let skip = ((page - 1) as usize).saturating_mul(size as usize);

    if skip >= envelopes.len() {
        return Vec::new();
    }

    envelopes
        .into_iter()
        .skip(skip)
        .take(size as usize)
        .collect()
}

fn addresses_from(addrs: &mail_parser::Address<'_>) -> Vec<Address> {
    addrs
        .clone()
        .into_list()
        .into_iter()
        .filter_map(|a| {
            let email = a.address?.into_owned();
            if email.is_empty() {
                return None;
            }
            let name = a.name.map(|s| s.into_owned());
            Some(Address { name, email })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use crate::{
        client::EmailClientStd,
        flag::{Flag, IanaFlag},
        m2dir::convert::*,
    };

    #[test]
    fn flag_meta_line_round_trip_iana() {
        let parsed = flag_from_meta_line("\\Seen");
        assert!(parsed.is_seen());
        assert_eq!(flag_to_meta_line(&parsed), "\\Seen");

        let parsed = flag_from_meta_line("$Forwarded");
        assert!(parsed.is_forwarded());
        assert_eq!(flag_to_meta_line(&parsed), "$Forwarded");
    }

    #[test]
    fn flag_meta_line_round_trip_custom() {
        let parsed = flag_from_meta_line("custom-label");
        assert_eq!(parsed.iana(), None);
        assert_eq!(flag_to_meta_line(&parsed), "custom-label");
    }

    #[test]
    fn flag_meta_line_trims_whitespace() {
        let parsed = flag_from_meta_line("  \\Seen  ");
        assert!(parsed.is_seen());
    }

    #[test]
    fn flag_to_meta_line_uses_canonical_iana_spelling() {
        let line = flag_to_meta_line(&Flag::from_iana(IanaFlag::MdnSent));
        assert_eq!(line, "$MDNSent");
    }

    #[test]
    fn dispatcher_round_trips_against_handbuilt_store() {
        use io_m2dir::client::M2dirClient;

        let dir = tempdir().unwrap();
        let m2 = M2dirClient::new(dir.path().to_string_lossy().into_owned());
        m2.init_store().unwrap();

        let mut client: EmailClientStd = m2.into();

        // Empty store: list_mailboxes returns nothing.
        let mailboxes = client.list_mailboxes(false).unwrap();
        assert!(mailboxes.is_empty());

        // Create a mailbox.
        client.create_mailbox("inbox").unwrap();
        let mailboxes = client.list_mailboxes(false).unwrap();
        assert_eq!(mailboxes.len(), 1);
        assert_eq!(mailboxes[0].name, "inbox");

        // Add a message with a known flag set.
        let raw = b"From: alice@example.org\r\nSubject: hi\r\nDate: Tue, 15 Apr 1994 08:12:31 GMT\r\n\r\nbody\r\n".to_vec();
        let flags = vec![Flag::from_iana(IanaFlag::Seen), Flag::from_raw("custom")];
        let id = client.add_message("inbox", &flags, raw.clone()).unwrap();

        // List envelopes: subject, flags survive the round trip.
        let envelopes = client.list_envelopes("inbox", None, None, false).unwrap();
        assert_eq!(envelopes.len(), 1);
        assert_eq!(envelopes[0].id, id);
        assert_eq!(envelopes[0].subject, "hi");
        assert_eq!(envelopes[0].flags.len(), 2);
        assert!(envelopes[0].flags.iter().any(|f| f.is_seen()));
        assert!(envelopes[0].flags.iter().any(|f| f.raw() == "custom"));

        // get_message returns the original bytes.
        let fetched = client.get_message("inbox", &id).unwrap();
        assert_eq!(fetched, raw);

        // add_flags adds a flag.
        client
            .add_flags(
                "inbox",
                &[id.as_str()],
                &[Flag::from_iana(IanaFlag::Flagged)],
            )
            .unwrap();
        let envelopes = client.list_envelopes("inbox", None, None, false).unwrap();
        assert!(envelopes[0].flags.iter().any(|f| f.is_flagged()));

        // delete_message removes the entry.
        client.delete_message("inbox", &id).unwrap();
        let envelopes = client.list_envelopes("inbox", None, None, false).unwrap();
        assert!(envelopes.is_empty());

        // delete_mailbox removes the folder.
        client.delete_mailbox("inbox").unwrap();
        let mailboxes = client.list_mailboxes(false).unwrap();
        assert!(mailboxes.is_empty());

        drop(dir);
    }
}
