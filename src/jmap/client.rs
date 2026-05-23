//! JMAP backend implementations of the [`EmailClientStd`] private
//! dispatch methods.

use alloc::{
    collections::BTreeMap,
    string::{String, ToString},
    vec,
    vec::Vec,
};

use io_jmap::{
    client::{JmapClientStd, JmapClientStdError},
    rfc8620::{changes::JmapChangesError, error::JmapMethodError},
    rfc8621::{
        capabilities,
        email::{EmailImport, EmailProperty},
        email_set::JmapEmailSetArgs,
        email_submission::{
            EmailAddressWithParameters, EmailSubmissionCreate, Envelope as JmapEnvelope,
        },
        mailbox::{MailboxFilter, MailboxRole},
        mailbox_changes::JmapMailboxChangesError,
        mailbox_set::JmapMailboxSetArgs,
    },
};
use pimalaya_stream::{std::stream::StreamStd, tls::Tls};
use url::Url;

#[cfg(feature = "search")]
use crate::search::query::SearchEmailsQuery;
use crate::{
    client::{EmailClientStd, EmailClientStdError},
    envelope::{Envelope, EnvelopeDiff, FlagUpdate},
    flag::{Flag, FlagOp},
    jmap::{
        convert::{compute_position_limit, keyword_from, mailbox_filter},
        envelope_diff,
        envelope_list::envelope_properties,
        message_get::resolve_download_url,
    },
    mailbox::{Mailbox, MailboxDiff},
};

#[cfg(feature = "search")]
use crate::jmap::envelope_search::{build, post_filter};

impl EmailClientStd {
    /// Registers the JMAP backend. See [`Self::with_imap`] for the
    /// ordering rule.
    pub fn with_jmap(mut self, client: io_jmap::client::JmapClientStd) -> Self {
        self.jmap = Some(client);
        if !self.order.contains(&crate::client::BackendKind::Jmap) {
            self.order.push(crate::client::BackendKind::Jmap);
        }
        self
    }

    /// Borrows the underlying JMAP client when registered. Same
    /// portability caveat as [`Self::as_imap`].
    pub fn as_jmap(&self) -> Option<&io_jmap::client::JmapClientStd> {
        self.jmap.as_ref()
    }

    /// Mutable variant of [`Self::as_jmap`].
    pub fn as_jmap_mut(&mut self) -> Option<&mut io_jmap::client::JmapClientStd> {
        self.jmap.as_mut()
    }

    pub(crate) fn list_mailboxes_jmap(&mut self) -> Result<Vec<Mailbox>, EmailClientStdError> {
        let client = self.jmap.as_mut().expect("jmap slot registered");
        let output = client.mailbox_query(None, None, None, None, None)?;
        Ok(output.mailboxes.into_iter().map(Mailbox::from).collect())
    }

    pub(crate) fn list_envelopes_jmap(
        &mut self,
        mailbox: &str,
        page: Option<u32>,
        page_size: Option<u32>,
    ) -> Result<Vec<Envelope>, EmailClientStdError> {
        let client = self.jmap.as_mut().expect("jmap slot registered");

        let (position, limit) = compute_position_limit(page, page_size);
        let filter = mailbox_filter(mailbox);
        let output =
            client.email_query(filter, None, position, limit, Some(envelope_properties()))?;

        Ok(output.emails.into_iter().map(Envelope::from).collect())
    }

    #[cfg(feature = "search")]
    pub(crate) fn search_envelopes_jmap(
        &mut self,
        mailbox: &str,
        query: Option<&SearchEmailsQuery>,
        page: Option<u32>,
        page_size: Option<u32>,
    ) -> Result<Vec<Envelope>, EmailClientStdError> {
        let client = self.jmap.as_mut().expect("jmap slot registered");

        let base = mailbox_filter(mailbox).unwrap_or_default();
        let converted = build(query, base)?;

        let (position, limit) = if converted.post_filters.is_empty() {
            compute_position_limit(page, page_size)
        } else {
            (None, None)
        };

        let output = client.email_query(
            Some(converted.filter),
            Some(converted.sort),
            position,
            limit,
            Some(envelope_properties()),
        )?;

        let mut envelopes: Vec<_> = output.emails.into_iter().map(Envelope::from).collect();

        if !converted.post_filters.is_empty() {
            envelopes.retain(|env| post_filter(env, &converted.post_filters));

            let total = envelopes.len();
            let size = page_size.map(|n| n as usize);
            let start = ((page.unwrap_or(1).max(1) - 1) as usize).saturating_mul(size.unwrap_or(0));
            let end = match size {
                Some(n) => start.saturating_add(n).min(total),
                None => total,
            };
            if start >= total {
                envelopes.clear();
            } else {
                envelopes = envelopes[start..end].to_vec();
            }
        }

        Ok(envelopes)
    }

    pub(crate) fn store_flags_jmap(
        &mut self,
        ids: &[&str],
        flags: &[Flag],
        op: FlagOp,
    ) -> Result<(), EmailClientStdError> {
        let client = self.jmap.as_mut().expect("jmap slot registered");

        let mut args = JmapEmailSetArgs::default();

        for id in ids {
            for flag in flags {
                let keyword = keyword_from(flag);
                match op {
                    FlagOp::Add | FlagOp::Set => {
                        args.set_keyword(id.to_string(), keyword);
                    }
                    FlagOp::Remove => {
                        args.unset_keyword(id.to_string(), keyword);
                    }
                }
            }
        }

        let _ = client.email_set(args)?;

        Ok(())
    }

    pub(crate) fn get_message_jmap(&mut self, id: &str) -> Result<Vec<u8>, EmailClientStdError> {
        let client = self.jmap.as_mut().expect("jmap slot registered");

        let session = client
            .session()
            .ok_or(JmapClientStdError::MissingSession)?
            .clone();

        let output = client.email_get(
            vec![id.to_string()],
            Some(vec![EmailProperty::BlobId]),
            false,
            false,
            0,
        )?;

        let email = output
            .emails
            .into_iter()
            .next()
            .ok_or_else(|| EmailClientStdError::MessageNotFound(id.to_string()))?;
        let blob_id = email
            .blob_id
            .ok_or(EmailClientStdError::OperationFailed("retrieve blob id"))?;

        let account_id = session
            .primary_accounts
            .get(capabilities::MAIL)
            .cloned()
            .unwrap_or_default();
        let url_str = resolve_download_url(&session.download_url, &account_id, &blob_id);
        let download_url =
            Url::parse(&url_str).map_err(|_| EmailClientStdError::InvalidUrl(url_str))?;

        // Fastmail (and any provider where the API host differs from
        // the download host) serves blobs from a separate authority.
        // Reusing the existing API stream would issue the download GET
        // against the API host and get redirected, which io-jmap
        // surfaces as `UnexpectedRedirect`. When the authorities
        // differ, open a dedicated stream just for the download.
        if same_authority(&session.api_url, &download_url) {
            return Ok(client.blob_download(&download_url)?);
        }

        let host = download_url
            .host_str()
            .ok_or_else(|| EmailClientStdError::InvalidUrl(download_url.to_string()))?;
        let port = download_url.port_or_known_default().unwrap_or(443);
        let mut tls = Tls::default();
        tls.rustls.alpn = vec!["http/1.1".into()];
        let stream = StreamStd::connect_tls(host, port, &tls)
            .map_err(|_| EmailClientStdError::OperationFailed("open download stream"))?;
        let mut download_client = JmapClientStd::new(stream, client.http_auth().clone());

        Ok(download_client.blob_download(&download_url)?)
    }

    pub(crate) fn add_message_jmap(
        &mut self,
        mailbox: &str,
        flags: &[Flag],
        raw: Vec<u8>,
    ) -> Result<String, EmailClientStdError> {
        let client = self.jmap.as_mut().expect("jmap slot registered");

        let session = client
            .session()
            .ok_or(JmapClientStdError::MissingSession)?
            .clone();
        let account_id = session
            .primary_accounts
            .get(capabilities::MAIL)
            .cloned()
            .unwrap_or_default();
        let upload_url_str = session.upload_url.replace("{accountId}", &account_id);
        let upload_url = Url::parse(&upload_url_str)
            .map_err(|_| EmailClientStdError::InvalidUrl(upload_url_str))?;
        let blob = client.blob_upload(&upload_url, "message/rfc822", raw)?;

        let mq = client.mailbox_query(None, None, None, None, None)?;
        let mailbox_id = mq
            .mailboxes
            .iter()
            .find(|m| m.name.as_deref() == Some(mailbox))
            .and_then(|m| m.id.clone())
            .ok_or_else(|| EmailClientStdError::MailboxNotFound(mailbox.to_string()))?;

        let mut mailbox_ids = BTreeMap::new();
        mailbox_ids.insert(mailbox_id, true);

        let keywords = if flags.is_empty() {
            None
        } else {
            Some(flags.iter().map(|f| (keyword_from(f), true)).collect())
        };

        let mut emails = BTreeMap::new();
        emails.insert(
            "new".to_string(),
            EmailImport {
                blob_id: blob.blob_id,
                mailbox_ids,
                keywords,
                received_at: None,
            },
        );

        let mut output = client.email_import(emails)?;
        if !output.not_created.is_empty() {
            return Err(EmailClientStdError::OperationFailed("import"));
        }

        let id = output
            .created
            .remove("new")
            .and_then(|email| email.id)
            .ok_or(EmailClientStdError::OperationFailed("import"))?;

        Ok(id)
    }

    pub(crate) fn create_mailbox_jmap(&mut self, name: &str) -> Result<(), EmailClientStdError> {
        use io_jmap::rfc8621::mailbox::MailboxCreate;

        let client = self.jmap.as_mut().expect("jmap slot registered");

        let mut create = BTreeMap::new();
        create.insert(
            "new".to_string(),
            MailboxCreate {
                name: Some(name.to_string()),
                ..MailboxCreate::default()
            },
        );

        let args = JmapMailboxSetArgs {
            create: Some(create),
            ..JmapMailboxSetArgs::default()
        };

        let output = client.mailbox_set(args)?;
        if !output.not_created.is_empty() {
            return Err(EmailClientStdError::OperationFailed("mailbox create"));
        }

        Ok(())
    }

    pub(crate) fn delete_mailbox_jmap(&mut self, name: &str) -> Result<(), EmailClientStdError> {
        let client = self.jmap.as_mut().expect("jmap slot registered");

        let mq = client.mailbox_query(None, None, None, None, None)?;
        let id = mq
            .mailboxes
            .iter()
            .find(|m| m.name.as_deref() == Some(name))
            .and_then(|m| m.id.clone())
            .ok_or_else(|| EmailClientStdError::MailboxNotFound(name.to_string()))?;

        let args = JmapMailboxSetArgs {
            destroy: Some(vec![id]),
            ..JmapMailboxSetArgs::default()
        };

        let output = client.mailbox_set(args)?;
        if !output.not_destroyed.is_empty() {
            return Err(EmailClientStdError::OperationFailed("mailbox delete"));
        }

        Ok(())
    }

    pub(crate) fn delete_message_jmap(&mut self, id: &str) -> Result<(), EmailClientStdError> {
        let client = self.jmap.as_mut().expect("jmap slot registered");

        let mut args = JmapEmailSetArgs::default();
        args.destroy(id.to_string());

        let output = client.email_set(args)?;
        if !output.not_destroyed.is_empty() {
            return Err(EmailClientStdError::OperationFailed("message delete"));
        }

        Ok(())
    }

    pub(crate) fn copy_messages_jmap(
        &mut self,
        to: &str,
        ids: &[&str],
    ) -> Result<(), EmailClientStdError> {
        let client = self.jmap.as_mut().expect("jmap slot registered");

        let mq = client.mailbox_query(None, None, None, None, None)?;
        let dst_id = mq
            .mailboxes
            .iter()
            .find(|m| m.name.as_deref() == Some(to))
            .and_then(|m| m.id.clone())
            .ok_or_else(|| EmailClientStdError::MailboxNotFound(to.to_string()))?;

        let mut args = JmapEmailSetArgs::default();
        for id in ids {
            args.add_to_mailbox(id.to_string(), dst_id.clone());
        }

        let _ = client.email_set(args)?;

        Ok(())
    }

    pub(crate) fn move_messages_jmap(
        &mut self,
        from: &str,
        to: &str,
        ids: &[&str],
    ) -> Result<(), EmailClientStdError> {
        let client = self.jmap.as_mut().expect("jmap slot registered");

        let mq = client.mailbox_query(None, None, None, None, None)?;
        let src_id = mq
            .mailboxes
            .iter()
            .find(|m| m.name.as_deref() == Some(from))
            .and_then(|m| m.id.clone())
            .ok_or_else(|| EmailClientStdError::MailboxNotFound(from.to_string()))?;
        let dst_id = mq
            .mailboxes
            .iter()
            .find(|m| m.name.as_deref() == Some(to))
            .and_then(|m| m.id.clone())
            .ok_or_else(|| EmailClientStdError::MailboxNotFound(to.to_string()))?;

        let mut args = JmapEmailSetArgs::default();
        for id in ids {
            args.remove_from_mailbox(id.to_string(), src_id.clone());
            args.add_to_mailbox(id.to_string(), dst_id.clone());
        }

        let _ = client.email_set(args)?;

        Ok(())
    }

    pub(crate) fn send_message_jmap(
        &mut self,
        raw: Vec<u8>,
        from: &str,
        to: &[&str],
    ) -> Result<(), EmailClientStdError> {
        if to.is_empty() {
            return Err(EmailClientStdError::MissingInput("to"));
        }

        let client = self.jmap.as_mut().expect("jmap slot registered");

        let identity_id = client
            .identity_get(None)?
            .identities
            .into_iter()
            .find(|i| i.email == from)
            .map(|i| i.id)
            .ok_or_else(|| EmailClientStdError::IdentityNotFound(from.to_string()))?;

        let drafts_id = client
            .mailbox_query(
                Some(MailboxFilter {
                    role: Some(MailboxRole::Drafts),
                    ..MailboxFilter::default()
                }),
                None,
                None,
                None,
                None,
            )?
            .mailboxes
            .into_iter()
            .next()
            .and_then(|m| m.id)
            .ok_or_else(|| EmailClientStdError::MailboxNotFound("drafts".to_string()))?;

        let session = client
            .session()
            .ok_or(JmapClientStdError::MissingSession)?
            .clone();
        let account_id = session
            .primary_accounts
            .get(capabilities::MAIL)
            .cloned()
            .unwrap_or_default();
        let upload_url_str = session.upload_url.replace("{accountId}", &account_id);
        let upload_url = Url::parse(&upload_url_str)
            .map_err(|_| EmailClientStdError::InvalidUrl(upload_url_str))?;
        let blob = client.blob_upload(&upload_url, "message/rfc822", raw)?;

        let mut mailbox_ids = BTreeMap::new();
        mailbox_ids.insert(drafts_id, true);

        let mut imports = BTreeMap::new();
        imports.insert(
            "new".to_string(),
            EmailImport {
                blob_id: blob.blob_id,
                mailbox_ids,
                keywords: None,
                received_at: None,
            },
        );

        let import = client.email_import(imports)?;
        if !import.not_created.is_empty() || import.created.is_empty() {
            return Err(EmailClientStdError::OperationFailed("import"));
        }
        let email_id = import
            .created
            .values()
            .next()
            .and_then(|e| e.id.clone())
            .ok_or(EmailClientStdError::OperationFailed("import"))?;

        let envelope = JmapEnvelope {
            mail_from: EmailAddressWithParameters {
                email: from.to_string(),
                parameters: None,
            },
            rcpt_to: to
                .iter()
                .map(|addr| EmailAddressWithParameters {
                    email: (*addr).to_string(),
                    parameters: None,
                })
                .collect(),
        };

        let mut submissions = BTreeMap::new();
        submissions.insert(
            "send".to_string(),
            EmailSubmissionCreate {
                identity_id,
                email_id,
                envelope: Some(envelope),
            },
        );

        let submission = client.email_submission_set(submissions)?;
        if !submission.not_created.is_empty() || submission.created.is_empty() {
            return Err(EmailClientStdError::OperationFailed("submission"));
        }

        Ok(())
    }

    pub(crate) fn diff_envelopes_jmap(
        &mut self,
        _mailbox: &str,
        state: Option<&[u8]>,
    ) -> Result<EnvelopeDiff, EmailClientStdError> {
        let client = self.jmap.as_mut().expect("jmap slot registered");

        // First sync or unusable cached state: capture the new state
        // cheaply via `Email/get` with an empty id list.
        let Some(since_state) = state.and_then(envelope_diff::decode) else {
            return baseline_jmap(client);
        };

        let mut created_ids: Vec<String> = Vec::new();
        let mut updated_ids: Vec<String> = Vec::new();
        let mut destroyed_ids: Vec<String> = Vec::new();
        let mut cursor = since_state;

        loop {
            let changes = match client.email_changes(cursor.clone(), None) {
                Ok(c) => c,
                Err(err) if envelope_diff::is_cannot_calculate_changes(&err) => {
                    return baseline_jmap(client);
                }
                Err(err) => return Err(err.into()),
            };

            created_ids.extend(changes.created);
            updated_ids.extend(changes.updated);
            destroyed_ids.extend(changes.destroyed);

            if !changes.has_more_changes {
                cursor = changes.new_state;
                break;
            }

            cursor = changes.new_state;
        }

        let properties = envelope_properties();

        let new_envelopes = if created_ids.is_empty() {
            Vec::new()
        } else {
            let output =
                client.email_get(created_ids, Some(properties.clone()), false, false, 0)?;
            output.emails.into_iter().map(Envelope::from).collect()
        };

        let flag_updates = if updated_ids.is_empty() {
            Vec::new()
        } else {
            let output = client.email_get(updated_ids, Some(properties), false, false, 0)?;
            output
                .emails
                .into_iter()
                .map(Envelope::from)
                .map(|env| FlagUpdate {
                    id: env.id,
                    flags: env.flags,
                })
                .collect()
        };

        Ok(EnvelopeDiff::Incremental {
            new_state: envelope_diff::encode(&cursor),
            flag_updates,
            new_envelopes,
            vanished_ids: destroyed_ids,
        })
    }

    pub(crate) fn diff_mailboxes_jmap(
        &mut self,
        state: Option<&[u8]>,
    ) -> Result<MailboxDiff, EmailClientStdError> {
        let client = self.jmap.as_mut().expect("jmap slot registered");

        // First sync or unusable cached state: capture the current
        // `Mailbox/state` via an empty `Mailbox/get`.
        let Some(since_state) = state.and_then(envelope_diff::decode) else {
            let output = client.mailbox_get(Some(Vec::new()), None)?;
            return Ok(MailboxDiff::Changed {
                new_state: Some(envelope_diff::encode(&output.new_state)),
            });
        };

        // Single `Mailbox/changes` round: any non-empty bucket or a
        // server bumping the state means the caller must re-list.
        match client.mailbox_changes(since_state.clone(), None) {
            Ok(changes)
                if !changes.has_more_changes
                    && changes.created.is_empty()
                    && changes.updated.is_empty()
                    && changes.destroyed.is_empty() =>
            {
                Ok(MailboxDiff::Unchanged {
                    new_state: envelope_diff::encode(&changes.new_state),
                })
            }
            Ok(changes) => Ok(MailboxDiff::Changed {
                new_state: Some(envelope_diff::encode(&changes.new_state)),
            }),
            Err(JmapClientStdError::MailboxChanges(JmapMailboxChangesError::Changes(
                JmapChangesError::Method(JmapMethodError::CannotCalculateChanges { .. }),
            ))) => Ok(MailboxDiff::Changed { new_state: None }),
            Err(err) => Err(err.into()),
        }
    }
}

/// Captures a fresh JMAP `Email/state` checkpoint via an empty
/// `Email/get`. Used on the first sync, when the stored state is
/// unusable, or when the server returns `cannotCalculateChanges`.
fn baseline_jmap(client: &mut JmapClientStd) -> Result<EnvelopeDiff, EmailClientStdError> {
    let output = client.email_get(Vec::new(), None, false, false, 0)?;
    Ok(EnvelopeDiff::FullListRequired {
        new_state: Some(envelope_diff::encode(&output.new_state)),
    })
}

/// Returns `true` when both URLs share the same host and effective
/// port. Used to decide whether the active JMAP stream (bound to the
/// API authority) can serve a blob download, or whether a fresh
/// connection to the download authority is required.
fn same_authority(a: &Url, b: &Url) -> bool {
    a.host() == b.host() && a.port_or_known_default() == b.port_or_known_default()
}
