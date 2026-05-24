//! Maildir backend implementations of the [`EmailClientStd`] private
//! dispatch methods.

use alloc::{string::String, vec::Vec};

use io_maildir::{flag::MaildirFlags as MdFlags, maildir::MaildirSubdir};

#[cfg(feature = "search")]
use crate::search::query::SearchEmailsQuery;
use crate::{
    client::{EmailClientStd, EmailClientStdError},
    envelope::Envelope,
    flag::{Flag, FlagOp},
    mailbox::Mailbox,
    maildir::convert::{flag_to_maildir, open_maildir, paginate},
};

#[cfg(feature = "search")]
use crate::maildir::envelope_search::{compare, filter_references_body, matches};

impl EmailClientStd {
    /// Registers the Maildir backend. See [`Self::with_imap`] for the
    /// ordering rule.
    pub fn with_maildir(mut self, client: io_maildir::client::MaildirClient) -> Self {
        self.maildir = Some(client);
        if !self.order.contains(&crate::client::BackendKind::Maildir) {
            self.order.push(crate::client::BackendKind::Maildir);
        }
        self
    }

    /// Borrows the underlying Maildir client when registered. Same
    /// portability caveat as [`Self::as_imap`].
    pub fn as_maildir(&self) -> Option<&io_maildir::client::MaildirClient> {
        self.maildir.as_ref()
    }

    /// Mutable variant of [`Self::as_maildir`].
    pub fn as_maildir_mut(&mut self) -> Option<&mut io_maildir::client::MaildirClient> {
        self.maildir.as_mut()
    }

    pub(crate) fn maildir_list_mailboxes(&mut self) -> Result<Vec<Mailbox>, EmailClientStdError> {
        let client = self.maildir.as_mut().expect("maildir slot registered");
        let maildirs = client.list_maildirs()?;
        let mut mailboxes: Vec<_> = maildirs.into_iter().map(Mailbox::from).collect();
        mailboxes.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(mailboxes)
    }

    /// Parallel envelope listing backed by the std::thread::scope
    /// worker pool in [`MaildirClient::read_entries_par`].
    ///
    /// [`MaildirClient::read_entries_par`]: io_maildir::client::MaildirClient::read_entries_par
    pub(crate) fn maildir_list_envelopes_par(
        &mut self,
        mailbox: &str,
        page: Option<u32>,
        page_size: Option<u32>,
    ) -> Result<Vec<Envelope>, EmailClientStdError> {
        let client = self.maildir.as_mut().expect("maildir slot registered");

        let maildir = open_maildir(client, mailbox)?;
        let entries: Vec<_> = client.list_entries(maildir)?.into_iter().collect();
        let messages = client.read_entries_par(&entries)?;

        let mut envelopes: Vec<_> = messages.into_iter().map(Envelope::from).collect();
        envelopes.sort_by(|a, b| b.date.cmp(&a.date));

        Ok(paginate(envelopes, page, page_size))
    }

    #[cfg(feature = "search")]
    pub(crate) fn maildir_search_envelopes(
        &mut self,
        mailbox: &str,
        query: Option<&SearchEmailsQuery>,
        page: Option<u32>,
        page_size: Option<u32>,
    ) -> Result<Vec<Envelope>, EmailClientStdError> {
        let client = self.maildir.as_mut().expect("maildir slot registered");

        let maildir = open_maildir(client, mailbox)?;
        let entries: Vec<_> = client.list_entries(maildir)?.into_iter().collect();
        let messages = client.read_entries_par(&entries)?;

        let mut envelopes: Vec<_> = messages.into_iter().map(Envelope::from).collect();

        if let Some(query) = query {
            if let Some(filter) = &query.filter {
                if filter_references_body(filter) {
                    return Err(EmailClientStdError::OperationFailed(
                        "envelopes search `body` filter is not yet supported on Maildir",
                    ));
                }
                envelopes.retain(|env| matches(env, filter));
            }

            match query.sort.as_deref() {
                Some(sort) if !sort.is_empty() => {
                    envelopes.sort_by(|a, b| compare(a, b, sort));
                }
                _ => envelopes.sort_by(|a, b| b.date.cmp(&a.date)),
            }
        } else {
            envelopes.sort_by(|a, b| b.date.cmp(&a.date));
        }

        Ok(paginate(envelopes, page, page_size))
    }

    pub(crate) fn maildir_store_flags(
        &mut self,
        mailbox: &str,
        ids: &[&str],
        flags: &[Flag],
        op: FlagOp,
    ) -> Result<(), EmailClientStdError> {
        let client = self.maildir.as_mut().expect("maildir slot registered");

        let maildir = open_maildir(client, mailbox)?;
        let md_flags: MdFlags = flags.iter().filter_map(flag_to_maildir).collect();

        for id in ids {
            match op {
                FlagOp::Add => {
                    client.add_flags(maildir.clone(), *id, md_flags.clone())?;
                }
                FlagOp::Set => {
                    client.set_flags(maildir.clone(), *id, md_flags.clone())?;
                }
                FlagOp::Remove => {
                    client.remove_flags(maildir.clone(), *id, md_flags.clone())?;
                }
            }
        }

        Ok(())
    }

    pub(crate) fn maildir_get_message(
        &mut self,
        mailbox: &str,
        id: &str,
    ) -> Result<Vec<u8>, EmailClientStdError> {
        let client = self.maildir.as_mut().expect("maildir slot registered");

        let maildir = open_maildir(client, mailbox)?;
        let message = client.get(maildir, id)?;

        Ok(message.into())
    }

    pub(crate) fn maildir_add_message(
        &mut self,
        mailbox: &str,
        flags: &[Flag],
        raw: Vec<u8>,
    ) -> Result<String, EmailClientStdError> {
        let client = self.maildir.as_mut().expect("maildir slot registered");

        let maildir = open_maildir(client, mailbox)?;
        let md_flags: MdFlags = flags.iter().filter_map(flag_to_maildir).collect();

        let (id, _) = client.store(maildir, MaildirSubdir::Cur, md_flags, raw)?;

        Ok(id)
    }

    pub(crate) fn maildir_create_mailbox(&mut self, name: &str) -> Result<(), EmailClientStdError> {
        let client = self.maildir.as_mut().expect("maildir slot registered");
        let path = client.root().join(name);
        client.create_maildir(path)?;
        Ok(())
    }

    pub(crate) fn maildir_delete_mailbox(&mut self, name: &str) -> Result<(), EmailClientStdError> {
        let client = self.maildir.as_mut().expect("maildir slot registered");
        let path = client.root().join(name);
        client.delete_maildir(path)?;
        Ok(())
    }

    pub(crate) fn maildir_copy_messages(
        &mut self,
        from: &str,
        to: &str,
        ids: &[&str],
    ) -> Result<(), EmailClientStdError> {
        let client = self.maildir.as_mut().expect("maildir slot registered");

        let src = open_maildir(client, from)?;
        let dst = open_maildir(client, to)?;

        for id in ids {
            client.copy(*id, src.clone(), dst.clone(), None)?;
        }

        Ok(())
    }

    pub(crate) fn maildir_move_messages(
        &mut self,
        from: &str,
        to: &str,
        ids: &[&str],
    ) -> Result<(), EmailClientStdError> {
        let client = self.maildir.as_mut().expect("maildir slot registered");

        let src = open_maildir(client, from)?;
        let dst = open_maildir(client, to)?;

        for id in ids {
            client.r#move(*id, src.clone(), dst.clone(), None)?;
        }

        Ok(())
    }
}
