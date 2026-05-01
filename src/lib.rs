#![cfg_attr(docsrs, feature(doc_cfg, doc_auto_cfg))]
#![doc = include_str!("../README.md")]
#![cfg_attr(
    not(any(feature = "imap", feature = "maildir", feature = "std")),
    no_std
)]

extern crate alloc;

pub mod address;
#[cfg(feature = "client")]
pub mod client;
pub mod envelope;
pub mod flag;
#[cfg(feature = "imap")]
pub mod imap;
#[cfg(feature = "jmap")]
pub mod jmap;
pub mod mailbox;
#[cfg(feature = "maildir")]
pub mod maildir;
#[cfg(feature = "smtp")]
pub mod smtp;
