#![no_std]
#![cfg_attr(docsrs, feature(doc_cfg, doc_auto_cfg))]
#![doc = include_str!("../README.md")]

#[cfg_attr(any(feature = "imap", feature = "jmap"), macro_use)]
extern crate alloc;
#[cfg(feature = "client")]
extern crate std;

pub mod address;
#[cfg(feature = "client")]
#[cfg(any(
    feature = "imap",
    feature = "jmap",
    feature = "maildir",
    feature = "smtp"
))]
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
#[cfg(feature = "search")]
pub mod search;
#[cfg(feature = "smtp")]
pub mod smtp;
