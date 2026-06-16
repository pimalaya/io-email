#![no_std]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![doc = include_str!("../README.md")]

extern crate alloc;
#[cfg(feature = "client")]
extern crate std;

pub mod address;
#[cfg(feature = "client")]
#[cfg(any(
    feature = "imap",
    feature = "jmap",
    feature = "gmail",
    feature = "maildir",
    feature = "m2dir",
    feature = "smtp"
))]
pub mod client;
pub mod envelope;
pub mod flag;
pub mod mailbox;
pub mod message;
#[cfg(feature = "search")]
pub mod search;

#[cfg(feature = "gmail")]
pub mod gmail;
#[cfg(feature = "imap")]
pub mod imap;
#[cfg(feature = "jmap")]
pub mod jmap;
#[cfg(feature = "m2dir")]
pub mod m2dir;
#[cfg(feature = "maildir")]
pub mod maildir;
#[cfg(feature = "smtp")]
pub mod smtp;
