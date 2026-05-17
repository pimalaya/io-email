# I/O Email [![Documentation](https://img.shields.io/docsrs/io-email?style=flat&logo=docs.rs&logoColor=white)](https://docs.rs/io-email/latest/io_email) [![Matrix](https://img.shields.io/badge/chat-%23pimalaya-blue?style=flat&logo=matrix&logoColor=white)](https://matrix.to/#/#pimalaya:matrix.org) [![Mastodon](https://img.shields.io/badge/news-%40pimalaya-blue?style=flat&logo=mastodon&logoColor=white)](https://fosstodon.org/@pimalaya)

Email client library, written in Rust

## Table of contents

- [Features](#features)
- [Backend coverage](#backend-coverage)
- [Examples](#examples)
  - [As a no-std coroutine library](#as-a-no-std-coroutine-library)
  - [As a std client](#as-a-std-client)
- [More examples](#more-examples)
- [License](#license)
- [Social](#social)
- [Sponsoring](#sponsoring)

## Features

- **Shared types**: a strict least-common-denominator data model (`Mailbox`, `Envelope`, `Address`, `Flag`) that fits IMAP, JMAP, Maildir and SMTP at once. Backend-specific fields (IMAP `SPECIAL-USE` attributes, JMAP roles and rights, Maildir paths, …) intentionally stay on the per-protocol crates.
- **I/O-free** coroutines per backend: each shared operation is exposed as a per-backend coroutine (e.g. `io_email::imap::mailbox_list::MailboxList`) that wraps the underlying `io-imap` / `io-jmap` / `io-maildir` state machine and produces shared types on completion. No sockets, no async runtime, no `std` required by the protocol layer.
- **Standard, blocking unified client** (requires `client` feature): `EmailClientStd` is a sum type over the per-backend std clients (`ImapClientStd`, `JmapClientStd`, `MaildirClient`, `SmtpClientStd`). After construction, callers see one type and one method set: `list_mailboxes`, `list_envelopes`, `add_flags` / `set_flags` / `delete_flags`, `get_message`, `add_message`, `copy_messages`, `move_messages`, `send_message`. Operations a backend cannot perform return `UnsupportedOperation` instead of being hidden behind a trait bound.
- **Backend selection** via cargo features: `imap`, `jmap`, `maildir`, `smtp` (all on by default). Construction stays asymmetric on purpose: each backend has its own connect story (URLs, TLS, sessions, paths), so an `EmailClientStd` is built from a fully-initialised per-backend client via `From` impls.

*The `io-email` library is written in [Rust](https://www.rust-lang.org/), and relies on [cargo features](https://doc.rust-lang.org/cargo/reference/features.html) to enable or disable functionalities. Default features can be found in the `features` section of the [`Cargo.toml`](https://github.com/pimalaya/io-email/blob/master/Cargo.toml), or on [docs.rs](https://docs.rs/crate/io-email/latest/features).*

## Backend coverage

The shared operations exposed on `EmailClientStd`, and which backends implement them. Anything not listed remains accessible on the inner per-backend client by pattern-matching the enum.

| Operation         | IMAP | JMAP | Maildir | SMTP |
|-------------------|:----:|:----:|:-------:|:----:|
| `list_mailboxes`  |  yes |  yes |   yes   |  no  |
| `list_envelopes`  |  yes |  yes |   yes   |  no  |
| `get_message`     |  yes |  yes |   yes   |  no  |
| `add_message`     |  yes |  yes |   yes   |  no  |
| `add_flags`       |  yes |  yes |   yes   |  no  |
| `set_flags`       |  yes |  yes |   yes   |  no  |
| `delete_flags`    |  yes |  yes |   yes   |  no  |
| `copy_messages`   |  yes |  yes |   yes   |  no  |
| `move_messages`   |  yes |  yes |   yes   |  no  |
| `send_message`    |  no  |  yes |    no   |  yes |

For protocol-level capabilities (RFC and SASL coverage, wire format, …), see the per-backend crates: [io-imap], [io-jmap], [io-maildir], [io-smtp].

[io-imap]: https://github.com/pimalaya/io-imap
[io-jmap]: https://github.com/pimalaya/io-jmap
[io-maildir]: https://github.com/pimalaya/io-maildir
[io-smtp]: https://github.com/pimalaya/io-smtp

## Examples

`io-email` can be consumed two ways, depending on how much of the I/O stack you want to own.

### As a no-std coroutine library

No `client` feature required: works in `#![no_std]`, no sockets, no async runtime. Each shared operation has a per-backend coroutine that wraps the underlying `io-imap` / `io-jmap` / `io-maildir` state machine and produces a shared type (e.g. `Vec<Mailbox>`) on completion. You own the loop and the bytes; the library only produces command bytes and consumes server responses.

List every IMAP mailbox against a blocking TCP socket, producing the shared `Mailbox` type:

```rust,ignore
use std::{io::{Read, Write}, net::TcpStream};

use io_email::imap::mailbox_list::*;
use io_imap::context::ImapContext;

let mut stream = TcpStream::connect("imap.example.com:143").unwrap();
let mut buf = [0u8; 16 * 1024];

// Assumes the caller has already driven the greeting + login coroutines
// on this stream (see io-imap). `ImapContext` is then the authenticated
// session state.
let context = ImapContext::new();

let mut coroutine = MailboxList::new(context);
let mut arg: Option<&[u8]> = None;

let mailboxes = loop {
    match coroutine.resume(arg.take()) {
        MailboxListResult::Ok(mailboxes) => break mailboxes,
        MailboxListResult::WantsRead => {
            let n = stream.read(&mut buf).unwrap();
            arg = Some(&buf[..n]);
        }
        MailboxListResult::WantsWrite(bytes) => {
            stream.write_all(&bytes).unwrap();
            arg = None;
        }
        MailboxListResult::Err(err) => panic!("{err}"),
    }
};

for mbox in mailboxes {
    println!("{}", mbox.name);
}
```

The same pattern applies to every `io_email::{imap,jmap,maildir}::*` module: `MailboxList`, `EnvelopeList`, `MessageGet`, `MessageAdd`, `MessageCopy`, `MessageMove`, `FlagAdd`, `FlagSet`, `FlagDelete`. JMAP coroutines additionally surface a `WantsRedirect { url, .. }` shape, Maildir coroutines use filesystem `Wants*` variants (`WantsDirRead`, `WantsFileCreate`, `WantsRename`, …); see the [io-jmap](https://github.com/pimalaya/io-jmap) and [io-maildir](https://github.com/pimalaya/io-maildir) READMEs for the result-enum shapes.

### As a std client

Enable the `client` feature (on by default). `EmailClientStd` is a sum type over the per-backend std clients; construction goes through each backend's own `connect` / `new` story, then a `From` impl lifts it into the unified client.

```toml,ignore
[dependencies]
io-email = { version = "0.0.1", features = ["client", "imap", "jmap", "smtp", "maildir", "serde"] }
```

Connect to IMAP, list mailboxes through the shared API:

```rust,ignore
use io_email::client::EmailClientStd;
use io_imap::client::ImapClientStd;
use pimalaya_stream::{sasl::SaslLogin, tls::Tls};
use secrecy::SecretString;
use url::Url;

let url = Url::parse("imaps://imap.example.com")?;
let tls = Tls::default();
let sasl = SaslLogin {
    username: "alice@example.com".into(),
    password: SecretString::from("hunter2".to_owned()),
};

let imap = ImapClientStd::connect(&url, &tls, false, Some(sasl))?;
let mut client: EmailClientStd = imap.into();

for mbox in client.list_mailboxes(true)? {
    println!("{}: total={:?} unread={:?}", mbox.name, mbox.total, mbox.unread);
}
```

Swap the backend without changing the call site. Pointing the same code at a local Maildir:

```rust,ignore
use io_email::client::EmailClientStd;
use io_maildir::client::MaildirClient;

let maildir = MaildirClient::new("/home/alice/Maildir");
let mut client: EmailClientStd = maildir.into();

for mbox in client.list_mailboxes(true)? {
    println!("{}: total={:?} unread={:?}", mbox.name, mbox.total, mbox.unread);
}
```

Or at JMAP, after running session discovery on the per-backend client:

```rust,ignore
use io_email::client::EmailClientStd;
use io_jmap::client::JmapClientStd;
use pimalaya_stream::tls::Tls;
use secrecy::SecretString;
use url::Url;

let http_auth = SecretString::from("Bearer your-token-here");
let session_url = Url::parse("https://api.fastmail.com/jmap/session/")?;
let tls = Tls::default();

let mut jmap = JmapClientStd::connect(&session_url, &tls, http_auth)?;
jmap.session_get(&session_url)?;

let mut client: EmailClientStd = jmap.into();

for mbox in client.list_mailboxes(true)? {
    println!("{}: total={:?} unread={:?}", mbox.name, mbox.total, mbox.unread);
}
```

Sending a message requires an SMTP or JMAP backend:

```rust,ignore
use io_email::client::EmailClientStd;
use io_smtp::{
    client::SmtpClientStd,
    rfc5321::types::{domain::Domain, ehlo_domain::EhloDomain},
};
use pimalaya_stream::{sasl::SaslPlain, tls::Tls};
use secrecy::SecretString;
use url::Url;

let url = Url::parse("smtps://smtp.example.com")?;
let tls = Tls::default();
let domain: EhloDomain<'_> = Domain::parse(b"localhost")?.into();
let sasl = SaslPlain {
    authzid: None,
    authcid: "alice@example.com".into(),
    passwd: SecretString::from("hunter2".to_owned()),
};

let smtp = SmtpClientStd::connect(&url, &tls, false, domain, Some(sasl))?;
let mut client: EmailClientStd = smtp.into();

let raw =
    b"From: alice@example.com\r\nTo: bob@example.com\r\nSubject: Test\r\n\r\nHello!".to_vec();
client.send_message(raw, "alice@example.com", &["bob@example.com"])?;
```

Operations the active backend cannot perform return `EmailClientStdError::UnsupportedOperation`; backend-specific operations stay reachable on the inner client by pattern-matching the enum.

## More examples

Have a look at projects built on top of this library:

- [himalaya](https://github.com/pimalaya/himalaya): CLI to manage emails

## License

This project is licensed under either of:

- [MIT license](LICENSE-MIT)
- [Apache License, Version 2.0](LICENSE-APACHE)

at your option.

## Social

- Chat on [Matrix](https://matrix.to/#/#pimalaya:matrix.org)
- News on [Mastodon](https://fosstodon.org/@pimalaya) or [RSS](https://fosstodon.org/@pimalaya.rss)
- Mail at [pimalaya.org@posteo.net](mailto:pimalaya.org@posteo.net)

## Sponsoring

[![nlnet](https://nlnet.nl/logo/banner-160x60.png)](https://nlnet.nl/)

Special thanks to the [NLnet foundation](https://nlnet.nl/) and the [European Commission](https://www.ngi.eu/) that have been financially supporting the project for years:

- 2022 → 2023: [NGI Assure](https://nlnet.nl/project/Himalaya/)
- 2023 → 2024: [NGI Zero Entrust](https://nlnet.nl/project/Pimalaya/)
- 2024 → 2026: [NGI Zero Core](https://nlnet.nl/project/Pimalaya-PIM/)
- *2027 in preparation…*

If you appreciate the project, feel free to donate using one of the following providers:

[![GitHub](https://img.shields.io/badge/-GitHub%20Sponsors-fafbfc?logo=GitHub%20Sponsors)](https://github.com/sponsors/soywod)
[![Ko-fi](https://img.shields.io/badge/-Ko--fi-ff5e5a?logo=Ko-fi&logoColor=ffffff)](https://ko-fi.com/soywod)
[![Buy Me a Coffee](https://img.shields.io/badge/-Buy%20Me%20a%20Coffee-ffdd00?logo=Buy%20Me%20A%20Coffee&logoColor=000000)](https://www.buymeacoffee.com/soywod)
[![Liberapay](https://img.shields.io/badge/-Liberapay-f6c915?logo=Liberapay&logoColor=222222)](https://liberapay.com/soywod)
[![thanks.dev](https://img.shields.io/badge/-thanks.dev-000000?logo=data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iMjQuMDk3IiBoZWlnaHQ9IjE3LjU5NyIgY2xhc3M9InctMzYgbWwtMiBsZzpteC0wIHByaW50Om14LTAgcHJpbnQ6aW52ZXJ0IiB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciPjxwYXRoIGQ9Ik05Ljc4MyAxNy41OTdINy4zOThjLTEuMTY4IDAtMi4wOTItLjI5Ny0yLjc3My0uODktLjY4LS41OTMtMS4wMi0xLjQ2Mi0xLjAyLTIuNjA2di0xLjM0NmMwLTEuMDE4LS4yMjctMS43NS0uNjc4LTIuMTk1LS40NTItLjQ0Ni0xLjIzMi0uNjY5LTIuMzQtLjY2OUgwVjcuNzA1aC41ODdjMS4xMDggMCAxLjg4OC0uMjIyIDIuMzQtLjY2OC40NTEtLjQ0Ni42NzctMS4xNzcuNjc3LTIuMTk1VjMuNDk2YzAtMS4xNDQuMzQtMi4wMTMgMS4wMjEtMi42MDZDNS4zMDUuMjk3IDYuMjMgMCA3LjM5OCAwaDIuMzg1djEuOTg3aC0uOTg1Yy0uMzYxIDAtLjY4OC4wMjctLjk4LjA4MmExLjcxOSAxLjcxOSAwIDAgMC0uNzM2LjMwN2MtLjIwNS4xNTYtLjM1OC4zODQtLjQ2LjY4Mi0uMTAzLjI5OC0uMTU0LjY4Mi0uMTU0IDEuMTUxVjUuMjNjMCAuODY3LS4yNDkgMS41ODYtLjc0NSAyLjE1NS0uNDk3LjU2OS0xLjE1OCAxLjAwNC0xLjk4MyAxLjMwNXYuMjE3Yy44MjUuMyAxLjQ4Ni43MzYgMS45ODMgMS4zMDUuNDk2LjU3Ljc0NSAxLjI4Ny43NDUgMi4xNTR2MS4wMjFjMCAuNDcuMDUxLjg1NC4xNTMgMS4xNTIuMTAzLjI5OC4yNTYuNTI1LjQ2MS42ODIuMTkzLjE1Ny40MzcuMjYuNzMyLjMxMi4yOTUuMDUuNjIzLjA3Ni45ODQuMDc2aC45ODVabTE0LjMxNC03LjcwNmgtLjU4OGMtMS4xMDggMC0xLjg4OC4yMjMtMi4zNC42NjktLjQ1LjQ0Ni0uNjc3IDEuMTc3LS42NzcgMi4xOTVWMTQuMWMwIDEuMTQ0LS4zNCAyLjAxMy0xLjAyIDIuNjA2LS42OC41OTMtMS42MDUuODktMi43NzQuODloLTIuMzg0di0xLjk4OGguOTg0Yy4zNjIgMCAuNjg4LS4wMjcuOTgtLjA4LjI5Mi0uMDU1LjUzOC0uMTU3LjczNy0uMzA4LjIwNC0uMTU3LjM1OC0uMzg0LjQ2LS42ODIuMTAzLS4yOTguMTU0LS42ODIuMTU0LTEuMTUydi0xLjAyYzAtLjg2OC4yNDgtMS41ODYuNzQ1LTIuMTU1LjQ5Ny0uNTcgMS4xNTgtMS4wMDQgMS45ODMtMS4zMDV2LS4yMTdjLS44MjUtLjMwMS0xLjQ4Ni0uNzM2LTEuOTgzLTEuMzA1LS40OTctLjU3LS43NDUtMS4yODgtLjc0NS0yLjE1NXYtMS4wMmMwLS40Ny0uMDUxLS44NTQtLjE1NC0xLjE1Mi0uMTAyLS4yOTgtLjI1Ni0uNTI2LS40Ni0uNjgyYTEuNzE5IDEuNzE5IDAgMCAwLS43MzctLjMwNyA1LjM5NSA1LjM5NSAwIDAgMC0uOTgtLjA4MmgtLjk4NFYwaDIuMzg0YzEuMTY5IDAgMi4wOTMuMjk3IDIuNzc0Ljg5LjY4LjU5MyAxLjAyIDEuNDYyIDEuMDIgMi42MDZ2MS4zNDZjMCAxLjAxOC4yMjYgMS43NS42NzggMi4xOTUuNDUxLjQ0NiAxLjIzMS42NjggMi4zNC42NjhoLjU4N3oiIGZpbGw9IiNmZmYiLz48L3N2Zz4=)](https://thanks.dev/soywod)
[![PayPal](https://img.shields.io/badge/-PayPal-0079c1?logo=PayPal&logoColor=ffffff)](https://www.paypal.com/paypalme/soywod)
