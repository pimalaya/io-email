# I/O Email [![Documentation](https://img.shields.io/docsrs/io-email?style=flat&logo=docs.rs&logoColor=white)](https://docs.rs/io-email/latest/io_email) [![Matrix](https://img.shields.io/badge/chat-%23pimalaya-blue?style=flat&logo=matrix&logoColor=white)](https://matrix.to/#/#pimalaya:matrix.org) [![Mastodon](https://img.shields.io/badge/news-%40pimalaya-blue?style=flat&logo=mastodon&logoColor=white)](https://fosstodon.org/@pimalaya)

Email client library, written in Rust.

## Table of contents

- [Features](#features)
- [Usage](#usage)
  - [As a no-std coroutine library](#as-a-no-std-coroutine-library)
  - [As a std client](#as-a-std-client)
- [Real world usage](#real-world-usage)
- [License](#license)
- [AI disclosure](#ai-disclosure)
- [Social](#social)
- [Sponsoring](#sponsoring)

## Features

- Shared least-common-denominator types (`Mailbox`, `Envelope`, `Address`, `Flag`) that fit IMAP, JMAP, Maildir and SMTP.
- One I/O-free coroutine per backend / operation, wrapping the underlying [io-imap] / [io-jmap] / [io-maildir] / [io-smtp] state machine and producing shared types on completion.
- `EmailClientStd` (feature `client`): blocking unified client over all enabled backends.

| Operation         | IMAP | JMAP | Maildir | SMTP |
|-------------------|:----:|:----:|:-------:|:----:|
| `list_mailboxes`  |  yes |  yes |   yes   |      |
| `list_envelopes`  |  yes |  yes |   yes   |      |
| `get_message`     |  yes |  yes |   yes   |      |
| `add_message`     |  yes |  yes |   yes   |      |
| `add_flags`       |  yes |  yes |   yes   |      |
| `set_flags`       |  yes |  yes |   yes   |      |
| `delete_flags`    |  yes |  yes |   yes   |      |
| `copy_messages`   |  yes |  yes |   yes   |      |
| `move_messages`   |  yes |  yes |   yes   |      |
| `send_message`    |      |  yes |         |  yes |

*The `io-email` library is written in [Rust](https://www.rust-lang.org/), and relies on [cargo features](https://doc.rust-lang.org/cargo/reference/features.html) to enable or disable functionalities. Default features can be found in the `features` section of the [`Cargo.toml`](https://github.com/pimalaya/io-imap/blob/master/Cargo.toml), or on [docs.rs](https://docs.rs/crate/io-imap/latest/features).*

[io-imap]: https://github.com/pimalaya/io-imap
[io-jmap]: https://github.com/pimalaya/io-jmap
[io-maildir]: https://github.com/pimalaya/io-maildir
[io-smtp]: https://github.com/pimalaya/io-smtp

## Usage

### As a no-std coroutine library

```toml,ignore
[dependencies]
io-email = "0.0.1"
```

List IMAP mailboxes over a blocking TCP socket:

```rust,ignore
use std::{io::{Read, Write}, net::TcpStream};

use io_email::mailbox::imap::list::*;
use io_imap::context::ImapContext;

let mut stream = TcpStream::connect("imap.example.com:143").unwrap();
let mut buf = [0u8; 16 * 1024];

let context = ImapContext::new();
let mut coroutine = ImapMailboxList::new(context);
let mut arg: Option<&[u8]> = None;

let mailboxes = loop {
    match coroutine.resume(arg.take()) {
        ImapMailboxListResult::Ok(mailboxes) => break mailboxes,
        ImapMailboxListResult::WantsRead => {
            let n = stream.read(&mut buf).unwrap();
            arg = Some(&buf[..n]);
        }
        ImapMailboxListResult::WantsWrite(bytes) => {
            stream.write_all(&bytes).unwrap();
            arg = None;
        }
        ImapMailboxListResult::Err(err) => panic!("{err}"),
    }
};

for mbox in mailboxes {
    println!("{}", mbox.name);
}
```

JMAP coroutines also surface `WantsRedirect { url, .. }`; Maildir coroutines use filesystem variants (`WantsDirRead`, `WantsFileCreate`, `WantsRename`, ...).

### As a std client

```toml,ignore
[dependencies]
io-email = { version = "0.0.1", features = ["client", "imap", "rustls-ring"] }
```

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
let mut client = EmailClientStd::from(imap);

for mbox in client.list_mailboxes(true)? {
    println!("{}: total={:?} unread={:?}", mbox.name, mbox.total, mbox.unread);
}
```

Same call site against Maildir:

```rust,ignore
use io_email::client::EmailClientStd;
use io_maildir::client::MaildirClient;

let maildir = MaildirClient::new("/home/alice/Maildir");
let mut client = EmailClientStd::from(maildir);

for mbox in client.list_mailboxes(true)? {
    println!("{}: total={:?} unread={:?}", mbox.name, mbox.total, mbox.unread);
}
```

## Real world usage

Have a look at projects built on top of this library:

- [himalaya](https://github.com/pimalaya/himalaya): CLI to manage emails

## License

This project is licensed under either of:

- [MIT license](LICENSE-MIT)
- [Apache License, Version 2.0](LICENSE-APACHE)

at your option.

## AI disclosure

This project is developed with AI assistance. This section documents how, so users and downstream packagers can make informed decisions.

- **Tools**: Claude Code (Anthropic), Opus 4.7, invoked locally with a persistent project-scoped memory and a small set of repo-specific rules.

- **Used for**: Refactors, mechanical multi-file edits, boilerplate (feature gates, error enums, derive macros, trait impls), test scaffolding, doc polish, exploratory design conversations.

- **Not used for**: Engineering, critical code, git manipulation (commit, merge, rebase…), real-world tests.

- **Verification**: Every AI-assisted change is read, compiled, tested, and formatted before commit (`nix develop --command cargo check / cargo test / cargo fmt`). Behavioural correctness is verified against the relevant RFC or upstream spec, not assumed from the model output. Tests are never adjusted to fit AI-generated code; the code is adjusted to fit correct behaviour.

- **Limitations**: AI models occasionally produce code that compiles and passes tests but is subtly wrong: off-by-one errors, missed edge cases, plausible but nonexistent APIs, stale RFC references. The verification workflow catches most of this; it does not catch all of it. Bug reports are welcome and taken seriously.

- **Last reviewed**: 31/05/2026

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
