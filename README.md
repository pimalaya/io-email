# I/O Email [![Documentation](https://img.shields.io/docsrs/io-email?style=flat&logo=docs.rs&logoColor=white)](https://docs.rs/io-email/latest/io_email) [![Matrix](https://img.shields.io/badge/chat-%23pimalaya-blue?style=flat&logo=matrix&logoColor=white)](https://matrix.to/#/#pimalaya:matrix.org) [![Mastodon](https://img.shields.io/badge/news-%40pimalaya-blue?style=flat&logo=mastodon&logoColor=white)](https://fosstodon.org/@pimalaya)

Email client library, written in Rust.

This library is composed of 2 feature-gated layers:

- Low-level **I/O-free** coroutines: these `no_std`-compatible state machines wrap the underlying [io-imap], [io-jmap], [io-maildir], [io-m2dir] and [io-smtp] coroutines and surface a shared least-common-denominator type on completion
- Mid-level **std client**: a standard, blocking unified client that dispatches the shared API across every registered backend

## Table of contents

- [Features](#features)
- [Backend coverage](#backend-coverage)
- [Usage](#usage)
  - [Coroutines](#coroutines)
  - [Std client](#std-client)
- [Examples](#examples)
- [AI disclosure](#ai-disclosure)
- [License](#license)
- [Social](#social)
- [Sponsoring](#sponsoring)

## Features

- **Shared LCD types**: `Mailbox`, `Envelope`, `Address`, `Flag` that fit IMAP, JMAP, Maildir, m2dir and SMTP.
- **I/O-free** coroutines: `no_std` state machines per (backend, operation), wrapping the underlying io-* coroutine and producing a shared type on completion.
- **Unified std client** (`client` feature): blocking dispatcher that routes shared-API calls to the highest-priority registered backend (Maildir → m2dir → JMAP → IMAP for storage, JMAP → SMTP for send).
- **TLS** for the network backends (gated by the same `rustls-ring` / `rustls-aws` / `native-tls` features as the underlying io-* crates).
- Optional **search DSL** (`search` feature) and **serde** round-trip on every shared type (`serde` feature).

> [!TIP]
> I/O Email is written in [Rust](https://www.rust-lang.org/) and uses [cargo features](https://doc.rust-lang.org/cargo/reference/features.html) to gate backend support. The default feature set is declared in [Cargo.toml](./Cargo.toml) or on [docs.rs](https://docs.rs/crate/io-email/latest/features).

[io-imap]: https://github.com/pimalaya/io-imap
[io-jmap]: https://github.com/pimalaya/io-jmap
[io-maildir]: https://github.com/pimalaya/io-maildir
[io-m2dir]: https://github.com/pimalaya/io-m2dir
[io-smtp]: https://github.com/pimalaya/io-smtp

## Backend coverage

| Operation                              | IMAP | JMAP | Maildir | m2dir | SMTP |
|----------------------------------------|:----:|:----:|:-------:|:-----:|:----:|
| `list_mailboxes`                       |  yes |  yes |   yes   |  yes  |      |
| `create_mailbox`                       |  yes |  yes |   yes   |  yes  |      |
| `delete_mailbox`                       |  yes |  yes |   yes   |  yes  |      |
| `diff_mailboxes`                       |      |  yes |         |       |      |
| `list_envelopes`                       |  yes |  yes |   yes   |  yes  |      |
| `search_envelopes` (feature `search`)  |  yes |  yes |   yes   |  yes  |      |
| `diff_envelopes`                       |  yes |  yes |         |       |      |
| `watch_mailbox`                        |  yes |  yes |         |       |      |
| `get_message`                          |  yes |  yes |   yes   |  yes  |      |
| `add_message`                          |  yes |  yes |   yes   |  yes  |      |
| `copy_messages`                        |  yes |  yes |   yes   |  yes  |      |
| `move_messages`                        |  yes |  yes |   yes   |  yes  |      |
| `delete_message`                       |  yes |  yes |   yes   |  yes  |      |
| `store_flags`                          |  yes |  yes |   yes   |  yes  |      |
| `send_message`                         |      |  yes |         |       |  yes |

## Usage

I/O Email can be consumed two ways, depending on how much of the I/O stack you want to own. Each mode is gated by cargo features.

Whichever mode you pick, every shared-API coroutine implements the backend trait of the protocol it targets (`ImapCoroutine`, `JmapCoroutine`, `MaildirCoroutine`, `M2dirCoroutine`, `SmtpCoroutine`). The `resume(...)` method returns the matching `<Backend>CoroutineState<Yield, Return>` with two variants:

- `Yielded(Y)`: intermediate. `Y` is the backend's standard `<Backend>Yield` (`WantsRead` / `WantsWrite` for the network backends, `WantsDirRead` / `WantsFileCreate` / `WantsRename` etc. for the filesystem ones), plus a dedicated `Event(WatchEvent)` variant on watch coroutines.
- `Complete(R)`: terminal. By convention `R = Result<Output, Error>` carrying the operation's final value typed against the shared `Mailbox` / `Envelope` / shared payload.

The std client owns the resume loop for you; the I/O-free mode hands it back so you can drive the same coroutine under any blocking, async, or fuzz harness.

### Coroutines

No `client` feature required: every wrapper lives under `<domain>::<protocol>::<op>` (for example `mailbox::imap::list::ImapMailboxList`, `message::jmap::add::JmapMessageAdd`) and is built straight from the shared inputs. You own the loop and the syscalls; the library only produces operations and consumes their results.

Create a fresh Maildir mailbox against a blocking caller (the same shape works under async or in-memory replay):

```rust,no_run
use std::fs;

use io_email::mailbox::maildir::create::MaildirMailboxCreate;
use io_maildir::{
    coroutine::*,
    path::{FsPath, MaildirPath},
    store::MaildirStore,
};

let store = MaildirStore { root: FsPath::new("/path/to/root"), maildirpp: false };

let mut coroutine = MaildirMailboxCreate::new(&store, "Archive").unwrap();
let mut arg: Option<MaildirReply> = None;

loop {
    match coroutine.resume(arg.take()) {
        MaildirCoroutineState::Complete(Ok(())) => break,
        MaildirCoroutineState::Complete(Err(err)) => panic!("{err}"),
        MaildirCoroutineState::Yielded(MaildirYield::WantsDirCreate(paths)) => {
            for path in paths {
                fs::create_dir_all(path.as_str()).unwrap();
            }
            arg = Some(MaildirReply::DirCreate);
        }
        MaildirCoroutineState::Yielded(other) => unreachable!("unexpected {other:?}"),
    }
}

println!("created Maildir mailbox Archive");
```

Network backends follow the same pattern but yield `WantsRead` / `WantsWrite(Vec<u8>)` instead; see [io-imap], [io-jmap] and [io-smtp] for the full TCP / TLS / authentication setup that authenticates the stream before the wrapper coroutine runs.

### Std client

Enable the `client` feature (pulled in by every backend feature) and at least one backend. `EmailClientStd::new()` starts empty; `with_<protocol>(client)` plugs in an already-built per-protocol client, while `connect_<protocol>(url, tls, ...)` opens the connection through the underlying io-* crate and fills the slot in one shot.

```toml,ignore
[dependencies]
io-email = "0.1.0"
```

```rust,no_run
use io_email::{client::EmailClientStd, maildir::client::MaildirClient};
use pimalaya_stream::{sasl::SaslLogin, tls::Tls};
use secrecy::SecretString;
use url::Url;

let url = Url::parse("imaps://imap.example.com").unwrap();
let tls = Tls::default();
let sasl = SaslLogin {
    username: "alice@example.com".into(),
    password: SecretString::from("hunter2".to_owned()),
};

let mut client = EmailClientStd::new()
    .with_maildir(MaildirClient::new("/home/alice/Maildir"))
    .connect_imap(&url, &tls, false, Some(sasl), None)
    .unwrap();

for mbox in client.list_mailboxes(/* with_counts */ true).unwrap() {
    println!("{}: total={:?} unread={:?}", mbox.name, mbox.total, mbox.unread);
}
```

Dispatch priority on storage reads walks the registered backends `Maildir → m2dir → JMAP → IMAP` (local before network, cheap before expensive); send routes `JMAP → SMTP`. Pick which slots to fill based on the workload (local-first sync vs network-first transactional client).

## Examples

See complete examples at [./examples](https://github.com/pimalaya/io-email/blob/master/examples).

Have also a look at real-world projects built on top of this library:

- [Himalaya CLI](https://github.com/pimalaya/himalaya): CLI to manage emails
- [Himalaya TUI](https://github.com/pimalaya/himalaya-tui): TUI to manage emails
- [Neverest](https://github.com/pimalaya/neverest): CLI to synchronize emails

## AI disclosure

This project is developed with AI assistance. This section documents how, so users and downstream packagers can make informed decisions.

- **Tools**: Claude Code (Anthropic), Opus 4.7, invoked locally with a persistent project-scoped memory and a small set of repo-specific rules.

- **Used for**: Refactors, mechanical multi-file edits, boilerplate (feature gates, error enums, derive macros, trait impls), test scaffolding, doc polish, exploratory design conversations.

- **Not used for**: Engineering, critical code, git manipulation (commit, merge, rebase…), real-world tests.

- **Verification**: Every AI-assisted change is read, compiled, tested, and formatted before commit (`nix develop --command cargo check / cargo test / cargo
fmt`). Behavioural correctness is verified against the relevant RFC or upstream spec, not assumed from the model output. Tests are never adjusted to fit
AI-generated code; the code is adjusted to fit correct behaviour.

- **Limitations**: AI models occasionally produce code that compiles and passes tests but is subtly wrong: off-by-one errors, missed edge cases, plausible
but nonexistent APIs, stale RFC references. The verification workflow catches most of this; it does not catch all of it. Bug reports are welcome and taken
seriously.

- **Last reviewed**: 06/06/2026

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
