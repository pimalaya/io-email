# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Added basic I/O-free coroutines.

- Added standard, blocking client.

- Added shared `WatchEvent` enum (`EnvelopeAdded`, `EnvelopeRemoved`, `FlagsAdded`, `FlagsRemoved`, `KeepAlive`) in `event::WatchEvent`.

- Added `WatchStream` in `watch::WatchStream`: long-lived `Iterator<Item = Result<WatchEvent, _>>` over a bounded mpsc channel, with `close()`, `try_recv()` and `recv_timeout()` accessors and best-effort cooperative shutdown on drop.

- Added `EmailClientStd::watch_envelopes(self, mailbox)` LCD dispatcher.

- Added the IMAP IDLE driver (`imap::watch`): consumes the IMAP slot, SELECTs the watched mailbox, seeds an envelope/flag shadow, then loops on `io_imap::rfc2177::idle::ImapIdle` and re-fetches the mailbox on every untagged change to produce pre-diffed `EnvelopeAdded` / `EnvelopeRemoved` / `FlagsAdded` / `FlagsRemoved` events. Cooperative shutdown via a 2-second IDLE read-timeout.

- Added the JMAP push driver (`jmap::watch`): consumes the JMAP slot, seeds an envelope/keyword shadow via `Email/query` + `Email/get`, opens an SSE channel to the session's `eventSourceUrl` (via `io-http`), then loops on `StateChange` frames; every `Email/state` move triggers `Email/changes` + `Email/get` (filtered by `mailboxIds`) and the result is diffed against the shadow into `WatchEvent`s. Server-side `ping=10` gives a ten-second shutdown ceiling.

- Added the Maildir notify driver (`maildir::watch`): consumes the Maildir slot, seeds an envelope/flag shadow, then spawns a `notify::RecommendedWatcher` over `cur/` and `new/`. Filesystem events trigger a debounced full re-scan; the diff against the shadow streams as `WatchEvent`s.

- Added `From<std::io::Error>` and `From<io_http::client::HttpClientStdError>` for `EmailClientStdError`.

- Added optional `io-http` and `notify` dependencies, enabled by the `jmap` and `maildir` features respectively for the push / fsnotify transports.

[unreleased]: https://github.com/pimalaya/io-email/compare/root..HEAD
