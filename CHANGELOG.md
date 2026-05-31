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

- Added the IMAP watch translator (`imap::watch`): consumes the IMAP slot, calls `io_imap::client::ImapClientStd::watch_mailbox` (IDLE + QRESYNC composition lives in io-imap), and re-emits each `ImapMailboxWatchEvent` over the shared `WatchStream` channel. Translation is straightforward: FETCH items → shared `Envelope`, IMAP `Flag` → shared `Flag`, uid → string id, plus the mailbox name. No socket I/O, no shadow, no protocol state machine in io-email anymore.

- Added the JMAP push driver (`jmap::watch`): consumes the JMAP slot, seeds an envelope/keyword shadow via `Email/query` + `Email/get`, opens an SSE channel to the session's `eventSourceUrl` (via `io-http`), then loops on `StateChange` frames; every `Email/state` move triggers `Email/changes` + `Email/get` (filtered by `mailboxIds`) and the result is diffed against the shadow into `WatchEvent`s. Server-side `ping=10` gives a ten-second shutdown ceiling.

- Added the Maildir notify driver (`maildir::watch`): consumes the Maildir slot, seeds an envelope/flag shadow, then spawns a `notify::RecommendedWatcher` over `cur/` and `new/`. Filesystem events trigger a debounced full re-scan; the diff against the shadow streams as `WatchEvent`s.

- Added `From<std::io::Error>` and `From<io_http::client::HttpClientStdError>` for `EmailClientStdError`.

- Added optional `io-http` and `notify` dependencies, enabled by the `jmap` and `maildir` features respectively for the push / fsnotify transports.

- Added `ping()` on `ImapClientStd` and `SmtpClientStd` (delegates to the inner `noop()`), plus a shared `EmailClientStd::ping()` that pings every registered network backend to reset server-side inactivity timers on long-idle sessions. Storage backends and JMAP are skipped.

### Changed

- IMAP `list_mailboxes` now filters out rows carrying the `\Noselect` attribute (RFC 3501 §6.3.8) so the shared API only returns mailboxes that can actually be SELECTed.

- Added an `auto_id: Option<Vec<(IString<'static>, NString<'static>)>>` argument to `ImapClientStd::connect` and `EmailClientStd::connect_imap`; the field is forwarded to the inner io-imap connect, where the auth coroutine chains an RFC 2971 `ID` round-trip after authentication. None skips the exchange (default), Some(empty) sends `ID NIL`, Some(non-empty) sends `ID (key val …)`.

[unreleased]: https://github.com/pimalaya/io-email/compare/root..HEAD
