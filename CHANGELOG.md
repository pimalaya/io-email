# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-06-06

### Added

- Added basic I/O-free coroutines.

- Added standard, blocking client.

- Added shared `WatchEvent` enum (`EnvelopeAdded`, `EnvelopeRemoved`, `FlagsAdded`, `FlagsRemoved`, `KeepAlive`) in `envelope::event::WatchEvent`.

- Added `EmailClientStd::watch_mailbox(self, mailbox, shutdown, tx)` LCD dispatcher that wires the per-protocol watcher onto a `Sender<WatchEvent>`.

- Added the IMAP watch translator (`envelope::imap::watch::ImapWatchMailbox`): consumes the IMAP slot, calls io-imap's IDLE + QRESYNC watcher, and re-emits each `ImapMailboxWatchEvent` as a shared `WatchEvent` over the supplied channel.

- Added the JMAP push driver (`envelope::jmap::watch::JmapWatchMailbox`): consumes the JMAP slot, seeds an envelope/keyword shadow via `Email/query` + `Email/get`, opens an SSE channel to the session's `eventSourceUrl` (via `io-http`), then loops on `StateChange` frames; every `Email/state` move triggers `Email/changes` + `Email/get` (filtered by `mailboxIds`) and the result is diffed against the shadow into `WatchEvent`s. Server-side `ping=10` gives a ten-second shutdown ceiling.

- Added optional `io-http` dependency, enabled by the `jmap` feature for the push transport.

- Added `ping()` on `ImapClientStd` and `SmtpClientStd` (delegates to the inner `noop()`), plus a shared `EmailClientStd::ping()` that pings every registered network backend to reset server-side inactivity timers on long-idle sessions. Storage backends and JMAP are skipped.

### Changed

- IMAP `list_mailboxes` now filters out rows carrying the `\Noselect` attribute (RFC 3501 §6.3.8) so the shared API only returns mailboxes that can actually be SELECTed.

- Added an `auto_id: Option<Vec<(IString<'static>, NString<'static>)>>` argument to `ImapClientStd::connect` and `EmailClientStd::connect_imap`; the field is forwarded to the inner io-imap connect, where the auth coroutine chains an RFC 2971 `ID` round-trip after authentication. None skips the exchange (default), Some(empty) sends `ID NIL`, Some(non-empty) sends `ID (key val …)`.

- Reorganised the module layout from protocol-first to domain-first: per-protocol coroutines live under `mailbox/<protocol>/`, `message/<protocol>/`, `envelope/<protocol>/` and `flag/<protocol>/`; each domain root carries the shared `types.rs`. Protocol-specific cross-cutting (client + convert) stays at `<protocol>/`.

[unreleased]: https://github.com/pimalaya/io-email/compare/v0.1.0..HEAD
[0.1.0]: https://github.com/pimalaya/io-email/compare/root..v0.1.0
