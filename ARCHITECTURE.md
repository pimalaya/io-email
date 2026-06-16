# io-email architecture

Read the [Pimalaya ARCHITECTURE](https://github.com/pimalaya/.github/blob/master/ARCHITECTURE.md) first: it describes the conventions every Pimalaya repository shares (the sans-I/O coroutine approach, `no_std`, module and error rules, code style, licensing). This document only covers what is specific to io-email, and assumes you know that shared context.

If a statement here conflicts with the code, the code wins; please flag it.

## Where io-email fits

io-email is a **domain library**: the backend-agnostic email API. It is the email sibling of [io-addressbook](https://github.com/pimalaya/io-addressbook) and [io-calendar](https://github.com/pimalaya/io-calendar), and the layer [himalaya](https://github.com/pimalaya/himalaya) builds its shared commands on. It owns no wire protocol or on-disk format of its own; instead it adapts the protocol/storage libraries below it to one common shape:

- [io-imap](https://github.com/pimalaya/io-imap) (IMAP), [io-jmap](https://github.com/pimalaya/io-jmap) (JMAP), [io-gmail](https://github.com/pimalaya/io-gmail) (Gmail REST), [io-smtp](https://github.com/pimalaya/io-smtp) (SMTP submission);
- [io-maildir](https://github.com/pimalaya/io-maildir) (Maildir) and [io-m2dir](https://github.com/pimalaya/io-m2dir) (m2dir) for local storage;
- each backend is behind its own cargo feature, so a consumer compiles in only the backends it uses.

The crate has two layers: the I/O-free coroutines (`no_std` core) and the `client` feature (the blocking dispatcher). There is no CLI.

## Two ideas: a shared shape, and per-backend adapters

### The shared shape (least common denominator)

`address.rs` and the `*/types.rs` modules define the backend-agnostic vocabulary every command speaks: `Envelope`, `Mailbox`, `Flag` (+ `IanaFlag`, `FlagOp`), `Address`, plus the incremental-sync deltas `EnvelopeDiff` / `MailboxDiff` / `FlagUpdate`. These are a strict least common denominator: a field appears only if every targeted backend can supply it. Backend-only concepts (IMAP attributes, JMAP roles/rights, Gmail label colors) deliberately do not appear here; reach for the protocol library directly when you need them.

`Flag` is the subtle one: it keeps both the raw wire spelling and an optional `IanaFlag` classification, so `\Seen`, `$seen` and `seen` collapse to one logical flag across backends while custom keywords pass through untouched.

### Per-backend adapters

For each shared operation, every backend provides its own coroutine that composes the protocol library's coroutine(s) and converts the result to the shared shape. These adapters are the bulk of the crate. They follow the standard coroutine template (one `new`, a `State` enum, `fmt::Display`, the crate `try!` macro), and each backend's coroutine `Yield` is that backend library's yield, so io-email's client can drive it directly.

The shape of an adapter depends on the backend's wire model:

- **JMAP** batches: `JmapEnvelopeList` is one `Email/query` + `Email/get` round-trip.
- **Gmail** has no batch: `gmail::EnvelopeList` is a `messages.list` followed by one `messages.get` per id, walking page tokens; flag/copy/move loop one `messages.modify` per id. Gmail's label model is mapped here too: labels are mailboxes, and the flag-like system labels back the shared flags (notably `\Seen` is the *absence* of `UNREAD`, an inverted polarity). See `src/gmail/convert.rs`.
- **Maildir/m2dir** are local filesystem coroutines.

## The dispatcher

`EmailClientStd` (`client` feature, `src/client.rs`) is a thin bag of optional per-backend client slots (`imap`, `jmap`, `gmail`, `smtp`, `maildir`, `m2dir`), each registered via `with_<backend>` or the TLS-gated `connect_<backend>`. Its shared methods (`list_mailboxes`, `list_envelopes`, `store_flags`, `get_message`, `add_message`, `create_mailbox`, `delete_mailbox`, `delete_message`, `copy_messages`, `move_messages`, `send_message`, plus `diff_*` / `watch_mailbox`) dispatch to the first registered backend in a fixed priority order:

- storage reads/mutations: **Maildir -> M2dir -> JMAP -> Gmail -> IMAP** (local before network, cheap before expensive);
- sending: **JMAP -> Gmail -> SMTP** (JMAP and Gmail send via their own API; IMAP/Maildir accounts fall back to a co-registered SMTP slot).

When no registered backend implements an operation, the call returns `NoBackendRegistered` / `UnsupportedOperation`. Not every backend implements every op: the Gmail backend, for instance, has no `add_message` (no insert primitive in io-gmail), `search`, `diff` or `watch`, so it simply has no arm in those dispatchers.

## Module layout

Domains across the top, backends underneath each, plus one top-level module per backend for its client and conversion helpers.

```
src/
  lib.rs              crate root: no_std, module + feature gates
  client.rs           (client) EmailClientStd dispatcher + EmailClientStdError
  address.rs          shared Address
  envelope/           types.rs, event.rs (WatchEvent), then imap/ jmap/ gmail/ m2dir/ maildir/
  flag/               types.rs (Flag, IanaFlag, FlagOp), then imap/ jmap/ gmail/ m2dir/ maildir/
  mailbox/            types.rs (Mailbox, MailboxRole, MailboxDiff), then per-backend
  message/            per-backend (imap/ jmap/ gmail/ m2dir/ maildir/ smtp/)
  imap/  jmap/  gmail/  maildir/  m2dir/  smtp/    each: client.rs (+ convert.rs)
  search/             shared search query DSL (filter + sort grammar, parser)
```

Inside a domain, `types.rs` is the shared shape and each `<backend>/` subdir holds that backend's adapters (`list.rs`, `store.rs`, `get.rs`, ...). The top-level `<backend>/client.rs` is the blocking per-backend client `EmailClientStd` registers; `<backend>/convert.rs` holds the shared conversion helpers (flag/keyword maps, address parsing, pagination math).

## The Gmail backend, specifically

Added on top of io-gmail, gated by the `gmail` feature. It implements the operations Gmail supports through io-gmail's current surface: `list_mailboxes` / `create_mailbox` / `delete_mailbox` (labels), `list_envelopes`, `store_flags`, `get_message`, `delete_message`, `copy_messages`, `move_messages`, `send_message`. `src/gmail/client.rs` wraps io-gmail's `GmailClientStd`; `src/gmail/convert.rs` owns the label<->mailbox and system-label<->flag mapping. Gmail-native operations the shared API cannot express (threads, drafts, history, label visibility, raw attachment access) are intentionally absent here: consume io-gmail directly for those (himalaya does, via its protocol-specific `gmail` command).
