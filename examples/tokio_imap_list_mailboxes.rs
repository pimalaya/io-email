//! Tokio example: drive [`io_email::mailbox::imap::list::ImapMailboxList`]
//! across async I/O.
//!
//! The point of this example isn't to be a useful client (no TLS, no
//! greeting / login, no QRESYNC) but to exercise the
//! [`ImapCoroutine`] / [`ImapCoroutineState`] / [`ImapYield`] triad
//! against tokio's `AsyncRead` / `AsyncWrite` and see whether anything
//! introduces friction across `.await` points.
//!
//! Run against a plain IMAP server (no TLS) with:
//!
//! ```sh
//! HOST=imap.example.com PORT=143 \
//!     cargo run --example tokio_imap_list_mailboxes
//! ```
//!
//! The example skips authentication; against a real server the LIST
//! will fail before the mailbox list is returned. The interesting part
//! is that everything *compiles* and that the driver loop's borrow
//! pattern is identical to the sync one in [`io_email::client`].

use std::{env, error::Error};

use io_email::mailbox::imap::list::{ImapMailboxList, ImapMailboxListError};
use io_imap::{
    codec::fragmentizer::Fragmentizer,
    coroutine::{ImapCoroutine, ImapCoroutineState, ImapYield},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

const READ_BUFFER_SIZE: usize = 16 * 1024;
const FRAGMENTIZER_MAX_MESSAGE_SIZE: u32 = 100 * 1024 * 1024;

#[derive(Debug, thiserror::Error)]
enum TokioRunError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    ImapMailboxList(#[from] ImapMailboxListError),
}

/// Tokio twin of [`io_email::client::ImapContext`]: bundles a
/// blocking-ish stream (here a tokio [`TcpStream`]) with its
/// fragmentizer so the driver can advance any IMAP coroutine.
struct ImapTokioContext {
    stream: TcpStream,
    fragmentizer: Fragmentizer,
}

impl ImapTokioContext {
    fn new(stream: TcpStream) -> Self {
        Self {
            stream,
            fragmentizer: Fragmentizer::new(FRAGMENTIZER_MAX_MESSAGE_SIZE),
        }
    }
}

/// Async twin of `EmailClientStd::run_imap`. Identical control flow;
/// only `read` / `write_all` gained an `.await`.
async fn run_imap<C, O, E>(ctx: &mut ImapTokioContext, mut coroutine: C) -> Result<O, TokioRunError>
where
    C: ImapCoroutine<Yield = ImapYield, Return = Result<O, E>>,
    TokioRunError: From<E>,
{
    let mut buf = [0u8; READ_BUFFER_SIZE];
    let mut bytes: Option<&[u8]> = None;

    loop {
        match coroutine.resume(&mut ctx.fragmentizer, bytes) {
            ImapCoroutineState::Yielded(ImapYield::WantsRead) => {
                let n = ctx.stream.read(&mut buf).await?;
                bytes = Some(&buf[..n]);
            }
            ImapCoroutineState::Yielded(ImapYield::WantsWrite(out)) => {
                ctx.stream.write_all(&out).await?;
                bytes = None;
            }
            ImapCoroutineState::Complete(Ok(out)) => return Ok(out),
            ImapCoroutineState::Complete(Err(err)) => return Err(err.into()),
        }
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    env_logger::init();

    let host = env::var("HOST").unwrap_or_else(|_| "127.0.0.1".into());
    let port: u16 = env::var("PORT")
        .unwrap_or_else(|_| "143".into())
        .parse()
        .expect("PORT must be a u16");

    let stream = TcpStream::connect((host.as_str(), port)).await?;
    let mut ctx = ImapTokioContext::new(stream);

    // Skipping greeting / login here on purpose; the goal is to show
    // the driver compiling, not to be a real client.
    let mailboxes = run_imap(&mut ctx, ImapMailboxList::new(false)).await?;
    for mbox in mailboxes {
        println!("{}", mbox.name);
    }
    Ok(())
}

/// Compile-time check that the driver future is `Send` (i.e. can be
/// `tokio::spawn`-ed).
#[allow(
    dead_code,
    unreachable_code,
    unused_variables,
    clippy::diverging_sub_expression
)]
fn assert_run_imap_future_is_send() {
    fn assert_send<T: Send>(_: T) {}
    assert_send(async {
        let mut ctx: ImapTokioContext = unreachable!();
        let _ = run_imap(&mut ctx, ImapMailboxList::new(false)).await;
    });
}
