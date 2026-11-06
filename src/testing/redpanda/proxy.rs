//! A TCP proxy between the client and the broker that sees Kafka frames.
//!
//! Each frame is a 4-byte big-endian length, then the body. A request body
//! starts with the API key (i16) and API version (i16), then the correlation
//! id (i32). A reply body starts with the correlation id.

use std::{
    cell::{Cell, RefCell},
    collections::{HashMap, HashSet},
    convert::Infallible,
    future::{Future, poll_fn},
    io,
    net::SocketAddr,
    pin::pin,
    task::Poll,
    time::Duration,
};

use compio::{
    BufResult,
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::sleep,
};
use futures_util::{
    StreamExt,
    future::{self, Either},
    stream::FuturesUnordered,
};

/// Serves clients only inside `serve`.
pub struct Proxy {
    listener: TcpListener,
    addr: SocketAddr,
    upstream: SocketAddr,
    /// Request frames seen, by API key.
    counts: RefCell<HashMap<i16, usize>>,
    arm: Cell<Option<Arm>>,
}

impl Proxy {
    /// Listens on a free port of 127.0.0.1 and connects each client to `upstream`.
    pub async fn start(upstream: SocketAddr) -> Proxy {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind proxy");
        let addr = listener.local_addr().unwrap();
        Proxy {
            listener,
            addr,
            upstream,
            counts: RefCell::default(),
            arm: Cell::default(),
        }
    }

    /// Runs `work` while the proxy accepts and forwards. When `work` ends,
    /// every connection of the proxy closes.
    pub async fn serve<T>(&self, work: impl Future<Output = T>) -> T {
        match future::select(pin!(work), pin!(self.accept_loop())).await {
            Either::Left((output, _)) => output,
            Either::Right((never, _)) => match never {},
        }
    }

    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Request frames of `api_key` seen so far.
    pub fn count(&self, api_key: i16) -> usize {
        self.counts.borrow().get(&api_key).copied().unwrap_or(0)
    }

    /// Close both sides after forwarding request number `nth` of `api_key`,
    /// before its reply.
    pub fn drop_after(&self, api_key: i16, nth: usize) {
        self.drop_after_range(api_key, nth, nth, Duration::ZERO);
    }

    /// Forward requests number `first..=last` of `api_key`, then close both
    /// sides. None of their replies reach the client, even when the broker
    /// answers an early one before the client sends the last. The close
    /// comes `linger` after the last forward. A broker drops the requests of
    /// a closed connection it has not handled yet.
    pub fn drop_after_range(&self, api_key: i16, first: usize, last: usize, linger: Duration) {
        assert!(1 <= first && first <= last, "proxy arm {first}..={last}");
        self.arm.set(Some(Arm {
            api_key,
            hold_from: first,
            close_after: last,
            linger,
        }));
    }

    async fn accept_loop(&self) -> Infallible {
        // `incoming` keeps its accept in flight between polls, so a client
        // is never lost to a dropped accept.
        let mut incoming = self.listener.incoming();
        let mut connections = FuturesUnordered::new();
        loop {
            let accepted = poll_fn(|context| {
                while let Poll::Ready(Some(())) = connections.poll_next_unpin(context) {}
                incoming.poll_next_unpin(context)
            })
            .await;
            if let Some(Ok(client)) = accepted {
                connections.push(self.connection(client));
            }
        }
    }

    /// Forwards both ways until one side ends, then closes both.
    async fn connection(&self, client: TcpStream) {
        let Ok(broker) = TcpStream::connect(self.upstream).await else {
            return;
        };
        let _ = client.set_nodelay(true);
        let _ = broker.set_nodelay(true);
        // Correlation ids of requests whose replies must not reach the client.
        let held = RefCell::new(HashSet::new());
        let requests = pin!(self.requests(&client, &broker, &held));
        let replies = pin!(replies(&broker, &client, &held));
        let _ = future::select(requests, replies).await;
    }

    /// Client to broker.
    async fn requests(
        &self,
        mut client: &TcpStream,
        mut broker: &TcpStream,
        held: &RefCell<HashSet<i32>>,
    ) -> io::Result<()> {
        loop {
            let frame = read_frame(&mut client).await?;
            if frame.len() < 12 {
                return Err(io::Error::other("short request frame"));
            }
            let api_key = i16::from_be_bytes([frame[4], frame[5]]);
            let correlation = i32::from_be_bytes([frame[8], frame[9], frame[10], frame[11]]);
            let number = {
                let mut counts = self.counts.borrow_mut();
                let count = counts.entry(api_key).or_default();
                *count += 1;
                *count
            };
            let mut close = None;
            if let Some(armed) = self.arm.get()
                && armed.api_key == api_key
                && number >= armed.hold_from
            {
                // Held before the write, so the reply cannot pass first.
                held.borrow_mut().insert(correlation);
                if number >= armed.close_after {
                    self.arm.set(None);
                    close = Some(armed.linger);
                }
            }
            broker.write_all(frame).await.0?;
            if let Some(linger) = close {
                sleep(linger).await;
                return Ok(());
            }
        }
    }
}

/// Close both sides after the proxy forwards request number `close_after` of
/// `api_key`. Replies to requests number `hold_from..=close_after` of
/// `api_key` are never forwarded. Numbers count from 1 over the life of the
/// proxy, across connections. The arm fires once.
#[derive(Clone, Copy, Debug)]
struct Arm {
    api_key: i16,
    hold_from: usize,
    close_after: usize,
    /// Wait between the last forward and the close, so the broker can handle
    /// the held requests.
    linger: Duration,
}

/// Broker to client.
async fn replies(
    mut broker: &TcpStream,
    mut client: &TcpStream,
    held: &RefCell<HashSet<i32>>,
) -> io::Result<()> {
    loop {
        let frame = read_frame(&mut broker).await?;
        if frame.len() < 8 {
            return Err(io::Error::other("short reply frame"));
        }
        let correlation = i32::from_be_bytes([frame[4], frame[5], frame[6], frame[7]]);
        // Skipped, not closed: the requests side closes after the last held
        // request, so an early held reply must not cut the range short.
        if held.borrow().contains(&correlation) {
            continue;
        }
        client.write_all(frame).await.0?;
    }
}

/// One whole frame, length prefix included.
async fn read_frame(stream: &mut &TcpStream) -> io::Result<Vec<u8>> {
    let BufResult(result, length) = stream.read_exact([0; 4]).await;
    result?;
    let body = usize::try_from(i32::from_be_bytes(length)).map_err(io::Error::other)?;
    if body > MAX_FRAME {
        return Err(io::Error::other("frame too large"));
    }
    // `read_exact` fills the whole capacity, which `with_capacity` sets.
    let BufResult(result, rest) = stream.read_exact(Vec::with_capacity(body)).await;
    result?;
    Ok([length.as_slice(), &rest].concat())
}

/// Larger frames close the connection.
const MAX_FRAME: usize = 256 << 20;
