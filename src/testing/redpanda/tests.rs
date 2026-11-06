//! Smoke tests of the Redpanda harness and the frame proxy.

use std::{io, time::Duration};

use super::{ConsumedRecord, Proxy, Redpanda};
use compio::{
    BufResult,
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    time::timeout,
};

#[compio::test]
async fn start_produce_consume_restart() {
    let mut broker = Redpanda::start().await;
    broker.create_topic("smoke", 2).await;
    broker
        .rpk(
            &[
                "topic", "produce", "smoke", "-p", "1", "-k", "key1", "-H", "pos:7",
            ],
            b"value1\n",
        )
        .await;

    let check = |records: Vec<ConsumedRecord>| {
        let [record] = records.as_slice() else {
            panic!("one record: {records:?}")
        };
        assert_eq!((record.partition, record.offset), (1, 0));
        assert_eq!(record.key, b"key1");
        assert_eq!(record.value, b"value1");
        assert_eq!(record.headers, [("pos".to_string(), b"7".to_vec())]);
        assert!(record.timestamp > 0);
    };
    check(broker.consume("smoke", 1).await);
    broker.restart().await;
    check(broker.consume("smoke", 1).await);
}

#[compio::test]
async fn proxy_counts_and_drops() {
    let broker = Redpanda::start().await;
    let proxy = Proxy::start(broker.addr()).await;
    proxy
        .serve(async {
            let mut stream = TcpStream::connect(proxy.addr()).await.unwrap();
            stream.write_all(api_versions_v0(1)).await.0.unwrap();
            let reply = read_frame(&mut stream).await.expect("ApiVersions reply");
            assert_eq!(
                i32::from_be_bytes(reply[..4].try_into().unwrap()),
                1,
                "correlation id"
            );
            assert_eq!(
                i16::from_be_bytes(reply[4..6].try_into().unwrap()),
                0,
                "error code"
            );
            assert_eq!(proxy.count(API_VERSIONS), 1);

            proxy.drop_after(API_VERSIONS, 2);
            let mut stream = TcpStream::connect(proxy.addr()).await.unwrap();
            stream.write_all(api_versions_v0(2)).await.0.unwrap();
            let read = timeout(READ_LIMIT, stream.read(Vec::with_capacity(64))).await;
            match read.expect("the proxy did not close the socket") {
                BufResult(Ok(0), _) => {}
                BufResult(Err(error), _) if error.kind() == io::ErrorKind::ConnectionReset => {}
                BufResult(Ok(_), rest) => {
                    panic!("the reply after the armed drop reached the client: {rest:?}")
                }
                BufResult(Err(error), _) => panic!("the proxy did not close the socket: {error}"),
            }
            assert_eq!(proxy.count(API_VERSIONS), 2);
        })
        .await;
}

/// ApiVersions v0: request header v1 and an empty body.
fn api_versions_v0(correlation: i32) -> Vec<u8> {
    let client_id = b"harness";
    let mut body = Vec::new();
    body.extend(API_VERSIONS.to_be_bytes());
    body.extend(0i16.to_be_bytes());
    body.extend(correlation.to_be_bytes());
    body.extend(i16::try_from(client_id.len()).unwrap().to_be_bytes());
    body.extend(client_id);
    let mut frame = i32::try_from(body.len()).unwrap().to_be_bytes().to_vec();
    frame.extend(body);
    frame
}

/// The body of one reply frame.
async fn read_frame(stream: &mut TcpStream) -> io::Result<Vec<u8>> {
    let read = async {
        let BufResult(result, length) = stream.read_exact([0; 4]).await;
        result?;
        let body = usize::try_from(i32::from_be_bytes(length)).unwrap();
        // `read_exact` fills the whole capacity, which `with_capacity` sets.
        let BufResult(result, body) = stream.read_exact(Vec::with_capacity(body)).await;
        result.map(|()| body)
    };
    timeout(READ_LIMIT, read).await.map_err(io::Error::other)?
}

const API_VERSIONS: i16 = 18;

const READ_LIMIT: Duration = Duration::from_secs(10);
