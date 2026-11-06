//! A local Redpanda for the integration tests, and a frame proxy.
//!
//! `Redpanda::start` needs `redpanda` and `rpk` on `PATH` (`nix develop`).

#![allow(dead_code)]

mod proxy;
#[cfg(test)]
mod tests;

pub use proxy::Proxy;

use std::{
    fs,
    io::{self, ErrorKind},
    net::{SocketAddr, TcpListener},
    path::PathBuf,
    pin::pin,
    process::{ExitStatus, Stdio},
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};

use compio::{
    BufResult,
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    process,
    time::{sleep, timeout},
};
use futures_util::future::{self, Either, LocalBoxFuture};
use tempfile::TempDir;

/// A one-node Redpanda on 127.0.0.1 in a temp dir. `Drop` kills it with
/// `SIGKILL` and removes the dir.
pub struct Redpanda {
    dir: TempDir,
    ports: Ports,
    process: Option<Process>,
}

impl Redpanda {
    pub async fn start() -> Redpanda {
        let dir = tempfile::Builder::new()
            .prefix("redpanda-")
            .tempdir()
            .expect("create redpanda temp dir");
        let data = dir.path().join("data");
        fs::create_dir(&data).expect("create redpanda data dir");
        // Cluster properties go in `.bootstrap.yaml` next to `redpanda.yaml`.
        // Redpanda ignores them in the node file.
        fs::write(
            dir.path().join(".bootstrap.yaml"),
            "auto_create_topics_enabled: false\n",
        )
        .expect("write .bootstrap.yaml");
        let mut redpanda = Redpanda {
            dir,
            ports: free_ports(),
            process: None,
        };
        // A port another process took between the check and the bind: new
        // ports, a few times.
        for _ in 0..START_TRIES {
            fs::write(
                redpanda.dir.path().join("redpanda.yaml"),
                node_yaml(&data, redpanda.ports),
            )
            .expect("write redpanda.yaml");
            match redpanda.spawn().await {
                Ok(()) => return redpanda,
                Err(failure) if failure.contains(ADDRESS_IN_USE) => {
                    eprintln!("redpanda: a port was in use, new ports: {failure}");
                    redpanda.ports = free_ports();
                }
                Err(failure) => panic!("{failure}"),
            }
        }
        panic!("redpanda: ports in use after {START_TRIES} tries");
    }

    /// Stops the broker and starts it again on the same data dir and ports.
    pub async fn restart(&mut self) {
        self.stop().await;
        // The clients know the ports, so a port in use is tried again on the
        // same ports.
        for _ in 0..START_TRIES {
            match self.spawn().await {
                Ok(()) => return,
                Err(failure) if failure.contains(ADDRESS_IN_USE) => {
                    eprintln!("redpanda: a port was in use at the restart: {failure}");
                    sleep(Duration::from_secs(1)).await;
                }
                Err(failure) => panic!("{failure}"),
            }
        }
        panic!("redpanda: ports in use after {START_TRIES} restart tries");
    }

    /// The Kafka API address.
    pub fn addr(&self) -> SocketAddr {
        SocketAddr::from(([127, 0, 0, 1], self.ports.kafka))
    }

    pub async fn create_topic(&self, name: &str, partitions: u32) {
        let partitions = partitions.to_string();
        let out = self
            .rpk(&["topic", "create", name, "-p", &partitions], b"")
            .await;
        let out = String::from_utf8_lossy(&out);
        assert!(
            out.lines()
                .any(|line| line.split_whitespace().eq([name, "OK"])),
            "rpk topic create {name}: {out}"
        );
    }

    /// Reads the first `count` records of the topic, sorted by partition and
    /// offset. Waits until `count` records exist, up to `RPK_LIMIT`.
    pub async fn consume(&self, name: &str, count: usize) -> Vec<ConsumedRecord> {
        // A JSON object per line with hex bytes. `--format json` replaces bytes
        // that are not UTF-8. The header list ends with an empty array, since
        // `%h{..}` repeats its text after each header.
        const FORMAT: &str = concat!(
            r#"{"partition":%p,"offset":%o,"timestamp":%d,"#,
            r#""key":"%k{hex}","value":"%v{hex}","#,
            r#""headers":[%h{["%k{hex}","%v{hex}"],}[]]}\n"#,
        );
        let num = count.to_string();
        let out = self
            .rpk(
                &[
                    "topic", "consume", name, "--offset", "start", "--num", &num, "--format",
                    FORMAT,
                ],
                b"",
            )
            .await;
        let mut records = serde_json::Deserializer::from_slice(&out)
            .into_iter::<serde_json::Value>()
            .map(|value| parse_record(&value.expect("rpk consume output is JSON")))
            .collect::<Vec<_>>();
        assert_eq!(records.len(), count, "rpk consume {name}");
        records.sort_by_key(|record| (record.partition, record.offset));
        records
    }

    /// Runs `rpk` against this broker and returns its stdout. Panics on a
    /// failure or after `RPK_LIMIT`.
    pub async fn rpk(&self, args: &[&str], stdin: &[u8]) -> Vec<u8> {
        let brokers = format!("brokers={}", self.addr());
        let admin = format!("admin.hosts=127.0.0.1:{}", self.ports.admin);
        let mut child = process::Command::new("rpk")
            .args(args)
            .args(["-X", &brokers, "-X", &admin])
            .stdin(Stdio::piped())
            .unwrap()
            .stdout(Stdio::piped())
            .unwrap()
            .stderr(Stdio::piped())
            .unwrap()
            .spawn()
            .unwrap_or_else(|error| match error.kind() {
                ErrorKind::NotFound => {
                    panic!("`rpk` is not on PATH; run the tests inside `nix develop`")
                }
                _ => panic!("spawn rpk: {error}"),
            });
        let mut input = child.stdin.take().unwrap();
        let mut output = child.stdout.take().unwrap();
        let mut errors = child.stderr.take().unwrap();
        // Dropping `input` at the end closes rpk's stdin.
        let write = async move { input.write_all(stdin.to_vec()).await.0 };
        let pipes = pin!(future::join3(
            write,
            output.read_to_end(Vec::new()),
            errors.read_to_end(Vec::new()),
        ));
        let (timed_out, (written, BufResult(_, stdout), BufResult(_, stderr))) =
            match future::select(pipes, pin!(sleep(RPK_LIMIT))).await {
                Either::Left((outputs, _)) => (false, outputs),
                Either::Right(((), pipes)) => {
                    // A kill closes the pipes, so the reads still end.
                    let _ = child.kill();
                    (true, pipes.await)
                }
            };
        let status = child.wait().await.expect("wait for rpk");
        written.expect("write rpk stdin");
        if !timed_out && status.success() {
            return stdout;
        }
        panic!(
            "rpk {args:?}: {}\nstdout: {}\nstderr: {}",
            if timed_out {
                "timed out".to_string()
            } else {
                status.to_string()
            },
            String::from_utf8_lossy(&stdout),
            String::from_utf8_lossy(&stderr),
        )
    }

    /// Starts the process and waits until it is ready. `Err` holds why not,
    /// with the log tail.
    async fn spawn(&mut self) -> Result<(), String> {
        let log = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.log_path())
            .expect("open redpanda log");
        // Each test runs its own broker. The default 10 000 network IOCBs
        // per broker would use up the system's `fs.aio-max-nr` (65 536) at
        // about six brokers.
        let child = process::Command::new("redpanda")
            .arg("--redpanda-cfg")
            .arg(self.dir.path().join("redpanda.yaml"))
            .args([
                "--smp",
                "1",
                "--memory",
                "1G",
                "--overprovisioned",
                "--unsafe-bypass-fsync=true",
                "--max-networking-io-control-blocks=1000",
            ])
            .stdin(Stdio::null())
            .unwrap()
            .stdout(log.try_clone().unwrap())
            .unwrap()
            .stderr(log)
            .unwrap()
            .spawn()
            .unwrap_or_else(|error| match error.kind() {
                ErrorKind::NotFound => {
                    panic!("`redpanda` is not on PATH; run the tests inside `nix develop`")
                }
                _ => panic!("spawn redpanda: {error}"),
            });
        self.process = Some(Process {
            pid: child.id(),
            exit: Box::pin(child.wait()),
        });
        self.wait_ready().await
    }

    fn log_path(&self) -> PathBuf {
        self.dir.path().join("redpanda.log")
    }

    /// Admin `ready` can return 200 before the Kafka API listens, so this
    /// waits for both. Readiness has no event, so it is polled.
    async fn wait_ready(&mut self) -> Result<(), String> {
        let (admin, addr) = (self.ports.admin, self.addr());
        let ready = async {
            while !(admin_ready(admin).await && TcpStream::connect(addr).await.is_ok()) {
                sleep(Duration::from_millis(100)).await;
            }
        };
        let exit = &mut self.process.as_mut().unwrap().exit;
        let result = match timeout(READY_LIMIT, future::select(pin!(ready), exit)).await {
            Ok(Either::Left(_)) => return Ok(()),
            Ok(Either::Right((status, _))) => Ok(status),
            Err(_) => Err(()),
        };
        let failure = match result {
            Ok(status) => {
                // The exit future is done: the process is reaped.
                self.process = None;
                format!("exited before ready: {status:?}")
            }
            Err(()) => {
                self.stop().await;
                format!("not ready after {READY_LIMIT:?}")
            }
        };
        Err(format!("redpanda {failure}\n{}", self.log_tail()))
    }

    /// `SIGTERM`, then `SIGKILL` after `STOP_LIMIT`.
    async fn stop(&mut self) {
        let Some(mut process) = self.process.take() else {
            return;
        };
        signal("TERM", process.pid).await;
        if timeout(STOP_LIMIT, &mut process.exit).await.is_err() {
            signal("KILL", process.pid).await;
            let _ = process.exit.await;
        }
    }

    fn log_tail(&self) -> String {
        let log = fs::read_to_string(self.log_path()).unwrap_or_default();
        let lines = log.lines().collect::<Vec<_>>();
        lines[lines.len().saturating_sub(40)..].join("\n")
    }
}

impl Drop for Redpanda {
    fn drop(&mut self) {
        // Drop cannot await: the dropped `exit` future reaps the process.
        if let Some(process) = self.process.take() {
            let _ = std::process::Command::new("kill")
                .args(["-KILL", &process.pid.to_string()])
                .status();
        }
    }
}

/// One record as `rpk topic consume` reads it. A null key, value, or header
/// value reads as empty: `rpk` prints both the same way.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConsumedRecord {
    pub partition: i32,
    pub offset: i64,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub headers: Vec<(String, Vec<u8>)>,
    /// Milliseconds since the epoch.
    pub timestamp: i64,
}

#[derive(Clone, Copy)]
struct Ports {
    rpc: u16,
    kafka: u16,
    admin: u16,
}

/// A running `redpanda`.
struct Process {
    pid: u32,
    /// Ends when the process exits. It owns the child, and reaps it even
    /// after it is dropped.
    exit: LocalBoxFuture<'static, io::Result<ExitStatus>>,
}

/// Three ports no other test of this process uses. Port 0 would give
/// ephemeral ports, which a parallel test's sockets can take between the
/// check and Redpanda's bind, and again while a broker restarts. So each
/// process takes its ports from its own block below the ephemeral range
/// (32768 and up on Linux), one by one, and skips a port that does not
/// bind.
fn free_ports() -> Ports {
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    let block = std::process::id() as usize % PORT_BLOCKS;
    let take = || loop {
        let at = NEXT.fetch_add(1, Ordering::Relaxed);
        assert!(at < PORT_BLOCK, "the port block of this process is used up");
        let port = u16::try_from(FIRST_PORT + block * PORT_BLOCK + at).expect("below 32768");
        if TcpListener::bind(("127.0.0.1", port)).is_ok() {
            return port;
        }
    };
    Ports {
        rpc: take(),
        kafka: take(),
        admin: take(),
    }
}

fn node_yaml(data: &std::path::Path, ports: Ports) -> String {
    format!(
        "redpanda:
  data_directory: {data}
  developer_mode: true
  seed_servers: []
  rpc_server: {{ address: 127.0.0.1, port: {rpc} }}
  kafka_api: [{{ address: 127.0.0.1, port: {kafka} }}]
  advertised_kafka_api: [{{ address: 127.0.0.1, port: {kafka} }}]
  admin: [{{ address: 127.0.0.1, port: {admin} }}]
",
        data = data.display(),
        rpc = ports.rpc,
        kafka = ports.kafka,
        admin = ports.admin,
    )
}

async fn admin_ready(port: u16) -> bool {
    let request = format!("GET /v1/status/ready HTTP/1.0\r\nHost: 127.0.0.1:{port}\r\n\r\n");
    let exchange = async {
        let mut stream = TcpStream::connect(("127.0.0.1", port)).await.ok()?;
        stream.write_all(request).await.0.ok()?;
        Some(stream.read_to_end(Vec::new()).await.1)
    };
    // A late reply is dropped with its connection.
    let Ok(Some(reply)) = timeout(Duration::from_secs(2), exchange).await else {
        return false;
    };
    let status_line = reply
        .split(|&byte| byte == b'\n')
        .next()
        .unwrap_or_default();
    String::from_utf8_lossy(status_line)
        .split_whitespace()
        .nth(1)
        == Some("200")
}

async fn signal(name: &str, pid: u32) {
    let _ = process::Command::new("kill")
        .args([&format!("-{name}"), &pid.to_string()])
        .status()
        .await;
}

fn parse_record(value: &serde_json::Value) -> ConsumedRecord {
    let int = |field: &str| {
        value[field]
            .as_i64()
            .unwrap_or_else(|| panic!("rpk record {field}: {value}"))
    };
    let headers = value["headers"]
        .as_array()
        .expect("rpk record headers")
        .iter()
        .filter_map(|pair| match pair.as_array().map(Vec::as_slice) {
            Some([]) => None,
            Some([key, header_value]) => Some((
                String::from_utf8(hex(key)).expect("header key is UTF-8"),
                hex(header_value),
            )),
            _ => panic!("rpk record header: {pair}"),
        })
        .collect();
    ConsumedRecord {
        partition: i32::try_from(int("partition")).unwrap(),
        offset: int("offset"),
        key: hex(&value["key"]),
        value: hex(&value["value"]),
        headers,
        timestamp: int("timestamp"),
    }
}

fn hex(value: &serde_json::Value) -> Vec<u8> {
    let text = value
        .as_str()
        .unwrap_or_else(|| panic!("hex string: {value}"));
    assert!(text.len().is_multiple_of(2), "hex string: {text}");
    (0..text.len())
        .step_by(2)
        .map(|at| {
            u8::from_str_radix(&text[at..at + 2], 16)
                .unwrap_or_else(|_| panic!("hex string: {text}"))
        })
        .collect()
}

const READY_LIMIT: Duration = Duration::from_secs(60);

const STOP_LIMIT: Duration = Duration::from_secs(30);

const RPK_LIMIT: Duration = Duration::from_secs(60);

const START_TRIES: usize = 5;

const ADDRESS_IN_USE: &str = "Address already in use";

/// Ports for brokers: `PORT_BLOCKS` blocks of `PORT_BLOCK` from `FIRST_PORT`,
/// all below the ephemeral range.
const FIRST_PORT: usize = 12_000;

const PORT_BLOCK: usize = 120;

const PORT_BLOCKS: usize = 160;
