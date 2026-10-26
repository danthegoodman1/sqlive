//! A load test for the chat server. It starts the release `chat-example`
//! binary, opens live streams spread across rooms, posts chats at a fixed
//! rate, and reports how long each chat took to reach every viewer of its
//! room, and the server's memory and CPU time.
//!
//!     cargo build --release -p chat-example
//!     target/release/chat-load [ROOMS] [VIEWERS] [POSTS_PER_SECOND] [SECONDS]
//!
//! The database lives in `target/bench/chat-load`, on disk, since every
//! post commits and waits for fsync; `CHAT_LOAD_DIR` moves it.

use serde::Deserialize;
use std::collections::BTreeSet;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

const PORT: u16 = 3190;
/// Viewers joining at once.
const JOINING: usize = 256;
/// Connections posting chats.
const POSTERS: usize = 16;
/// Clock ticks per second in `/proc/<pid>/stat`.
const TICKS: f64 = 100.0;

#[derive(Deserialize)]
struct RoomResult {
    chats: Vec<Chat>,
}

#[derive(Deserialize)]
struct Chat {
    body: String,
}

/// When each post was sent, in microseconds since the test began.
struct Sent {
    began: Instant,
    at: Vec<AtomicU64>,
}

impl Sent {
    fn now(&self) -> u64 {
        self.began.elapsed().as_micros() as u64
    }

    fn mark(&self, seq: usize) {
        self.at[seq].store(self.now(), Ordering::Relaxed);
    }

    /// Microseconds from post `seq`'s send to now.
    fn since(&self, seq: usize) -> Option<u64> {
        let sent = self.at.get(seq)?.load(Ordering::Relaxed);
        (sent > 0).then(|| self.now().saturating_sub(sent))
    }
}

fn arg(n: usize, default: usize) -> usize {
    std::env::args()
        .nth(n)
        .map_or(default, |arg| arg.parse().expect("a number"))
}

/// A field of `/proc/<pid>/status`, in megabytes.
fn status_mb(pid: u32, field: &str) -> f64 {
    let status = std::fs::read_to_string(format!("/proc/{pid}/status")).unwrap_or_default();
    status
        .lines()
        .find(|line| line.starts_with(field))
        .and_then(|line| line.split_whitespace().nth(1)?.parse::<f64>().ok())
        .map_or(0.0, |kb| kb / 1024.0)
}

/// User and system CPU seconds the process has used.
fn cpu_seconds(pid: u32) -> f64 {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).unwrap_or_default();
    // utime and stime are the 12th and 13th fields after the command name.
    let rest = stat.rsplit_once(')').map_or("", |(_, rest)| rest);
    let fields: Vec<&str> = rest.split_whitespace().collect();
    let ticks = |i: usize| {
        fields
            .get(i)
            .and_then(|f| f.parse::<f64>().ok())
            .unwrap_or(0.0)
    };
    (ticks(11) + ticks(12)) / TICKS
}

/// The `p` quantile of sorted microseconds, in milliseconds.
fn percentile(sorted: &[u64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let i = ((sorted.len() - 1) as f64 * p).round() as usize;
    sorted[i] as f64 / 1000.0
}

/// One viewer: joins its room, counts itself joined when its first result
/// arrives, then records each new chat's delivery latency until the server
/// closes the stream.
async fn view(
    room: usize,
    sent: Arc<Sent>,
    joined: Arc<AtomicUsize>,
    permit: OwnedSemaphorePermit,
) -> Vec<u64> {
    // HTTP/1.0, so the body arrives as plain bytes until the server closes.
    let mut stream = TcpStream::connect(("127.0.0.1", PORT))
        .await
        .expect("a connection");
    let request = format!("GET /rooms/r{room}/live HTTP/1.0\r\nHost: load\r\n\r\n");
    stream
        .write_all(request.as_bytes())
        .await
        .expect("a request");
    let mut permit = Some(permit);
    let mut latencies = Vec::new();
    let mut seen = BTreeSet::new();
    let mut pending = String::new();
    let mut buffer = vec![0u8; 64 * 1024];
    loop {
        let n = stream.read(&mut buffer).await.unwrap_or(0);
        if n == 0 {
            return latencies;
        }
        pending.push_str(&String::from_utf8_lossy(&buffer[..n]));
        while let Some(end) = pending.find("\n\n") {
            let event: String = pending.drain(..end + 2).collect();
            if !event.contains("event: result") {
                continue;
            }
            if permit.take().is_some() {
                joined.fetch_add(1, Ordering::Relaxed);
            }
            let Some(data) = event.lines().find_map(|line| line.strip_prefix("data: ")) else {
                continue;
            };
            let result: RoomResult = serde_json::from_str(data).expect("a result");
            for chat in result.chats {
                let Some(seq) = chat.body.strip_prefix("load ").and_then(|s| s.parse().ok()) else {
                    continue;
                };
                if seen.insert(seq)
                    && let Some(latency) = sent.since(seq)
                {
                    latencies.push(latency);
                }
            }
        }
    }
}

/// Reads one HTTP/1.1 response: its head, then Content-Length bytes.
async fn response(stream: &mut TcpStream, buffer: &mut [u8]) -> String {
    let mut response = Vec::new();
    loop {
        let n = stream.read(buffer).await.expect("a response");
        assert!(n > 0, "the server closed a posting connection");
        response.extend_from_slice(&buffer[..n]);
        let text = String::from_utf8_lossy(&response);
        if let Some(head_end) = text.find("\r\n\r\n") {
            let length = text[..head_end]
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().ok())?
                })
                .unwrap_or(0);
            if response.len() >= head_end + 4 + length {
                return text.into_owned();
            }
        }
    }
}

/// One poster: a keep-alive connection that sends posts `first`,
/// `first + POSTERS`, and so on, each at its time after `start`.
async fn post(
    first: usize,
    posts: usize,
    rate: f64,
    rooms: usize,
    start: Instant,
    sent: Arc<Sent>,
) -> Vec<u64> {
    let mut stream = TcpStream::connect(("127.0.0.1", PORT))
        .await
        .expect("a connection");
    let mut latencies = Vec::new();
    let mut buffer = vec![0u8; 4096];
    for seq in (first..posts).step_by(POSTERS) {
        let due = start + Duration::from_secs_f64(seq as f64 / rate);
        tokio::time::sleep_until(due.into()).await;
        let body = format!(r#"{{"author":"load","body":"load {seq}"}}"#);
        let request = format!(
            "POST /rooms/r{}/chats HTTP/1.1\r\nHost: load\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
            seq % rooms,
            body.len()
        );
        let started = Instant::now();
        sent.mark(seq);
        stream.write_all(request.as_bytes()).await.expect("a post");
        let text = response(&mut stream, &mut buffer).await;
        assert!(text.starts_with("HTTP/1.1 200"), "a post failed: {text}");
        latencies.push(started.elapsed().as_micros() as u64);
    }
    latencies
}

#[tokio::main]
async fn main() {
    let rooms = arg(1, 1_000);
    let viewers = arg(2, 10_000);
    let rate = arg(3, 100) as f64;
    let seconds = arg(4, 30);
    let posts = (rate as usize) * seconds;

    let dir = std::env::var_os("CHAT_LOAD_DIR")
        .map_or_else(|| PathBuf::from("target/bench/chat-load"), PathBuf::from);
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("a load directory");
    let server_path = std::env::current_exe()
        .expect("this program")
        .with_file_name("chat-example");
    let mut server = Command::new(&server_path)
        .env("CHAT_DB", dir.join("load.db"))
        .env("CHAT_ADDR", format!("127.0.0.1:{PORT}"))
        .stdout(Stdio::piped())
        .spawn()
        .unwrap_or_else(|e| panic!("starting {}: {e}", server_path.display()));
    let pid = server.id();
    let mut ready = String::new();
    BufReader::new(server.stdout.take().expect("piped"))
        .read_line(&mut ready)
        .expect("the server's ready line");
    let rss_idle = status_mb(pid, "VmRSS:");

    let sent = Arc::new(Sent {
        began: Instant::now(),
        at: (0..posts).map(|_| AtomicU64::new(0)).collect(),
    });
    let joined = Arc::new(AtomicUsize::new(0));
    let joining = Arc::new(Semaphore::new(JOINING));
    let started = Instant::now();
    let mut viewing = Vec::with_capacity(viewers);
    for viewer in 0..viewers {
        let permit = joining.clone().acquire_owned().await.expect("open");
        viewing.push(tokio::spawn(view(
            viewer % rooms,
            sent.clone(),
            joined.clone(),
            permit,
        )));
    }
    while joined.load(Ordering::Relaxed) < viewers {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let join_time = started.elapsed();
    let rss_joined = status_mb(pid, "VmRSS:");
    let cpu_before = cpu_seconds(pid);

    let start = Instant::now();
    let posting: Vec<_> = (0..POSTERS)
        .map(|first| tokio::spawn(post(first, posts, rate, rooms, start, sent.clone())))
        .collect();
    let mut post_latencies = Vec::new();
    for poster in posting {
        post_latencies.extend(poster.await.expect("a poster"));
    }
    let posting_time = start.elapsed();
    // The last chats reach their viewers.
    tokio::time::sleep(Duration::from_secs(2)).await;
    let cpu = cpu_seconds(pid) - cpu_before;
    let rss_end = status_mb(pid, "VmRSS:");
    let rss_peak = status_mb(pid, "VmHWM:");
    server.kill().expect("the server stops");
    server.wait().expect("the server exits");

    let mut latencies = Vec::new();
    for viewer in viewing {
        latencies.extend(viewer.await.expect("a viewer"));
    }
    latencies.sort_unstable();
    post_latencies.sort_unstable();
    let mut per_room = vec![0usize; rooms];
    for viewer in 0..viewers {
        per_room[viewer % rooms] += 1;
    }
    let expected: usize = (0..posts).map(|seq| per_room[seq % rooms]).sum();

    println!("{rooms} rooms, {viewers} viewers; {posts} posts at {rate:.0}/s for {seconds} s");
    println!(
        "joined in {:.2} s ({:.0} joins/s); posted in {:.2} s ({:.0} posts/s)",
        join_time.as_secs_f64(),
        viewers as f64 / join_time.as_secs_f64(),
        posting_time.as_secs_f64(),
        posts as f64 / posting_time.as_secs_f64()
    );
    println!(
        "delivered {} of {expected} chats to viewers; delivery ms p50 {:.1}, p90 {:.1}, p99 {:.1}, max {:.1}",
        latencies.len(),
        percentile(&latencies, 0.5),
        percentile(&latencies, 0.9),
        percentile(&latencies, 0.99),
        percentile(&latencies, 1.0)
    );
    println!(
        "post ms p50 {:.1}, p99 {:.1}",
        percentile(&post_latencies, 0.5),
        percentile(&post_latencies, 0.99)
    );
    println!(
        "server RSS: {rss_idle:.0} MB idle, {rss_joined:.0} MB with viewers ({:.1} KB each), {rss_end:.0} MB at the end, {rss_peak:.0} MB peak; CPU while posting: {cpu:.1} s ({:.0}% of one core)",
        (rss_joined - rss_idle) * 1024.0 / viewers as f64,
        cpu / posting_time.as_secs_f64() * 100.0
    );
}
