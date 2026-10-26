//! SqLive's benchmarks: write throughput, query latency, the read-set
//! index, and memory per subscription and per retained event.
//!
//!     cargo run --release -p sqlive-bench [writes|queries|index|memory]
//!
//! With no argument, every benchmark runs. Databases live in
//! `target/bench`, on disk, since every commit waits for fsync;
//! `SQLIVE_BENCH_DIR` moves them.

mod index;
mod memory;
mod queries;
mod writes;

use sqlive::rusqlite::Transaction;
use std::alloc::{GlobalAlloc, Layout, System};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

/// Counts the bytes the Rust heap holds, for the memory benchmark. The
/// counter is one contended atomic, so it counts only in a process that
/// runs that benchmark alone.
struct Counting;

static COUNTING: AtomicBool = AtomicBool::new(false);
static HELD: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let block = unsafe { System.alloc(layout) };
        if !block.is_null() && COUNTING.load(Ordering::Relaxed) {
            HELD.fetch_add(layout.size(), Ordering::Relaxed);
        }
        block
    }

    unsafe fn dealloc(&self, block: *mut u8, layout: Layout) {
        unsafe { System.dealloc(block, layout) };
        if COUNTING.load(Ordering::Relaxed) {
            HELD.fetch_sub(layout.size(), Ordering::Relaxed);
        }
    }

    unsafe fn realloc(&self, block: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        let moved = unsafe { System.realloc(block, layout, size) };
        if !moved.is_null() && COUNTING.load(Ordering::Relaxed) {
            HELD.fetch_add(size, Ordering::Relaxed);
            HELD.fetch_sub(layout.size(), Ordering::Relaxed);
        }
        moved
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

/// Bytes the Rust heap holds.
pub fn heap() -> usize {
    HELD.load(Ordering::Relaxed)
}

/// Bytes SQLite's own allocator holds, page caches included.
pub fn sqlite_heap() -> i64 {
    // SAFETY: reads a counter SQLite keeps for the process.
    unsafe { sqlive::rusqlite::ffi::sqlite3_memory_used() }
}

pub fn per_second(count: usize, elapsed: Duration) -> f64 {
    count as f64 / elapsed.as_secs_f64()
}

/// A fresh directory for one benchmark's databases.
pub fn fresh_dir(name: &str) -> PathBuf {
    let root = std::env::var_os("SQLIVE_BENCH_DIR")
        .map_or_else(|| PathBuf::from("target/bench"), PathBuf::from);
    let dir = root.join(name);
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("a benchmark directory");
    dir
}

/// The chat example's table, with integer rooms.
pub const CHATS: &str =
    "CREATE TABLE chats (id INTEGER PRIMARY KEY, room INTEGER, created_at INTEGER, body TEXT);
     CREATE INDEX chats_by_room ON chats (room, created_at);";

/// The chat example's live query.
pub const QUERY: &str = "SELECT * FROM chats WHERE room = ?1 ORDER BY created_at DESC LIMIT 10";

/// A chat of typical length, 60 bytes.
pub const BODY: &str = "a chat message of typical length, sixty bytes long, give or ";

/// Fills `rooms` rooms with twelve chats each, so each room's window of ten
/// is full.
pub fn fill(tx: &Transaction<'_>, rooms: i64) -> sqlive::rusqlite::Result<()> {
    let mut insert =
        tx.prepare("INSERT INTO chats (room, created_at, body) VALUES (?1, ?2, ?3)")?;
    for room in 0..rooms {
        for created_at in 0..12 {
            insert.execute((room, created_at, BODY))?;
        }
    }
    Ok(())
}

fn main() -> sqlive::Result<()> {
    let which = std::env::args().nth(1);
    if which.as_deref() == Some("memory") {
        // Before any allocation, so every block is counted both ways.
        COUNTING.store(true, Ordering::Relaxed);
        return memory::run();
    }
    if std::env::var_os("SQLIVE_BENCH_MEMSTATUS").is_some_and(|value| value == "0") {
        // SAFETY: runs before SQLite initializes.
        unsafe {
            sqlive::rusqlite::ffi::sqlite3_config(
                sqlive::rusqlite::ffi::SQLITE_CONFIG_MEMSTATUS,
                0,
            );
        }
    }
    let runs = |name: &str| which.as_deref().is_none_or(|which| which == name);
    if runs("writes") {
        writes::run()?;
    }
    if runs("queries") {
        queries::run()?;
    }
    if runs("index") {
        index::run()?;
    }
    if which.is_none() {
        // The memory benchmark counts every allocation, so it runs alone.
        let status = std::process::Command::new(std::env::current_exe().expect("this program"))
            .arg("memory")
            .status()
            .expect("the memory benchmark runs");
        assert!(status.success(), "the memory benchmark failed");
    }
    Ok(())
}
