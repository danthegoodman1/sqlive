//! Write throughput through `LiveDb::write`, against plain rusqlite with the
//! same settings: WAL and `synchronous = FULL`, so every commit waits for
//! fsync.

use crate::{BODY, CHATS, fresh_dir, per_second};
use sqlive::LiveDb;
use sqlive::rusqlite::{self, Connection, Transaction, TransactionBehavior};
use std::path::Path;
use std::time::{Duration, Instant};

const SINGLES: usize = 1_000;
const BATCHES: usize = 100;
const BATCH: usize = 1_000;

struct Workload {
    name: &'static str,
    commits: usize,
    rows_per_commit: usize,
    /// Untimed setup, in one transaction.
    setup: fn(&Transaction<'_>) -> rusqlite::Result<()>,
    /// The `i`th commit's work.
    work: fn(&Transaction<'_>, usize) -> rusqlite::Result<()>,
}

fn nothing(_: &Transaction<'_>) -> rusqlite::Result<()> {
    Ok(())
}

/// Every row the batched updates and deletes reach.
fn filled(tx: &Transaction<'_>) -> rusqlite::Result<()> {
    let mut insert =
        tx.prepare("INSERT INTO chats (room, created_at, body) VALUES (?1, ?2, ?3)")?;
    for i in 0..BATCHES * BATCH {
        insert.execute((i as i64 % 100, i as i64, BODY))?;
    }
    Ok(())
}

fn insert_one(tx: &Transaction<'_>, i: usize) -> rusqlite::Result<()> {
    tx.execute(
        "INSERT INTO chats (room, created_at, body) VALUES (?1, ?2, ?3)",
        (i as i64 % 100, i as i64, BODY),
    )
    .map(|_| ())
}

fn insert_batch(tx: &Transaction<'_>, i: usize) -> rusqlite::Result<()> {
    let mut insert =
        tx.prepare_cached("INSERT INTO chats (room, created_at, body) VALUES (?1, ?2, ?3)")?;
    for j in i * BATCH..(i + 1) * BATCH {
        insert.execute((j as i64 % 100, j as i64, BODY))?;
    }
    Ok(())
}

fn update_batch(tx: &Transaction<'_>, i: usize) -> rusqlite::Result<()> {
    tx.execute(
        "UPDATE chats SET body = 'changed ' || id WHERE id BETWEEN ?1 AND ?2",
        ((i * BATCH + 1) as i64, ((i + 1) * BATCH) as i64),
    )
    .map(|_| ())
}

fn delete_batch(tx: &Transaction<'_>, i: usize) -> rusqlite::Result<()> {
    tx.execute(
        "DELETE FROM chats WHERE id BETWEEN ?1 AND ?2",
        ((i * BATCH + 1) as i64, ((i + 1) * BATCH) as i64),
    )
    .map(|_| ())
}

fn live(path: &Path, workload: &Workload) -> sqlive::Result<Duration> {
    let db = LiveDb::open(path.to_string_lossy())?;
    db.write(|tx| {
        tx.execute_batch(CHATS)?;
        (workload.setup)(tx)
    })?;
    let started = Instant::now();
    for i in 0..workload.commits {
        db.write(|tx| (workload.work)(tx, i))?;
    }
    Ok(started.elapsed())
}

fn plain(path: &Path, workload: &Workload) -> rusqlite::Result<Duration> {
    let mut conn = Connection::open(path)?;
    conn.execute_batch("PRAGMA journal_mode = WAL; PRAGMA synchronous = FULL;")?;
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    tx.execute_batch(CHATS)?;
    (workload.setup)(&tx)?;
    tx.commit()?;
    let started = Instant::now();
    for i in 0..workload.commits {
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        (workload.work)(&tx, i)?;
        tx.commit()?;
    }
    Ok(started.elapsed())
}

pub fn run() -> sqlive::Result<()> {
    let dir = fresh_dir("writes");
    let workloads = [
        Workload {
            name: "Insert 1 row per commit",
            commits: SINGLES,
            rows_per_commit: 1,
            setup: nothing,
            work: insert_one,
        },
        Workload {
            name: "Insert 1,000 rows per commit",
            commits: BATCHES,
            rows_per_commit: BATCH,
            setup: nothing,
            work: insert_batch,
        },
        Workload {
            name: "Update 1,000 rows per commit",
            commits: BATCHES,
            rows_per_commit: BATCH,
            setup: filled,
            work: update_batch,
        },
        Workload {
            name: "Delete 1,000 rows per commit",
            commits: BATCHES,
            rows_per_commit: BATCH,
            setup: filled,
            work: delete_batch,
        },
    ];
    println!(
        "\n## writes: WAL, synchronous = FULL, in {}\n",
        dir.display()
    );
    println!(
        "| Workload | SqLive commits/s | SqLive rows/s | rusqlite rows/s | SqLive / rusqlite |"
    );
    println!("| --- | ---: | ---: | ---: | ---: |");
    for (i, workload) in workloads.iter().enumerate() {
        let with = live(&dir.join(format!("live-{i}.db")), workload)?;
        let without = plain(&dir.join(format!("plain-{i}.db")), workload)?;
        let rows = workload.commits * workload.rows_per_commit;
        println!(
            "| {} | {:.0} | {:.0} | {:.0} | {:.2} |",
            workload.name,
            per_second(workload.commits, with),
            per_second(rows, with),
            per_second(rows, without),
            without.as_secs_f64() / with.as_secs_f64()
        );
    }
    Ok(())
}
