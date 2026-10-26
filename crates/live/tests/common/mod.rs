//! Helpers shared by the library's integration tests.

use sqlive::LiveDb;
use std::sync::atomic::{AtomicU64, Ordering};

/// A path in a fresh temporary directory.
pub fn temp_db(name: &str) -> String {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let dir = std::env::temp_dir().join(format!(
        "sqlive-test-{}-{name}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("temp dir");
    dir.join("db.sqlite").to_string_lossy().into_owned()
}

/// A fresh database with `schema` applied.
pub fn open(name: &str, schema: &str) -> LiveDb {
    let db = LiveDb::open(temp_db(name)).expect("open");
    db.write(|tx| tx.execute_batch(schema)).expect("schema");
    db
}
