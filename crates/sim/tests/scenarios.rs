//! Scripted runs of situations a random seed reaches only by chance.

use sqlive::{Error, LiveDb, Options};
use sqlive_sim::vfs::{FaultConfig, SimVfs};

fn options(vfs: &SimVfs) -> Options {
    Options {
        vfs: Some(vfs.name().to_owned()),
        ..Options::default()
    }
}

#[test]
fn a_commit_that_failed_to_sync_can_come_back_after_a_crash_and_streams_lag() {
    let mut faults = FaultConfig::none();
    // Every unsynced write reaches the media, so the failed commit's WAL
    // frames survive.
    faults.crash_keep_sector = 1.0;
    let vfs = SimVfs::new(7, faults);
    let db = LiveDb::open_with("scenario.db", options(&vfs)).unwrap();
    db.write(|tx| tx.execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY)"))
        .unwrap();
    let ((), before) = db
        .write(|tx| tx.execute_batch("INSERT INTO t VALUES (1)"))
        .unwrap();

    vfs.fail_next_sync();
    let failed = db.write(|tx| tx.execute_batch("INSERT INTO t VALUES (2)"));
    assert!(failed.is_err(), "the commit's WAL sync failed");
    assert_eq!(db.version(), before, "nothing was published");

    vfs.begin_crash();
    drop(db);
    vfs.finish_crash();
    let db = LiveDb::open_with("scenario.db", options(&vfs)).unwrap();
    assert_eq!(
        db.version(),
        before + 1,
        "the commit reached the WAL before its sync failed, so recovery keeps it"
    );
    let mut follower = db.changes_since(before).unwrap();
    assert!(
        matches!(follower.try_next(), Some(Err(Error::Lagged { .. }))),
        "a follower at the last published version must lag, since it never saw the recovered commit"
    );
    assert_eq!(count_rows(&vfs), 2);
}

#[test]
fn a_failed_vacuum_lags_every_stream_and_flags_the_next_write() {
    let vfs = SimVfs::new(8, FaultConfig::none());
    let db = LiveDb::open_with("scenario.db", options(&vfs)).unwrap();
    db.write(|tx| {
        tx.execute_batch(
            "CREATE TABLE t (a);
             INSERT INTO t VALUES (1), (2);
             DELETE FROM t WHERE a = 1;",
        )
    })
    .unwrap();
    let before = db.version();
    let mut follower = db.changes().unwrap();

    vfs.fail_next_sync();
    assert!(db.vacuum().is_err(), "the vacuum's sync failed");
    assert_eq!(db.version(), before, "nothing was published");
    assert!(
        matches!(follower.try_next(), Some(Err(Error::Lagged { .. }))),
        "the vacuum may have renumbered rowids, so a follower must reload now"
    );

    let ((), version) = db
        .write(|tx| tx.execute_batch("INSERT INTO t VALUES (3)"))
        .unwrap();
    let event = db.changes_since(version - 1).unwrap().try_next().unwrap();
    assert!(
        event.unwrap().schema_changed,
        "the next write tells every consumer to reload"
    );
}

fn count_rows(vfs: &SimVfs) -> i64 {
    let conn = sqlive::rusqlite::Connection::open_with_flags_and_vfs(
        "scenario.db",
        sqlive::rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        vfs.name(),
    )
    .unwrap();
    conn.query_row("SELECT count(*) FROM t", [], |r| r.get(0))
        .unwrap()
}
