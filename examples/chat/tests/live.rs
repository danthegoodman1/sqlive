//! The chat's live rooms: pushes to the posting room, skips elsewhere, the
//! ten-chat window, chat order, rooms closing, catching up after a lag,
//! persistence across a restart, and the HTTP routes.

use chat_example::{Chat, RoomResult, RoomStats, router};
use sqlive::Options;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::watch;

fn temp_db(name: &str) -> String {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let dir = std::env::temp_dir().join(format!(
        "chat-test-{}-{name}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("temp dir");
    dir.join("chat.db").to_string_lossy().into_owned()
}

/// Waits until the watched value satisfies `done`.
async fn until<T: Clone>(receiver: &mut watch::Receiver<T>, done: impl Fn(&T) -> bool) -> T {
    tokio::time::timeout(
        Duration::from_secs(5),
        receiver.wait_for(|value| done(value)),
    )
    .await
    .expect("in time")
    .expect("the room is open")
    .clone()
}

fn bodies(result: &RoomResult) -> Vec<&str> {
    result.chats.iter().map(|chat| chat.body.as_str()).collect()
}

fn checked(stats: &RoomStats) -> u64 {
    stats.reruns + stats.skips
}

#[tokio::test]
async fn a_chat_reaches_its_room_and_skips_the_others() {
    let chat = Chat::open(&temp_db("rooms")).unwrap();
    let mut a = chat.watch("a").unwrap();
    let mut b = chat.watch("b").unwrap();
    assert!(a.results.borrow_and_update().chats.is_empty());
    let before_b = b.results.borrow_and_update().clone();

    chat.post("a", "ann", "hello").unwrap();
    let result = until(&mut a.results, |r| !r.chats.is_empty()).await;
    assert_eq!(bodies(&result), ["hello"]);
    let stats_a = until(&mut a.stats, |s| checked(s) >= 1).await;
    assert_eq!(
        stats_a,
        RoomStats {
            reruns: 1,
            skips: 0
        }
    );
    let stats_b = until(&mut b.stats, |s| checked(s) >= 1).await;
    assert_eq!(
        stats_b,
        RoomStats {
            reruns: 0,
            skips: 1
        }
    );
    assert!(
        !b.results.has_changed().unwrap(),
        "room b got no new result"
    );
    assert_eq!(*b.results.borrow(), before_b);
}

#[tokio::test]
async fn a_room_shows_its_ten_newest_chats() {
    let chat = Chat::open(&temp_db("window")).unwrap();
    let mut a = chat.watch("a").unwrap();
    for i in 0..11 {
        chat.post("a", "ann", &format!("chat {i}")).unwrap();
    }
    let result = until(&mut a.results, |r| {
        r.chats.first().is_some_and(|c| c.body == "chat 10")
    })
    .await;
    let expected: Vec<String> = (1..=10).rev().map(|i| format!("chat {i}")).collect();
    assert_eq!(bodies(&result), expected);
    assert!(
        result
            .read_set
            .contains("where room = 'a' AND created_at >= "),
        "a full window bounds the read set: {}",
        result.read_set
    );
    // A chat older than the window cannot change the list.
    // A re-run can see several commits, and those after the first then skip.
    let stats = until(&mut a.stats, |s| checked(s) == 11).await;
    chat.db()
        .write(|tx| {
            tx.execute(
                "INSERT INTO chats (room, author, body, created_at) VALUES ('a', 'old', 'late', 1)",
                [],
            )
        })
        .unwrap();
    let after = until(&mut a.stats, |s| checked(s) > checked(&stats)).await;
    assert_eq!(
        after,
        RoomStats {
            skips: stats.skips + 1,
            ..stats
        },
        "an older chat skips"
    );
}

#[tokio::test]
async fn a_post_sorts_after_every_earlier_chat_in_its_room() {
    let chat = Chat::open(&temp_db("order")).unwrap();
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let ahead = now + 3_600_000;
    chat.db()
        .write(|tx| {
            tx.execute(
                "INSERT INTO chats (room, author, body, created_at) VALUES ('a', 'ann', 'ahead', ?1)",
                [ahead],
            )
        })
        .unwrap();
    chat.post("a", "bo", "after").unwrap();
    chat.post("b", "bo", "elsewhere").unwrap();
    let a = chat.watch("a").unwrap();
    let chats = a.results.borrow().chats.clone();
    assert_eq!(
        chats.iter().map(|c| c.body.as_str()).collect::<Vec<_>>(),
        ["after", "ahead"]
    );
    assert_eq!(chats[0].created_at, ahead + 1);
    // Another room's clock stays its own.
    let b = chat.watch("b").unwrap();
    assert!(b.results.borrow().chats[0].created_at < ahead);
}

#[tokio::test]
async fn a_room_closes_when_its_last_viewer_leaves() {
    let chat = Chat::open(&temp_db("close")).unwrap();
    let first = chat.watch("a").unwrap();
    let mut second = chat.watch("a").unwrap();
    chat.post("a", "ann", "hello").unwrap();
    until(&mut second.stats, |s| s.reruns == 1).await;
    drop(first);
    // The room stays open for its other viewer.
    assert_eq!(chat.watch("a").unwrap().stats.borrow().reruns, 1);
    drop(second);
    // Reopening the room starts it over.
    let again = chat.watch("a").unwrap();
    assert_eq!(*again.stats.borrow(), RoomStats::default());
    assert_eq!(bodies(&again.results.borrow()), ["hello"]);
}

#[tokio::test]
async fn a_follower_behind_the_log_queries_every_room_again() {
    let options = Options {
        log_capacity: 2,
        ..Options::default()
    };
    let chat = Chat::open_with(&temp_db("lag"), options).unwrap();
    let mut a = chat.watch("a").unwrap();
    let mut b = chat.watch("b").unwrap();
    {
        // An outstanding borrow blocks the watch's sender, so the follower
        // stalls on the first commit it checks while the log moves on.
        let _stall = b.stats.borrow();
        for i in 0..5 {
            chat.post("a", "ann", &format!("chat {i}")).unwrap();
        }
    }
    let result = until(&mut a.results, |r| {
        r.chats.first().is_some_and(|c| c.body == "chat 4")
    })
    .await;
    assert_eq!(result.chats.len(), 5);
    // No commit touched room b, yet it re-ran when the follower caught up.
    until(&mut b.stats, |s| s.reruns >= 1).await;
}

#[tokio::test]
async fn chats_survive_a_restart() {
    let path = temp_db("restart");
    {
        let chat = Chat::open(&path).unwrap();
        for i in 0..3 {
            chat.post("a", "ann", &format!("chat {i}")).unwrap();
        }
    }
    let chat = Chat::open(&path).unwrap();
    let viewer = chat.watch("a").unwrap();
    assert_eq!(
        bodies(&viewer.results.borrow()),
        ["chat 2", "chat 1", "chat 0"]
    );
}

/// Sends a request and reads until the response holds `want`.
async fn http(port: u16, request: String, want: &str) -> String {
    let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .unwrap();
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut response = String::new();
    let mut buffer = [0u8; 4096];
    tokio::time::timeout(Duration::from_secs(5), async {
        while !response.contains(want) {
            let n = stream.read(&mut buffer).await.unwrap();
            assert!(n > 0, "the connection closed before {want:?}: {response}");
            response.push_str(&String::from_utf8_lossy(&buffer[..n]));
        }
    })
    .await
    .unwrap_or_else(|_| panic!("no {want:?} in time: {response}"));
    response
}

#[tokio::test]
async fn the_page_and_routes_serve_over_http() {
    let chat = Arc::new(Chat::open(&temp_db("http")).unwrap());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(axum::serve(listener, router(chat)).into_future());

    let page = http(
        port,
        "GET / HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n".into(),
        "</html>",
    )
    .await;
    assert!(page.starts_with("HTTP/1.1 200"), "{page}");
    assert!(page.contains("10 most recent chats"));

    let live = tokio::spawn(http(
        port,
        "GET /rooms/lobby/live HTTP/1.1\r\nHost: x\r\n\r\n".into(),
        "\"body\":\"over http\"",
    ));
    let body = r#"{"author":"ann","body":"over http"}"#;
    // The live stream joins the room before the post lands, or reads the
    // chat in its first result; either way it sees it.
    let posted = http(
        port,
        format!(
            "POST /rooms/lobby/chats HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        ),
        "\"version\"",
    )
    .await;
    assert!(posted.starts_with("HTTP/1.1 200"), "{posted}");
    let stream = live.await.unwrap();
    assert!(stream.contains("text/event-stream"), "{stream}");
    assert!(stream.contains("event: result"), "{stream}");

    let rejected = http(
        port,
        "POST /rooms/lobby/chats HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\nContent-Length: 26\r\nConnection: close\r\n\r\n{\"author\":\"\",\"body\":\"x\"}  ".into(),
        "\r\n\r\n",
    )
    .await;
    assert!(rejected.starts_with("HTTP/1.1 400"), "{rejected}");

    // Room ids are limited to 64 characters, however many bytes they take.
    let room = |n| "%E4%BC%9A".repeat(n);
    let wide = http(
        port,
        format!("GET /rooms/{}/live HTTP/1.1\r\nHost: x\r\n\r\n", room(64)),
        "event: result",
    )
    .await;
    assert!(wide.starts_with("HTTP/1.1 200"), "{wide}");
    let long = http(
        port,
        format!(
            "GET /rooms/{}/live HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n",
            room(65)
        ),
        "\r\n\r\n",
    )
    .await;
    assert!(long.starts_with("HTTP/1.1 400"), "{long}");
}
