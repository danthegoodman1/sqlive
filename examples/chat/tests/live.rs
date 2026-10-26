//! The chat's live rooms: pushes to the posting room, skips elsewhere,
//! re-runs that change nothing, slow viewers, viewers who join ahead of the
//! follower, the ten-chat window, chat order, rooms closing, catching up
//! after a lag, persistence across a restart, and the HTTP routes.

use chat_example::{Chat, RoomResult, RoomStats, Viewer, router};
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

/// The viewer's next result.
async fn next(viewer: &Viewer) -> Arc<RoomResult> {
    tokio::time::timeout(Duration::from_secs(5), viewer.next_result())
        .await
        .expect("a result in time")
}

/// Waits for a result that satisfies `done`.
async fn next_until(viewer: &Viewer, done: impl Fn(&RoomResult) -> bool) -> Arc<RoomResult> {
    loop {
        let result = next(viewer).await;
        if done(&result) {
            return result;
        }
    }
}

fn bodies(result: &RoomResult) -> Vec<&str> {
    result.chats.iter().map(|chat| chat.body.as_str()).collect()
}

fn checked(stats: &RoomStats) -> u64 {
    stats.reruns + stats.skips
}

/// Re-runs, unchanged re-runs and skips.
fn counts(stats: &RoomStats) -> (u64, u64, u64) {
    (stats.reruns, stats.unchanged, stats.skips)
}

#[tokio::test]
async fn a_chat_reaches_its_room_and_skips_the_others() {
    let chat = Chat::open(&temp_db("rooms")).unwrap();
    let mut a = chat.watch("a").unwrap();
    let mut b = chat.watch("b").unwrap();
    assert!(next(&a).await.chats.is_empty());
    assert!(next(&b).await.chats.is_empty());

    chat.post("a", "ann", "hello").unwrap();
    assert_eq!(bodies(&*next(&a).await), ["hello"]);
    let stats_a = until(&mut a.stats, |s| checked(s) >= 1).await;
    assert_eq!(counts(&stats_a), (1, 0, 0));
    let stats_b = until(&mut b.stats, |s| checked(s) >= 1).await;
    assert_eq!(counts(&stats_b), (0, 0, 1));
    assert!(b.try_result().is_none(), "room b got no new result");
}

#[tokio::test]
async fn a_rerun_that_finds_the_same_rows_pushes_nothing() {
    let chat = Chat::open(&temp_db("unchanged")).unwrap();
    let mut a = chat.watch("a").unwrap();
    let b = chat.watch("b").unwrap();
    let joined = next(&a).await.version;
    {
        // An outstanding borrow blocks the watch's sender, so the follower
        // stalls on the first commit while the chat is posted and deleted.
        let _stall = b.stats.borrow();
        chat.post("a", "ann", "gone soon").unwrap();
        chat.db()
            .write(|tx| tx.execute("DELETE FROM chats WHERE body = 'gone soon'", []))
            .unwrap();
    }
    // The insert intersects room a, but its re-run finds the empty room
    // the viewer already holds; the delete then predates the result.
    let stats = until(&mut a.stats, |s| checked(s) == 2).await;
    assert_eq!(counts(&stats), (1, 1, 1));
    assert!(
        stats.version > joined,
        "the room's version moves on without a push"
    );
    assert!(
        a.try_result().is_none(),
        "an unchanged re-run pushes nothing"
    );
}

#[tokio::test]
async fn a_viewer_who_holds_the_current_rows_resumes_without_them() {
    let chat = Chat::open(&temp_db("resume")).unwrap();
    chat.post("a", "ann", "hello").unwrap();
    let held = next(&chat.watch("a").unwrap()).await.hash;
    let current = chat.resume("a", held).unwrap();
    assert!(
        current.try_result().is_none(),
        "the viewer holds these rows"
    );
    chat.post("a", "bo", "again").unwrap();
    assert_eq!(bodies(&*next(&current).await), ["again", "hello"]);
    // A viewer holding older rows gets the room's rows at once.
    let behind = chat.resume("a", held).unwrap();
    let rows = behind.try_result().expect("the room's newer rows");
    assert_eq!(bodies(&rows), ["again", "hello"]);
}

#[tokio::test]
async fn a_slow_viewer_skips_to_the_newest_result() {
    let chat = Chat::open(&temp_db("slow")).unwrap();
    let mut a = chat.watch("a").unwrap();
    for i in 0..3 {
        chat.post("a", "ann", &format!("chat {i}")).unwrap();
    }
    until(&mut a.stats, |s| checked(s) == 3).await;
    // The viewer read nothing meanwhile, so only the newest result waits.
    let waiting = a.try_result().expect("a result waits");
    assert_eq!(bodies(&waiting), ["chat 2", "chat 1", "chat 0"]);
    assert!(a.try_result().is_none());
}

#[tokio::test]
async fn a_viewer_who_joins_ahead_of_the_follower_brings_the_room_along() {
    let chat = Chat::open(&temp_db("ahead")).unwrap();
    let mut first = chat.watch("a").unwrap();
    let b = chat.watch("b").unwrap();
    next(&first).await;
    next(&b).await;
    let second = {
        // The follower re-runs room b for the first commit, then stalls
        // outside the hub's lock counting it, before it reaches the second.
        let _stall = b.stats.borrow();
        chat.post("b", "bo", "elsewhere").unwrap();
        chat.post("a", "ann", "hello").unwrap();
        chat.watch("a").unwrap()
    };
    // The new viewer read the chat first, so both viewers get it then.
    assert_eq!(bodies(&*next(&second).await), ["hello"]);
    let pushed = first.try_result().expect("the join pushed the new rows");
    assert_eq!(bodies(&pushed), ["hello"]);
    // The follower then finds nothing new for room a.
    until(&mut first.stats, |s| s.skips == 2).await;
    assert!(first.try_result().is_none());
}

#[tokio::test]
async fn a_room_shows_its_ten_newest_chats() {
    let chat = Chat::open(&temp_db("window")).unwrap();
    let mut a = chat.watch("a").unwrap();
    for i in 0..11 {
        chat.post("a", "ann", &format!("chat {i}")).unwrap();
    }
    let result = next_until(&a, |r| r.chats.first().is_some_and(|c| c.body == "chat 10")).await;
    let expected: Vec<String> = (1..=10).rev().map(|i| format!("chat {i}")).collect();
    assert_eq!(bodies(&result), expected);
    assert!(
        result
            .read_set
            .contains("where room = 'a' AND created_at >= "),
        "a full window bounds the read set: {}",
        result.read_set
    );
    // A chat older than the window cannot change the list. A re-run can see
    // several commits, and those after the first then skip.
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
    let result = next(&chat.watch("a").unwrap()).await;
    assert_eq!(bodies(&result), ["after", "ahead"]);
    assert_eq!(result.chats[0].created_at, ahead + 1);
    // Another room's clock stays its own.
    let result = next(&chat.watch("b").unwrap()).await;
    assert!(result.chats[0].created_at < ahead);
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
    assert_eq!(counts(&again.stats.borrow()), (0, 0, 0));
    assert_eq!(bodies(&*next(&again).await), ["hello"]);
}

#[tokio::test]
async fn a_follower_behind_the_log_queries_every_room_again() {
    let options = Options {
        log_capacity: 2,
        ..Options::default()
    };
    let chat = Chat::open_with(&temp_db("lag"), options).unwrap();
    let a = chat.watch("a").unwrap();
    let mut b = chat.watch("b").unwrap();
    {
        // An outstanding borrow blocks the watch's sender, so the follower
        // stalls on the first commit it checks while the log moves on.
        let _stall = b.stats.borrow();
        for i in 0..5 {
            chat.post("a", "ann", &format!("chat {i}")).unwrap();
        }
    }
    let result = next_until(&a, |r| r.chats.first().is_some_and(|c| c.body == "chat 4")).await;
    assert_eq!(result.chats.len(), 5);
    // No commit touched room b, yet it re-ran when the follower caught up,
    // and found its rows unchanged.
    until(&mut b.stats, |s| s.unchanged >= 1).await;
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
    let result = next(&chat.watch("a").unwrap()).await;
    assert_eq!(bodies(&result), ["chat 2", "chat 1", "chat 0"]);
}

/// A request whose response a test reads in steps.
struct Response {
    stream: tokio::net::TcpStream,
    text: String,
}

async fn request(port: u16, request: &str) -> Response {
    let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .unwrap();
    stream.write_all(request.as_bytes()).await.unwrap();
    Response {
        stream,
        text: String::new(),
    }
}

impl Response {
    /// Reads until the response holds `want`.
    async fn until(&mut self, want: &str) {
        let mut buffer = [0u8; 4096];
        let (stream, text) = (&mut self.stream, &mut self.text);
        tokio::time::timeout(Duration::from_secs(5), async {
            while !text.contains(want) {
                let n = stream.read(&mut buffer).await.unwrap();
                assert!(n > 0, "the connection closed before {want:?}: {text}");
                text.push_str(&String::from_utf8_lossy(&buffer[..n]));
            }
        })
        .await
        .unwrap_or_else(|_| panic!("no {want:?} in time: {}", self.text));
    }
}

/// Sends a request and reads until the response holds `want`.
async fn http(port: u16, request_text: String, want: &str) -> String {
    let mut response = request(port, &request_text).await;
    response.until(want).await;
    response.text
}

fn post(room: &str, author: &str, body: &str) -> String {
    let json = format!(r#"{{"author":"{author}","body":"{body}"}}"#);
    format!(
        "POST /rooms/{room}/chats HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{json}",
        json.len()
    )
}

/// Serves `chat` on a free port and returns the port.
async fn serve(chat: &Arc<Chat>) -> u16 {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(axum::serve(listener, router(chat.clone())).into_future());
    port
}

#[tokio::test]
async fn the_page_and_routes_serve_over_http() {
    let chat = Arc::new(Chat::open(&temp_db("http")).unwrap());
    let port = serve(&chat).await;

    let page = http(
        port,
        "GET / HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n".into(),
        "</html>",
    )
    .await;
    assert!(page.starts_with("HTTP/1.1 200"), "{page}");
    assert!(page.contains("10 most recent chats"));

    let mut live = request(port, "GET /rooms/lobby/live HTTP/1.1\r\nHost: x\r\n\r\n").await;
    live.until("event: stats").await;
    let posted = http(port, post("lobby", "ann", "over http"), "\"version\"").await;
    assert!(posted.starts_with("HTTP/1.1 200"), "{posted}");
    live.until("\"body\":\"over http\"").await;
    assert!(live.text.contains("text/event-stream"), "{}", live.text);
    assert!(live.text.contains("retry: 1000"), "{}", live.text);
    // Each result carries its rows' hash as its id.
    let held = next(&chat.watch("lobby").unwrap()).await.hash;
    assert!(live.text.contains(&format!("id: {held}")), "{}", live.text);

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

/// A browser that reconnects, as after a deploy, sends the id of the last
/// result it received and gets rows only once they change.
#[tokio::test]
async fn a_reconnecting_browser_gets_rows_only_when_they_changed() {
    let chat = Arc::new(Chat::open(&temp_db("reconnect")).unwrap());
    let port = serve(&chat).await;
    chat.post("lobby", "ann", "before").unwrap();
    let held = next(&chat.watch("lobby").unwrap()).await.hash;

    let mut resumed = request(
        port,
        &format!("GET /rooms/lobby/live HTTP/1.1\r\nHost: x\r\nLast-Event-ID: {held}\r\n\r\n"),
    )
    .await;
    resumed.until("event: stats").await;
    http(port, post("lobby", "bo", "after"), "\"version\"").await;
    resumed.until("\"body\":\"after\"").await;
    assert_eq!(
        resumed.text.matches("event: result").count(),
        1,
        "only the changed rows arrive: {}",
        resumed.text
    );
}

#[tokio::test]
async fn closing_the_chat_ends_every_live_stream() {
    let chat = Arc::new(Chat::open(&temp_db("closing")).unwrap());
    let port = serve(&chat).await;
    let mut live = request(port, "GET /rooms/lobby/live HTTP/1.1\r\nHost: x\r\n\r\n").await;
    live.until("event: result").await;
    chat.close();
    // The response's last chunk.
    live.until("\r\n0\r\n\r\n").await;
}
