//! Serves the live chat on 127.0.0.1:3000, keeping chats in `chat.db`.
//!
//!     cargo run -p chat-example
//!
//! `CHAT_DB` and `CHAT_ADDR` override the database path and the address.
//! Ctrl-C or SIGTERM ends every live stream, so browsers reconnect, then
//! stops the server.

use chat_example::{Chat, router};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // SQLite's memory statistics take a global lock on every allocation,
    // which serializes the readers; they must go before SQLite starts.
    // SAFETY: no SQLite connection exists yet.
    unsafe {
        sqlive::rusqlite::ffi::sqlite3_config(sqlive::rusqlite::ffi::SQLITE_CONFIG_MEMSTATUS, 0);
    }
    let path = std::env::var("CHAT_DB").unwrap_or_else(|_| "chat.db".to_owned());
    let addr = std::env::var("CHAT_ADDR").unwrap_or_else(|_| "127.0.0.1:3000".to_owned());
    let chat = Arc::new(Chat::open(&path)?);
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    println!("live chat on http://{addr}, chats in {path}");
    let closing = chat.clone();
    axum::serve(listener, router(chat))
        .with_graceful_shutdown(async move {
            stopped().await;
            // Live streams end, so their browsers reconnect, and the server
            // can finish its open requests and exit.
            closing.close();
        })
        .await?;
    Ok(())
}

/// Waits for Ctrl-C or, on Unix, SIGTERM, which deploy tools send.
async fn stopped() {
    let interrupt = tokio::signal::ctrl_c();
    #[cfg(unix)]
    let terminate = async {
        use tokio::signal::unix::{SignalKind, signal};
        match signal(SignalKind::terminate()) {
            Ok(mut terminate) => {
                terminate.recv().await;
            }
            Err(_) => std::future::pending::<()>().await,
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        _ = interrupt => {}
        () = terminate => {}
    }
}
