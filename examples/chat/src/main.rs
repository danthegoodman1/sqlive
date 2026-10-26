//! Serves the live chat on 127.0.0.1:3000, keeping chats in `chat.db`.
//!
//!     cargo run -p chat-example
//!
//! `CHAT_DB` and `CHAT_ADDR` override the database path and the address.

use chat_example::{Chat, router};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::var("CHAT_DB").unwrap_or_else(|_| "chat.db".to_owned());
    let addr = std::env::var("CHAT_ADDR").unwrap_or_else(|_| "127.0.0.1:3000".to_owned());
    let chat = Arc::new(Chat::open(&path)?);
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    println!("live chat on http://{addr}, chats in {path}");
    axum::serve(listener, router(chat)).await?;
    Ok(())
}
