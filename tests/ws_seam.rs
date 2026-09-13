//! ws-kit seam tests: the `TokenExtractor` boundary between the raw query
//! string and the display name the server assigns.
//!
//! ws-kit percent-decodes query values, so tokens can be arbitrary UTF-8 —
//! these tests pin the name-sanitization contract (truncate, guest fallback)
//! including a multi-byte token that must not panic the ws handler.

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use tokio::net::TcpStream;
use tokio_tungstenite::{connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream};

use serde_json::Value;

type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;

async fn spawn_server() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = crdt_demo::router(Arc::new(crdt_demo::AppState::new()));
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    format!("ws://{addr}/ws")
}

async fn connect(url: &str) -> Ws {
    let (ws, _) = connect_async(url).await.unwrap();
    ws
}

/// Receives JSON messages until one is a `presence` roster with exactly
/// `participants` entries, then returns the names.
async fn presence_with(ws: &mut Ws, participants: usize) -> Vec<String> {
    let deadline = tokio::time::Duration::from_secs(5);
    loop {
        let msg = tokio::time::timeout(deadline, ws.next())
            .await
            .expect("timed out waiting for presence")
            .expect("connection closed")
            .unwrap();
        if let Message::Text(t) = msg {
            let v: Value = serde_json::from_str(t.as_str()).unwrap();
            if v["type"] == "presence" {
                let roster = v["participants"].as_array().unwrap();
                if roster.len() == participants {
                    return roster
                        .iter()
                        .map(|p| p[1].as_str().unwrap().to_string())
                        .collect();
                }
            }
        }
    }
}

#[tokio::test]
async fn absent_empty_and_blank_tokens_become_guest() {
    let url = spawn_server().await;

    // Listener holds the roster stable while the guests connect.
    // (It connected without a token, so it counts as a guest too.)
    let mut listener = connect(&url).await;

    // No query at all.
    let mut guest_a = connect(&url).await;
    // Empty token value.
    let mut guest_b = connect(&format!("{url}?token=")).await;
    // Whitespace-only token (decodes to " ").
    let mut guest_c = connect(&format!("{url}?token=%20")).await;

    let names = presence_with(&mut listener, 4).await;
    assert_eq!(names.iter().filter(|n| *n == "guest").count(), 4);
    assert_eq!(names.len(), 4);

    guest_a.close(None).await.unwrap();
    guest_b.close(None).await.unwrap();
    guest_c.close(None).await.unwrap();
}

#[tokio::test]
async fn long_ascii_token_truncated_to_24_bytes() {
    let url = spawn_server().await;
    let mut listener = connect(&url).await;

    let long = "A".repeat(40);
    let mut named = connect(&format!("{url}?token={long}")).await;

    let names = presence_with(&mut listener, 2).await;
    assert!(
        names.contains(&"A".repeat(24)),
        "expected 24-byte truncation, got {names:?}"
    );

    named.close(None).await.unwrap();
}

#[tokio::test]
async fn multibyte_token_truncated_at_char_boundary() {
    let url = spawn_server().await;
    let mut listener = connect(&url).await;

    // 7 CJK chars (21 bytes) + é (2 bytes) + 1 CJK char (3 bytes) = 26 bytes.
    // Byte 24 falls inside the final char, so a naive truncate(24) would
    // panic the handler and kill the connection.
    let token = "%E4%BA%BA".repeat(7) + "%C3%A9" + "%E4%BA%BA";
    let mut named = connect(&format!("{url}?token={token}")).await;
    let expected = "人人人人人人人é".to_string();

    let names = presence_with(&mut listener, 2).await;
    assert!(
        names.contains(&expected),
        "expected boundary-safe truncation to {expected:?}, got {names:?}"
    );

    // The named client itself is still connected and functional.
    let init = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let msg = named.next().await.unwrap().unwrap();
            if let Message::Text(t) = msg {
                let v: Value = serde_json::from_str(t.as_str()).unwrap();
                if v["type"] == "init" {
                    return v;
                }
            }
        }
    })
    .await
    .expect("named client never received init");
    assert_eq!(init["site_id"], 2);
}
