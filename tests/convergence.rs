//! End-to-end convergence: real WebSocket clients against a server bound to
//! an ephemeral port (`127.0.0.1:0`).
//!
//! Each client mirrors the browser replica exactly — apply every canonical op
//! it receives to a crdts-kit [`RgaString`] — and after interleaved and
//! concurrent edits, every replica (including a late joiner replaying the op
//! log) must render identical text.

use std::sync::Arc;
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio::net::TcpStream;
use tokio_tungstenite::{connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream};

use crdts_kit::text::{OperationId, RgaString, TextOperation};

type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// Boots the demo on an ephemeral port and returns the `ws://` endpoint.
async fn spawn_server() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = crdt_demo::router(Arc::new(crdt_demo::AppState::new()));
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    format!("ws://{addr}/ws")
}

/// A mirror of the browser replica in `static/app.js`: the same integration
/// rule (start after `origin_left`, skip nodes with greater ids) applied to a
/// flat node list. Exercising this against the real server validates the exact
/// algorithm the shipped client runs.
#[derive(Default)]
struct Replica {
    nodes: Vec<(OperationId, char, bool)>,
}

impl Replica {
    fn apply(&mut self, op: &TextOperation) {
        match op {
            TextOperation::Insert {
                id,
                content,
                origin_left,
                ..
            } => {
                let ch = content.chars().next().unwrap();
                let mut pos = 0;
                if let Some(left) = origin_left {
                    pos = match self.nodes.iter().position(|(nid, _, _)| nid == left) {
                        Some(i) => i + 1,
                        None => 0,
                    };
                }
                while pos < self.nodes.len() && self.nodes[pos].0 > *id {
                    pos += 1;
                }
                self.nodes.insert(pos, (id.clone(), ch, false));
            }
            TextOperation::Delete { target, .. } => {
                if let Some(node) = self.nodes.iter_mut().find(|(nid, _, _)| nid == target) {
                    node.2 = true;
                }
            }
        }
    }

    fn text(&self) -> String {
        self.nodes
            .iter()
            .filter(|(_, _, t)| !t)
            .map(|(_, c, _)| c)
            .collect()
    }

    fn id_at(&self, visible: usize) -> OperationId {
        self.nodes
            .iter()
            .filter(|(_, _, t)| !t)
            .nth(visible)
            .expect("no visible char at position")
            .0
            .clone()
    }
}

struct Client {
    ws: Ws,
    replica: Replica,
    reference: RgaString,
    site: u32,
}

impl Client {
    /// Connects and consumes the `init` snapshot (skipping presence traffic).
    async fn connect(url: &str) -> Self {
        let (ws, _) = connect_async(url).await.unwrap();
        let mut client = Client {
            ws,
            replica: Replica::default(),
            reference: RgaString::new(),
            site: 0,
        };
        loop {
            let v = client.recv_json().await;
            if v["type"] == "init" {
                client.site = v["site_id"].as_u64().unwrap() as u32;
                for op in v["ops"].as_array().unwrap() {
                    client.apply(op);
                }
                break;
            }
        }
        client
    }

    /// Feeds one canonical op to both the JS-algorithm mirror and crdts-kit
    /// itself, so the two can be cross-checked.
    fn apply(&mut self, op: &Value) {
        let op: TextOperation = serde_json::from_value(op.clone()).unwrap();
        self.replica.apply(&op);
        self.reference.apply(&op);
    }

    async fn recv_json(&mut self) -> Value {
        loop {
            let msg = tokio::time::timeout(Duration::from_secs(5), self.ws.next())
                .await
                .expect("timed out waiting for server message")
                .expect("connection closed")
                .unwrap();
            if let Message::Text(t) = msg {
                if let Ok(v) = serde_json::from_str::<Value>(t.as_str()) {
                    return v;
                }
            }
        }
    }

    async fn send(&mut self, msg: Value) {
        self.ws.send(Message::text(msg.to_string())).await.unwrap();
    }

    async fn send_insert(&mut self, pos: usize, text: &str) {
        self.send(json!({"type": "insertAt", "pos": pos, "text": text}))
            .await;
    }

    /// Sends the delete intent the browser would send: the absolute id of the
    /// character at visible index `pos`, per the client's own replica.
    async fn send_delete(&mut self, pos: usize) {
        let target = self.replica.id_at(pos);
        self.send(json!({
            "type": "deleteChar",
            "target": {"site_id": target.site_id, "counter": target.counter},
        }))
        .await;
    }

    /// Integrates canonical ops until `expected` more have arrived.
    async fn drain(&mut self, expected: usize) {
        let mut seen = 0;
        while seen < expected {
            let v = self.recv_json().await;
            if v["type"] == "op" {
                self.apply(&v["op"]);
                seen += 1;
            }
        }
    }

    fn text(&self) -> String {
        assert_eq!(
            self.replica.text(),
            self.reference.text(),
            "JS-port and crdts-kit replicas diverged"
        );
        self.replica.text()
    }
}

#[tokio::test]
async fn sequential_edits_converge() {
    let url = spawn_server().await;
    let mut a = Client::connect(&url).await;
    let mut b = Client::connect(&url).await;
    assert_eq!(a.site, 1);
    assert_eq!(b.site, 2);

    a.send_insert(0, "Hello").await;
    a.drain(5).await;
    b.drain(5).await;
    assert_eq!(a.text(), "Hello");
    assert_eq!(b.text(), "Hello");

    b.send_insert(5, "!").await;
    a.drain(1).await;
    b.drain(1).await;

    assert_eq!(a.text(), "Hello!");
    assert_eq!(b.text(), "Hello!");

    let c = Client::connect(&url).await;
    assert_eq!(c.site, 3);
    assert_eq!(
        c.text(),
        "Hello!",
        "late joiner replayed the op log divergently"
    );
}

#[tokio::test]
async fn concurrent_same_position_inserts_converge() {
    let url = spawn_server().await;
    let mut a = Client::connect(&url).await;
    let mut b = Client::connect(&url).await;

    a.send_insert(0, "X").await;
    b.send_insert(0, "Y").await;

    a.drain(2).await;
    b.drain(2).await;

    let text = a.text();
    assert_eq!(text, b.text(), "replicas diverged after concurrent inserts");
    assert_eq!(text.chars().count(), 2);
    assert!(
        text.contains('X') && text.contains('Y'),
        "lost a concurrent insert"
    );

    let c = Client::connect(&url).await;
    assert_eq!(c.text(), text);
}

#[tokio::test]
async fn concurrent_deletes_converge() {
    let url = spawn_server().await;
    let mut a = Client::connect(&url).await;
    a.send_insert(0, "Hello").await;
    a.drain(5).await;

    let mut b = Client::connect(&url).await;
    assert_eq!(
        b.text(),
        "Hello",
        "late joiner already has the doc from init"
    );

    // Both tabs backspace the same first character ('H') at the same moment;
    // id-targeted deletes make the second one an idempotent no-op.
    a.send_delete(0).await;
    b.send_delete(0).await;

    a.drain(2).await;
    b.drain(2).await;

    assert_eq!(a.text(), "ello");
    assert_eq!(
        b.text(),
        "ello",
        "replicas diverged after concurrent deletes"
    );
}

#[tokio::test]
async fn three_way_concurrent_burst_converges() {
    let url = spawn_server().await;
    let mut a = Client::connect(&url).await;
    a.send_insert(0, "crdt").await;
    a.drain(4).await;

    let mut b = Client::connect(&url).await;
    let mut c = Client::connect(&url).await;
    assert_eq!(b.text(), "crdt", "late joiner gets state from init");
    assert_eq!(c.text(), "crdt");

    b.send_insert(4, "-kit").await;
    b.drain(4).await;
    a.drain(4).await;
    c.drain(4).await;

    a.send_insert(0, "> ").await;
    a.drain(2).await;
    b.drain(2).await;
    c.drain(2).await;

    assert_eq!(a.text(), "> crdt-kit");
    assert_eq!(b.text(), "> crdt-kit");
    assert_eq!(c.text(), "> crdt-kit");

    // True three-way concurrency: everyone types at a different position
    // without waiting for the other two.
    a.send_insert(2, "A").await;
    b.send_insert(10, "B").await;
    c.send_insert(5, "C").await;

    a.drain(3).await;
    b.drain(3).await;
    c.drain(3).await;

    let text = a.text();
    assert_eq!(text.chars().count(), 13);
    assert_eq!(text, b.text());
    assert_eq!(text, c.text());
    assert!(text.contains('A') && text.contains('B') && text.contains('C'));
}

#[tokio::test]
async fn token_query_becomes_display_name() {
    let url = spawn_server().await;
    let mut a = Client::connect(&url).await;
    let _b = tokio_tungstenite::connect_async(format!("{url}?token=Alice"))
        .await
        .unwrap();

    for _ in 0..10 {
        let v = a.recv_json().await;
        if v["type"] == "presence" {
            let names: Vec<String> = v["participants"]
                .as_array()
                .unwrap()
                .iter()
                .map(|p| p[1].as_str().unwrap().to_string())
                .collect();
            if names.iter().any(|n| n == "Alice") {
                return;
            }
        }
    }
    panic!("roster never contained Alice");
}
