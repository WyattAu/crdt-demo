//! crdt-demo — live multi-user collaborative editing over WebSockets.
//!
//! Architecture: **server-translated intents**. Each browser holds a mirror
//! replica (a small JS port of the RGA integration rule) and sends *intents*:
//! inserts carry a visible position (resolved by the server against its
//! replica), deletes carry the absolute character id (idempotent under
//! concurrency). The server owns the authoritative [`CrdtDocument`]
//! (crdts-kit), translates every intent into canonical RGA operations,
//! appends them to an in-memory op log, and fans them out to every connected
//! tab (ws-kit `Room`). Clients — the author included — integrate the
//! canonical ops and re-render.
//!
//! Why not generate ops on the client? The translation point gives every op a
//! single causal order for free: intents are serialized by the document mutex
//! and broadcast while it is held, and tokio broadcast channels are FIFO per
//! receiver — so an op's origins always reach a client before the op that
//! references them. Convergence does not depend on this ordering (RGA
//! integration is order-independent), but it keeps the demo client minimal.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{RawQuery, State};
use axum::http::{header, request::Parts};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use crdts_kit::document::{CrdtDocument, DocumentId, ParticipantId};
use crdts_kit::text::{OperationId, TextOperation};
use futures::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use ws_kit::extractor::TokenExtractor;
use ws_kit::room::{Room, RoomManager};

/// Shared server state: one document, its op log, the fan-out rooms, and a
/// site-id dispenser.
pub struct AppState {
    doc: Mutex<CrdtDocument>,
    log: Mutex<Vec<TextOperation>>,
    rooms: RoomManager,
    extractor: TokenExtractor,
    next_site: AtomicU32,
}

/// Thread-shared handle to [`AppState`].
pub type SharedState = Arc<AppState>;

impl Default for AppState {
    fn default() -> Self {
        Self::new()
    }
}

impl AppState {
    /// Creates the demo state: a single empty document in room `"main"`.
    pub fn new() -> Self {
        let doc = CrdtDocument::new(DocumentId("main".to_string()));
        Self {
            doc: Mutex::new(doc),
            log: Mutex::new(Vec::new()),
            rooms: RoomManager::new(),
            extractor: TokenExtractor::default(),
            next_site: AtomicU32::new(1),
        }
    }
}

/// Messages a client may send.
///
/// Inserts are *positional intents* (resolved by the server against its
/// replica); deletes are *id-targeted* (absolute, so a concurrent delete of
/// an already-deleted character is an idempotent no-op everywhere).
#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum ClientMsg {
    /// Insert `text` at visible index `pos` of the sender's current view.
    InsertAt {
        /// Visible character index in the sender's view.
        pos: usize,
        /// Text to insert (usually a single character).
        text: String,
    },
    /// Tombstone the character with identity `target`.
    DeleteChar {
        /// Identity of the character to delete.
        target: OperationId,
    },
}

/// Messages the server sends.
#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum ServerMsg {
    /// Site assignment plus the full op log (join snapshot).
    Init {
        /// Site id assigned to this connection.
        site_id: u32,
        /// Every operation ever applied to the document, in causal order.
        ops: Vec<TextOperation>,
    },
    /// A canonical RGA operation (sent to every tab, author included).
    Op {
        /// Site that authored the intent.
        from: u32,
        /// The canonical operation.
        op: TextOperation,
    },
    /// Presence roster update.
    Presence {
        /// `(site_id, name)` for every connected tab.
        participants: Vec<(u32, String)>,
    },
}

/// Builds the demo router: embedded static frontend plus the WebSocket route.
pub fn router(state: SharedState) -> Router {
    Router::new()
        .route("/", get(index))
        .route("/app.js", get(app_js))
        .route("/ws", get(ws_handler))
        .with_state(state)
}

async fn index() -> Html<&'static str> {
    Html(include_str!("../static/index.html"))
}

#[allow(clippy::unused_async)]
async fn app_js() -> Response {
    (
        [(header::CONTENT_TYPE, "application/javascript")],
        include_str!("../static/app.js"),
    )
        .into_response()
}

#[allow(clippy::unused_async)]
async fn ws_handler(
    State(state): State<SharedState>,
    RawQuery(query): RawQuery,
    parts: Parts,
    ws: WebSocketUpgrade,
) -> Response {
    // Unauthenticated demo: ws-kit's TokenExtractor picks a display name
    // (?token=Alice); authentication itself is deliberately out of scope.
    let name = state
        .extractor
        .extract_token(&parts, query.as_deref().unwrap_or(""))
        .map(|t| {
            let mut t = t.trim().to_string();
            t.truncate(24);
            if t.is_empty() {
                "guest".to_string()
            } else {
                t
            }
        })
        .unwrap_or_else(|| "guest".to_string());
    let site = state.next_site.fetch_add(1, Ordering::Relaxed);
    ws.on_upgrade(move |socket| handle_socket(socket, state, site, name))
}

async fn handle_socket(socket: WebSocket, state: SharedState, site: u32, name: String) {
    let (mut tx, mut rx) = socket.split();
    let room: Arc<Room> = state.rooms.get_or_create("main");
    let mut bcast = room.subscribe();

    let init = {
        let mut doc = state.doc.lock().unwrap();
        doc.join(ParticipantId(site), &name);
        let ops = state.log.lock().unwrap().clone();
        ServerMsg::Init { site_id: site, ops }
    };
    if tx
        .send(Message::Text(serde_json::to_string(&init).unwrap().into()))
        .await
        .is_err()
    {
        return;
    }

    tracing::info!(site, name, "joined");
    broadcast_presence(&state, &room);

    loop {
        tokio::select! {
            item = bcast.recv() => match item {
                Ok(text) => {
                    if tx.send(Message::Text(text.into())).await.is_err() {
                        break;
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                    // Missed ops cannot be patched incrementally; drop the
                    // client and let it reconnect for a fresh snapshot.
                    tracing::warn!(site, lagged = n, "client lagged; forcing reconnect");
                    break;
                }
                Err(_) => break,
            },
            item = rx.next() => match item {
                Some(Ok(Message::Text(text))) => {
                    match serde_json::from_str::<ClientMsg>(&text) {
                        Ok(intent) => handle_intent(&state, &room, site, intent),
                        Err(e) => tracing::warn!(site, error = %e, "malformed client message"),
                    }
                }
                Some(Ok(Message::Close(_))) | None | Some(Err(_)) => break,
                Some(Ok(_)) => {}
            },
        }
    }

    state.doc.lock().unwrap().leave(&ParticipantId(site));
    tracing::info!(site, name, "left");
    broadcast_presence(&state, &room);
}

/// Translates one client intent into canonical RGA ops and fans them out.
///
/// Broadcast happens while the document lock is held, so op generation and
/// fan-out are atomic: every receiver observes the same causal order.
fn handle_intent(state: &SharedState, room: &Room, site: u32, intent: ClientMsg) {
    let mut doc = state.doc.lock().unwrap();
    let participant = ParticipantId(site);
    let ops = match intent {
        ClientMsg::InsertAt { pos, text } => doc.insert_text(participant, pos, &text).0,
        ClientMsg::DeleteChar { target } => {
            // Deletes need no position translation: the tombstone targets an
            // absolute character id. The delete op's own id is only bookkeeping
            // (it never becomes a node); the document version keeps it unique.
            let id = OperationId {
                site_id: participant.0,
                counter: doc.version + 1,
            };
            let op = TextOperation::Delete { id, target };
            doc.apply_ops(std::slice::from_ref(&op));
            vec![op]
        }
    };
    state.log.lock().unwrap().extend(ops.iter().cloned());
    for op in ops {
        let msg = ServerMsg::Op { from: site, op };
        let _ = room.broadcast(serde_json::to_string(&msg).unwrap());
    }
    tracing::debug!(site, "intent applied");
}

fn broadcast_presence(state: &SharedState, room: &Room) {
    let participants = roster(&state.doc.lock().unwrap());
    let msg = ServerMsg::Presence { participants };
    let _ = room.broadcast(serde_json::to_string(&msg).unwrap());
}

fn roster(doc: &CrdtDocument) -> Vec<(u32, String)> {
    let mut list: Vec<(u32, String)> = doc
        .participants
        .iter()
        .map(|(id, info)| (id.0, info.name.clone()))
        .collect();
    list.sort();
    list
}
