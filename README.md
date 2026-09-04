# crdt-demo

> Live multi-user collaborative editing on an RGA text CRDT — **one Rust binary, zero frontend framework**. `cargo run`, open two tabs, type in both, watch every keystroke converge.

Powered by two small crates:

- [`crdts-kit`](https://crates.io/crates/crdts-kit) — RGA replicated string: per-character identity, tombstoned deletes, origin-based sibling ordering ([repo](https://github.com/WyattAu/crdts-kit))
- [`ws-kit`](https://crates.io/crates/ws-kit) — broadcast rooms, connection presence, token extraction for axum WebSockets ([repo](https://github.com/WyattAu/ws-kit))

## 15-second quickstart

```bash
cargo run
# open http://localhost:8080 in two (or more) tabs — type in all of them at once
```

Name yourself by appending `?token=Alice` to the URL (ws-kit's `TokenExtractor` turns the token into a display name; authentication itself is deliberately out of scope). The header shows your site id, the live roster, and a running count of ops applied.

## What you'll see

Every keystroke becomes an RGA operation. Every tab — including the one that typed it — applies the same canonical ops to its own replica and re-renders. Type in two tabs simultaneously and the characters interleave deterministically; delete the same character from two tabs and the second delete is an idempotent no-op. Close a tab and the roster updates; open a new tab and it replays the full op log and catches up instantly.

## Architecture

```
        tab 1 (site #1)             tab 2 (site #2)
   ┌────────────────────┐       ┌────────────────────┐
   │  mirror replica    │       │  mirror replica    │
   │  (mini-RGA in JS)  │       │  (mini-RGA in JS)  │
   └─────▲─────────▲────┘       └─────▲─────────▲────┘
 intent │         │ canonical op     │         │
 {"insertAt",     │ {"Insert",{id,   │         │
  pos,text}       │  origin_left,…}} │         │
   └─────┼─────────┴───────────────────┘         │
         ▼                                       │
      ┌──────────────────────────────────────────┴───┐
      │ axum :8080 (single binary)                   │
      │  ┌────────────────────────────┐              │
      │  │ CrdtDocument  (crdts-kit)  │  intent →    │
      │  │ authoritative replica +    │  canonical   │
      │  │ in-memory op log           │  RGA ops     │
      │  └─────────────┬──────────────┘              │
      │  ┌─────────────▼──────────────┐              │
      │  │ ws-kit Room "main"         │──broadcast──▶│ to every tab
      │  │ (FIFO fan-out + presence)  │              │
      │  └────────────────────────────┘              │
      └──────────────────────────────────────────────┘
```

### The design choice: server-translated intents

Collaborative-editing demos usually pick one of two shapes:

1. **Client-side op generation** — each replica performs the CRDT operation locally and broadcasts the raw op. Maximally decentralized, but the browser must implement local insert, clock management, and origin resolution — the whole CRDT, in JS.
2. **Server-translated intents** (chosen here) — the browser sends *what* it wants (`insertAt pos 3`, `deleteChar #42`) and the server's authoritative `CrdtDocument` translates intents into canonical RGA ops. The browser only implements the *integrate-and-render* half: ~60 lines of JS (the same skip rule crdts-kit uses, ported).

Option 2 is the simpler correct one for a demo, and it gets causal delivery for free: intents are serialized by the document mutex and broadcast while it is held, and tokio broadcast channels are FIFO per receiver — so an op's origins always arrive before any op that references them. Convergence does not depend on this ordering (RGA integration is order-independent), but it lets clients stay dumb.

Two deliberate wrinkles:

- **Inserts are positional intents, deletes are id-targeted.** An insert names a visible index (resolved by the server against its replica); a delete names the character's absolute `OperationId`, so two tabs backspacing the same character converge on a clean no-op rather than eating a neighbor. This split mirrors how real editors behave under concurrency.
- **The author's own ops also come back through the broadcast.** One code path for every client — no local echo, no optimistic UI to reconcile. On localhost the round trip is ~1 ms.

### Protocol

| Direction | Message | Meaning |
|---|---|---|
| client → server | `{"type":"insertAt","pos":3,"text":"x"}` | insert intent at a visible index |
| client → server | `{"type":"deleteChar","target":{"site_id":2,"counter":7}}` | tombstone an absolute character id |
| server → client | `{"type":"init","site_id":9,"ops":[…]}` | site assignment + full op log snapshot |
| server → client | `{"type":"op","from":1,"op":{"Insert":{…}}}` | canonical RGA op, broadcast to all tabs (author included) |
| server → client | `{"type":"presence","participants":[[1,"Alice"]]}` | roster update |

A lagging client that drops ops is disconnected on `RecvError::Lagged` and auto-reconnects, which re-serves the snapshot — resynchronization via reconnect rather than a delta patch.

## Why CRDT?

Merging concurrent edits is the classic distributed-systems trap. Last-write-wins drops characters. Operational Transform needs a central server to transform every op against every other op — and gets the transform functions subtly wrong. Locks destroy the offline, latency-free feel of typing.

A CRDT sidesteps all of it: give every character a permanent identity and a deterministic rule for ordering ties, and every replica that has seen the same set of operations computes the *same text* — no coordination, no leader, no merge conflicts. Order-independent convergence is a mathematical property of the data structure, not a protocol you have to get right at runtime. This demo runs the simplest possible transport (one server, FIFO broadcast), but the replicas would converge just as well over reordered, delayed, or peer-relayed delivery — that's the property the property tests below actually verify.

## The bug proptest found

crdts-kit's RGA ships with a property test that generates thousands of random concurrent edit sequences and delivers every operation to every replica in a *different causally-valid order* ([`tests/proptest.rs`](https://github.com/WyattAu/crdts-kit/blob/main/tests/proptest.rs), `convergence_under_arbitrary_delivery_orders`).

The first cut of the integration rule only ordered *same-origin siblings* by id — the textbook RGA description. The property test caught replicas ending up with different text when the same operations arrived in different interleavings: with chained inserts (a character inserted immediately after another concurrently-inserted character), comparing only siblings of the *same origin* is not enough to make the result arrival-order-independent. The fix is to skip forward past **every** node with a greater id — total order, not sibling order — which is exactly what [`find_insert_position`](https://github.com/WyattAu/crdts-kit/blob/main/crdts-kit/src/text.rs) does now, and what the ~60-line JS port in `static/app.js` mirrors. That one-line-idea fix took the property test from "finds a divergence within seconds" to hundreds of cases per run, every run.

It's a nice Show-HN microstory: the bug wasn't in the network code or the concurrency code — it was in a comment that said "the textbook rule", and only a randomized property test could see it.

## Repo layout

```
crdt-demo/
├── src/
│   ├── lib.rs        # state, wire protocol, intent→op translation, WS handler, router
│   └── main.rs       # tracing + bind :8080 + serve
├── static/
│   ├── index.html    # embedded via include_str! — pediment-token styling
│   └── app.js        # mirror replica: integrate canonical ops, render, send intents
└── tests/
    ├── convergence.rs# real WS clients on 127.0.0.1:0 — interleaved + concurrent convergence
    └── http.rs       # static route smoke tests
```

Tests boot the real server on an ephemeral port and drive it with real WebSocket clients whose replicas mirror the shipped browser code — plus a crdts-kit replica cross-checking every applied op:

```bash
cargo test
```

## Limitations (on purpose)

- Single room (`main`), single node, op log in memory — no persistence, restart = blank doc.
- Keyboard undo/redo and IME composition are out of scope (the textarea is a pure renderer; `beforeinput` is intercepted).
- Positional insert intents can land one character off if your view is stale by the time the server translates them; deletes don't have this problem (they're id-targeted).
- No auth: `?token=` is a costume, not a credential.

## License

MIT OR Apache-2.0
