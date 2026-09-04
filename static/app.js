"use strict";

const ta = document.getElementById("editor");
const siteBadge = document.getElementById("site");
const statusBadge = document.getElementById("status");
const peersBadge = document.getElementById("peers");
const opsBadge = document.getElementById("ops");

let nodes = [];
let applied = 0;
let ws = null;

const cmp = (a, b) => (a.site_id !== b.site_id ? a.site_id - b.site_id : a.counter - b.counter);
const idEq = (a, b) => a.site_id === b.site_id && a.counter === b.counter;
const text = () => nodes.filter((n) => !n.tomb).map((n) => n.ch).join("");

function insertPos(id, originLeft) {
  let pos = 0;
  if (originLeft) {
    const i = nodes.findIndex((n) => idEq(n.id, originLeft));
    if (i === -1) return 0;
    pos = i + 1;
  }
  while (pos < nodes.length && cmp(nodes[pos].id, id) > 0) pos++;
  return pos;
}

function visibleBefore(i) {
  let v = 0;
  for (let k = 0; k < i; k++) if (!nodes[k].tomb) v++;
  return v;
}

function applyOp(envelope) {
  const [kind, op] = Object.entries(envelope)[0];
  if (kind === "Insert") {
    const pos = insertPos(op.id, op.origin_left);
    const at = visibleBefore(pos);
    nodes.splice(pos, 0, { id: op.id, ch: op.content, tomb: false });
    shiftCaret(at, op.content.length);
  } else {
    const i = nodes.findIndex((n) => idEq(n.id, op.target));
    if (i !== -1 && !nodes[i].tomb) {
      const at = visibleBefore(i);
      nodes[i].tomb = true;
      shiftCaret(at, -1);
    }
  }
  applied++;
}

function shiftCaret(at, delta) {
  const s = ta.selectionStart;
  const e = ta.selectionEnd;
  if (s > at) ta.setSelectionRange(s + delta, e + delta);
}

function render() {
  const s = ta.selectionStart;
  const e = ta.selectionEnd;
  ta.value = text();
  ta.setSelectionRange(s, e);
  opsBadge.textContent = applied;
}

function send(msg) {
  if (ws && ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(msg));
}

function idAt(pos) {
  let v = -1;
  for (const n of nodes) {
    if (!n.tomb) {
      v++;
      if (v === pos) return n.id;
    }
  }
  return null;
}

const delOne = (pos) => {
  const id = idAt(pos);
  if (id) send({ type: "deleteChar", target: id });
};

ta.addEventListener("beforeinput", (e) => {
  e.preventDefault();
  const start = ta.selectionStart;
  const end = ta.selectionEnd;
  const delRange = () => {
    for (let i = end - 1; i >= start; i--) delOne(i);
  };
  if (e.inputType.startsWith("insert") && e.data) {
    delRange();
    send({ type: "insertAt", pos: start, text: e.data });
  } else if (e.inputType === "insertLineBreak" || e.inputType === "insertParagraph") {
    delRange();
    send({ type: "insertAt", pos: start, text: "\n" });
  } else if (e.inputType === "deleteContentBackward") {
    if (start === end && start > 0) delOne(start - 1);
    else delRange();
  } else if (e.inputType === "deleteContentForward") {
    if (start === end && end < ta.value.length) delOne(start);
    else delRange();
  } else if (e.inputType === "deleteByCut") {
    delRange();
  }
});

function connect() {
  const proto = location.protocol === "https:" ? "wss" : "ws";
  ws = new WebSocket(`${proto}://${location.host}/ws${location.search}`);
  ws.onopen = () => {
    statusBadge.textContent = "live";
    statusBadge.className = "live";
    ta.disabled = false;
    ta.focus();
  };
  ws.onmessage = (ev) => {
    const m = JSON.parse(ev.data);
    if (m.type === "init") {
      nodes = [];
      applied = 0;
      siteBadge.textContent = "site #" + m.site_id;
      for (const op of m.ops) applyOp(op);
      render();
      const n = ta.value.length;
      ta.setSelectionRange(n, n);
      return;
    }
    if (m.type === "op") applyOp(m.op);
    if (m.type === "presence") peersBadge.textContent = m.participants.map(([, n]) => n).join(", ") || "—";
    render();
  };
  ws.onclose = () => {
    statusBadge.textContent = "reconnecting…";
    statusBadge.className = "down";
    ta.disabled = true;
    setTimeout(connect, 800);
  };
}

render();
connect();
