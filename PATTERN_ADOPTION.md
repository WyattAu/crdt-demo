# PATTERN_ADOPTION

Crdt-demo's declared relationship to the WyattAu pattern canon
(`pattern-standards`, references/catalog.md). Machine-checked by
`pattern-standards/scripts/adoption-check.py`.

| Pattern | Status | Evidence |
|---|---|---|
| T23 | adopted | src/lib.rs:255 (`handle_intent` translates client intent into canonical RGA ops; deletes become idempotent tombstone no-ops), tests/convergence.rs:1 (interleaved concurrent edits converge on every replica, including a late joiner replaying the op log), static/app.js (browser mirror replica) |
