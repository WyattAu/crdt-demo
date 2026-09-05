# Deploying crdt-demo

The demo is a single static binary (axum + tokio) with the frontend embedded
at compile time — no runtime assets, no database. It binds `0.0.0.0` on
`$PORT` (default `8080`).

## Prereq: build the image

The repo root contains a multi-stage `Dockerfile` (rust:1.85-slim →
debian:bookworm-slim, non-root user, ~118 MB):

```sh
docker build -t crdt-demo .
```

## Option A: Fly.io

`fly.toml` in this repo defines the app (`crdt-demo`, smallest VM:
`shared-cpu-1x` / 256 MB, auto-stop when idle).

```sh
fly launch --no-deploy        # first time only; it will pick up fly.toml
fly deploy
fly status
```

The live URL will be `https://crdt-demo.fly.dev` (or the hostname `fly
launch` assigned).

## Option B: Render.com

`render.yaml` in this repo is a Render blueprint (Docker runtime, health
check on `/`).

```sh
render blueprint launch       # from this directory
```

or push the repo to GitHub and create the service via
**New + → Blueprint**; Render detects `render.yaml` automatically. Render
injects `$PORT` itself — the binary honors it, no config needed.

## Option C: local Docker

```sh
docker build -t crdt-demo .
docker run --rm -p 8080:8080 crdt-demo
# open http://localhost:8080 in two tabs

# custom port:
docker run --rm -e PORT=9090 -p 9090:9090 crdt-demo
```

## Health checks

There is no dedicated `/healthz` endpoint. `GET /` returns **200** with the
page HTML and serves as the liveness probe — `fly.toml`'s
`http_service.checks` and `render.yaml`'s `healthCheckPath` are both
configured to use it.

Routes: `GET /` (page), `GET /app.js`, `GET /ws` (WebSocket).
