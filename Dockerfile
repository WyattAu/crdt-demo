# Multi-stage build: compile in Rust toolchain, ship only the binary.
# Static assets are embedded at compile time (include_str!), so the runtime
# image needs nothing but the single executable.

FROM rust:1.85-slim AS build
WORKDIR /app

# Cache dependencies: build with stub sources first, so dependency crates
# are only recompiled when Cargo.toml/Cargo.lock change.
COPY Cargo.toml Cargo.lock ./
RUN mkdir src \
    && echo "" > src/lib.rs \
    && echo "fn main() {}" > src/main.rs \
    && cargo build --release

# Real build: copy sources, bump timestamps so the stub artifacts are replaced.
COPY src src
COPY static static
RUN touch src/lib.rs src/main.rs && cargo build --release


FROM debian:bookworm-slim
RUN groupadd --system --gid 10001 crdt \
    && useradd --system --uid 10001 --gid crdt crdt

COPY --from=build /app/target/release/crdt-demo /usr/local/bin/crdt-demo

ENV PORT=8080
EXPOSE 8080

USER crdt
CMD ["crdt-demo"]
