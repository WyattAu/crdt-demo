//! Binary entry point: tracing init, bind, serve.

use std::sync::Arc;

use crdt_demo::{router, AppState, SharedState};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let state: SharedState = Arc::new(AppState::new());
    let app = router(state);

    let port: u16 = std::env::var("PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(8080);
    let listener = tokio::net::TcpListener::bind(("0.0.0.0", port))
        .await
        .expect("failed to bind");
    tracing::info!("crdt-demo listening on http://localhost:{port} — open it in two tabs");

    axum::serve(listener, app).await.expect("server error");
}
