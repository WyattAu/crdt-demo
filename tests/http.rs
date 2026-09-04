//! Static frontend routes: the embedded index page and client script.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use std::sync::Arc;
use tower::ServiceExt;

#[tokio::test]
async fn index_is_served() {
    let app = crdt_demo::router(Arc::new(crdt_demo::AppState::new()));
    let res = app
        .oneshot(Request::get("/").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    let body = String::from_utf8_lossy(&body);
    assert!(body.contains("crdt-demo"), "index should mention the demo");
    assert!(
        body.contains("/app.js"),
        "index should load the client script"
    );
}

#[tokio::test]
async fn app_js_is_served() {
    let app = crdt_demo::router(Arc::new(crdt_demo::AppState::new()));
    let res = app
        .oneshot(Request::get("/app.js").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(
        res.headers()["content-type"],
        "application/javascript",
        "app.js must be served as JavaScript"
    );
    let body = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    assert!(
        String::from_utf8_lossy(&body).contains("insertAt"),
        "client script should speak the intent protocol"
    );
}
