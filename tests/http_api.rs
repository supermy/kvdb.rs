use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

use kvdb_rs::http_server::AppState;
use kvdb_rs::{Config, ConfigManager, StorageEngine};

fn build_config(dir: &tempfile::TempDir) -> Config {
    let mut config = Config::default();
    config.storage.db_path = dir.path().join("db").to_string_lossy().to_string();
    config.server.bind = "127.0.0.1:0".to_string();
    config.server.http_bind = "127.0.0.1:0".to_string();
    config
}

fn build_router(dir: &tempfile::TempDir) -> (axum::Router, Config) {
    let config = build_config(dir);
    let config_mgr = Arc::new(ConfigManager::new(config.clone()));
    let storage = Arc::new(StorageEngine::open(&config.storage.db_path, &config).unwrap());
    let state = Arc::new(AppState {
        config: config_mgr,
        storage,
    });
    let app = axum::Router::new()
        .route("/health", axum::routing::get(kvdb_rs::http_server::health))
        .route(
            "/config",
            axum::routing::get(kvdb_rs::http_server::get_config),
        )
        .route("/stats", axum::routing::get(kvdb_rs::http_server::stats))
        .route(
            "/metrics",
            axum::routing::get(kvdb_rs::http_server::metrics),
        )
        .with_state(state);
    (app, config)
}

#[tokio::test]
async fn http_health_returns_ok() {
    let dir = tempfile::tempdir().unwrap();
    let (app, _) = build_router(&dir);
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn http_config_returns_json() {
    let dir = tempfile::tempdir().unwrap();
    let (app, config) = build_router(&dir);
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/config")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["server"]["bind"].as_str().unwrap(), config.server.bind);
}

#[tokio::test]
async fn http_metrics_contains_up() {
    let dir = tempfile::tempdir().unwrap();
    let (app, _) = build_router(&dir);
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/metrics")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let text = String::from_utf8(body.to_vec()).unwrap();
    assert!(text.contains("kvdb_up 1"));
}
