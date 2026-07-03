use std::sync::Arc;

use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
};

use crate::config::{Config, ConfigManager};
use crate::storage::StorageEngine;

/// HTTP 管理接口状态：共享配置管理器与存储引擎。
pub struct AppState {
    pub config: Arc<ConfigManager>,
    pub storage: Arc<StorageEngine>,
}

/// 启动 HTTP 管理接口，监听指定地址。
pub async fn serve(
    config: Arc<ConfigManager>,
    storage: Arc<StorageEngine>,
    bind: &str,
) -> anyhow::Result<()> {
    let state = Arc::new(AppState { config, storage });
    let app = Router::new()
        .route("/health", get(health))
        .route("/config", get(get_config).put(put_config))
        .route("/stats", get(stats))
        .route("/metrics", get(metrics))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(bind).await?;
    tracing::info!("http management interface listening on {}", bind);
    axum::serve(listener, app).await?;
    Ok(())
}

/// /health：健康检查，存储引擎可访问时返回 200，否则 503。
pub async fn health(State(state): State<Arc<AppState>>) -> Response {
    // 通过轻量级的 get 探测存储引擎可用性。
    match state.storage.get("metadata", b"").is_ok() {
        true => (StatusCode::OK, "OK\n").into_response(),
        false => (StatusCode::SERVICE_UNAVAILABLE, "Unavailable\n").into_response(),
    }
}

/// /config GET：返回当前运行时配置（JSON）。
pub async fn get_config(State(state): State<Arc<AppState>>) -> Json<Config> {
    Json(state.config.get())
}

/// /config PUT：热更新配置，校验失败时回滚并返回 400。
pub async fn put_config(
    State(state): State<Arc<AppState>>,
    Json(new_config): Json<Config>,
) -> Response {
    match state.config.update(new_config) {
        Ok(()) => (StatusCode::OK, "OK\n").into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, format!("{e}\n")).into_response(),
    }
}

/// /stats：返回 RocksDB 内部统计信息（属性字符串）。
pub async fn stats(State(state): State<Arc<AppState>>) -> Response {
    // 优先读取 "rocksdb.stats" 属性；不可用时返回空。
    let stats = state
        .storage
        .property_value("rocksdb.stats")
        .unwrap_or_default()
        .unwrap_or_else(|| "rocksdb.stats not available".to_string());
    (StatusCode::OK, stats).into_response()
}

/// /metrics：Prometheus 格式基础指标。
pub async fn metrics(State(state): State<Arc<AppState>>) -> Response {
    let cfg = state.config.get();
    let body = format!(
        "# HELP kvdb_up Server is up\n\
         # TYPE kvdb_up gauge\n\
         kvdb_up 1\n\
         # HELP kvdb_maxclients Maximum number of clients\n\
         # TYPE kvdb_maxclients gauge\n\
         kvdb_maxclients {}\n\
         # HELP kvdb_db_path Database path\n\
         # TYPE kvdb_db_path gauge\n\
         kvdb_db_path{{path=\"{}\"}} 1\n",
        cfg.server.maxclients, cfg.storage.db_path,
    );
    ([("content-type", "text/plain; charset=utf-8")], body).into_response()
}
