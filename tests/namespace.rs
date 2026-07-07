use std::sync::Arc;

use bytes::Bytes;

use kvdb_rs::cluster::ClusterState;
use kvdb_rs::cmd::{ClientState, CommandContext, CommandTable};
use kvdb_rs::protocol::RespValue;
use kvdb_rs::pubsub::PubSubHub;
use kvdb_rs::replication::ReplicationState;
use kvdb_rs::thread_pool::ThreadPool;
use kvdb_rs::{Config, ConfigManager, StorageEngine};

fn setup() -> (Arc<StorageEngine>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let mut config = Config::default();
    config.storage.db_path = dir.path().join("db").to_string_lossy().to_string();
    let config = Arc::new(ConfigManager::new(config));
    let storage =
        Arc::new(StorageEngine::open(&config.get().storage.db_path, &config.get()).unwrap());
    (storage, dir)
}

fn ctx_with_ns(
    storage: Arc<StorageEngine>,
    namespace: &[u8],
    table: Arc<CommandTable>,
) -> CommandContext {
    let config = Arc::new(ConfigManager::new(Config::default()));
    let pubsub = Arc::new(PubSubHub::new());
    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
    CommandContext {
        storage,
        config,
        tx_pool: ThreadPool::new(1),
        client: ClientState::default(),
        pubsub,
        pubsub_tx: tx,
        client_id: 0,
        lua: Arc::new(kvdb_rs::lua::LuaEngine::new(Arc::clone(&table)).unwrap()),
        replication: ReplicationState::new(),
        cluster: ClusterState::new(),
        namespace: Bytes::copy_from_slice(namespace),
    }
}

fn dispatch(ctx: &CommandContext, table: &CommandTable, cmd: &str, args: &[&str]) -> RespValue {
    let args: Vec<Bytes> = args
        .iter()
        .map(|s| Bytes::copy_from_slice(s.as_bytes()))
        .collect();
    table.dispatch(ctx, cmd.as_bytes(), &args)
}

#[test]
fn namespace_isolation() {
    let (storage, _dir) = setup();
    let table = CommandTable::new();

    let table = Arc::new(table);
    let ns1 = ctx_with_ns(Arc::clone(&storage), b"ns1", Arc::clone(&table));
    let ns2 = ctx_with_ns(Arc::clone(&storage), b"ns2", Arc::clone(&table));

    dispatch(&ns1, &table, "SET", &["k", "v1"]);
    dispatch(&ns1, &table, "SADD", &["set", "a"]);
    dispatch(&ns1, &table, "XADD", &["stream", "1-0", "f", "v"]);

    // ns2 看不到 ns1 的键
    assert_eq!(
        dispatch(&ns2, &table, "GET", &["k"]),
        RespValue::BulkString(None)
    );
    assert_eq!(
        dispatch(&ns2, &table, "EXISTS", &["k"]),
        RespValue::Integer(0)
    );
    assert_eq!(
        dispatch(&ns2, &table, "SMEMBERS", &["set"]),
        RespValue::Array(vec![])
    );
    assert_eq!(
        dispatch(&ns2, &table, "XLEN", &["stream"]),
        RespValue::Integer(0)
    );
    assert_eq!(dispatch(&ns2, &table, "DBSIZE", &[]), RespValue::Integer(0));

    // ns2 写入同名键
    dispatch(&ns2, &table, "SET", &["k", "v2"]);
    assert_eq!(
        dispatch(&ns2, &table, "GET", &["k"]),
        RespValue::BulkString(Some(Bytes::from_static(b"v2")))
    );

    // ns1 的键保持不变
    assert_eq!(
        dispatch(&ns1, &table, "GET", &["k"]),
        RespValue::BulkString(Some(Bytes::from_static(b"v1")))
    );
}

#[test]
fn namespace_backward_compatible() {
    let (storage, _dir) = setup();
    let table = Arc::new(CommandTable::new());

    // 无 namespace 上下文写入旧格式数据（String 类型）
    let old = ctx_with_ns(Arc::clone(&storage), b"", Arc::clone(&table));
    dispatch(&old, &table, "SET", &["k", "old_v"]);

    // 带 namespace 的上下文应能读取旧格式 String 键（metadata 层回退）
    let ns = ctx_with_ns(Arc::clone(&storage), b"ns1", Arc::clone(&table));
    assert_eq!(
        dispatch(&ns, &table, "GET", &["k"]),
        RespValue::BulkString(Some(Bytes::from_static(b"old_v")))
    );

    // ns 上下文写入新键不会破坏旧数据
    dispatch(&ns, &table, "SET", &["k", "new_v"]);
    assert_eq!(
        dispatch(&ns, &table, "GET", &["k"]),
        RespValue::BulkString(Some(Bytes::from_static(b"new_v")))
    );

    // 无 namespace 上下文读取旧键值不变
    assert_eq!(
        dispatch(&old, &table, "GET", &["k"]),
        RespValue::BulkString(Some(Bytes::from_static(b"old_v")))
    );
}

#[test]
fn namespace_flushdb_isolated() {
    let (storage, _dir) = setup();
    let table = Arc::new(CommandTable::new());

    let ns1 = ctx_with_ns(Arc::clone(&storage), b"ns1", Arc::clone(&table));
    dispatch(&ns1, &table, "SET", &["k", "v1"]);

    let ns2 = ctx_with_ns(Arc::clone(&storage), b"ns2", Arc::clone(&table));
    dispatch(&ns2, &table, "SET", &["k", "v2"]);

    // 清空 ns1，不影响 ns2
    dispatch(&ns1, &table, "FLUSHDB", &[]);
    assert_eq!(dispatch(&ns1, &table, "DBSIZE", &[]), RespValue::Integer(0));
    assert_eq!(
        dispatch(&ns2, &table, "GET", &["k"]),
        RespValue::BulkString(Some(Bytes::from_static(b"v2")))
    );
}
