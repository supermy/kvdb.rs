use std::sync::Arc;

use bytes::Bytes;

use kvdb_rs::cluster::ClusterState;
use kvdb_rs::cmd::{ClientState, CommandContext, CommandTable};
use kvdb_rs::protocol::RespValue;
use kvdb_rs::pubsub::PubSubHub;
use kvdb_rs::replication::ReplicationState;
use kvdb_rs::thread_pool::ThreadPool;
use kvdb_rs::{Config, ConfigManager, StorageEngine};

fn setup() -> (CommandContext, CommandTable, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let mut config = Config::default();
    config.storage.db_path = dir.path().join("db").to_string_lossy().to_string();
    let config = Arc::new(ConfigManager::new(config));
    let storage =
        Arc::new(StorageEngine::open(&config.get().storage.db_path, &config.get()).unwrap());
    let table = Arc::new(CommandTable::new());
    let lua = Arc::new(kvdb_rs::lua::LuaEngine::new(Arc::clone(&table)).unwrap());
    let pubsub = Arc::new(PubSubHub::new());
    let (pubsub_tx, _pubsub_rx) = tokio::sync::mpsc::unbounded_channel();
    let ctx = CommandContext {
        storage,
        config,
        tx_pool: ThreadPool::new(1),
        client: ClientState::default(),
        pubsub,
        pubsub_tx,
        client_id: 0,
        lua,
        replication: ReplicationState::new(),
        cluster: ClusterState::new(),
    };
    (ctx, CommandTable::new(), dir)
}

fn dispatch(ctx: &CommandContext, table: &CommandTable, cmd: &str, args: &[&str]) -> RespValue {
    let args: Vec<Bytes> = args
        .iter()
        .map(|s| Bytes::copy_from_slice(s.as_bytes()))
        .collect();
    table.dispatch(ctx, cmd.as_bytes(), &args)
}

#[test]
fn hset_hget_basic() {
    let (ctx, table, _dir) = setup();

    let reply = dispatch(&ctx, &table, "HSET", &["h", "f1", "v1", "f2", "v2"]);
    assert_eq!(reply, RespValue::Integer(2));

    let reply = dispatch(&ctx, &table, "HGET", &["h", "f1"]);
    assert_eq!(
        reply,
        RespValue::BulkString(Some(Bytes::from_static(b"v1")))
    );

    let reply = dispatch(&ctx, &table, "HGET", &["h", "f2"]);
    assert_eq!(
        reply,
        RespValue::BulkString(Some(Bytes::from_static(b"v2")))
    );

    let reply = dispatch(&ctx, &table, "HGET", &["h", "missing"]);
    assert_eq!(reply, RespValue::BulkString(None));
}

#[test]
fn hset_update_existing_field() {
    let (ctx, table, _dir) = setup();

    let reply = dispatch(&ctx, &table, "HSET", &["h", "f1", "v1"]);
    assert_eq!(reply, RespValue::Integer(1));

    let reply = dispatch(&ctx, &table, "HSET", &["h", "f1", "v1_new"]);
    assert_eq!(reply, RespValue::Integer(0));

    let reply = dispatch(&ctx, &table, "HGET", &["h", "f1"]);
    assert_eq!(
        reply,
        RespValue::BulkString(Some(Bytes::from_static(b"v1_new")))
    );
}

#[test]
fn hmget_returns_array() {
    let (ctx, table, _dir) = setup();

    dispatch(&ctx, &table, "HSET", &["h", "a", "1", "b", "2"]);
    let reply = dispatch(&ctx, &table, "HMGET", &["h", "a", "missing", "b"]);
    assert_eq!(
        reply,
        RespValue::Array(vec![
            RespValue::BulkString(Some(Bytes::from_static(b"1"))),
            RespValue::BulkString(None),
            RespValue::BulkString(Some(Bytes::from_static(b"2"))),
        ])
    );
}

#[test]
fn hgetall_returns_fields_and_values() {
    let (ctx, table, _dir) = setup();

    dispatch(&ctx, &table, "HSET", &["h", "a", "1", "b", "2"]);
    let reply = dispatch(&ctx, &table, "HGETALL", &["h"]);
    let RespValue::Array(arr) = reply else {
        panic!("expected array, got {:?}", reply);
    };
    assert_eq!(arr.len(), 4);

    let mut map = std::collections::HashMap::new();
    for chunk in arr.chunks_exact(2) {
        let key = match &chunk[0] {
            RespValue::BulkString(Some(b)) => String::from_utf8_lossy(b).to_string(),
            _ => panic!("expected bulk string key"),
        };
        let value = match &chunk[1] {
            RespValue::BulkString(Some(b)) => String::from_utf8_lossy(b).to_string(),
            _ => panic!("expected bulk string value"),
        };
        map.insert(key, value);
    }
    assert_eq!(map.len(), 2);
    assert_eq!(map.get("a"), Some(&"1".to_string()));
    assert_eq!(map.get("b"), Some(&"2".to_string()));
}

#[test]
fn hdel_removes_fields() {
    let (ctx, table, _dir) = setup();

    dispatch(&ctx, &table, "HSET", &["h", "a", "1", "b", "2"]);
    let reply = dispatch(&ctx, &table, "HDEL", &["h", "a", "missing"]);
    assert_eq!(reply, RespValue::Integer(1));

    let reply = dispatch(&ctx, &table, "HLEN", &["h"]);
    assert_eq!(reply, RespValue::Integer(1));

    let reply = dispatch(&ctx, &table, "HEXISTS", &["h", "a"]);
    assert_eq!(reply, RespValue::Integer(0));

    let reply = dispatch(&ctx, &table, "HEXISTS", &["h", "b"]);
    assert_eq!(reply, RespValue::Integer(1));
}

#[test]
fn hlen_counts_fields() {
    let (ctx, table, _dir) = setup();

    let reply = dispatch(&ctx, &table, "HLEN", &["empty"]);
    assert_eq!(reply, RespValue::Integer(0));

    dispatch(&ctx, &table, "HSET", &["h", "a", "1", "b", "2", "c", "3"]);
    let reply = dispatch(&ctx, &table, "HLEN", &["h"]);
    assert_eq!(reply, RespValue::Integer(3));
}

#[test]
fn wrongtype_when_key_is_string() {
    let (ctx, table, _dir) = setup();

    let reply = dispatch(&ctx, &table, "SET", &["h", "string-value"]);
    assert_eq!(reply, RespValue::SimpleString("OK".to_string()));

    let reply = dispatch(&ctx, &table, "HSET", &["h", "f", "v"]);
    assert_eq!(
        reply,
        RespValue::Error(
            "ERR WRONGTYPE Operation against a key holding the wrong kind of value".to_string()
        )
    );

    let reply = dispatch(&ctx, &table, "HGET", &["h", "f"]);
    assert_eq!(
        reply,
        RespValue::Error(
            "ERR WRONGTYPE Operation against a key holding the wrong kind of value".to_string()
        )
    );
}
