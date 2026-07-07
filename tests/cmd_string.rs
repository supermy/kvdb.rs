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
        namespace: Bytes::new(),
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
fn set_get_del_basic() {
    let (ctx, table, _dir) = setup();

    assert_eq!(
        dispatch(&ctx, &table, "SET", &["k", "v"]),
        RespValue::SimpleString("OK".to_string())
    );
    assert_eq!(
        dispatch(&ctx, &table, "GET", &["k"]),
        RespValue::BulkString(Some(Bytes::from_static(b"v")))
    );
    assert_eq!(
        dispatch(&ctx, &table, "EXISTS", &["k"]),
        RespValue::Integer(1)
    );
    assert_eq!(dispatch(&ctx, &table, "DEL", &["k"]), RespValue::Integer(1));
    assert_eq!(
        dispatch(&ctx, &table, "GET", &["k"]),
        RespValue::BulkString(None)
    );
    assert_eq!(
        dispatch(&ctx, &table, "EXISTS", &["k"]),
        RespValue::Integer(0)
    );
}

#[test]
fn exists_and_del_work_across_types() {
    let (ctx, table, _dir) = setup();

    // Hash
    dispatch(&ctx, &table, "HSET", &["h", "f", "v"]);
    assert_eq!(
        dispatch(&ctx, &table, "EXISTS", &["h"]),
        RespValue::Integer(1)
    );
    assert_eq!(dispatch(&ctx, &table, "DEL", &["h"]), RespValue::Integer(1));
    assert_eq!(
        dispatch(&ctx, &table, "HGET", &["h", "f"]),
        RespValue::BulkString(None)
    );

    // Set
    dispatch(&ctx, &table, "SADD", &["s", "m"]);
    assert_eq!(
        dispatch(&ctx, &table, "EXISTS", &["s"]),
        RespValue::Integer(1)
    );
    assert_eq!(dispatch(&ctx, &table, "DEL", &["s"]), RespValue::Integer(1));
    assert_eq!(
        dispatch(&ctx, &table, "SISMEMBER", &["s", "m"]),
        RespValue::Integer(0)
    );

    // ZSet
    dispatch(&ctx, &table, "ZADD", &["z", "1", "m"]);
    assert_eq!(
        dispatch(&ctx, &table, "EXISTS", &["z"]),
        RespValue::Integer(1)
    );
    assert_eq!(dispatch(&ctx, &table, "DEL", &["z"]), RespValue::Integer(1));
    assert_eq!(
        dispatch(&ctx, &table, "ZSCORE", &["z", "m"]),
        RespValue::BulkString(None)
    );

    // List
    dispatch(&ctx, &table, "LPUSH", &["l", "a"]);
    assert_eq!(
        dispatch(&ctx, &table, "EXISTS", &["l"]),
        RespValue::Integer(1)
    );
    assert_eq!(dispatch(&ctx, &table, "DEL", &["l"]), RespValue::Integer(1));
    assert_eq!(
        dispatch(&ctx, &table, "LLEN", &["l"]),
        RespValue::Integer(0)
    );

    // Stream
    dispatch(&ctx, &table, "XADD", &["st", "1-0", "f", "v"]);
    assert_eq!(
        dispatch(&ctx, &table, "EXISTS", &["st"]),
        RespValue::Integer(1)
    );
    assert_eq!(
        dispatch(&ctx, &table, "DEL", &["st"]),
        RespValue::Integer(1)
    );
    assert_eq!(
        dispatch(&ctx, &table, "XLEN", &["st"]),
        RespValue::Integer(0)
    );
}

#[test]
fn del_counts_only_existing_keys() {
    let (ctx, table, _dir) = setup();

    dispatch(&ctx, &table, "SET", &["k1", "v"]);
    dispatch(&ctx, &table, "SET", &["k2", "v"]);
    assert_eq!(
        dispatch(&ctx, &table, "DEL", &["k1", "no-such", "k2"]),
        RespValue::Integer(2)
    );
}

#[test]
fn mset_writes_multiple_keys() {
    let (ctx, table, _dir) = setup();

    assert_eq!(
        dispatch(&ctx, &table, "MSET", &["a", "1", "b", "2", "c", "3"]),
        RespValue::SimpleString("OK".to_string())
    );
    assert_eq!(
        dispatch(&ctx, &table, "MGET", &["a", "b", "c"]),
        RespValue::Array(vec![
            RespValue::BulkString(Some(Bytes::from_static(b"1"))),
            RespValue::BulkString(Some(Bytes::from_static(b"2"))),
            RespValue::BulkString(Some(Bytes::from_static(b"3"))),
        ])
    );
}

#[test]
fn mset_rejects_odd_arguments() {
    let (ctx, table, _dir) = setup();

    let reply = dispatch(&ctx, &table, "MSET", &["a", "1", "b"]);
    assert!(matches!(reply, RespValue::Error(_)), "expected error");
}

#[test]
fn incr_decr_append_basic() {
    let (ctx, table, _dir) = setup();

    assert_eq!(
        dispatch(&ctx, &table, "INCR", &["n"]),
        RespValue::Integer(1)
    );
    assert_eq!(
        dispatch(&ctx, &table, "INCR", &["n"]),
        RespValue::Integer(2)
    );
    assert_eq!(
        dispatch(&ctx, &table, "DECR", &["n"]),
        RespValue::Integer(1)
    );

    dispatch(&ctx, &table, "SET", &["s", "hello"]);
    assert_eq!(
        dispatch(&ctx, &table, "APPEND", &["s", " world"]),
        RespValue::Integer(11)
    );
    assert_eq!(
        dispatch(&ctx, &table, "GET", &["s"]),
        RespValue::BulkString(Some(Bytes::from_static(b"hello world")))
    );
}

#[test]
fn set_nx_xx_options() {
    let (ctx, table, _dir) = setup();

    // NX on missing key → OK
    assert_eq!(
        dispatch(&ctx, &table, "SET", &["k", "v1", "NX"]),
        RespValue::SimpleString("OK".to_string())
    );
    assert_eq!(
        dispatch(&ctx, &table, "GET", &["k"]),
        RespValue::BulkString(Some(Bytes::from_static(b"v1")))
    );

    // NX on existing key → nil
    assert_eq!(
        dispatch(&ctx, &table, "SET", &["k", "v2", "NX"]),
        RespValue::BulkString(None)
    );
    assert_eq!(
        dispatch(&ctx, &table, "GET", &["k"]),
        RespValue::BulkString(Some(Bytes::from_static(b"v1")))
    );

    // XX on existing key → OK
    assert_eq!(
        dispatch(&ctx, &table, "SET", &["k", "v3", "XX"]),
        RespValue::SimpleString("OK".to_string())
    );
    assert_eq!(
        dispatch(&ctx, &table, "GET", &["k"]),
        RespValue::BulkString(Some(Bytes::from_static(b"v3")))
    );

    // XX on missing key → nil
    assert_eq!(
        dispatch(&ctx, &table, "SET", &["missing", "v", "XX"]),
        RespValue::BulkString(None)
    );
}

#[test]
fn set_ex_px_options() {
    let (ctx, table, _dir) = setup();

    // SET with EX
    dispatch(&ctx, &table, "SET", &["k1", "v", "EX", "100"]);
    let ttl = dispatch(&ctx, &table, "TTL", &["k1"]);
    assert!(
        matches!(ttl, RespValue::Integer(n) if n > 0 && n <= 100),
        "expected TTL in (0, 100], got {:?}",
        ttl
    );

    // SET with PX
    dispatch(&ctx, &table, "SET", &["k2", "v", "PX", "5000"]);
    let pttl = dispatch(&ctx, &table, "PTTL", &["k2"]);
    assert!(
        matches!(pttl, RespValue::Integer(n) if n > 0 && n <= 5000),
        "expected PTTL in (0, 5000], got {:?}",
        pttl
    );
}

#[test]
fn expire_ttl_persist() {
    let (ctx, table, _dir) = setup();

    // EXPIRE on String
    dispatch(&ctx, &table, "SET", &["s", "v"]);
    assert_eq!(
        dispatch(&ctx, &table, "TTL", &["s"]),
        RespValue::Integer(-1)
    );
    assert_eq!(
        dispatch(&ctx, &table, "EXPIRE", &["s", "100"]),
        RespValue::Integer(1)
    );
    let ttl = dispatch(&ctx, &table, "TTL", &["s"]);
    assert!(matches!(ttl, RespValue::Integer(n) if n > 0 && n <= 100));

    // PERSIST
    assert_eq!(
        dispatch(&ctx, &table, "PERSIST", &["s"]),
        RespValue::Integer(1)
    );
    assert_eq!(
        dispatch(&ctx, &table, "TTL", &["s"]),
        RespValue::Integer(-1)
    );

    // EXPIRE on Hash (composite type)
    dispatch(&ctx, &table, "HSET", &["h", "f", "v"]);
    assert_eq!(
        dispatch(&ctx, &table, "EXPIRE", &["h", "200"]),
        RespValue::Integer(1)
    );
    let ttl = dispatch(&ctx, &table, "TTL", &["h"]);
    assert!(matches!(ttl, RespValue::Integer(n) if n > 0 && n <= 200));

    // TTL on missing key → -2
    assert_eq!(
        dispatch(&ctx, &table, "TTL", &["noexist"]),
        RespValue::Integer(-2)
    );

    // EXPIRE on missing key → 0
    assert_eq!(
        dispatch(&ctx, &table, "EXPIRE", &["noexist", "100"]),
        RespValue::Integer(0)
    );
}

#[test]
fn wrongtype_message_consistent_across_types() {
    // 统一的 WRONGTYPE 错误消息：所有数据类型在类型不匹配时必须返回同一字符串。
    let expected_msg = "WRONGTYPE Operation against a key holding the wrong kind of value";

    let (ctx, table, _dir) = setup();

    // String key 上调用复合类型命令
    dispatch(&ctx, &table, "SET", &["k", "v"]);

    // Hash 命令
    let reply = dispatch(&ctx, &table, "HSET", &["k", "f", "v"]);
    assert!(
        matches!(reply, RespValue::Error(ref e) if e == expected_msg),
        "HSET on String: {:?}",
        reply
    );

    // List 命令
    let reply = dispatch(&ctx, &table, "LPUSH", &["k", "v"]);
    assert!(
        matches!(reply, RespValue::Error(ref e) if e == expected_msg),
        "LPUSH on String: {:?}",
        reply
    );

    // Set 命令
    let reply = dispatch(&ctx, &table, "SADD", &["k", "m"]);
    assert!(
        matches!(reply, RespValue::Error(ref e) if e == expected_msg),
        "SADD on String: {:?}",
        reply
    );

    // ZSet 命令
    let reply = dispatch(&ctx, &table, "ZADD", &["k", "1", "m"]);
    assert!(
        matches!(reply, RespValue::Error(ref e) if e == expected_msg),
        "ZADD on String: {:?}",
        reply
    );

    // Bitmap 命令
    let reply = dispatch(&ctx, &table, "SETBIT", &["k", "0", "1"]);
    assert!(
        matches!(reply, RespValue::Error(ref e) if e == expected_msg),
        "SETBIT on String: {:?}",
        reply
    );

    // Stream 命令
    let reply = dispatch(&ctx, &table, "XADD", &["k", "1-0", "f", "v"]);
    assert!(
        matches!(reply, RespValue::Error(ref e) if e == expected_msg),
        "XADD on String: {:?}",
        reply
    );

    // String 命令调用复合类型 key
    dispatch(&ctx, &table, "HSET", &["h", "f", "v"]);
    let reply = dispatch(&ctx, &table, "GET", &["h"]);
    assert!(
        matches!(reply, RespValue::Error(ref e) if e == expected_msg),
        "GET on Hash: {:?}",
        reply
    );

    dispatch(&ctx, &table, "LPUSH", &["l", "v"]);
    let reply = dispatch(&ctx, &table, "GET", &["l"]);
    assert!(
        matches!(reply, RespValue::Error(ref e) if e == expected_msg),
        "GET on List: {:?}",
        reply
    );

    dispatch(&ctx, &table, "SADD", &["s", "m"]);
    let reply = dispatch(&ctx, &table, "GET", &["s"]);
    assert!(
        matches!(reply, RespValue::Error(ref e) if e == expected_msg),
        "GET on Set: {:?}",
        reply
    );

    dispatch(&ctx, &table, "ZADD", &["z", "1", "m"]);
    let reply = dispatch(&ctx, &table, "GET", &["z"]);
    assert!(
        matches!(reply, RespValue::Error(ref e) if e == expected_msg),
        "GET on ZSet: {:?}",
        reply
    );

    dispatch(&ctx, &table, "SETBIT", &["b", "0", "1"]);
    let reply = dispatch(&ctx, &table, "GET", &["b"]);
    assert!(
        matches!(reply, RespValue::Error(ref e) if e == expected_msg),
        "GET on Bitmap: {:?}",
        reply
    );

    dispatch(&ctx, &table, "XADD", &["st", "1-0", "f", "v"]);
    let reply = dispatch(&ctx, &table, "GET", &["st"]);
    assert!(
        matches!(reply, RespValue::Error(ref e) if e == expected_msg),
        "GET on Stream: {:?}",
        reply
    );
}
