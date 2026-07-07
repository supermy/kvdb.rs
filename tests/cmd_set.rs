use bytes::{Buf, BytesMut};
use std::collections::HashSet;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use kvdb_rs::protocol::{RespParser, RespValue};
use kvdb_rs::{Config, ConfigManager, Server, StorageEngine};

fn build_config(dir: &tempfile::TempDir) -> Config {
    let mut config = Config::default();
    config.storage.db_path = dir.path().join("db").to_string_lossy().to_string();
    config.server.bind = "127.0.0.1:0".to_string();
    config
}

async fn send_cmd(stream: &mut TcpStream, parts: &[&str]) -> RespValue {
    let mut buf = Vec::new();
    buf.extend_from_slice(format!("*{}\r\n", parts.len()).as_bytes());
    for p in parts {
        buf.extend_from_slice(format!("${}\r\n{}\r\n", p.len(), p).as_bytes());
    }
    stream.write_all(&buf).await.unwrap();
    stream.flush().await.unwrap();

    let mut read_buf = BytesMut::with_capacity(4096);
    loop {
        if let Some((value, consumed)) = RespParser::parse_one(&read_buf) {
            read_buf.advance(consumed);
            return value;
        }
        let n = stream.read_buf(&mut read_buf).await.unwrap();
        assert!(n > 0, "server closed connection");
    }
}

fn array_to_set(value: RespValue) -> HashSet<bytes::Bytes> {
    match value {
        RespValue::Array(items) => items
            .into_iter()
            .map(|v| match v {
                RespValue::BulkString(Some(b)) => b,
                _ => panic!("expected bulk string, got {:?}", v),
            })
            .collect(),
        _ => panic!("expected array, got {:?}", value),
    }
}

fn assert_error(value: RespValue) {
    assert!(
        matches!(value, RespValue::Error(_)),
        "expected error, got {:?}",
        value
    );
}

#[tokio::test]
async fn test_set_basic() {
    let dir = tempfile::tempdir().unwrap();
    let config = Arc::new(ConfigManager::new(build_config(&dir)));
    let storage =
        Arc::new(StorageEngine::open(&config.get().storage.db_path, &config.get()).unwrap());
    let server = Server::bind(config, storage).await.unwrap();
    let addr = server.local_addr().unwrap();
    tokio::spawn(async move {
        server.run().await.unwrap();
    });
    let mut stream = TcpStream::connect(addr).await.unwrap();

    // SADD 新建集合
    let reply = send_cmd(&mut stream, &["SADD", "s", "a", "b", "c"]).await;
    assert_eq!(reply, RespValue::Integer(3));

    // SADD 部分已存在
    let reply = send_cmd(&mut stream, &["SADD", "s", "a", "d"]).await;
    assert_eq!(reply, RespValue::Integer(1));

    // SCARD
    let reply = send_cmd(&mut stream, &["SCARD", "s"]).await;
    assert_eq!(reply, RespValue::Integer(4));

    // SISMEMBER
    assert_eq!(
        send_cmd(&mut stream, &["SISMEMBER", "s", "a"]).await,
        RespValue::Integer(1)
    );
    assert_eq!(
        send_cmd(&mut stream, &["SISMEMBER", "s", "z"]).await,
        RespValue::Integer(0)
    );

    // SMEMBERS
    let reply = send_cmd(&mut stream, &["SMEMBERS", "s"]).await;
    assert_eq!(
        array_to_set(reply),
        HashSet::from_iter([
            bytes::Bytes::from_static(b"a"),
            bytes::Bytes::from_static(b"b"),
            bytes::Bytes::from_static(b"c"),
            bytes::Bytes::from_static(b"d"),
        ])
    );

    // SREM
    let reply = send_cmd(&mut stream, &["SREM", "s", "a", "z"]).await;
    assert_eq!(reply, RespValue::Integer(1));
    assert_eq!(
        send_cmd(&mut stream, &["SCARD", "s"]).await,
        RespValue::Integer(3)
    );

    // SINTER / SUNION
    let reply = send_cmd(&mut stream, &["SADD", "s2", "b", "c", "e"]).await;
    assert_eq!(reply, RespValue::Integer(3));

    let reply = send_cmd(&mut stream, &["SINTER", "s", "s2"]).await;
    assert_eq!(
        array_to_set(reply),
        HashSet::from_iter([
            bytes::Bytes::from_static(b"b"),
            bytes::Bytes::from_static(b"c"),
        ])
    );

    let reply = send_cmd(&mut stream, &["SUNION", "s", "s2"]).await;
    assert_eq!(
        array_to_set(reply),
        HashSet::from_iter([
            bytes::Bytes::from_static(b"b"),
            bytes::Bytes::from_static(b"c"),
            bytes::Bytes::from_static(b"d"),
            bytes::Bytes::from_static(b"e"),
        ])
    );

    // 与不存在 key 求交集为空
    assert_eq!(
        send_cmd(&mut stream, &["SINTER", "s", "noexist"]).await,
        RespValue::Array(vec![])
    );

    // SDIFF
    let reply = send_cmd(&mut stream, &["SDIFF", "s", "s2"]).await;
    assert_eq!(
        array_to_set(reply),
        HashSet::from_iter([bytes::Bytes::from_static(b"d"),])
    );

    // 与不存在 key 求并集不变
    let reply = send_cmd(&mut stream, &["SUNION", "s", "noexist"]).await;
    assert_eq!(
        array_to_set(reply),
        HashSet::from_iter([
            bytes::Bytes::from_static(b"b"),
            bytes::Bytes::from_static(b"c"),
            bytes::Bytes::from_static(b"d"),
        ])
    );
}

#[tokio::test]
async fn test_set_duplicate_members() {
    let dir = tempfile::tempdir().unwrap();
    let config = Arc::new(ConfigManager::new(build_config(&dir)));
    let storage =
        Arc::new(StorageEngine::open(&config.get().storage.db_path, &config.get()).unwrap());
    let server = Server::bind(config, storage).await.unwrap();
    let addr = server.local_addr().unwrap();
    tokio::spawn(async move {
        server.run().await.unwrap();
    });
    let mut stream = TcpStream::connect(addr).await.unwrap();

    // 同一命令内重复 member 只计数一次
    let reply = send_cmd(&mut stream, &["SADD", "dup", "x", "x", "y"]).await;
    assert_eq!(reply, RespValue::Integer(2));

    let reply = send_cmd(&mut stream, &["SREM", "dup", "x", "x"]).await;
    assert_eq!(reply, RespValue::Integer(1));

    assert_eq!(
        send_cmd(&mut stream, &["SCARD", "dup"]).await,
        RespValue::Integer(1)
    );
}

#[tokio::test]
async fn test_set_nonexistent() {
    let dir = tempfile::tempdir().unwrap();
    let config = Arc::new(ConfigManager::new(build_config(&dir)));
    let storage =
        Arc::new(StorageEngine::open(&config.get().storage.db_path, &config.get()).unwrap());
    let server = Server::bind(config, storage).await.unwrap();
    let addr = server.local_addr().unwrap();
    tokio::spawn(async move {
        server.run().await.unwrap();
    });
    let mut stream = TcpStream::connect(addr).await.unwrap();

    assert_eq!(
        send_cmd(&mut stream, &["SREM", "noexist", "a"]).await,
        RespValue::Integer(0)
    );
    assert_eq!(
        send_cmd(&mut stream, &["SISMEMBER", "noexist", "a"]).await,
        RespValue::Integer(0)
    );
    assert_eq!(
        send_cmd(&mut stream, &["SMEMBERS", "noexist"]).await,
        RespValue::Array(vec![])
    );
    assert_eq!(
        send_cmd(&mut stream, &["SCARD", "noexist"]).await,
        RespValue::Integer(0)
    );
}

#[tokio::test]
async fn test_set_wrong_type() {
    let dir = tempfile::tempdir().unwrap();
    let config = Arc::new(ConfigManager::new(build_config(&dir)));
    let storage =
        Arc::new(StorageEngine::open(&config.get().storage.db_path, &config.get()).unwrap());
    let server = Server::bind(config, storage).await.unwrap();
    let addr = server.local_addr().unwrap();
    tokio::spawn(async move {
        server.run().await.unwrap();
    });
    let mut stream = TcpStream::connect(addr).await.unwrap();

    send_cmd(&mut stream, &["SET", "str", "val"]).await;

    assert_error(send_cmd(&mut stream, &["SADD", "str", "m"]).await);
    assert_error(send_cmd(&mut stream, &["SREM", "str", "m"]).await);
    assert_error(send_cmd(&mut stream, &["SISMEMBER", "str", "m"]).await);
    assert_error(send_cmd(&mut stream, &["SMEMBERS", "str"]).await);
    assert_error(send_cmd(&mut stream, &["SCARD", "str"]).await);
    assert_error(send_cmd(&mut stream, &["SINTER", "str", "s"]).await);
    assert_error(send_cmd(&mut stream, &["SUNION", "str", "s"]).await);
}

#[tokio::test]
async fn test_set_ops_chunked() {
    let dir = tempfile::tempdir().unwrap();
    let config = Arc::new(ConfigManager::new(build_config(&dir)));
    let storage =
        Arc::new(StorageEngine::open(&config.get().storage.db_path, &config.get()).unwrap());
    let server = Server::bind(config, storage).await.unwrap();
    let addr = server.local_addr().unwrap();
    tokio::spawn(async move {
        server.run().await.unwrap();
    });
    let mut stream = TcpStream::connect(addr).await.unwrap();

    let total = 3000usize;
    let mut a_members: Vec<String> = (0..total).map(|i| format!("a{:08}", i)).collect();
    let mut b_members: Vec<String> = (0..total).map(|i| format!("b{:08}", i)).collect();
    // 交集：偶数索引
    let common: Vec<String> = (0..total)
        .step_by(2)
        .map(|i| format!("c{:08}", i))
        .collect();
    a_members.extend(common.clone());
    b_members.extend(common.clone());

    let mut a_cmd = vec!["SADD", "A"];
    let a_refs: Vec<&str> = a_members.iter().map(|s| s.as_str()).collect();
    a_cmd.extend(a_refs.iter().copied());
    let reply = send_cmd(&mut stream, &a_cmd).await;
    assert_eq!(reply, RespValue::Integer((total + common.len()) as i64));

    let mut b_cmd = vec!["SADD", "B"];
    let b_refs: Vec<&str> = b_members.iter().map(|s| s.as_str()).collect();
    b_cmd.extend(b_refs.iter().copied());
    let reply = send_cmd(&mut stream, &b_cmd).await;
    assert_eq!(reply, RespValue::Integer((total + common.len()) as i64));

    let reply = send_cmd(&mut stream, &["SINTER", "A", "B"]).await;
    let inter = array_to_set(reply);
    assert_eq!(inter.len(), common.len());
    for m in &common {
        assert!(inter.contains(&bytes::Bytes::copy_from_slice(m.as_bytes())));
    }

    let reply = send_cmd(&mut stream, &["SDIFF", "A", "B"]).await;
    let diff = array_to_set(reply);
    assert_eq!(diff.len(), total);

    let reply = send_cmd(&mut stream, &["SUNION", "A", "B"]).await;
    let union = array_to_set(reply);
    assert_eq!(union.len(), 2 * total + common.len());
}

fn assert_wrongtype(value: RespValue) {
    match &value {
        RespValue::Error(e) => assert!(
            e.contains("WRONGTYPE"),
            "expected WRONGTYPE error, got: {}",
            e
        ),
        other => panic!("expected WRONGTYPE error, got {:?}", other),
    }
}

#[tokio::test]
async fn test_set_wrongtype_short_string() {
    // 短 String 值（payload < 16 字节）也必须返回 WRONGTYPE 而非 Protocol 错误。
    let dir = tempfile::tempdir().unwrap();
    let config = Arc::new(ConfigManager::new(build_config(&dir)));
    let storage =
        Arc::new(StorageEngine::open(&config.get().storage.db_path, &config.get()).unwrap());
    let server = Server::bind(config, storage).await.unwrap();
    let addr = server.local_addr().unwrap();
    tokio::spawn(async move {
        server.run().await.unwrap();
    });
    let mut stream = TcpStream::connect(addr).await.unwrap();

    // 短字符串 "v" → String 编码仅 10 字节，远小于 metadata 最小 25 字节
    send_cmd(&mut stream, &["SET", "str", "v"]).await;
    assert_wrongtype(send_cmd(&mut stream, &["SADD", "str", "m"]).await);
    assert_wrongtype(send_cmd(&mut stream, &["SISMEMBER", "str", "m"]).await);
    assert_wrongtype(send_cmd(&mut stream, &["SMEMBERS", "str"]).await);
    assert_wrongtype(send_cmd(&mut stream, &["SCARD", "str"]).await);
    assert_wrongtype(send_cmd(&mut stream, &["SINTER", "str"]).await);
    assert_wrongtype(send_cmd(&mut stream, &["SUNION", "str"]).await);
}

#[tokio::test]
async fn test_set_arg_errors() {
    let dir = tempfile::tempdir().unwrap();
    let config = Arc::new(ConfigManager::new(build_config(&dir)));
    let storage =
        Arc::new(StorageEngine::open(&config.get().storage.db_path, &config.get()).unwrap());
    let server = Server::bind(config, storage).await.unwrap();
    let addr = server.local_addr().unwrap();
    tokio::spawn(async move {
        server.run().await.unwrap();
    });
    let mut stream = TcpStream::connect(addr).await.unwrap();

    assert_error(send_cmd(&mut stream, &["SADD", "k"]).await);
    assert_error(send_cmd(&mut stream, &["SREM", "k"]).await);
    assert_error(send_cmd(&mut stream, &["SISMEMBER", "k"]).await);
    assert_error(send_cmd(&mut stream, &["SMEMBERS"]).await);
    assert_error(send_cmd(&mut stream, &["SCARD"]).await);
    assert_error(send_cmd(&mut stream, &["SINTER"]).await);
    assert_error(send_cmd(&mut stream, &["SUNION"]).await);
}
