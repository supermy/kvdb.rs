use bytes::{Buf, BytesMut};
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

async fn setup_server() -> (tempfile::TempDir, TcpStream) {
    let dir = tempfile::tempdir().unwrap();
    let config = Arc::new(ConfigManager::new(build_config(&dir)));
    let storage =
        Arc::new(StorageEngine::open(&config.get().storage.db_path, &config.get()).unwrap());
    let server = Server::bind(config, storage).await.unwrap();
    let addr = server.local_addr().unwrap();

    tokio::spawn(async move {
        server.run().await.unwrap();
    });

    let stream = TcpStream::connect(addr).await.unwrap();
    (dir, stream)
}

fn bulk(s: &str) -> RespValue {
    RespValue::BulkString(Some(bytes::Bytes::from(s.to_string())))
}

#[tokio::test]
async fn zadd_zcard_zscore_basic() {
    let (_dir, mut stream) = setup_server().await;

    // ZADD 返回新增数量
    let reply = send_cmd(&mut stream, &["ZADD", "z", "1", "a", "2", "b", "3", "c"]).await;
    assert_eq!(reply, RespValue::Integer(3));

    // ZCARD
    let reply = send_cmd(&mut stream, &["ZCARD", "z"]).await;
    assert_eq!(reply, RespValue::Integer(3));

    // ZSCORE
    let reply = send_cmd(&mut stream, &["ZSCORE", "z", "b"]).await;
    assert_eq!(reply, bulk("2"));

    // ZSCORE 不存在的 member
    let reply = send_cmd(&mut stream, &["ZSCORE", "z", "notexist"]).await;
    assert_eq!(reply, RespValue::BulkString(None));

    // 更新已有 member 的 score 不计入新增
    let reply = send_cmd(&mut stream, &["ZADD", "z", "5", "a"]).await;
    assert_eq!(reply, RespValue::Integer(0));

    let reply = send_cmd(&mut stream, &["ZSCORE", "z", "a"]).await;
    assert_eq!(reply, bulk("5"));

    let reply = send_cmd(&mut stream, &["ZCARD", "z"]).await;
    assert_eq!(reply, RespValue::Integer(3));
}

#[tokio::test]
async fn zrange_and_withscores() {
    let (_dir, mut stream) = setup_server().await;

    send_cmd(
        &mut stream,
        &["ZADD", "z", "1", "a", "2", "b", "3", "c", "4", "d"],
    )
    .await;

    // 全范围
    let reply = send_cmd(&mut stream, &["ZRANGE", "z", "0", "-1"]).await;
    assert_eq!(
        reply,
        RespValue::Array(vec![bulk("a"), bulk("b"), bulk("c"), bulk("d")])
    );

    // 子范围
    let reply = send_cmd(&mut stream, &["ZRANGE", "z", "1", "2"]).await;
    assert_eq!(reply, RespValue::Array(vec![bulk("b"), bulk("c")]));

    // WITHSCORES
    let reply = send_cmd(&mut stream, &["ZRANGE", "z", "0", "-1", "WITHSCORES"]).await;
    assert_eq!(
        reply,
        RespValue::Array(vec![
            bulk("a"),
            bulk("1"),
            bulk("b"),
            bulk("2"),
            bulk("c"),
            bulk("3"),
            bulk("d"),
            bulk("4"),
        ])
    );

    // 空范围
    let reply = send_cmd(&mut stream, &["ZRANGE", "z", "10", "20"]).await;
    assert_eq!(reply, RespValue::Array(vec![]));
}

#[tokio::test]
async fn zrangebyscore() {
    let (_dir, mut stream) = setup_server().await;

    send_cmd(
        &mut stream,
        &["ZADD", "z", "1", "a", "2", "b", "3", "c", "4", "d"],
    )
    .await;

    let reply = send_cmd(&mut stream, &["ZRANGEBYSCORE", "z", "2", "3"]).await;
    assert_eq!(reply, RespValue::Array(vec![bulk("b"), bulk("c")]));

    let reply = send_cmd(&mut stream, &["ZRANGEBYSCORE", "z", "-inf", "+inf"]).await;
    assert_eq!(
        reply,
        RespValue::Array(vec![bulk("a"), bulk("b"), bulk("c"), bulk("d")])
    );

    let reply = send_cmd(
        &mut stream,
        &["ZRANGEBYSCORE", "z", "2.5", "3.5", "WITHSCORES"],
    )
    .await;
    assert_eq!(reply, RespValue::Array(vec![bulk("c"), bulk("3")]));
}

#[tokio::test]
async fn zrank_and_zrem() {
    let (_dir, mut stream) = setup_server().await;

    send_cmd(&mut stream, &["ZADD", "z", "1", "a", "2", "b", "3", "c"]).await;

    let reply = send_cmd(&mut stream, &["ZRANK", "z", "b"]).await;
    assert_eq!(reply, RespValue::Integer(1));

    let reply = send_cmd(&mut stream, &["ZRANK", "z", "notexist"]).await;
    assert_eq!(reply, RespValue::BulkString(None));

    // ZREM 返回删除数量并更新 size
    let reply = send_cmd(&mut stream, &["ZREM", "z", "b", "notexist"]).await;
    assert_eq!(reply, RespValue::Integer(1));

    let reply = send_cmd(&mut stream, &["ZCARD", "z"]).await;
    assert_eq!(reply, RespValue::Integer(2));

    let reply = send_cmd(&mut stream, &["ZRANGE", "z", "0", "-1"]).await;
    assert_eq!(reply, RespValue::Array(vec![bulk("a"), bulk("c")]));
}

#[tokio::test]
async fn zadd_multiple_scores_same_member() {
    let (_dir, mut stream) = setup_server().await;

    // 同一命令内对同一 member 多次设置，以最后一次为准，只计一次新增
    let reply = send_cmd(&mut stream, &["ZADD", "z", "1", "a", "2", "a", "3", "a"]).await;
    assert_eq!(reply, RespValue::Integer(1));

    let reply = send_cmd(&mut stream, &["ZSCORE", "z", "a"]).await;
    assert_eq!(reply, bulk("3"));
}

#[tokio::test]
async fn wrong_type_error() {
    let (_dir, mut stream) = setup_server().await;

    send_cmd(&mut stream, &["SET", "k", "v"]).await;

    let reply = send_cmd(&mut stream, &["ZADD", "k", "1", "m"]).await;
    assert!(
        matches!(reply, RespValue::Error(ref e) if e.contains("WRONGTYPE")),
        "expected WRONGTYPE, got {:?}",
        reply
    );

    let reply = send_cmd(&mut stream, &["ZCARD", "k"]).await;
    assert!(
        matches!(reply, RespValue::Error(ref e) if e.contains("WRONGTYPE")),
        "expected WRONGTYPE, got {:?}",
        reply
    );
}

#[tokio::test]
async fn empty_key_returns_zero_or_empty() {
    let (_dir, mut stream) = setup_server().await;

    let reply = send_cmd(&mut stream, &["ZCARD", "nonexistent"]).await;
    assert_eq!(reply, RespValue::Integer(0));

    let reply = send_cmd(&mut stream, &["ZRANGE", "nonexistent", "0", "-1"]).await;
    assert_eq!(reply, RespValue::Array(vec![]));

    let reply = send_cmd(&mut stream, &["ZSCORE", "nonexistent", "m"]).await;
    assert_eq!(reply, RespValue::BulkString(None));

    let reply = send_cmd(&mut stream, &["ZRANK", "nonexistent", "m"]).await;
    assert_eq!(reply, RespValue::BulkString(None));

    let reply = send_cmd(&mut stream, &["ZREM", "nonexistent", "m"]).await;
    assert_eq!(reply, RespValue::Integer(0));
}
