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
async fn zrevrange_and_withscores() {
    let (_dir, mut stream) = setup_server().await;

    send_cmd(
        &mut stream,
        &["ZADD", "z", "1", "a", "2", "b", "3", "c", "4", "d"],
    )
    .await;

    let reply = send_cmd(&mut stream, &["ZREVRANGE", "z", "0", "-1"]).await;
    assert_eq!(
        reply,
        RespValue::Array(vec![bulk("d"), bulk("c"), bulk("b"), bulk("a")])
    );

    let reply = send_cmd(&mut stream, &["ZREVRANGE", "z", "1", "2"]).await;
    assert_eq!(reply, RespValue::Array(vec![bulk("c"), bulk("b")]));

    let reply = send_cmd(&mut stream, &["ZREVRANGE", "z", "0", "-1", "WITHSCORES"]).await;
    assert_eq!(
        reply,
        RespValue::Array(vec![
            bulk("d"),
            bulk("4"),
            bulk("c"),
            bulk("3"),
            bulk("b"),
            bulk("2"),
            bulk("a"),
            bulk("1"),
        ])
    );
}

#[tokio::test]
async fn zrevrangebyscore() {
    let (_dir, mut stream) = setup_server().await;

    send_cmd(
        &mut stream,
        &["ZADD", "z", "1", "a", "2", "b", "3", "c", "4", "d"],
    )
    .await;

    let reply = send_cmd(&mut stream, &["ZREVRANGEBYSCORE", "z", "3", "2"]).await;
    assert_eq!(reply, RespValue::Array(vec![bulk("c"), bulk("b")]));

    let reply = send_cmd(&mut stream, &["ZREVRANGEBYSCORE", "z", "+inf", "-inf"]).await;
    assert_eq!(
        reply,
        RespValue::Array(vec![bulk("d"), bulk("c"), bulk("b"), bulk("a")])
    );

    let reply = send_cmd(
        &mut stream,
        &["ZREVRANGEBYSCORE", "z", "3.5", "2.5", "WITHSCORES"],
    )
    .await;
    assert_eq!(reply, RespValue::Array(vec![bulk("c"), bulk("3")]));
}

#[tokio::test]
async fn zrevrank_and_zincrby() {
    let (_dir, mut stream) = setup_server().await;

    send_cmd(&mut stream, &["ZADD", "z", "1", "a", "2", "b", "3", "c"]).await;

    let reply = send_cmd(&mut stream, &["ZREVRANK", "z", "b"]).await;
    assert_eq!(reply, RespValue::Integer(1));

    let reply = send_cmd(&mut stream, &["ZREVRANK", "z", "notexist"]).await;
    assert_eq!(reply, RespValue::BulkString(None));

    // ZINCRBY 对已有 member 增加
    let reply = send_cmd(&mut stream, &["ZINCRBY", "z", "5", "b"]).await;
    assert_eq!(reply, bulk("7"));

    let reply = send_cmd(&mut stream, &["ZSCORE", "z", "b"]).await;
    assert_eq!(reply, bulk("7"));

    // 增加后 b 成为最高分，反向排名为 0
    let reply = send_cmd(&mut stream, &["ZREVRANK", "z", "b"]).await;
    assert_eq!(reply, RespValue::Integer(0));

    // ZINCRBY 对不存在的 member 等价于设置 score
    let reply = send_cmd(&mut stream, &["ZINCRBY", "z", "10", "d"]).await;
    assert_eq!(reply, bulk("10"));

    let reply = send_cmd(&mut stream, &["ZCARD", "z"]).await;
    assert_eq!(reply, RespValue::Integer(4));
}

#[tokio::test]
async fn zrangebyscore_with_limit() {
    let (_dir, mut stream) = setup_server().await;

    send_cmd(
        &mut stream,
        &[
            "ZADD", "z", "1", "a", "2", "b", "3", "c", "4", "d", "5", "e",
        ],
    )
    .await;

    // LIMIT offset count
    let reply = send_cmd(
        &mut stream,
        &["ZRANGEBYSCORE", "z", "1", "5", "LIMIT", "1", "2"],
    )
    .await;
    assert_eq!(reply, RespValue::Array(vec![bulk("b"), bulk("c")]));

    // LIMIT 配合 WITHSCORES
    let reply = send_cmd(
        &mut stream,
        &[
            "ZRANGEBYSCORE",
            "z",
            "1",
            "5",
            "WITHSCORES",
            "LIMIT",
            "2",
            "2",
        ],
    )
    .await;
    assert_eq!(
        reply,
        RespValue::Array(vec![bulk("c"), bulk("3"), bulk("d"), bulk("4")])
    );

    // offset 超出范围
    let reply = send_cmd(
        &mut stream,
        &["ZRANGEBYSCORE", "z", "1", "5", "LIMIT", "10", "2"],
    )
    .await;
    assert_eq!(reply, RespValue::Array(vec![]));
}

#[tokio::test]
async fn zrevrangebyscore_with_limit() {
    let (_dir, mut stream) = setup_server().await;

    send_cmd(
        &mut stream,
        &[
            "ZADD", "z", "1", "a", "2", "b", "3", "c", "4", "d", "5", "e",
        ],
    )
    .await;

    let reply = send_cmd(
        &mut stream,
        &["ZREVRANGEBYSCORE", "z", "5", "1", "LIMIT", "1", "2"],
    )
    .await;
    assert_eq!(reply, RespValue::Array(vec![bulk("d"), bulk("c")]));

    let reply = send_cmd(
        &mut stream,
        &[
            "ZREVRANGEBYSCORE",
            "z",
            "5",
            "1",
            "WITHSCORES",
            "LIMIT",
            "0",
            "3",
        ],
    )
    .await;
    assert_eq!(
        reply,
        RespValue::Array(vec![
            bulk("e"),
            bulk("5"),
            bulk("d"),
            bulk("4"),
            bulk("c"),
            bulk("3"),
        ])
    );
}

#[tokio::test]
async fn zrange_does_not_load_whole_set() {
    let (_dir, mut stream) = setup_server().await;

    // 构造一个包含多个 score 的较大 ZSet，验证只返回指定 rank 范围
    let mut owned_parts = vec!["ZADD".to_string(), "z".to_string()];
    for i in 0..50 {
        owned_parts.push((i + 1).to_string());
        owned_parts.push(format!("m{:02}", i));
    }
    let parts: Vec<&str> = owned_parts.iter().map(|s| s.as_str()).collect();
    send_cmd(&mut stream, &parts).await;

    let reply = send_cmd(&mut stream, &["ZRANGE", "z", "10", "12"]).await;
    let mut expected: Vec<RespValue> = Vec::new();
    for i in 10..=12 {
        expected.push(bulk(&format!("m{:02}", i)));
    }
    assert_eq!(reply, RespValue::Array(expected));

    let reply = send_cmd(&mut stream, &["ZREVRANGE", "z", "0", "2"]).await;
    assert_eq!(
        reply,
        RespValue::Array(vec![bulk("m49"), bulk("m48"), bulk("m47"),])
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

    let reply = send_cmd(&mut stream, &["ZREVRANK", "nonexistent", "m"]).await;
    assert_eq!(reply, RespValue::BulkString(None));

    let reply = send_cmd(&mut stream, &["ZREM", "nonexistent", "m"]).await;
    assert_eq!(reply, RespValue::Integer(0));
}

#[tokio::test]
async fn zadd_nx_only_add_new() {
    let (_dir, mut stream) = setup_server().await;

    send_cmd(&mut stream, &["ZADD", "z", "1", "a"]).await;

    // NX: 仅新增，已有 member 被跳过
    let reply = send_cmd(&mut stream, &["ZADD", "z", "NX", "5", "a", "2", "b"]).await;
    assert_eq!(reply, RespValue::Integer(1));

    let reply = send_cmd(&mut stream, &["ZSCORE", "z", "a"]).await;
    assert_eq!(reply, bulk("1"), "NX should not update existing member");

    let reply = send_cmd(&mut stream, &["ZSCORE", "z", "b"]).await;
    assert_eq!(reply, bulk("2"));
}

#[tokio::test]
async fn zadd_xx_only_update_existing() {
    let (_dir, mut stream) = setup_server().await;

    send_cmd(&mut stream, &["ZADD", "z", "1", "a"]).await;

    // XX: 仅更新，新 member 被跳过
    let reply = send_cmd(&mut stream, &["ZADD", "z", "XX", "5", "a", "2", "b"]).await;
    assert_eq!(reply, RespValue::Integer(0));

    let reply = send_cmd(&mut stream, &["ZSCORE", "z", "a"]).await;
    assert_eq!(reply, bulk("5"));

    let reply = send_cmd(&mut stream, &["ZSCORE", "z", "b"]).await;
    assert_eq!(reply, RespValue::BulkString(None));
}

#[tokio::test]
async fn zadd_gt_only_greater() {
    let (_dir, mut stream) = setup_server().await;

    send_cmd(&mut stream, &["ZADD", "z", "5", "a", "5", "b"]).await;

    // GT: 只更新比当前大的；a=5→10 更新，b=5→3 跳过
    let reply = send_cmd(&mut stream, &["ZADD", "z", "GT", "10", "a", "3", "b"]).await;
    assert_eq!(reply, RespValue::Integer(0));

    let reply = send_cmd(&mut stream, &["ZSCORE", "z", "a"]).await;
    assert_eq!(reply, bulk("10"));

    let reply = send_cmd(&mut stream, &["ZSCORE", "z", "b"]).await;
    assert_eq!(reply, bulk("5"));
}

#[tokio::test]
async fn zadd_lt_only_less() {
    let (_dir, mut stream) = setup_server().await;

    send_cmd(&mut stream, &["ZADD", "z", "5", "a", "5", "b"]).await;

    // LT: 只更新比当前小的；a=5→3 更新，b=5→10 跳过
    let reply = send_cmd(&mut stream, &["ZADD", "z", "LT", "3", "a", "10", "b"]).await;
    assert_eq!(reply, RespValue::Integer(0));

    let reply = send_cmd(&mut stream, &["ZSCORE", "z", "a"]).await;
    assert_eq!(reply, bulk("3"));

    let reply = send_cmd(&mut stream, &["ZSCORE", "z", "b"]).await;
    assert_eq!(reply, bulk("5"));
}

#[tokio::test]
async fn zadd_ch_returns_changed() {
    let (_dir, mut stream) = setup_server().await;

    send_cmd(&mut stream, &["ZADD", "z", "1", "a"]).await;

    // CH: 返回 changed (added + updated)
    let reply = send_cmd(&mut stream, &["ZADD", "z", "CH", "5", "a", "2", "b"]).await;
    assert_eq!(reply, RespValue::Integer(2));

    let reply = send_cmd(&mut stream, &["ZSCORE", "z", "a"]).await;
    assert_eq!(reply, bulk("5"));
    let reply = send_cmd(&mut stream, &["ZSCORE", "z", "b"]).await;
    assert_eq!(reply, bulk("2"));
}

#[tokio::test]
async fn zadd_incr_mode() {
    let (_dir, mut stream) = setup_server().await;

    send_cmd(&mut stream, &["ZADD", "z", "5", "a"]).await;

    // INCR: 增量更新，返回新 score
    let reply = send_cmd(&mut stream, &["ZADD", "z", "INCR", "3", "a"]).await;
    assert_eq!(reply, bulk("8"));

    // INCR 对不存在的 member 等价于设置 score
    let reply = send_cmd(&mut stream, &["ZADD", "z", "INCR", "2", "b"]).await;
    assert_eq!(reply, bulk("2"));

    // INCR + NX 对已有 member 跳过返回 nil
    let reply = send_cmd(&mut stream, &["ZADD", "z", "INCR", "NX", "1", "a"]).await;
    assert_eq!(reply, RespValue::BulkString(None));

    // INCR + XX 对不存在 member 返回 nil
    let reply = send_cmd(&mut stream, &["ZADD", "z", "INCR", "XX", "1", "noexist"]).await;
    assert_eq!(reply, RespValue::BulkString(None));
}

#[tokio::test]
async fn zadd_rejects_invalid_combinations() {
    let (_dir, mut stream) = setup_server().await;

    // NX 与 XX 互斥
    let reply = send_cmd(&mut stream, &["ZADD", "z", "NX", "XX", "1", "a"]).await;
    assert!(matches!(reply, RespValue::Error(_)));

    // GT 与 LT 互斥
    let reply = send_cmd(&mut stream, &["ZADD", "z", "GT", "LT", "1", "a"]).await;
    assert!(matches!(reply, RespValue::Error(_)));

    // INCR 只允许单个 score-member
    let reply = send_cmd(&mut stream, &["ZADD", "z", "INCR", "1", "a", "2", "b"]).await;
    assert!(matches!(reply, RespValue::Error(_)));
}

#[tokio::test]
async fn zadd_gt_adds_new_members() {
    let (_dir, mut stream) = setup_server().await;

    // GT 可以新增 member（无现有 score 时视为 +inf 才不会插入，但 Redis 中 GT 允许新增）
    let reply = send_cmd(&mut stream, &["ZADD", "z", "GT", "1", "a"]).await;
    assert_eq!(reply, RespValue::Integer(1));
    let reply = send_cmd(&mut stream, &["ZSCORE", "z", "a"]).await;
    assert_eq!(reply, bulk("1"));
}
