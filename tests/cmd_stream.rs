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

fn bulk(s: &str) -> RespValue {
    RespValue::BulkString(Some(bytes::Bytes::from(s.to_string())))
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

#[tokio::test]
async fn xadd_xlen_xrange_basic() {
    let (_dir, mut stream) = setup_server().await;

    let reply = send_cmd(
        &mut stream,
        &[
            "XADD", "mystream", "1-0", "field1", "value1", "field2", "value2",
        ],
    )
    .await;
    assert_eq!(reply, bulk("1-0"));

    let reply = send_cmd(&mut stream, &["XLEN", "mystream"]).await;
    assert_eq!(reply, RespValue::Integer(1));

    let reply = send_cmd(&mut stream, &["XRANGE", "mystream", "-", "+"]).await;
    assert_eq!(
        reply,
        RespValue::Array(vec![RespValue::Array(vec![
            bulk("1-0"),
            RespValue::Array(vec![
                bulk("field1"),
                bulk("value1"),
                bulk("field2"),
                bulk("value2"),
            ]),
        ])])
    );
}

#[tokio::test]
async fn xadd_auto_id() {
    let (_dir, mut stream) = setup_server().await;

    let id1 = send_cmd(&mut stream, &["XADD", "s", "*", "k", "v1"]).await;
    let id2 = send_cmd(&mut stream, &["XADD", "s", "*", "k", "v2"]).await;

    // 两次自动生成的 ID 不应相同，且均为 ms-seq 格式
    assert_ne!(id1, id2);
    let id1_str = match id1 {
        RespValue::BulkString(Some(b)) => String::from_utf8_lossy(&b).to_string(),
        _ => panic!("expected bulk string"),
    };
    assert!(id1_str.contains('-'));

    let len = send_cmd(&mut stream, &["XLEN", "s"]).await;
    assert_eq!(len, RespValue::Integer(2));
}

#[tokio::test]
async fn xrange_range_and_count() {
    let (_dir, mut stream) = setup_server().await;

    send_cmd(&mut stream, &["XADD", "s", "1-0", "k", "a"]).await;
    send_cmd(&mut stream, &["XADD", "s", "2-0", "k", "b"]).await;
    send_cmd(&mut stream, &["XADD", "s", "3-0", "k", "c"]).await;
    send_cmd(&mut stream, &["XADD", "s", "4-0", "k", "d"]).await;

    let reply = send_cmd(&mut stream, &["XRANGE", "s", "2-0", "3-0"]).await;
    assert_eq!(
        reply,
        RespValue::Array(vec![
            RespValue::Array(vec![
                bulk("2-0"),
                RespValue::Array(vec![bulk("k"), bulk("b")])
            ]),
            RespValue::Array(vec![
                bulk("3-0"),
                RespValue::Array(vec![bulk("k"), bulk("c")])
            ]),
        ])
    );

    let reply = send_cmd(&mut stream, &["XRANGE", "s", "-", "+", "COUNT", "2"]).await;
    if let RespValue::Array(items) = reply {
        assert_eq!(items.len(), 2);
    } else {
        panic!("expected array");
    }
}

#[tokio::test]
async fn xread_basic() {
    let (_dir, mut stream) = setup_server().await;

    send_cmd(&mut stream, &["XADD", "s1", "1-0", "k", "a"]).await;
    send_cmd(&mut stream, &["XADD", "s1", "2-0", "k", "b"]).await;
    send_cmd(&mut stream, &["XADD", "s2", "1-0", "k", "x"]).await;

    let reply = send_cmd(&mut stream, &["XREAD", "STREAMS", "s1", "s2", "0-0", "0-0"]).await;
    // 返回格式：[[stream_name, [entry...]], ...]
    if let RespValue::Array(streams) = reply {
        assert_eq!(streams.len(), 2);
    } else {
        panic!("expected array, got {:?}", reply);
    }

    // 使用更大的起始 ID 过滤
    let reply = send_cmd(&mut stream, &["XREAD", "STREAMS", "s1", "1-0"]).await;
    if let RespValue::Array(streams) = reply {
        assert_eq!(streams.len(), 1);
        if let RespValue::Array(ref inner) = streams[0] {
            assert_eq!(inner[0], bulk("s1"));
        } else {
            panic!("expected stream array");
        }
    } else {
        panic!("expected array");
    }
}

#[tokio::test]
async fn xadd_wrong_type() {
    let (_dir, mut stream) = setup_server().await;

    send_cmd(&mut stream, &["SET", "k", "v"]).await;
    let reply = send_cmd(&mut stream, &["XADD", "k", "1-0", "f", "v"]).await;
    assert!(
        matches!(reply, RespValue::Error(ref e) if e.contains("WRONGTYPE")),
        "expected WRONGTYPE, got {:?}",
        reply
    );

    send_cmd(&mut stream, &["SADD", "setk", "m"]).await;
    let reply = send_cmd(&mut stream, &["XLEN", "setk"]).await;
    assert!(
        matches!(reply, RespValue::Error(ref e) if e.contains("WRONGTYPE")),
        "expected WRONGTYPE, got {:?}",
        reply
    );
}

#[tokio::test]
async fn xadd_duplicate_id_rejected() {
    let (_dir, mut stream) = setup_server().await;

    send_cmd(&mut stream, &["XADD", "s", "1-5", "k", "v"]).await;
    let reply = send_cmd(&mut stream, &["XADD", "s", "1-5", "k", "v2"]).await;
    assert!(
        matches!(reply, RespValue::Error(_)),
        "expected error for duplicate ID, got {:?}",
        reply
    );

    // 更小的 ID 同样被拒绝
    let reply = send_cmd(&mut stream, &["XADD", "s", "1-4", "k", "v3"]).await;
    assert!(
        matches!(reply, RespValue::Error(_)),
        "expected error for decreasing ID, got {:?}",
        reply
    );
}

#[tokio::test]
async fn xread_empty_stream() {
    let (_dir, mut stream) = setup_server().await;

    let reply = send_cmd(&mut stream, &["XREAD", "STREAMS", "nosuch", "0-0"]).await;
    assert_eq!(reply, RespValue::Array(vec![]));
}
