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
    buf.extend_from_slice(format!("*{}", parts.len()).as_bytes());
    buf.extend_from_slice(b"\r\n");
    for p in parts {
        buf.extend_from_slice(format!("${}", p.len()).as_bytes());
        buf.extend_from_slice(b"\r\n");
        buf.extend_from_slice(p.as_bytes());
        buf.extend_from_slice(b"\r\n");
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
    RespValue::BulkString(Some(bytes::Bytes::from(s.to_owned())))
}

fn array(items: &[&str]) -> RespValue {
    RespValue::Array(items.iter().map(|s| bulk(s)).collect())
}

async fn start_server() -> (tempfile::TempDir, TcpStream) {
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
async fn test_push_pop_and_length() {
    let (_dir, mut stream) = start_server().await;

    // RPUSH 新建列表并返回长度
    assert_eq!(
        send_cmd(&mut stream, &["RPUSH", "mylist", "a", "b", "c"]).await,
        RespValue::Integer(3)
    );
    assert_eq!(
        send_cmd(&mut stream, &["LLEN", "mylist"]).await,
        RespValue::Integer(3)
    );

    // LPUSH 多个元素，顺序为从左侧依次插入
    assert_eq!(
        send_cmd(&mut stream, &["LPUSH", "mylist", "x", "y"]).await,
        RespValue::Integer(5)
    );
    assert_eq!(
        send_cmd(&mut stream, &["LRANGE", "mylist", "0", "-1"]).await,
        array(&["y", "x", "a", "b", "c"])
    );

    // LPOP / RPOP
    assert_eq!(send_cmd(&mut stream, &["LPOP", "mylist"]).await, bulk("y"));
    assert_eq!(send_cmd(&mut stream, &["RPOP", "mylist"]).await, bulk("c"));
    assert_eq!(
        send_cmd(&mut stream, &["LLEN", "mylist"]).await,
        RespValue::Integer(3)
    );
    assert_eq!(
        send_cmd(&mut stream, &["LRANGE", "mylist", "0", "-1"]).await,
        array(&["x", "a", "b"])
    );
}

#[tokio::test]
async fn test_lrange_and_lindex() {
    let (_dir, mut stream) = start_server().await;

    send_cmd(&mut stream, &["RPUSH", "list", "one", "two", "three"]).await;

    // 正索引与负索引
    assert_eq!(
        send_cmd(&mut stream, &["LINDEX", "list", "0"]).await,
        bulk("one")
    );
    assert_eq!(
        send_cmd(&mut stream, &["LINDEX", "list", "-1"]).await,
        bulk("three")
    );
    assert_eq!(
        send_cmd(&mut stream, &["LINDEX", "list", "5"]).await,
        RespValue::BulkString(None)
    );

    // LRANGE 边界与负范围
    assert_eq!(
        send_cmd(&mut stream, &["LRANGE", "list", "0", "0"]).await,
        array(&["one"])
    );
    assert_eq!(
        send_cmd(&mut stream, &["LRANGE", "list", "-2", "-1"]).await,
        array(&["two", "three"])
    );
    assert_eq!(
        send_cmd(&mut stream, &["LRANGE", "list", "1", "10"]).await,
        array(&["two", "three"])
    );
    assert_eq!(
        send_cmd(&mut stream, &["LRANGE", "list", "5", "10"]).await,
        RespValue::Array(vec![])
    );
}

#[tokio::test]
async fn test_pop_until_empty() {
    let (_dir, mut stream) = start_server().await;

    send_cmd(&mut stream, &["RPUSH", "emptylist", "a", "b"]).await;
    assert_eq!(
        send_cmd(&mut stream, &["LPOP", "emptylist"]).await,
        bulk("a")
    );
    assert_eq!(
        send_cmd(&mut stream, &["LPOP", "emptylist"]).await,
        bulk("b")
    );
    assert_eq!(
        send_cmd(&mut stream, &["LPOP", "emptylist"]).await,
        RespValue::BulkString(None)
    );
    assert_eq!(
        send_cmd(&mut stream, &["LLEN", "emptylist"]).await,
        RespValue::Integer(0)
    );
    assert_eq!(
        send_cmd(&mut stream, &["LRANGE", "emptylist", "0", "-1"]).await,
        RespValue::Array(vec![])
    );
}

#[tokio::test]
async fn test_wrong_type() {
    let (_dir, mut stream) = start_server().await;

    send_cmd(&mut stream, &["SET", "strkey", "value"]).await;
    let reply = send_cmd(&mut stream, &["LLEN", "strkey"]).await;
    assert!(
        matches!(reply, RespValue::Error(ref e) if e.contains("WRONGTYPE")),
        "expected WRONGTYPE error, got {:?}",
        reply
    );

    let reply = send_cmd(&mut stream, &["LPUSH", "strkey", "x"]).await;
    assert!(matches!(reply, RespValue::Error(ref e) if e.contains("WRONGTYPE")));

    let reply = send_cmd(&mut stream, &["LRANGE", "strkey", "0", "-1"]).await;
    assert!(matches!(reply, RespValue::Error(ref e) if e.contains("WRONGTYPE")));
}
