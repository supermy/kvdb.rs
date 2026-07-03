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

async fn setup_server() -> (tempfile::TempDir, std::net::SocketAddr) {
    let dir = tempfile::tempdir().unwrap();
    let config = Arc::new(ConfigManager::new(build_config(&dir)));
    let storage =
        Arc::new(StorageEngine::open(&config.get().storage.db_path, &config.get()).unwrap());
    let server = Server::bind(config, storage).await.unwrap();
    let addr = server.local_addr().unwrap();
    tokio::spawn(async move {
        server.run().await.unwrap();
    });
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    (dir, addr)
}

#[tokio::test]
async fn transaction_multi_exec_queued() {
    let (_dir, addr) = setup_server().await;
    let mut stream = TcpStream::connect(addr).await.unwrap();

    assert_eq!(
        send_cmd(&mut stream, &["MULTI"]).await,
        RespValue::SimpleString("OK".to_string())
    );
    assert_eq!(
        send_cmd(&mut stream, &["SET", "t", "1"]).await,
        RespValue::SimpleString("QUEUED".to_string())
    );
    assert_eq!(
        send_cmd(&mut stream, &["GET", "t"]).await,
        RespValue::SimpleString("QUEUED".to_string())
    );
    let reply = send_cmd(&mut stream, &["EXEC"]).await;
    assert_eq!(
        reply,
        RespValue::Array(vec![
            RespValue::SimpleString("OK".to_string()),
            RespValue::BulkString(Some(bytes::Bytes::from_static(b"1"))),
        ])
    );
}

#[tokio::test]
async fn transaction_discard_clears_queue() {
    let (_dir, addr) = setup_server().await;
    let mut stream = TcpStream::connect(addr).await.unwrap();

    assert_eq!(
        send_cmd(&mut stream, &["MULTI"]).await,
        RespValue::SimpleString("OK".to_string())
    );
    assert_eq!(
        send_cmd(&mut stream, &["SET", "disc", "x"]).await,
        RespValue::SimpleString("QUEUED".to_string())
    );
    assert_eq!(
        send_cmd(&mut stream, &["DISCARD"]).await,
        RespValue::SimpleString("OK".to_string())
    );
    assert_eq!(
        send_cmd(&mut stream, &["GET", "disc"]).await,
        RespValue::BulkString(None)
    );
}

#[tokio::test]
async fn transaction_watch_detects_change() {
    let (_dir, addr) = setup_server().await;
    let mut watcher = TcpStream::connect(addr).await.unwrap();
    let mut mutator = TcpStream::connect(addr).await.unwrap();

    send_cmd(&mut watcher, &["SET", "watched", "init"]).await;
    assert_eq!(
        send_cmd(&mut watcher, &["WATCH", "watched"]).await,
        RespValue::SimpleString("OK".to_string())
    );
    assert_eq!(
        send_cmd(&mut watcher, &["MULTI"]).await,
        RespValue::SimpleString("OK".to_string())
    );
    assert_eq!(
        send_cmd(&mut watcher, &["GET", "watched"]).await,
        RespValue::SimpleString("QUEUED".to_string())
    );

    // 另一连接修改被监控键，导致 EXEC 失败。
    send_cmd(&mut mutator, &["SET", "watched", "changed"]).await;

    let reply = send_cmd(&mut watcher, &["EXEC"]).await;
    assert_eq!(reply, RespValue::Null);
}
