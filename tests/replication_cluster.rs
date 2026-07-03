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
async fn replicaof_switches_role() {
    let (_dir, addr) = setup_server().await;
    let mut stream = TcpStream::connect(addr).await.unwrap();

    assert_eq!(
        send_cmd(&mut stream, &["REPLICAOF", "192.168.1.1", "6379"]).await,
        RespValue::SimpleString("OK".to_string())
    );
    let role = send_cmd(&mut stream, &["ROLE"]).await;
    assert_eq!(
        role,
        RespValue::Array(vec![
            RespValue::BulkString(Some(bytes::Bytes::from_static(b"slave"))),
            RespValue::BulkString(Some(bytes::Bytes::from_static(b"192.168.1.1"))),
            RespValue::Integer(6379),
            RespValue::BulkString(Some(bytes::Bytes::from_static(b"connected"))),
            RespValue::Integer(0),
        ])
    );

    assert_eq!(
        send_cmd(&mut stream, &["REPLICAOF", "NO", "ONE"]).await,
        RespValue::SimpleString("OK".to_string())
    );
    let role = send_cmd(&mut stream, &["ROLE"]).await;
    assert_eq!(
        role,
        RespValue::Array(vec![
            RespValue::BulkString(Some(bytes::Bytes::from_static(b"master"))),
            RespValue::Integer(0),
        ])
    );
}

#[tokio::test]
async fn info_reports_replication_role() {
    let (_dir, addr) = setup_server().await;
    let mut stream = TcpStream::connect(addr).await.unwrap();

    send_cmd(&mut stream, &["REPLICAOF", "10.0.0.2", "6380"]).await;
    let info = send_cmd(&mut stream, &["INFO", "replication"]).await;
    if let RespValue::BulkString(Some(bytes)) = info {
        let text = String::from_utf8_lossy(&bytes);
        assert!(text.contains("role:slave:10.0.0.2:6380"));
    } else {
        panic!("INFO did not return bulk string");
    }
}

#[tokio::test]
async fn cluster_keyslot_basic() {
    let (_dir, addr) = setup_server().await;
    let mut stream = TcpStream::connect(addr).await.unwrap();

    // Redis 中 "foo" 的槽位为 12182。
    let slot = send_cmd(&mut stream, &["CLUSTER", "KEYSLOT", "foo"]).await;
    assert_eq!(slot, RespValue::Integer(12182));

    // Hash tag 使 {} 内内容参与计算。
    let slot = send_cmd(&mut stream, &["CLUSTER", "KEYSLOT", "{user}:1"]).await;
    let slot2 = send_cmd(&mut stream, &["CLUSTER", "KEYSLOT", "{user}:2"]).await;
    assert_eq!(slot, slot2);
}

#[tokio::test]
async fn cluster_slots_returns_stub() {
    let (_dir, addr) = setup_server().await;
    let mut stream = TcpStream::connect(addr).await.unwrap();

    let reply = send_cmd(&mut stream, &["CLUSTER", "SLOTS"]).await;
    if let RespValue::Array(items) = reply {
        assert!(!items.is_empty());
    } else {
        panic!("CLUSTER SLOTS did not return array");
    }
}

#[tokio::test]
async fn cluster_nodes_returns_stub() {
    let (_dir, addr) = setup_server().await;
    let mut stream = TcpStream::connect(addr).await.unwrap();

    let reply = send_cmd(&mut stream, &["CLUSTER", "NODES"]).await;
    if let RespValue::BulkString(Some(bytes)) = reply {
        let text = String::from_utf8_lossy(&bytes);
        assert!(text.contains("connected"));
    } else {
        panic!("CLUSTER NODES did not return bulk string");
    }
}
