use bytes::{Buf, BytesMut};
use sha1::{Digest, Sha1};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use kvdb_rs::protocol::{RespParser, RespValue};
use kvdb_rs::{Config, ConfigManager, Server, StorageEngine};

fn sha1_hex(script: &str) -> String {
    let mut hasher = Sha1::new();
    hasher.update(script.as_bytes());
    hasher
        .finalize()
        .iter()
        .map(|b| format!("{:02x}", b))
        .collect()
}

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
async fn eval_set_get_via_server() {
    let (_dir, addr) = setup_server().await;
    let mut stream = TcpStream::connect(addr).await.unwrap();

    let reply = send_cmd(
        &mut stream,
        &[
            "EVAL",
            "redis.call('SET', KEYS[1], ARGV[1]); return redis.call('GET', KEYS[1])",
            "1",
            "lua_key",
            "lua_value",
        ],
    )
    .await;
    assert_eq!(
        reply,
        RespValue::BulkString(Some(bytes::Bytes::from_static(b"lua_value")))
    );
}

#[tokio::test]
async fn evalsha_uses_script_cache() {
    let (_dir, addr) = setup_server().await;
    let mut stream = TcpStream::connect(addr).await.unwrap();

    let script = "return ARGV[1]";
    let sha1 = sha1_hex(script);
    let reply = send_cmd(&mut stream, &["EVALSHA", &sha1, "0", "hello"]).await;
    assert_eq!(
        reply,
        RespValue::Error("ERR NOSCRIPT No matching script. Please use EVAL.".to_string())
    );

    let reply = send_cmd(&mut stream, &["EVAL", script, "0", "hello"]).await;
    assert_eq!(
        reply,
        RespValue::BulkString(Some(bytes::Bytes::from_static(b"hello")))
    );

    let reply = send_cmd(&mut stream, &["EVALSHA", &sha1, "0", "hello"]).await;
    assert_eq!(
        reply,
        RespValue::BulkString(Some(bytes::Bytes::from_static(b"hello")))
    );
}
