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

#[tokio::test]
async fn smoke_set_get_del() {
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

    // SET
    let reply = send_cmd(&mut stream, &["SET", "foo", "bar"]).await;
    assert_eq!(reply, RespValue::SimpleString("OK".to_string()));
    tracing::info!("[SMOKE] SET foo bar -> OK");

    // GET
    let reply = send_cmd(&mut stream, &["GET", "foo"]).await;
    assert_eq!(
        reply,
        RespValue::BulkString(Some(bytes::Bytes::from_static(b"bar")))
    );
    tracing::info!("[SMOKE] GET foo -> bar");

    // DEL
    let reply = send_cmd(&mut stream, &["DEL", "foo"]).await;
    assert_eq!(reply, RespValue::Integer(1));

    // GET again
    let reply = send_cmd(&mut stream, &["GET", "foo"]).await;
    assert_eq!(reply, RespValue::BulkString(None));
}
