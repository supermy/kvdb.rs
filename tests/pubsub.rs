use bytes::{Buf, BytesMut};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{Duration, timeout};

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
    recv_value(stream).await
}

async fn recv_value(stream: &mut TcpStream) -> RespValue {
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
    tokio::time::sleep(Duration::from_millis(50)).await;
    (dir, addr)
}

#[tokio::test]
async fn pubsub_subscribe_and_receive_message() {
    let (_dir, addr) = setup_server().await;

    let mut sub = TcpStream::connect(addr).await.unwrap();
    let reply = send_cmd(&mut sub, &["SUBSCRIBE", "news"]).await;
    assert_eq!(
        reply,
        RespValue::Array(vec![RespValue::Array(vec![
            RespValue::BulkString(Some(bytes::Bytes::from_static(b"subscribe"))),
            RespValue::BulkString(Some(bytes::Bytes::from_static(b"news"))),
            RespValue::Integer(1),
        ])])
    );

    let mut pub_conn = TcpStream::connect(addr).await.unwrap();
    let reply = send_cmd(&mut pub_conn, &["PUBLISH", "news", "hello"]).await;
    assert_eq!(reply, RespValue::Integer(1));

    let msg = timeout(Duration::from_secs(2), recv_value(&mut sub))
        .await
        .expect("did not receive pubsub message in time");
    assert_eq!(
        msg,
        RespValue::Array(vec![
            RespValue::BulkString(Some(bytes::Bytes::from_static(b"message"))),
            RespValue::BulkString(Some(bytes::Bytes::from_static(b"news"))),
            RespValue::BulkString(Some(bytes::Bytes::from_static(b"hello"))),
        ])
    );
}

#[tokio::test]
async fn pubsub_unsubscribe_stops_messages() {
    let (_dir, addr) = setup_server().await;

    let mut sub = TcpStream::connect(addr).await.unwrap();
    send_cmd(&mut sub, &["SUBSCRIBE", "news"]).await;

    let mut pub_conn = TcpStream::connect(addr).await.unwrap();
    let reply = send_cmd(&mut pub_conn, &["PUBLISH", "news", "before"]).await;
    assert_eq!(reply, RespValue::Integer(1));

    // 消费推送的消息
    let _ = timeout(Duration::from_secs(2), recv_value(&mut sub))
        .await
        .expect("did not receive first message");

    let reply = send_cmd(&mut sub, &["UNSUBSCRIBE", "news"]).await;
    assert_eq!(
        reply,
        RespValue::Array(vec![RespValue::Array(vec![
            RespValue::BulkString(Some(bytes::Bytes::from_static(b"unsubscribe"))),
            RespValue::BulkString(Some(bytes::Bytes::from_static(b"news"))),
            RespValue::Integer(0),
        ])])
    );

    let reply = send_cmd(&mut pub_conn, &["PUBLISH", "news", "after"]).await;
    assert_eq!(reply, RespValue::Integer(0));

    // 不应再收到消息
    let result = timeout(Duration::from_millis(200), recv_value(&mut sub)).await;
    assert!(
        result.is_err(),
        "should not receive message after unsubscribe"
    );
}

#[tokio::test]
async fn pubsub_multiple_subscribers() {
    let (_dir, addr) = setup_server().await;

    let mut sub1 = TcpStream::connect(addr).await.unwrap();
    let mut sub2 = TcpStream::connect(addr).await.unwrap();
    send_cmd(&mut sub1, &["SUBSCRIBE", "chat"]).await;
    send_cmd(&mut sub2, &["SUBSCRIBE", "chat"]).await;

    let mut pub_conn = TcpStream::connect(addr).await.unwrap();
    let reply = send_cmd(&mut pub_conn, &["PUBLISH", "chat", "hi"]).await;
    assert_eq!(reply, RespValue::Integer(2));

    for sub in [&mut sub1, &mut sub2] {
        let msg = timeout(Duration::from_secs(2), recv_value(sub))
            .await
            .expect("subscriber did not receive message");
        assert_eq!(
            msg,
            RespValue::Array(vec![
                RespValue::BulkString(Some(bytes::Bytes::from_static(b"message"))),
                RespValue::BulkString(Some(bytes::Bytes::from_static(b"chat"))),
                RespValue::BulkString(Some(bytes::Bytes::from_static(b"hi"))),
            ])
        );
    }
}
