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

fn assert_int(value: RespValue, expected: i64) {
    assert_eq!(value, RespValue::Integer(expected));
}

fn assert_ok(value: RespValue) {
    assert_eq!(value, RespValue::SimpleString("OK".to_string()));
}

fn assert_error(value: RespValue) {
    match value {
        RespValue::Error(_) => {}
        other => panic!("expected error, got {:?}", other),
    }
}

#[tokio::test]
async fn bitmap_commands() {
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

    // SETBIT / GETBIT basic
    assert_int(send_cmd(&mut stream, &["SETBIT", "b1", "0", "1"]).await, 0);
    assert_int(send_cmd(&mut stream, &["GETBIT", "b1", "0"]).await, 1);
    assert_int(send_cmd(&mut stream, &["SETBIT", "b1", "0", "1"]).await, 1);
    assert_int(send_cmd(&mut stream, &["SETBIT", "b1", "0", "0"]).await, 1);
    assert_int(send_cmd(&mut stream, &["GETBIT", "b1", "0"]).await, 0);

    // SETBIT returns old value and extends the bitmap
    assert_int(send_cmd(&mut stream, &["SETBIT", "b2", "7", "1"]).await, 0);
    assert_int(send_cmd(&mut stream, &["SETBIT", "b2", "7", "1"]).await, 1);
    assert_int(send_cmd(&mut stream, &["GETBIT", "b2", "7"]).await, 1);
    assert_int(send_cmd(&mut stream, &["GETBIT", "b2", "6"]).await, 0);

    // SETBIT at large offset (crosses fragment boundary)
    assert_int(
        send_cmd(&mut stream, &["SETBIT", "b3", "8192", "1"]).await,
        0,
    );
    assert_int(send_cmd(&mut stream, &["GETBIT", "b3", "8192"]).await, 1);
    assert_int(send_cmd(&mut stream, &["GETBIT", "b3", "0"]).await, 0);

    // GETBIT on non-existent key / fragment
    assert_int(
        send_cmd(&mut stream, &["GETBIT", "nonexistent", "0"]).await,
        0,
    );
    assert_int(send_cmd(&mut stream, &["GETBIT", "b3", "999999"]).await, 0);

    // BITCOUNT full
    assert_int(send_cmd(&mut stream, &["BITCOUNT", "b1"]).await, 0);
    assert_int(send_cmd(&mut stream, &["BITCOUNT", "b2"]).await, 1);
    assert_int(send_cmd(&mut stream, &["BITCOUNT", "b3"]).await, 1);

    // Prepare bitmap with bits 0,1,2 set across bytes 0 and 1 for range tests.
    // byte0 = 0b00000111 = 0x07, byte1 = 0b00000001 = 0x01
    assert_int(send_cmd(&mut stream, &["SETBIT", "b4", "0", "1"]).await, 0);
    assert_int(send_cmd(&mut stream, &["SETBIT", "b4", "1", "1"]).await, 0);
    assert_int(send_cmd(&mut stream, &["SETBIT", "b4", "2", "1"]).await, 0);
    assert_int(send_cmd(&mut stream, &["SETBIT", "b4", "8", "1"]).await, 0);
    assert_int(send_cmd(&mut stream, &["BITCOUNT", "b4"]).await, 4);

    // BITCOUNT with byte range
    assert_int(
        send_cmd(&mut stream, &["BITCOUNT", "b4", "0", "0"]).await,
        3,
    );
    assert_int(
        send_cmd(&mut stream, &["BITCOUNT", "b4", "1", "1"]).await,
        1,
    );
    assert_int(
        send_cmd(&mut stream, &["BITCOUNT", "b4", "0", "1"]).await,
        4,
    );

    // BITCOUNT with negative indices
    assert_int(
        send_cmd(&mut stream, &["BITCOUNT", "b4", "-1", "-1"]).await,
        1,
    );
    assert_int(
        send_cmd(&mut stream, &["BITCOUNT", "b4", "-2", "-1"]).await,
        4,
    );
    assert_int(
        send_cmd(&mut stream, &["BITCOUNT", "b4", "0", "-1"]).await,
        4,
    );

    // BITCOUNT on non-existent key
    assert_int(
        send_cmd(&mut stream, &["BITCOUNT", "nokey", "0", "1"]).await,
        0,
    );

    // BITOP AND / OR / XOR / NOT
    // a: 0b00000011 (bits 0,1), b: 0b00000110 (bits 1,2)
    assert_int(send_cmd(&mut stream, &["SETBIT", "a", "0", "1"]).await, 0);
    assert_int(send_cmd(&mut stream, &["SETBIT", "a", "1", "1"]).await, 0);
    assert_int(send_cmd(&mut stream, &["SETBIT", "b", "1", "1"]).await, 0);
    assert_int(send_cmd(&mut stream, &["SETBIT", "b", "2", "1"]).await, 0);

    assert_int(
        send_cmd(&mut stream, &["BITOP", "AND", "dest", "a", "b"]).await,
        1,
    );
    assert_int(send_cmd(&mut stream, &["GETBIT", "dest", "0"]).await, 0);
    assert_int(send_cmd(&mut stream, &["GETBIT", "dest", "1"]).await, 1);
    assert_int(send_cmd(&mut stream, &["GETBIT", "dest", "2"]).await, 0);

    assert_int(
        send_cmd(&mut stream, &["BITOP", "OR", "dest", "a", "b"]).await,
        1,
    );
    assert_int(send_cmd(&mut stream, &["GETBIT", "dest", "0"]).await, 1);
    assert_int(send_cmd(&mut stream, &["GETBIT", "dest", "1"]).await, 1);
    assert_int(send_cmd(&mut stream, &["GETBIT", "dest", "2"]).await, 1);

    assert_int(
        send_cmd(&mut stream, &["BITOP", "XOR", "dest", "a", "b"]).await,
        1,
    );
    assert_int(send_cmd(&mut stream, &["GETBIT", "dest", "0"]).await, 1);
    assert_int(send_cmd(&mut stream, &["GETBIT", "dest", "1"]).await, 0);
    assert_int(send_cmd(&mut stream, &["GETBIT", "dest", "2"]).await, 1);

    assert_int(
        send_cmd(&mut stream, &["BITOP", "NOT", "dest", "a"]).await,
        1,
    );
    assert_int(send_cmd(&mut stream, &["GETBIT", "dest", "0"]).await, 0);
    assert_int(send_cmd(&mut stream, &["GETBIT", "dest", "1"]).await, 0);
    assert_int(send_cmd(&mut stream, &["GETBIT", "dest", "7"]).await, 1);
    assert_int(send_cmd(&mut stream, &["BITCOUNT", "dest"]).await, 6);

    // BITOP with missing source keys
    assert_int(
        send_cmd(&mut stream, &["BITOP", "OR", "dest2", "a", "missing"]).await,
        1,
    );
    assert_int(send_cmd(&mut stream, &["GETBIT", "dest2", "0"]).await, 1);
    assert_int(send_cmd(&mut stream, &["GETBIT", "dest2", "1"]).await, 1);
    assert_int(send_cmd(&mut stream, &["GETBIT", "dest2", "7"]).await, 0);

    assert_int(
        send_cmd(&mut stream, &["BITOP", "AND", "dest3", "a", "missing"]).await,
        1,
    );
    assert_int(send_cmd(&mut stream, &["GETBIT", "dest3", "0"]).await, 0);
    assert_int(send_cmd(&mut stream, &["GETBIT", "dest3", "1"]).await, 0);

    // BITOP with all missing keys
    assert_int(
        send_cmd(
            &mut stream,
            &["BITOP", "OR", "dest4", "missing1", "missing2"],
        )
        .await,
        0,
    );

    // Wrong type: SETBIT on a string key
    assert_ok(send_cmd(&mut stream, &["SET", "str", "hello"]).await);
    assert_error(send_cmd(&mut stream, &["SETBIT", "str", "0", "1"]).await);

    // Argument errors
    assert_error(send_cmd(&mut stream, &["SETBIT", "x"]).await);
    assert_error(send_cmd(&mut stream, &["GETBIT", "x"]).await);
    assert_error(send_cmd(&mut stream, &["BITCOUNT", "x", "0"]).await);
    assert_error(send_cmd(&mut stream, &["BITOP", "AND", "d"]).await);
    assert_error(send_cmd(&mut stream, &["SETBIT", "x", "0", "2"]).await);
    assert_error(send_cmd(&mut stream, &["SETBIT", "x", "abc", "1"]).await);
    assert_error(send_cmd(&mut stream, &["BITOP", "UNKNOWN", "d", "x"]).await);
}
