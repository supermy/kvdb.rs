use bytes::Bytes;

use super::{CommandContext, CommandTable, expect_arg_count};
use crate::encoding::decode_string;
use crate::error::{KvdbError, KvdbResult};
use crate::protocol::RespValue;
use crate::replication::ReplicationRole;
use crate::storage::CF_METADATA;

const CF: &str = CF_METADATA;

pub fn register(table: &mut CommandTable) {
    table.register("PING", ping);
    table.register("ECHO", echo);
    table.register("INFO", info);
    table.register("DBSIZE", dbsize);
    table.register("FLUSHDB", flushdb);
    table.register("REPLICAOF", replicaof);
    table.register("ROLE", role);
}

fn ping(_ctx: &CommandContext, args: &[Bytes]) -> KvdbResult<RespValue> {
    if args.is_empty() {
        Ok(RespValue::SimpleString("PONG".to_string()))
    } else {
        Ok(RespValue::BulkString(Some(args[0].clone())))
    }
}

fn echo(_ctx: &CommandContext, args: &[Bytes]) -> KvdbResult<RespValue> {
    expect_arg_count("ECHO", args, 1)?;
    Ok(RespValue::BulkString(Some(args[0].clone())))
}

fn info(ctx: &CommandContext, _args: &[Bytes]) -> KvdbResult<RespValue> {
    let cfg = ctx.config.get();
    let role_str = match ctx.replication.role() {
        ReplicationRole::Master => "master".to_string(),
        ReplicationRole::Replica { host, port } => format!("slave:{host}:{port}"),
    };
    let info = format!(
        "# Server\r\nkvdb_version:0.1.0\r\nconfig_file:{}\r\n\r\n# Clients\r\nmaxclients:{}\r\n\r\n# Persistence\r\ndb_path:{}\r\n\r\n# Replication\r\nrole:{}\r\nmaster_replid:{}\r\nmaster_repl_offset:{}\r\n",
        cfg.config_file
            .as_ref()
            .map(|p| p.display().to_string())
            .unwrap_or_default(),
        cfg.server.maxclients,
        cfg.storage.db_path,
        role_str,
        ctx.replication.master_replid(),
        ctx.replication.master_repl_offset(),
    );
    Ok(RespValue::BulkString(Some(Bytes::from(info))))
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

/// 统计 metadata 列族中未过期的 String 键数量。
/// 复合类型实现后，应扩展为按 metadata 类型统计所有数据类型。
fn dbsize(ctx: &CommandContext, _args: &[Bytes]) -> KvdbResult<RespValue> {
    let count = ctx
        .storage
        .full_scan(CF)?
        .into_iter()
        .filter(|(_, v)| {
            decode_string(v)
                .map(|(_, expire, _)| expire <= 0 || expire > now_ms())
                .unwrap_or(false)
        })
        .count() as i64;
    Ok(RespValue::Integer(count))
}

fn flushdb(ctx: &CommandContext, _args: &[Bytes]) -> KvdbResult<RespValue> {
    let keys: Vec<Vec<u8>> = ctx
        .storage
        .full_scan(CF)?
        .into_iter()
        .map(|(k, _)| k)
        .collect();
    for key in keys {
        ctx.storage.delete(CF, &key)?;
    }
    Ok(RespValue::SimpleString("OK".to_string()))
}

/// REPLICAOF host port / REPLICAOF NO ONE：骨架阶段仅切换角色，不建立真实连接。
fn replicaof(ctx: &CommandContext, args: &[Bytes]) -> KvdbResult<RespValue> {
    expect_arg_count("REPLICAOF", args, 2)?;
    let host = String::from_utf8_lossy(&args[0]);
    let port_str = String::from_utf8_lossy(&args[1]);
    if host.eq_ignore_ascii_case("no") && port_str.eq_ignore_ascii_case("one") {
        ctx.replication.set_master();
        return Ok(RespValue::SimpleString("OK".to_string()));
    }
    let port = port_str
        .parse::<u16>()
        .map_err(|_| KvdbError::Command("port must be a valid integer".to_string()))?;
    ctx.replication.set_replica(host.to_string(), port);
    Ok(RespValue::SimpleString("OK".to_string()))
}

fn role(ctx: &CommandContext, _args: &[Bytes]) -> KvdbResult<RespValue> {
    match ctx.replication.role() {
        ReplicationRole::Master => Ok(RespValue::Array(vec![
            RespValue::BulkString(Some(Bytes::from_static(b"master"))),
            RespValue::Integer(ctx.replication.local_offset() as i64),
        ])),
        ReplicationRole::Replica { host, port } => Ok(RespValue::Array(vec![
            RespValue::BulkString(Some(Bytes::from_static(b"slave"))),
            RespValue::BulkString(Some(Bytes::from(host))),
            RespValue::Integer(port as i64),
            RespValue::BulkString(Some(Bytes::from_static(b"connected"))),
            RespValue::Integer(ctx.replication.master_repl_offset() as i64),
        ])),
    }
}
