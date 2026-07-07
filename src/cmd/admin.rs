use bytes::Bytes;

use super::{CommandContext, CommandTable, expect_arg_count};
use crate::encoding::decode_string;
use crate::error::{KvdbError, KvdbResult};
use crate::protocol::RespValue;
use crate::replication::ReplicationRole;
use crate::storage::{CF_METADATA, CF_SUBKEY, CF_ZSET_SCORE};

const CF: &str = CF_METADATA;
const FLUSH_BATCH_SIZE: usize = 1024;

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
        "# Server\r\nkvdb_version:0.1.0\r\nconfig_file:{}\r\nnamespace:{}\r\n\r\n# Clients\r\nmaxclients:{}\r\n\r\n# Persistence\r\ndb_path:{}\r\n\r\n# Replication\r\nrole:{}\r\nmaster_replid:{}\r\nmaster_repl_offset:{}\r\n",
        cfg.config_file
            .as_ref()
            .map(|p| p.display().to_string())
            .unwrap_or_default(),
        cfg.server.namespace,
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

/// 构造当前 namespace 的扫描前缀。
/// namespace 非空时为 [ns_len][namespace]，可匹配 metadata 键与 subkey；
/// namespace 为空时返回空前缀，扫描全部键（均属于默认 namespace）。
fn namespace_prefix(ctx: &CommandContext) -> Vec<u8> {
    if ctx.namespace.is_empty() {
        Vec::new()
    } else {
        let mut p = Vec::with_capacity(1 + ctx.namespace.len());
        p.push(ctx.namespace.len() as u8);
        p.extend_from_slice(&ctx.namespace);
        p
    }
}

/// 统计当前 namespace 下未过期的键数量。
/// 使用分页迭代避免全量加载到内存；兼容 String 与复合类型。
fn dbsize(ctx: &CommandContext, _args: &[Bytes]) -> KvdbResult<RespValue> {
    let prefix = namespace_prefix(ctx);
    let mut count = 0i64;
    let mut start_key = Vec::new();
    loop {
        let (items, next_key) =
            ctx.storage
                .prefix_scan_page(CF, &prefix, &start_key, FLUSH_BATCH_SIZE)?;
        if items.is_empty() {
            break;
        }
        for (_, v) in items {
            if !is_expired_value(&v) {
                count += 1;
            }
        }
        match next_key {
            Some(k) => start_key = k,
            None => break,
        }
    }
    Ok(RespValue::Integer(count))
}

/// 判断 metadata value 是否已过期；String 类型使用 expire 字段，复合类型由命令自身处理过期。
fn is_expired_value(v: &[u8]) -> bool {
    if let Some((_, expire, _)) = decode_string(v) {
        expire > 0 && expire <= now_ms()
    } else {
        false
    }
}

/// 清空当前 namespace 下的所有数据：metadata + subkey + zset_score。
/// 使用分页批量删除，避免全量加载到内存。
fn flushdb(ctx: &CommandContext, _args: &[Bytes]) -> KvdbResult<RespValue> {
    let prefix = namespace_prefix(ctx);
    ctx.storage
        .delete_prefix(CF_METADATA, &prefix, FLUSH_BATCH_SIZE)?;
    ctx.storage
        .delete_prefix(CF_SUBKEY, &prefix, FLUSH_BATCH_SIZE)?;
    ctx.storage
        .delete_prefix(CF_ZSET_SCORE, &prefix, FLUSH_BATCH_SIZE)?;
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
