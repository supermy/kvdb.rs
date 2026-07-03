use bytes::Bytes;
use rocksdb::WriteBatch;
use std::collections::HashSet;

use super::{CommandContext, CommandTable, expect_arg_count, expect_min_arg_count};
use crate::encoding::{
    decode_metadata, encode_metadata, generate_version, metadata_key, parse_subkey, subkey,
};
use crate::error::{KvdbError, KvdbResult};
use crate::protocol::RespValue;
use crate::storage::{CF_METADATA, CF_SUBKEY, DataType};
use crate::types::Metadata;

/// Set 成员在 subkey 列族中的 value 恒为 NULL。
const SET_VALUE: &[u8] = &[];

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

fn wrong_type() -> KvdbError {
    KvdbError::Command(
        "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
    )
}

/// 读取并校验 Set 类型的 metadata；不存在或已过期返回 None，类型错误返回 Err。
fn read_set_metadata(ctx: &CommandContext, key: &[u8]) -> KvdbResult<Option<Metadata>> {
    match ctx.storage.get(CF_METADATA, key)? {
        Some(v) => {
            let meta = decode_metadata(&v)
                .ok_or_else(|| KvdbError::Protocol("invalid metadata encoding".to_string()))?;
            if meta.data_type() != Some(DataType::Set) {
                return Err(wrong_type());
            }
            if meta.is_expired(now_ms()) {
                return Ok(None);
            }
            Ok(Some(meta))
        }
        None => Ok(None),
    }
}

/// 加载指定 Set 当前版本的所有成员到内存 HashSet。
fn load_members(
    ctx: &CommandContext,
    user_key: &[u8],
    meta: &Metadata,
) -> KvdbResult<HashSet<Bytes>> {
    let prefix = metadata_key(user_key);
    let items = ctx.storage.prefix_scan(CF_SUBKEY, &prefix)?;
    let mut members = HashSet::with_capacity(meta.size as usize);
    for (k, _v) in items {
        if let Some((parsed_key, version, member)) = parse_subkey(&k) {
            if parsed_key == user_key && version == meta.version {
                members.insert(Bytes::copy_from_slice(member));
            }
        }
    }
    Ok(members)
}

pub fn register(table: &mut CommandTable) {
    table.register("SADD", sadd);
    table.register("SREM", srem);
    table.register("SISMEMBER", sismember);
    table.register("SMEMBERS", smembers);
    table.register("SCARD", scard);
    table.register("SINTER", sinter);
    table.register("SUNION", sunion);
}

fn sadd(ctx: &CommandContext, args: &[Bytes]) -> KvdbResult<RespValue> {
    expect_min_arg_count("SADD", args, 2)?;
    let user_key = &args[0];
    let meta_key = metadata_key(user_key);
    let (meta, is_new) = match read_set_metadata(ctx, &meta_key)? {
        Some(m) => (m, false),
        None => (Metadata::new(DataType::Set, generate_version()), true),
    };

    let mut added = 0i64;
    let mut batch = WriteBatch::default();
    // 同一命令内的重复 member 只计数一次，与 Redis 行为一致。
    let mut seen = HashSet::new();
    for member in &args[1..] {
        if !seen.insert(member.clone()) {
            continue;
        }
        let skey = subkey(user_key, meta.version, member);
        if ctx.storage.get(CF_SUBKEY, &skey)?.is_none() {
            ctx.storage
                .batch_put(&mut batch, CF_SUBKEY, &skey, SET_VALUE)?;
            added += 1;
        }
    }

    if added > 0 || is_new {
        let new_size = meta.size.saturating_add(added as u64);
        let mut new_meta = meta;
        new_meta.size = new_size;
        ctx.storage.batch_put(
            &mut batch,
            CF_METADATA,
            &meta_key,
            &encode_metadata(&new_meta),
        )?;
        ctx.storage.write(batch)?;
    }

    Ok(RespValue::Integer(added))
}

fn srem(ctx: &CommandContext, args: &[Bytes]) -> KvdbResult<RespValue> {
    expect_min_arg_count("SREM", args, 2)?;
    let user_key = &args[0];
    let meta_key = metadata_key(user_key);
    let meta = match read_set_metadata(ctx, &meta_key)? {
        Some(m) => m,
        None => return Ok(RespValue::Integer(0)),
    };

    let mut removed = 0i64;
    let mut batch = WriteBatch::default();
    let mut seen = HashSet::new();
    for member in &args[1..] {
        if !seen.insert(member.clone()) {
            continue;
        }
        let skey = subkey(user_key, meta.version, member);
        if ctx.storage.get(CF_SUBKEY, &skey)?.is_some() {
            ctx.storage.batch_delete(&mut batch, CF_SUBKEY, &skey)?;
            removed += 1;
        }
    }

    if removed > 0 {
        let new_size = meta.size.saturating_sub(removed as u64);
        let mut new_meta = meta;
        new_meta.size = new_size;
        ctx.storage.batch_put(
            &mut batch,
            CF_METADATA,
            &meta_key,
            &encode_metadata(&new_meta),
        )?;
        ctx.storage.write(batch)?;
    }

    Ok(RespValue::Integer(removed))
}

fn sismember(ctx: &CommandContext, args: &[Bytes]) -> KvdbResult<RespValue> {
    expect_arg_count("SISMEMBER", args, 2)?;
    let user_key = &args[0];
    let meta_key = metadata_key(user_key);
    let meta = match read_set_metadata(ctx, &meta_key)? {
        Some(m) => m,
        None => return Ok(RespValue::Integer(0)),
    };
    let skey = subkey(user_key, meta.version, &args[1]);
    let exists = ctx.storage.get(CF_SUBKEY, &skey)?.is_some();
    Ok(RespValue::Integer(if exists { 1 } else { 0 }))
}

fn smembers(ctx: &CommandContext, args: &[Bytes]) -> KvdbResult<RespValue> {
    expect_arg_count("SMEMBERS", args, 1)?;
    let user_key = &args[0];
    let meta_key = metadata_key(user_key);
    let meta = match read_set_metadata(ctx, &meta_key)? {
        Some(m) => m,
        None => return Ok(RespValue::Array(vec![])),
    };
    let members = load_members(ctx, user_key, &meta)?;
    let resp = members
        .into_iter()
        .map(|m| RespValue::BulkString(Some(m)))
        .collect();
    Ok(RespValue::Array(resp))
}

fn scard(ctx: &CommandContext, args: &[Bytes]) -> KvdbResult<RespValue> {
    expect_arg_count("SCARD", args, 1)?;
    let user_key = &args[0];
    let meta_key = metadata_key(user_key);
    let meta = match read_set_metadata(ctx, &meta_key)? {
        Some(m) => m,
        None => return Ok(RespValue::Integer(0)),
    };
    Ok(RespValue::Integer(meta.size as i64))
}

fn sinter(ctx: &CommandContext, args: &[Bytes]) -> KvdbResult<RespValue> {
    expect_min_arg_count("SINTER", args, 1)?;
    let mut result: Option<HashSet<Bytes>> = None;
    for user_key in args {
        let meta_key = metadata_key(user_key);
        let meta = match read_set_metadata(ctx, &meta_key)? {
            Some(m) => m,
            None => return Ok(RespValue::Array(vec![])),
        };
        let members = load_members(ctx, user_key, &meta)?;
        match result.as_mut() {
            Some(set) => set.retain(|m| members.contains(m)),
            None => result = Some(members),
        }
    }
    let set = result.unwrap_or_default();
    let resp = set
        .into_iter()
        .map(|m| RespValue::BulkString(Some(m)))
        .collect();
    Ok(RespValue::Array(resp))
}

fn sunion(ctx: &CommandContext, args: &[Bytes]) -> KvdbResult<RespValue> {
    expect_min_arg_count("SUNION", args, 1)?;
    let mut result = HashSet::new();
    for user_key in args {
        let meta_key = metadata_key(user_key);
        if let Some(meta) = read_set_metadata(ctx, &meta_key)? {
            let members = load_members(ctx, user_key, &meta)?;
            result.extend(members);
        }
    }
    let resp = result
        .into_iter()
        .map(|m| RespValue::BulkString(Some(m)))
        .collect();
    Ok(RespValue::Array(resp))
}
