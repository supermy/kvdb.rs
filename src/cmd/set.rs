use bytes::Bytes;
use rocksdb::WriteBatch;
use std::collections::HashSet;

use super::{
    CommandContext, CommandTable, expect_arg_count, expect_min_arg_count, wrong_type_error,
};
use crate::encoding::{decode_metadata, encode_metadata, generate_version};
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

/// 读取并校验 Set 类型的 metadata；不存在或已过期返回 None，类型错误返回 Err。
/// 统一模式：空值检查 → String 类型检查 → decode → 目标类型检查 → 过期检查。
/// 使用 ctx.get_meta 以兼容旧格式（无 namespace）数据。
fn read_set_metadata(ctx: &CommandContext, user_key: &[u8]) -> KvdbResult<Option<Metadata>> {
    match ctx.get_meta(user_key)? {
        Some(v) => {
            if v.is_empty() {
                return Err(KvdbError::Protocol("empty metadata value".to_string()));
            }
            // String 类型使用独立编码，优先判定以避免 decode_metadata 误解析短 payload。
            let type_code = v[0] & crate::types::FLAGS_TYPE_MASK;
            if type_code == DataType::String.code() {
                return Err(wrong_type_error());
            }
            let meta = decode_metadata(&v)
                .ok_or_else(|| KvdbError::Protocol("invalid set metadata".to_string()))?;
            if meta.data_type() != Some(DataType::Set) {
                return Err(wrong_type_error());
            }
            if meta.is_expired(now_ms()) {
                return Ok(None);
            }
            Ok(Some(meta))
        }
        None => Ok(None),
    }
}

/// Set 运算分页大小；限制单次命令的内存峰值。
const SET_OPS_CHUNK_SIZE: usize = 1024;
/// SUNION 结果上限，防止超大并集导致 OOM。
const SET_UNION_RESULT_LIMIT: usize = 1_000_000;

/// 加载指定 Set 当前版本的所有成员到内存 HashSet。
/// 生产环境下仅用于 SMEMBERS 等必须全量返回的命令；Set 运算请使用分页迭代。
fn load_members(
    ctx: &CommandContext,
    user_key: &[u8],
    meta: &Metadata,
) -> KvdbResult<HashSet<Bytes>> {
    let prefix = ctx.sub_key(user_key, meta.version, &[]);
    let items = ctx.storage.prefix_scan(CF_SUBKEY, &prefix)?;
    let mut members = HashSet::with_capacity(meta.size as usize);
    for (k, _v) in items {
        if let Some((parsed_key, version, member)) = ctx.parse_subkey(&k) {
            if parsed_key == user_key && version == meta.version {
                members.insert(Bytes::copy_from_slice(member));
            }
        }
    }
    Ok(members)
}

/// 分页迭代一个 Set 的成员，每次返回一页成员（不含 subkey 包装）。
fn iter_members_page(
    ctx: &CommandContext,
    user_key: &[u8],
    version: u64,
    start_key: &[u8],
) -> KvdbResult<(Vec<Bytes>, Option<Vec<u8>>)> {
    let prefix = ctx.sub_key(user_key, version, &[]);
    let (items, next_key) =
        ctx.storage
            .prefix_scan_page(CF_SUBKEY, &prefix, start_key, SET_OPS_CHUNK_SIZE)?;
    let members: Vec<Bytes> = items
        .into_iter()
        .filter_map(|(k, _)| {
            let (_, v, member) = ctx.parse_subkey(&k)?;
            if v == version {
                Some(Bytes::copy_from_slice(member))
            } else {
                None
            }
        })
        .collect();
    Ok((members, next_key))
}

/// 判断 member 是否存在于指定 Set 中。
fn member_exists(
    ctx: &CommandContext,
    user_key: &[u8],
    version: u64,
    member: &[u8],
) -> KvdbResult<bool> {
    let skey = ctx.sub_key(user_key, version, member);
    Ok(ctx.storage.get(CF_SUBKEY, &skey)?.is_some())
}

pub fn register(table: &mut CommandTable) {
    table.register("SADD", sadd);
    table.register("SREM", srem);
    table.register("SISMEMBER", sismember);
    table.register("SMEMBERS", smembers);
    table.register("SCARD", scard);
    table.register("SINTER", sinter);
    table.register("SDIFF", sdiff);
    table.register("SUNION", sunion);
}

fn sadd(ctx: &CommandContext, args: &[Bytes]) -> KvdbResult<RespValue> {
    expect_min_arg_count("SADD", args, 2)?;
    let user_key = &args[0];
    let meta_key = ctx.meta_key(user_key);
    let (meta, is_new) = match read_set_metadata(ctx, user_key)? {
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
        let skey = ctx.sub_key(user_key, meta.version, member);
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
    let meta_key = ctx.meta_key(user_key);
    let meta = match read_set_metadata(ctx, user_key)? {
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
        let skey = ctx.sub_key(user_key, meta.version, member);
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
    let meta = match read_set_metadata(ctx, user_key)? {
        Some(m) => m,
        None => return Ok(RespValue::Integer(0)),
    };
    let skey = ctx.sub_key(user_key, meta.version, &args[1]);
    let exists = ctx.storage.get(CF_SUBKEY, &skey)?.is_some();
    Ok(RespValue::Integer(if exists { 1 } else { 0 }))
}

fn smembers(ctx: &CommandContext, args: &[Bytes]) -> KvdbResult<RespValue> {
    expect_arg_count("SMEMBERS", args, 1)?;
    let user_key = &args[0];
    let meta = match read_set_metadata(ctx, user_key)? {
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
    let meta = match read_set_metadata(ctx, user_key)? {
        Some(m) => m,
        None => return Ok(RespValue::Integer(0)),
    };
    Ok(RespValue::Integer(meta.size as i64))
}

fn sinter(ctx: &CommandContext, args: &[Bytes]) -> KvdbResult<RespValue> {
    expect_min_arg_count("SINTER", args, 1)?;

    // 收集所有 key 的 metadata，按 size 升序排列，选择最小集合作为迭代基准。
    let mut metas: Vec<(Bytes, Metadata)> = Vec::with_capacity(args.len());
    for user_key in args {
        match read_set_metadata(ctx, user_key)? {
            Some(m) => metas.push((user_key.clone(), m)),
            None => return Ok(RespValue::Array(vec![])),
        }
    }
    metas.sort_by_key(|a| a.1.size);

    let (base_key, base_meta) = metas.remove(0);
    let others: Vec<(Bytes, u64)> = metas.into_iter().map(|(k, m)| (k, m.version)).collect();

    let mut result = Vec::new();
    let mut start_key = Vec::new();
    loop {
        let (members, next_key) = iter_members_page(ctx, &base_key, base_meta.version, &start_key)?;
        if members.is_empty() {
            break;
        }
        for member in members {
            let in_all = others
                .iter()
                .all(|(k, v)| member_exists(ctx, k, *v, &member).unwrap_or(false));
            if in_all {
                result.push(RespValue::BulkString(Some(member)));
            }
        }
        match next_key {
            Some(k) => start_key = k,
            None => break,
        }
    }
    Ok(RespValue::Array(result))
}

fn sdiff(ctx: &CommandContext, args: &[Bytes]) -> KvdbResult<RespValue> {
    expect_min_arg_count("SDIFF", args, 1)?;

    let first_meta = match read_set_metadata(ctx, &args[0])? {
        Some(m) => m,
        None => return Ok(RespValue::Array(vec![])),
    };
    let first_key = &args[0];
    let others: Vec<(Bytes, u64)> = args[1..]
        .iter()
        .filter_map(|k| {
            read_set_metadata(ctx, k)
                .ok()
                .flatten()
                .map(|m| (k.clone(), m.version))
        })
        .collect();

    let mut result = Vec::new();
    let mut start_key = Vec::new();
    loop {
        let (members, next_key) =
            iter_members_page(ctx, first_key, first_meta.version, &start_key)?;
        if members.is_empty() {
            break;
        }
        for member in members {
            let in_any_other = others
                .iter()
                .any(|(k, v)| member_exists(ctx, k, *v, &member).unwrap_or(false));
            if !in_any_other {
                result.push(RespValue::BulkString(Some(member)));
            }
        }
        match next_key {
            Some(k) => start_key = k,
            None => break,
        }
    }
    Ok(RespValue::Array(result))
}

fn sunion(ctx: &CommandContext, args: &[Bytes]) -> KvdbResult<RespValue> {
    expect_min_arg_count("SUNION", args, 1)?;

    let mut seen = HashSet::with_capacity(SET_UNION_RESULT_LIMIT.min(1024));
    let mut result = Vec::new();

    for user_key in args {
        let meta = match read_set_metadata(ctx, user_key)? {
            Some(m) => m,
            None => continue,
        };
        let mut start_key = Vec::new();
        loop {
            let (members, next_key) = iter_members_page(ctx, user_key, meta.version, &start_key)?;
            if members.is_empty() {
                break;
            }
            for member in members {
                if seen.len() >= SET_UNION_RESULT_LIMIT {
                    return Err(KvdbError::Command(
                        "SUNION result exceeds memory limit".to_string(),
                    ));
                }
                if seen.insert(member.clone()) {
                    result.push(RespValue::BulkString(Some(member)));
                }
            }
            match next_key {
                Some(k) => start_key = k,
                None => break,
            }
        }
    }
    Ok(RespValue::Array(result))
}
