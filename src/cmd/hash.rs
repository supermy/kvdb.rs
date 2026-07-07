use bytes::Bytes;
use rocksdb::WriteBatch;

use super::{
    CommandContext, CommandTable, expect_arg_count, expect_min_arg_count, wrong_type_error,
};
use crate::encoding::{decode_metadata, encode_metadata, generate_version};
use crate::error::{KvdbError, KvdbResult};
use crate::protocol::RespValue;
use crate::storage::{CF_METADATA, CF_SUBKEY};
use crate::types::{DataType, FLAGS_TYPE_MASK, Metadata};

const HASH_PAGE_SIZE: usize = 1024;

pub fn register(table: &mut CommandTable) {
    table.register("HSET", hset);
    table.register("HGET", hget);
    table.register("HMGET", hmget);
    table.register("HGETALL", hgetall);
    table.register("HDEL", hdel);
    table.register("HEXISTS", hexists);
    table.register("HLEN", hlen);
    table.register("HSCAN", hscan);
}

/// 读取并校验 Hash 类型的 metadata；不存在或已过期返回 None，类型错误返回 Err。
/// 使用 ctx.get_meta 以兼容旧格式（无 namespace）数据。
fn read_hash_meta(ctx: &CommandContext, user_key: &[u8]) -> KvdbResult<Option<Metadata>> {
    match ctx.get_meta(user_key)? {
        Some(v) => decode_and_check_hash(&v),
        None => Ok(None),
    }
}

fn decode_and_check_hash(v: &[u8]) -> KvdbResult<Option<Metadata>> {
    if v.is_empty() {
        return Err(KvdbError::Protocol("empty metadata value".to_string()));
    }
    // String 与复合类型共享 metadata 列族，通过 flags 低 4 位区分。
    let type_code = v[0] & FLAGS_TYPE_MASK;
    if type_code == DataType::String.code() {
        return Err(wrong_type_error());
    }
    let meta =
        decode_metadata(v).ok_or(KvdbError::Protocol("invalid metadata encoding".to_string()))?;
    if meta.data_type() != Some(DataType::Hash) {
        return Err(wrong_type_error());
    }
    Ok(Some(meta))
}

fn ensure_hash_meta(ctx: &CommandContext, user_key: &[u8]) -> KvdbResult<Metadata> {
    match read_hash_meta(ctx, user_key)? {
        Some(meta) => Ok(meta),
        None => Ok(Metadata::new(DataType::Hash, generate_version())),
    }
}

fn hset(ctx: &CommandContext, args: &[Bytes]) -> KvdbResult<RespValue> {
    expect_min_arg_count("HSET", args, 3)?;
    if args.len() % 2 == 0 {
        return Err(KvdbError::WrongArgCount("HSET"));
    }
    let user_key = &args[0];
    let mut meta = ensure_hash_meta(ctx, user_key)?;
    let mut batch = WriteBatch::default();
    let mut added: i64 = 0;
    for pair in args[1..].chunks_exact(2) {
        let field = &pair[0];
        let value = &pair[1];
        let sk = ctx.sub_key(user_key, meta.version, field);
        let existed = ctx.storage.get(CF_SUBKEY, &sk)?.is_some();
        ctx.storage.batch_put(&mut batch, CF_SUBKEY, &sk, value)?;
        if !existed {
            meta.size += 1;
            added += 1;
        }
    }
    ctx.storage.batch_put(
        &mut batch,
        CF_METADATA,
        &ctx.meta_key(user_key),
        &encode_metadata(&meta),
    )?;
    ctx.storage.write(batch)?;
    Ok(RespValue::Integer(added))
}

fn hget(ctx: &CommandContext, args: &[Bytes]) -> KvdbResult<RespValue> {
    expect_arg_count("HGET", args, 2)?;
    let user_key = &args[0];
    let field = &args[1];
    let meta = match read_hash_meta(ctx, user_key)? {
        Some(m) => m,
        None => return Ok(RespValue::BulkString(None)),
    };
    let sk = ctx.sub_key(user_key, meta.version, field);
    match ctx.storage.get(CF_SUBKEY, &sk)? {
        Some(v) => Ok(RespValue::BulkString(Some(Bytes::from(v)))),
        None => Ok(RespValue::BulkString(None)),
    }
}

fn hmget(ctx: &CommandContext, args: &[Bytes]) -> KvdbResult<RespValue> {
    expect_min_arg_count("HMGET", args, 2)?;
    let user_key = &args[0];
    let meta = match read_hash_meta(ctx, user_key)? {
        Some(m) => m,
        None => {
            return Ok(RespValue::Array(vec![
                RespValue::BulkString(None);
                args.len() - 1
            ]));
        }
    };
    let mut result = Vec::with_capacity(args.len() - 1);
    for field in &args[1..] {
        let sk = ctx.sub_key(user_key, meta.version, field);
        let val = ctx.storage.get(CF_SUBKEY, &sk)?.map(Bytes::from);
        result.push(RespValue::BulkString(val));
    }
    Ok(RespValue::Array(result))
}

fn hgetall(ctx: &CommandContext, args: &[Bytes]) -> KvdbResult<RespValue> {
    expect_arg_count("HGETALL", args, 1)?;
    let user_key = &args[0];
    let meta = match read_hash_meta(ctx, user_key)? {
        Some(m) => m,
        None => return Ok(RespValue::Array(vec![])),
    };
    let prefix = ctx.sub_key(user_key, meta.version, &[]);
    // 使用分页迭代读取 subkey，避免大 Hash 在读取阶段一次性加载到内存。
    let mut result = Vec::with_capacity(meta.size as usize * 2);
    let mut start_key = Vec::new();
    loop {
        let (items, next_key) =
            ctx.storage
                .prefix_scan_page(CF_SUBKEY, &prefix, &start_key, HASH_PAGE_SIZE)?;
        if items.is_empty() {
            break;
        }
        for (k, v) in items {
            let (_, _, field) = ctx
                .parse_subkey(&k)
                .ok_or(KvdbError::Protocol("invalid subkey encoding".to_string()))?;
            result.push(RespValue::BulkString(Some(Bytes::copy_from_slice(field))));
            result.push(RespValue::BulkString(Some(Bytes::from(v))));
        }
        match next_key {
            Some(k) => start_key = k,
            None => break,
        }
    }
    Ok(RespValue::Array(result))
}

fn hdel(ctx: &CommandContext, args: &[Bytes]) -> KvdbResult<RespValue> {
    expect_min_arg_count("HDEL", args, 2)?;
    let user_key = &args[0];
    let mut meta = match read_hash_meta(ctx, user_key)? {
        Some(m) => m,
        None => return Ok(RespValue::Integer(0)),
    };
    let mut batch = WriteBatch::default();
    let mut deleted: i64 = 0;
    for field in &args[1..] {
        let sk = ctx.sub_key(user_key, meta.version, field);
        if ctx.storage.get(CF_SUBKEY, &sk)?.is_some() {
            ctx.storage.batch_delete(&mut batch, CF_SUBKEY, &sk)?;
            meta.size = meta.size.saturating_sub(1);
            deleted += 1;
        }
    }
    if deleted > 0 {
        ctx.storage.batch_put(
            &mut batch,
            CF_METADATA,
            &ctx.meta_key(user_key),
            &encode_metadata(&meta),
        )?;
    }
    ctx.storage.write(batch)?;
    Ok(RespValue::Integer(deleted))
}

fn hexists(ctx: &CommandContext, args: &[Bytes]) -> KvdbResult<RespValue> {
    expect_arg_count("HEXISTS", args, 2)?;
    let user_key = &args[0];
    let field = &args[1];
    let meta = match read_hash_meta(ctx, user_key)? {
        Some(m) => m,
        None => return Ok(RespValue::Integer(0)),
    };
    let sk = ctx.sub_key(user_key, meta.version, field);
    Ok(RespValue::Integer(
        if ctx.storage.get(CF_SUBKEY, &sk)?.is_some() {
            1
        } else {
            0
        },
    ))
}

fn hlen(ctx: &CommandContext, args: &[Bytes]) -> KvdbResult<RespValue> {
    expect_arg_count("HLEN", args, 1)?;
    let user_key = &args[0];
    let meta = match read_hash_meta(ctx, user_key)? {
        Some(m) => m,
        None => return Ok(RespValue::Integer(0)),
    };
    Ok(RespValue::Integer(meta.size as i64))
}

fn parse_usize(s: &[u8]) -> KvdbResult<usize> {
    std::str::from_utf8(s)
        .map_err(|_| KvdbError::NotInteger)?
        .parse::<usize>()
        .map_err(|_| KvdbError::NotInteger)
}

fn encode_cursor(key: &[u8]) -> String {
    if key.is_empty() {
        return "0".to_string();
    }
    key.iter().map(|b| format!("{:02x}", b)).collect()
}

fn decode_cursor(s: &str) -> Option<Vec<u8>> {
    if s.is_empty() || s == "0" {
        return Some(Vec::new());
    }
    if s.len() % 2 != 0 {
        return None;
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).ok())
        .collect()
}

/// HSCAN key cursor [COUNT count]
/// 返回 [next_cursor, [field1, value1, field2, value2, ...]]，cursor 为 "0" 表示遍历结束。
fn hscan(ctx: &CommandContext, args: &[Bytes]) -> KvdbResult<RespValue> {
    expect_min_arg_count("HSCAN", args, 2)?;
    let user_key = &args[0];
    let cursor_str = std::str::from_utf8(&args[1])
        .map_err(|_| KvdbError::Command("invalid cursor".to_string()))?;
    let mut count = 10usize;
    if args.len() >= 4 && args[2].eq_ignore_ascii_case(b"COUNT") {
        count = parse_usize(&args[3])?;
    } else if args.len() > 2 {
        return Err(KvdbError::Command("syntax error".to_string()));
    }

    let start_key = decode_cursor(cursor_str)
        .ok_or_else(|| KvdbError::Command("invalid cursor".to_string()))?;

    let meta = match read_hash_meta(ctx, user_key)? {
        Some(m) => m,
        None => {
            return Ok(RespValue::Array(vec![
                RespValue::BulkString(Some(Bytes::from_static(b"0"))),
                RespValue::Array(vec![]),
            ]));
        }
    };

    let prefix = ctx.sub_key(user_key, meta.version, &[]);
    let (items, next_key) = ctx
        .storage
        .prefix_scan_page(CF_SUBKEY, &prefix, &start_key, count)?;
    let mut entries = Vec::with_capacity(items.len() * 2);
    for (k, v) in items {
        let (_, _, field) = ctx
            .parse_subkey(&k)
            .ok_or(KvdbError::Protocol("invalid subkey encoding".to_string()))?;
        entries.push(RespValue::BulkString(Some(Bytes::copy_from_slice(field))));
        entries.push(RespValue::BulkString(Some(Bytes::from(v))));
    }

    let next_cursor = encode_cursor(&next_key.unwrap_or_default());
    Ok(RespValue::Array(vec![
        RespValue::BulkString(Some(Bytes::from(next_cursor))),
        RespValue::Array(entries),
    ]))
}
