use bytes::Bytes;
use rocksdb::WriteBatch;

use super::{
    CommandContext, CommandTable, expect_arg_count, expect_min_arg_count, wrong_type_error,
};
use crate::encoding::{decode_metadata, encode_metadata, generate_version};
use crate::error::{KvdbError, KvdbResult};
use crate::protocol::RespValue;
use crate::storage::{CF_METADATA, CF_SUBKEY, DataType};
use crate::types::Metadata;

const LIST_PAGE_SIZE: usize = 1024;

pub fn register(table: &mut CommandTable) {
    table.register("LPUSH", lpush);
    table.register("RPUSH", rpush);
    table.register("LPOP", lpop);
    table.register("RPOP", rpop);
    table.register("LRANGE", lrange);
    table.register("LINDEX", lindex);
    table.register("LLEN", llen);
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

/// 读取并校验 List 类型的 metadata；不存在或已过期返回 None，类型错误返回 Err。
/// 统一模式：空值检查 → String 类型检查 → decode → 目标类型检查 → 过期检查。
/// 使用 ctx.get_meta 以兼容旧格式（无 namespace）数据。
fn load_list_meta(ctx: &CommandContext, user_key: &[u8]) -> KvdbResult<Option<Metadata>> {
    match ctx.get_meta(user_key)? {
        None => Ok(None),
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
                .ok_or_else(|| KvdbError::Protocol("invalid list metadata".to_string()))?;
            if meta.data_type() != Some(DataType::List) {
                return Err(wrong_type_error());
            }
            if meta.is_expired(now_ms()) {
                return Ok(None);
            }
            Ok(Some(meta))
        }
    }
}

fn parse_i64(data: &[u8]) -> KvdbResult<i64> {
    std::str::from_utf8(data)
        .map_err(|_| KvdbError::NotInteger)?
        .parse::<i64>()
        .map_err(|_| KvdbError::NotInteger)
}

fn parse_usize(data: &[u8]) -> KvdbResult<usize> {
    std::str::from_utf8(data)
        .map_err(|_| KvdbError::NotInteger)?
        .parse::<usize>()
        .map_err(|_| KvdbError::NotInteger)
}

/// List 索引使用带符号可比编码：翻转符号位后按 u64 大端序存储，
/// 使得 RocksDB 的字节序与 i64 数值序一致，负索引也能排在正索引之前。
fn encode_index(idx: i64) -> [u8; 8] {
    (idx as u64 ^ (1u64 << 63)).to_be_bytes()
}

fn decode_index(bytes: &[u8]) -> KvdbResult<i64> {
    let raw = bytes[..8]
        .try_into()
        .map_err(|_| KvdbError::Protocol("invalid list index bytes".to_string()))?;
    Ok((u64::from_be_bytes(raw) ^ (1u64 << 63)) as i64)
}

fn push(ctx: &CommandContext, args: &[Bytes], left: bool) -> KvdbResult<RespValue> {
    let name = if left { "LPUSH" } else { "RPUSH" };
    expect_min_arg_count(name, args, 2)?;

    let user_key = &args[0];
    let mut meta = match load_list_meta(ctx, user_key)? {
        Some(m) => m,
        None => Metadata::new(DataType::List, generate_version()),
    };

    let mut batch = WriteBatch::default();
    for element in &args[1..] {
        if left {
            meta.head -= 1;
            let sk = ctx.sub_key(user_key, meta.version, &encode_index(meta.head));
            batch.put_cf(ctx.storage.cf(CF_SUBKEY)?, &sk, element);
        } else {
            meta.tail += 1;
            let sk = ctx.sub_key(user_key, meta.version, &encode_index(meta.tail));
            batch.put_cf(ctx.storage.cf(CF_SUBKEY)?, &sk, element);
        }
        meta.size += 1;
    }
    batch.put_cf(
        ctx.storage.cf(CF_METADATA)?,
        ctx.meta_key(user_key),
        encode_metadata(&meta),
    );
    ctx.storage.write(batch)?;
    Ok(RespValue::Integer(meta.size as i64))
}

fn lpush(ctx: &CommandContext, args: &[Bytes]) -> KvdbResult<RespValue> {
    push(ctx, args, true)
}

fn rpush(ctx: &CommandContext, args: &[Bytes]) -> KvdbResult<RespValue> {
    push(ctx, args, false)
}

/// LPOP/RPOP key [count]
/// 无 count 时返回单个 bulk string；有 count 时返回数组（最多 count 个元素）。
fn pop(ctx: &CommandContext, args: &[Bytes], left: bool) -> KvdbResult<RespValue> {
    let name = if left { "LPOP" } else { "RPOP" };
    if args.len() != 1 && args.len() != 2 {
        return Err(KvdbError::WrongArgCount(name));
    }

    let user_key = &args[0];

    // 无 count 参数：单个弹出
    if args.len() == 1 {
        let mut meta = match load_list_meta(ctx, user_key)? {
            Some(m) if m.size > 0 => m,
            _ => return Ok(RespValue::BulkString(None)),
        };

        let index = if left { meta.head } else { meta.tail };
        let sk = ctx.sub_key(user_key, meta.version, &encode_index(index));
        let value = ctx.storage.get(CF_SUBKEY, &sk)?;

        let mut batch = WriteBatch::default();
        batch.delete_cf(ctx.storage.cf(CF_SUBKEY)?, &sk);
        if left {
            meta.head += 1;
        } else {
            meta.tail -= 1;
        }
        meta.size -= 1;
        if meta.size == 0 {
            meta.head = 0;
            meta.tail = -1;
        }
        batch.put_cf(
            ctx.storage.cf(CF_METADATA)?,
            ctx.meta_key(user_key),
            encode_metadata(&meta),
        );
        ctx.storage.write(batch)?;
        return Ok(RespValue::BulkString(value.map(Bytes::from)));
    }

    // 有 count 参数：批量弹出
    let count = parse_usize(&args[1])?;
    if count == 0 {
        return Ok(RespValue::Array(vec![]));
    }

    let mut meta = match load_list_meta(ctx, user_key)? {
        Some(m) if m.size > 0 => m,
        _ => return Ok(RespValue::Array(vec![])),
    };

    let actual = count.min(meta.size as usize);
    let mut result = Vec::with_capacity(actual);
    let mut batch = WriteBatch::default();
    let subkey_cf = ctx.storage.cf(CF_SUBKEY)?;

    for _ in 0..actual {
        let index = if left { meta.head } else { meta.tail };
        let sk = ctx.sub_key(user_key, meta.version, &encode_index(index));
        if let Some(v) = ctx.storage.get(CF_SUBKEY, &sk)? {
            result.push(RespValue::BulkString(Some(Bytes::from(v))));
            batch.delete_cf(subkey_cf, &sk);
        }
        if left {
            meta.head += 1;
        } else {
            meta.tail -= 1;
        }
        meta.size -= 1;
    }

    if meta.size == 0 {
        meta.head = 0;
        meta.tail = -1;
    }
    batch.put_cf(
        ctx.storage.cf(CF_METADATA)?,
        ctx.meta_key(user_key),
        encode_metadata(&meta),
    );
    ctx.storage.write(batch)?;
    Ok(RespValue::Array(result))
}

fn lpop(ctx: &CommandContext, args: &[Bytes]) -> KvdbResult<RespValue> {
    pop(ctx, args, true)
}

fn rpop(ctx: &CommandContext, args: &[Bytes]) -> KvdbResult<RespValue> {
    pop(ctx, args, false)
}

fn lrange(ctx: &CommandContext, args: &[Bytes]) -> KvdbResult<RespValue> {
    expect_arg_count("LRANGE", args, 3)?;
    let user_key = &args[0];
    let start = parse_i64(&args[1])?;
    let stop = parse_i64(&args[2])?;

    let meta = match load_list_meta(ctx, user_key)? {
        Some(m) => m,
        None => return Ok(RespValue::Array(vec![])),
    };
    if meta.size == 0 {
        return Ok(RespValue::Array(vec![]));
    }

    let len = meta.size as i64;
    let mut s = if start < 0 { start + len } else { start };
    let mut e = if stop < 0 { stop + len } else { stop };
    if s < 0 {
        s = 0;
    }
    if e >= len {
        e = len - 1;
    }
    if s > e {
        return Ok(RespValue::Array(vec![]));
    }

    let start_idx = meta.head + s;
    let end_idx = meta.head + e;
    let count = (e - s + 1) as usize;

    // 使用分页迭代读取指定 index 范围的 subkey，减少逐点 get 的 IO 次数。
    // 从 prefix 首项开始扫描，通过 idx 范围过滤，避免 start_key 被跳过导致首项丢失。
    let prefix = ctx.sub_key(user_key, meta.version, &[]);
    let mut result = Vec::with_capacity(count);
    let mut current_key = Vec::new();
    loop {
        let (items, next_key) =
            ctx.storage
                .prefix_scan_page(CF_SUBKEY, &prefix, &current_key, LIST_PAGE_SIZE)?;
        if items.is_empty() {
            break;
        }
        for (k, v) in items {
            let (_, _, sub) = ctx
                .parse_subkey(&k)
                .ok_or(KvdbError::Protocol("invalid list subkey".to_string()))?;
            let idx = decode_index(&sub[..8])?;
            if idx < start_idx {
                continue;
            }
            if idx > end_idx {
                return Ok(RespValue::Array(result));
            }
            result.push(RespValue::BulkString(Some(Bytes::from(v))));
            if result.len() >= count {
                return Ok(RespValue::Array(result));
            }
        }
        match next_key {
            Some(k) => current_key = k,
            None => break,
        }
    }
    Ok(RespValue::Array(result))
}

fn lindex(ctx: &CommandContext, args: &[Bytes]) -> KvdbResult<RespValue> {
    expect_arg_count("LINDEX", args, 2)?;
    let user_key = &args[0];
    let index = parse_i64(&args[1])?;

    let meta = match load_list_meta(ctx, user_key)? {
        Some(m) => m,
        None => return Ok(RespValue::BulkString(None)),
    };
    if meta.size == 0 {
        return Ok(RespValue::BulkString(None));
    }

    let len = meta.size as i64;
    let i = if index < 0 { index + len } else { index };
    if i < 0 || i >= len {
        return Ok(RespValue::BulkString(None));
    }

    let idx = meta.head + i;
    let sk = ctx.sub_key(user_key, meta.version, &encode_index(idx));
    let v = ctx.storage.get(CF_SUBKEY, &sk)?;
    Ok(RespValue::BulkString(v.map(Bytes::from)))
}

fn llen(ctx: &CommandContext, args: &[Bytes]) -> KvdbResult<RespValue> {
    expect_arg_count("LLEN", args, 1)?;
    let user_key = &args[0];
    let len = match load_list_meta(ctx, user_key)? {
        Some(m) => m.size,
        None => 0,
    };
    Ok(RespValue::Integer(len as i64))
}
