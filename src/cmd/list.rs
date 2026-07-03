use bytes::Bytes;
use rocksdb::WriteBatch;

use super::{CommandContext, CommandTable, expect_arg_count, expect_min_arg_count};
use crate::encoding::{decode_metadata, encode_metadata, generate_version, metadata_key, subkey};
use crate::error::{KvdbError, KvdbResult};
use crate::protocol::RespValue;
use crate::storage::{CF_METADATA, CF_SUBKEY, DataType};
use crate::types::Metadata;

const WRONGTYPE: &str = "WRONGTYPE Operation against a key holding the wrong kind of value";

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

fn load_list_meta(ctx: &CommandContext, user_key: &[u8]) -> KvdbResult<Option<Metadata>> {
    let meta_key = metadata_key(user_key);
    match ctx.storage.get(CF_METADATA, &meta_key)? {
        None => Ok(None),
        Some(v) => {
            let dtype = DataType::from_code(v[0] & 0x0F);
            if dtype != Some(DataType::List) {
                return Err(KvdbError::Command(WRONGTYPE.to_string()));
            }
            let meta = decode_metadata(&v)
                .ok_or_else(|| KvdbError::Protocol("invalid list metadata".to_string()))?;
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

fn push(ctx: &CommandContext, args: &[Bytes], left: bool) -> KvdbResult<RespValue> {
    let name = if left { "LPUSH" } else { "RPUSH" };
    expect_min_arg_count(name, args, 2)?;

    let user_key = &args[0];
    let meta_key = metadata_key(user_key);
    let mut meta = match load_list_meta(ctx, user_key)? {
        Some(m) => m,
        None => Metadata::new(DataType::List, generate_version()),
    };

    let mut batch = WriteBatch::default();
    for element in &args[1..] {
        if left {
            meta.head -= 1;
            let sk = subkey(user_key, meta.version, &meta.head.to_be_bytes());
            batch.put_cf(ctx.storage.cf(CF_SUBKEY)?, &sk, element);
        } else {
            meta.tail += 1;
            let sk = subkey(user_key, meta.version, &meta.tail.to_be_bytes());
            batch.put_cf(ctx.storage.cf(CF_SUBKEY)?, &sk, element);
        }
        meta.size += 1;
    }
    batch.put_cf(
        ctx.storage.cf(CF_METADATA)?,
        &meta_key,
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

fn pop(ctx: &CommandContext, args: &[Bytes], left: bool) -> KvdbResult<RespValue> {
    let name = if left { "LPOP" } else { "RPOP" };
    expect_arg_count(name, args, 1)?;

    let user_key = &args[0];
    let meta_key = metadata_key(user_key);
    let mut meta = match load_list_meta(ctx, user_key)? {
        Some(m) if m.size > 0 => m,
        _ => return Ok(RespValue::BulkString(None)),
    };

    let index = if left { meta.head } else { meta.tail };
    let sk = subkey(user_key, meta.version, &index.to_be_bytes());
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
        &meta_key,
        encode_metadata(&meta),
    );
    ctx.storage.write(batch)?;
    Ok(RespValue::BulkString(value.map(Bytes::from)))
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

    let mut result = Vec::with_capacity((e - s + 1) as usize);
    for i in s..=e {
        let idx = meta.head + i;
        let sk = subkey(user_key, meta.version, &idx.to_be_bytes());
        let v = ctx.storage.get(CF_SUBKEY, &sk)?;
        result.push(RespValue::BulkString(v.map(Bytes::from)));
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
    let sk = subkey(user_key, meta.version, &idx.to_be_bytes());
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
