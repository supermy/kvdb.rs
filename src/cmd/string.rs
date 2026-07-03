use bytes::Bytes;

use super::{CommandContext, CommandTable, expect_arg_count, expect_min_arg_count};
use crate::encoding::{decode_string, encode_string, metadata_key};
use crate::error::{KvdbError, KvdbResult};
use crate::protocol::RespValue;
use crate::storage::{CF_METADATA, DataType};
use crate::types::build_flags;

const CF: &str = CF_METADATA;
const STRING_FLAGS: u8 = build_flags(DataType::String);

pub fn register(table: &mut CommandTable) {
    table.register("GET", get);
    table.register("SET", set);
    table.register("MGET", mget);
    table.register("MSET", mset);
    table.register("DEL", del);
    table.register("EXISTS", exists);
    table.register("INCR", incr);
    table.register("DECR", decr);
    table.register("APPEND", append);
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

fn read_string(ctx: &CommandContext, key: &[u8]) -> KvdbResult<Option<Vec<u8>>> {
    match ctx.storage.get(CF, key)? {
        Some(v) => {
            let (flags, expire, payload) = decode_string(&v)
                .ok_or(KvdbError::Protocol("invalid string encoding".to_string()))?;
            if flags & 0x0F != DataType::String.code() {
                return Err(KvdbError::Command(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
                ));
            }
            if expire > 0 && expire <= now_ms() {
                return Ok(None);
            }
            Ok(Some(payload.to_vec()))
        }
        None => Ok(None),
    }
}

fn write_string(ctx: &CommandContext, key: &[u8], payload: &[u8]) -> KvdbResult<()> {
    let value = encode_string(STRING_FLAGS, 0, payload);
    ctx.storage.put(CF, key, &value)
}

fn get(ctx: &CommandContext, args: &[Bytes]) -> KvdbResult<RespValue> {
    expect_arg_count("GET", args, 1)?;
    let key = metadata_key(&args[0]);
    match read_string(ctx, &key)? {
        Some(v) => Ok(RespValue::BulkString(Some(Bytes::from(v)))),
        None => Ok(RespValue::BulkString(None)),
    }
}

fn set(ctx: &CommandContext, args: &[Bytes]) -> KvdbResult<RespValue> {
    expect_min_arg_count("SET", args, 2)?;
    let key = metadata_key(&args[0]);
    // 生成新版本号：即使覆盖同一 key，也会使旧 subkey（未来扩展复合类型时）失效。
    let value = encode_string(STRING_FLAGS, 0, &args[1]);
    ctx.storage.put(CF, &key, &value)?;
    Ok(RespValue::SimpleString("OK".to_string()))
}

fn mget(ctx: &CommandContext, args: &[Bytes]) -> KvdbResult<RespValue> {
    expect_min_arg_count("MGET", args, 1)?;
    let mut result = Vec::with_capacity(args.len());
    for key in args {
        let encoded = metadata_key(key);
        let value = read_string(ctx, &encoded)?;
        result.push(RespValue::BulkString(value.map(Bytes::from)));
    }
    Ok(RespValue::Array(result))
}

fn mset(ctx: &CommandContext, args: &[Bytes]) -> KvdbResult<RespValue> {
    if args.len() % 2 != 0 {
        return Err(KvdbError::WrongArgCount("MSET"));
    }
    for pair in args.chunks_exact(2) {
        let key = metadata_key(&pair[0]);
        let value = encode_string(STRING_FLAGS, 0, &pair[1]);
        ctx.storage.put(CF, &key, &value)?;
    }
    Ok(RespValue::SimpleString("OK".to_string()))
}

fn del(ctx: &CommandContext, args: &[Bytes]) -> KvdbResult<RespValue> {
    expect_min_arg_count("DEL", args, 1)?;
    let mut count = 0i64;
    for key in args {
        let encoded = metadata_key(key);
        if read_string(ctx, &encoded)?.is_some() {
            ctx.storage.delete(CF, &encoded)?;
            count += 1;
        }
    }
    Ok(RespValue::Integer(count))
}

fn exists(ctx: &CommandContext, args: &[Bytes]) -> KvdbResult<RespValue> {
    expect_min_arg_count("EXISTS", args, 1)?;
    let mut count = 0i64;
    for key in args {
        let encoded = metadata_key(key);
        if read_string(ctx, &encoded)?.is_some() {
            count += 1;
        }
    }
    Ok(RespValue::Integer(count))
}

fn incr(ctx: &CommandContext, args: &[Bytes]) -> KvdbResult<RespValue> {
    expect_arg_count("INCR", args, 1)?;
    let key = metadata_key(&args[0]);
    let current = match read_string(ctx, &key)? {
        Some(v) => parse_integer(&v)?,
        None => 0,
    };
    let new = current.checked_add(1).ok_or(KvdbError::OutOfRange)?;
    write_string(ctx, &key, new.to_string().as_bytes())?;
    Ok(RespValue::Integer(new))
}

fn decr(ctx: &CommandContext, args: &[Bytes]) -> KvdbResult<RespValue> {
    expect_arg_count("DECR", args, 1)?;
    let key = metadata_key(&args[0]);
    let current = match read_string(ctx, &key)? {
        Some(v) => parse_integer(&v)?,
        None => 0,
    };
    let new = current.checked_sub(1).ok_or(KvdbError::OutOfRange)?;
    write_string(ctx, &key, new.to_string().as_bytes())?;
    Ok(RespValue::Integer(new))
}

fn append(ctx: &CommandContext, args: &[Bytes]) -> KvdbResult<RespValue> {
    expect_arg_count("APPEND", args, 2)?;
    let key = metadata_key(&args[0]);
    let mut current = read_string(ctx, &key)?.unwrap_or_default();
    current.extend_from_slice(&args[1]);
    let len = current.len() as i64;
    write_string(ctx, &key, &current)?;
    Ok(RespValue::Integer(len))
}

fn parse_integer(data: &[u8]) -> KvdbResult<i64> {
    std::str::from_utf8(data)
        .map_err(|_| KvdbError::NotInteger)?
        .parse::<i64>()
        .map_err(|_| KvdbError::NotInteger)
}
