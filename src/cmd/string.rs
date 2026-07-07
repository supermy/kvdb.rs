use bytes::Bytes;
use rocksdb::WriteBatch;

use super::{
    CommandContext, CommandTable, expect_arg_count, expect_min_arg_count, wrong_type_error,
};
use crate::encoding::{decode_metadata, decode_string, encode_metadata, encode_string};
use crate::error::{KvdbError, KvdbResult};
use crate::protocol::RespValue;
use crate::storage::{CF_METADATA, CF_SUBKEY, CF_ZSET_SCORE};
use crate::types::{DataType, FLAGS_TYPE_MASK, build_flags};

const CF: &str = CF_METADATA;
const STRING_FLAGS: u8 = build_flags(DataType::String);
/// DEL 批量清理 subkey 时的单批次大小，避免单个 WriteBatch 过大。
const DEL_BATCH_SIZE: usize = 1024;

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
    table.register("EXPIRE", expire);
    table.register("PEXPIRE", pexpire);
    table.register("TTL", ttl);
    table.register("PTTL", pttl);
    table.register("PERSIST", persist);
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

/// 读取 String 值；使用 ctx.get_meta 以兼容旧格式（无 namespace）数据。
fn read_string(ctx: &CommandContext, user_key: &[u8]) -> KvdbResult<Option<Vec<u8>>> {
    match ctx.get_meta(user_key)? {
        Some(v) => {
            let (flags, expire, payload) = decode_string(&v)
                .ok_or(KvdbError::Protocol("invalid string encoding".to_string()))?;
            if flags & 0x0F != DataType::String.code() {
                return Err(wrong_type_error());
            }
            if expire > 0 && expire <= now_ms() {
                return Ok(None);
            }
            Ok(Some(payload.to_vec()))
        }
        None => Ok(None),
    }
}

fn write_string(ctx: &CommandContext, user_key: &[u8], payload: &[u8]) -> KvdbResult<()> {
    let key = ctx.meta_key(user_key);
    let value = encode_string(STRING_FLAGS, 0, payload);
    ctx.storage.put(CF, &key, &value)
}

fn get(ctx: &CommandContext, args: &[Bytes]) -> KvdbResult<RespValue> {
    expect_arg_count("GET", args, 1)?;
    match read_string(ctx, &args[0])? {
        Some(v) => Ok(RespValue::BulkString(Some(Bytes::from(v)))),
        None => Ok(RespValue::BulkString(None)),
    }
}

/// SET key value [EX seconds] [PX milliseconds] [NX|XX]
/// EX/PX 设置过期时间；NX 仅在 key 不存在时写入；XX 仅在 key 存在时写入。
/// NX/XX 条件不满足时返回 nil bulk string。
fn set(ctx: &CommandContext, args: &[Bytes]) -> KvdbResult<RespValue> {
    expect_min_arg_count("SET", args, 2)?;
    let user_key = &args[0];
    let value = &args[1];

    let mut expire_ms: i64 = 0;
    let mut nx = false;
    let mut xx = false;

    let mut i = 2;
    while i < args.len() {
        let opt = args[i].to_ascii_uppercase();
        match opt.as_slice() {
            b"EX" => {
                if i + 1 >= args.len() {
                    return Err(KvdbError::Command("syntax error".to_string()));
                }
                let secs = parse_i64(&args[i + 1])?;
                if secs <= 0 {
                    return Err(KvdbError::Command("invalid expire time".to_string()));
                }
                expire_ms = now_ms() + secs * 1000;
                i += 2;
            }
            b"PX" => {
                if i + 1 >= args.len() {
                    return Err(KvdbError::Command("syntax error".to_string()));
                }
                let ms = parse_i64(&args[i + 1])?;
                if ms <= 0 {
                    return Err(KvdbError::Command("invalid expire time".to_string()));
                }
                expire_ms = now_ms() + ms;
                i += 2;
            }
            b"NX" => {
                nx = true;
                i += 1;
            }
            b"XX" => {
                xx = true;
                i += 1;
            }
            _ => return Err(KvdbError::Command("syntax error".to_string())),
        }
    }
    if nx && xx {
        return Err(KvdbError::Command("syntax error".to_string()));
    }

    let exists = key_exists(ctx, user_key)?;
    if nx && exists {
        return Ok(RespValue::BulkString(None));
    }
    if xx && !exists {
        return Ok(RespValue::BulkString(None));
    }

    let key = ctx.meta_key(user_key);
    let encoded = encode_string(STRING_FLAGS, expire_ms, value);
    ctx.storage.put(CF, &key, &encoded)?;
    Ok(RespValue::SimpleString("OK".to_string()))
}

/// 检查 key 是否存在（未过期），兼容 String 与复合类型。
fn key_exists(ctx: &CommandContext, user_key: &[u8]) -> KvdbResult<bool> {
    match ctx.get_meta(user_key)? {
        Some(v) => Ok(!is_expired_value(&v)),
        None => Ok(false),
    }
}

/// 判断 metadata value 是否已过期。
fn is_expired_value(v: &[u8]) -> bool {
    if v.is_empty() {
        return false;
    }
    let type_code = v[0] & FLAGS_TYPE_MASK;
    let expire = if type_code == DataType::String.code() {
        match decode_string(v) {
            Some((_, exp, _)) => exp,
            None => return false,
        }
    } else {
        match decode_metadata(v) {
            Some(m) => m.expire,
            None => return false,
        }
    };
    expire > 0 && expire <= now_ms()
}

/// 读取 key 的过期时间戳（毫秒）。返回 None 表示无过期或 key 不存在。
fn read_expire(ctx: &CommandContext, user_key: &[u8]) -> KvdbResult<Option<Option<i64>>> {
    let v = match ctx.get_meta(user_key)? {
        Some(v) => v,
        None => return Ok(None), // key 不存在
    };
    if v.is_empty() {
        return Ok(None);
    }
    let type_code = v[0] & FLAGS_TYPE_MASK;
    let expire = if type_code == DataType::String.code() {
        let (_, exp, _) =
            decode_string(&v).ok_or(KvdbError::Protocol("invalid string encoding".to_string()))?;
        exp
    } else {
        let meta = decode_metadata(&v)
            .ok_or(KvdbError::Protocol("invalid metadata encoding".to_string()))?;
        meta.expire
    };
    Ok(Some(Some(expire).filter(|e| *e > 0)))
}

/// 设置 key 的过期时间（毫秒级时间戳），兼容 String 与复合类型。返回是否成功。
fn set_expire_ms(ctx: &CommandContext, user_key: &[u8], expire_ms: i64) -> KvdbResult<bool> {
    let v = match ctx.get_meta(user_key)? {
        Some(v) => v,
        None => return Ok(false),
    };
    let key = ctx.meta_key(user_key);
    if v.is_empty() {
        return Ok(false);
    }
    let type_code = v[0] & FLAGS_TYPE_MASK;
    let encoded = if type_code == DataType::String.code() {
        let (flags, _, payload) =
            decode_string(&v).ok_or(KvdbError::Protocol("invalid string encoding".to_string()))?;
        encode_string(flags, expire_ms, payload)
    } else {
        let mut meta = decode_metadata(&v)
            .ok_or(KvdbError::Protocol("invalid metadata encoding".to_string()))?;
        meta.expire = expire_ms;
        encode_metadata(&meta)
    };
    ctx.storage.put(CF, &key, &encoded)?;
    Ok(true)
}

/// 移除 key 的过期时间，兼容 String 与复合类型。返回是否成功。
fn clear_expire(ctx: &CommandContext, user_key: &[u8]) -> KvdbResult<bool> {
    set_expire_ms(ctx, user_key, 0)
}

fn expire(ctx: &CommandContext, args: &[Bytes]) -> KvdbResult<RespValue> {
    expect_arg_count("EXPIRE", args, 2)?;
    let secs = parse_i64(&args[1])?;
    let expire_ms = now_ms() + secs * 1000;
    let ok = set_expire_ms(ctx, &args[0], expire_ms)?;
    Ok(RespValue::Integer(if ok { 1 } else { 0 }))
}

fn pexpire(ctx: &CommandContext, args: &[Bytes]) -> KvdbResult<RespValue> {
    expect_arg_count("PEXPIRE", args, 2)?;
    let ms = parse_i64(&args[1])?;
    let expire_ms = now_ms() + ms;
    let ok = set_expire_ms(ctx, &args[0], expire_ms)?;
    Ok(RespValue::Integer(if ok { 1 } else { 0 }))
}

fn ttl(ctx: &CommandContext, args: &[Bytes]) -> KvdbResult<RespValue> {
    expect_arg_count("TTL", args, 1)?;
    match read_expire(ctx, &args[0])? {
        None => Ok(RespValue::Integer(-2)),
        Some(None) => Ok(RespValue::Integer(-1)),
        Some(Some(expire_ms)) => {
            let now = now_ms();
            if expire_ms <= now {
                return Ok(RespValue::Integer(-2));
            }
            Ok(RespValue::Integer((expire_ms - now + 999) / 1000))
        }
    }
}

fn pttl(ctx: &CommandContext, args: &[Bytes]) -> KvdbResult<RespValue> {
    expect_arg_count("PTTL", args, 1)?;
    match read_expire(ctx, &args[0])? {
        None => Ok(RespValue::Integer(-2)),
        Some(None) => Ok(RespValue::Integer(-1)),
        Some(Some(expire_ms)) => {
            let now = now_ms();
            if expire_ms <= now {
                return Ok(RespValue::Integer(-2));
            }
            Ok(RespValue::Integer(expire_ms - now))
        }
    }
}

fn persist(ctx: &CommandContext, args: &[Bytes]) -> KvdbResult<RespValue> {
    expect_arg_count("PERSIST", args, 1)?;
    match read_expire(ctx, &args[0])? {
        None => Ok(RespValue::Integer(0)),
        Some(None) => Ok(RespValue::Integer(0)),
        Some(Some(_)) => {
            clear_expire(ctx, &args[0])?;
            Ok(RespValue::Integer(1))
        }
    }
}

fn mget(ctx: &CommandContext, args: &[Bytes]) -> KvdbResult<RespValue> {
    expect_min_arg_count("MGET", args, 1)?;
    let mut result = Vec::with_capacity(args.len());
    for key in args {
        let value = read_string(ctx, key)?;
        result.push(RespValue::BulkString(value.map(Bytes::from)));
    }
    Ok(RespValue::Array(result))
}

/// MSET 使用 WriteBatch 一次性写入所有键值，保证同一命令内原子提交。
fn mset(ctx: &CommandContext, args: &[Bytes]) -> KvdbResult<RespValue> {
    if args.len() % 2 != 0 {
        return Err(KvdbError::WrongArgCount("MSET"));
    }
    let mut batch = WriteBatch::default();
    for pair in args.chunks_exact(2) {
        let key = ctx.meta_key(&pair[0]);
        let value = encode_string(STRING_FLAGS, 0, &pair[1]);
        ctx.storage.batch_put(&mut batch, CF, &key, &value)?;
    }
    ctx.storage.write(batch)?;
    Ok(RespValue::SimpleString("OK".to_string()))
}

/// DEL 为通用删除命令：删除任意类型的 key，包括其全部 subkey。
/// 对于复合类型（Hash/Set/List/ZSet/Bitmap/Stream），先分页扫描并批量删除
/// CF_SUBKEY（及 ZSet 的 CF_ZSET_SCORE）中的子键，再删除 metadata。
/// 使用 ctx.get_meta 以兼容旧格式（无 namespace）数据。
fn del(ctx: &CommandContext, args: &[Bytes]) -> KvdbResult<RespValue> {
    expect_min_arg_count("DEL", args, 1)?;
    let mut count = 0i64;
    for key in args {
        let meta_val = match ctx.get_meta(key)? {
            Some(v) => v,
            None => continue,
        };
        // 读取 flags 判定数据类型；String 类型无 subkey，直接删 metadata。
        if !meta_val.is_empty() {
            let type_code = meta_val[0] & FLAGS_TYPE_MASK;
            if type_code != DataType::String.code() {
                // 复合类型：需要清理 subkey
                if let Some(meta) = decode_metadata(&meta_val) {
                    let version = meta.version;
                    let is_zset = meta.data_type() == Some(DataType::ZSet);
                    purge_subkeys(ctx, key, version, is_zset)?;
                }
            }
        }
        let encoded = ctx.meta_key(key);
        ctx.storage.delete(CF, &encoded)?;
        count += 1;
    }
    Ok(RespValue::Integer(count))
}

/// 分页扫描并批量删除指定 key+version 的全部 subkey。
/// ZSet 额外清理 CF_ZSET_SCORE 列族。每批 DEL_BATCH_SIZE 条，避免 WriteBatch 过大。
fn purge_subkeys(
    ctx: &CommandContext,
    user_key: &[u8],
    version: u64,
    is_zset: bool,
) -> KvdbResult<()> {
    let prefix = ctx.sub_key(user_key, version, &[]);
    ctx.storage
        .delete_prefix(CF_SUBKEY, &prefix, DEL_BATCH_SIZE)?;
    if is_zset {
        ctx.storage
            .delete_prefix(CF_ZSET_SCORE, &prefix, DEL_BATCH_SIZE)?;
    }
    Ok(())
}

/// EXISTS 为通用存在性检查：metadata 列族中存在即计数，覆盖全部数据类型。
/// 使用 ctx.get_meta 以兼容旧格式（无 namespace）数据。
fn exists(ctx: &CommandContext, args: &[Bytes]) -> KvdbResult<RespValue> {
    expect_min_arg_count("EXISTS", args, 1)?;
    let mut count = 0i64;
    for key in args {
        if ctx.get_meta(key)?.is_some() {
            count += 1;
        }
    }
    Ok(RespValue::Integer(count))
}

/// INCR/DECR/APPEND 使用 per-key 互斥锁保证读-改-写原子性，防止并发丢失更新。
fn incr(ctx: &CommandContext, args: &[Bytes]) -> KvdbResult<RespValue> {
    expect_arg_count("INCR", args, 1)?;
    let _guard = ctx.storage.key_lock(&ctx.meta_key(&args[0]));
    let current = match read_string(ctx, &args[0])? {
        Some(v) => parse_integer(&v)?,
        None => 0,
    };
    let new = current.checked_add(1).ok_or(KvdbError::OutOfRange)?;
    write_string(ctx, &args[0], new.to_string().as_bytes())?;
    Ok(RespValue::Integer(new))
}

fn decr(ctx: &CommandContext, args: &[Bytes]) -> KvdbResult<RespValue> {
    expect_arg_count("DECR", args, 1)?;
    let _guard = ctx.storage.key_lock(&ctx.meta_key(&args[0]));
    let current = match read_string(ctx, &args[0])? {
        Some(v) => parse_integer(&v)?,
        None => 0,
    };
    let new = current.checked_sub(1).ok_or(KvdbError::OutOfRange)?;
    write_string(ctx, &args[0], new.to_string().as_bytes())?;
    Ok(RespValue::Integer(new))
}

fn append(ctx: &CommandContext, args: &[Bytes]) -> KvdbResult<RespValue> {
    expect_arg_count("APPEND", args, 2)?;
    let _guard = ctx.storage.key_lock(&ctx.meta_key(&args[0]));
    let mut current = read_string(ctx, &args[0])?.unwrap_or_default();
    current.extend_from_slice(&args[1]);
    let len = current.len() as i64;
    write_string(ctx, &args[0], &current)?;
    Ok(RespValue::Integer(len))
}

fn parse_i64(data: &[u8]) -> KvdbResult<i64> {
    std::str::from_utf8(data)
        .map_err(|_| KvdbError::NotInteger)?
        .parse::<i64>()
        .map_err(|_| KvdbError::NotInteger)
}

fn parse_integer(data: &[u8]) -> KvdbResult<i64> {
    parse_i64(data)
}
