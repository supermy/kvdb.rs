use bytes::Bytes;
use rocksdb::WriteBatch;

use super::{CommandContext, CommandTable, expect_arg_count, expect_min_arg_count};
use crate::encoding::{
    decode_metadata, encode_metadata, generate_version, metadata_key, parse_subkey, subkey,
};
use crate::error::{KvdbError, KvdbResult};
use crate::protocol::RespValue;
use crate::storage::{CF_METADATA, CF_SUBKEY};
use crate::types::{DataType, FLAGS_TYPE_MASK, Metadata};

const WRONGTYPE_MSG: &str = "WRONGTYPE Operation against a key holding the wrong kind of value";

pub fn register(table: &mut CommandTable) {
    table.register("HSET", hset);
    table.register("HGET", hget);
    table.register("HMGET", hmget);
    table.register("HGETALL", hgetall);
    table.register("HDEL", hdel);
    table.register("HEXISTS", hexists);
    table.register("HLEN", hlen);
}

fn wrongtype() -> KvdbError {
    KvdbError::Command(WRONGTYPE_MSG.to_string())
}

fn read_hash_meta(ctx: &CommandContext, user_key: &[u8]) -> KvdbResult<Option<Metadata>> {
    let key = metadata_key(user_key);
    match ctx.storage.get(CF_METADATA, &key)? {
        Some(v) => {
            if v.is_empty() {
                return Err(KvdbError::Protocol("empty metadata value".to_string()));
            }
            // String 与复合类型共享 metadata 列族，通过 flags 低 4 位区分。
            let type_code = v[0] & FLAGS_TYPE_MASK;
            if type_code == DataType::String.code() {
                return Err(wrongtype());
            }
            let meta = decode_metadata(&v)
                .ok_or(KvdbError::Protocol("invalid metadata encoding".to_string()))?;
            if meta.data_type() != Some(DataType::Hash) {
                return Err(wrongtype());
            }
            Ok(Some(meta))
        }
        None => Ok(None),
    }
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
        let sk = subkey(user_key, meta.version, field);
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
        &metadata_key(user_key),
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
    let sk = subkey(user_key, meta.version, field);
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
        let sk = subkey(user_key, meta.version, field);
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
    let prefix = subkey(user_key, meta.version, &[]);
    let items = ctx.storage.prefix_scan(CF_SUBKEY, &prefix)?;
    let mut result = Vec::with_capacity(items.len() * 2);
    for (k, v) in items {
        let (_, _, field) =
            parse_subkey(&k).ok_or(KvdbError::Protocol("invalid subkey encoding".to_string()))?;
        result.push(RespValue::BulkString(Some(Bytes::copy_from_slice(field))));
        result.push(RespValue::BulkString(Some(Bytes::from(v))));
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
        let sk = subkey(user_key, meta.version, field);
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
            &metadata_key(user_key),
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
    let sk = subkey(user_key, meta.version, field);
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
