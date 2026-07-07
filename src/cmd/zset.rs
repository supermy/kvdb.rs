use bytes::Bytes;
use rocksdb::WriteBatch;
use std::collections::HashMap;

use super::{CommandContext, CommandTable, expect_arg_count, expect_min_arg_count};
use crate::encoding::{decode_metadata, encode_metadata, generate_version};
use crate::error::{KvdbError, KvdbResult};
use crate::protocol::RespValue;
use crate::storage::{CF_METADATA, CF_SUBKEY, CF_ZSET_SCORE, DataType, Metadata};

const WRONGTYPE: &str = "WRONGTYPE Operation against a key holding the wrong kind of value";

pub fn register(table: &mut CommandTable) {
    table.register("ZADD", zadd);
    table.register("ZRANGE", zrange);
    table.register("ZRANGEBYSCORE", zrangebyscore);
    table.register("ZREVRANGE", zrevrange);
    table.register("ZREVRANGEBYSCORE", zrevrangebyscore);
    table.register("ZREM", zrem);
    table.register("ZRANK", zrank);
    table.register("ZREVRANK", zrevrank);
    table.register("ZSCORE", zscore);
    table.register("ZCARD", zcard);
    table.register("ZINCRBY", zincrby);
}

/// 将 f64 编码为 memcmpable 的 8 字节大端序：
/// 正数翻转符号位，负数翻转所有位，保证字节序与数值序一致。
fn encode_score(score: f64) -> [u8; 8] {
    let bits = score.to_bits();
    let mut bytes = bits.to_be_bytes();
    if bits & (1u64 << 63) == 0 {
        // 正数或 +0：翻转符号位
        bytes[0] ^= 0x80;
    } else {
        // 负数：翻转所有位
        for b in &mut bytes {
            *b ^= 0xFF;
        }
    }
    bytes
}

fn decode_score(bytes: [u8; 8]) -> f64 {
    let mut bits_bytes = bytes;
    if bits_bytes[0] & 0x80 != 0 {
        // 正数：符号位被翻转过
        bits_bytes[0] ^= 0x80;
    } else {
        // 负数：全部翻转过
        for b in &mut bits_bytes {
            *b ^= 0xFF;
        }
    }
    f64::from_bits(u64::from_be_bytes(bits_bytes))
}

fn parse_score(s: &[u8]) -> KvdbResult<f64> {
    let text = std::str::from_utf8(s).map_err(|_| KvdbError::NotInteger)?;
    let score = match text.to_ascii_lowercase().as_str() {
        "inf" | "+inf" => f64::INFINITY,
        "-inf" => f64::NEG_INFINITY,
        _ => text.parse::<f64>().map_err(|_| KvdbError::NotInteger)?,
    };
    if score.is_nan() {
        return Err(KvdbError::NotInteger);
    }
    Ok(score)
}

fn format_score(score: f64) -> String {
    if score.fract() == 0.0 && score.is_finite() {
        format!("{:.0}", score)
    } else {
        score.to_string()
    }
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

/// 读取 metadata 并做基础校验；String 类型直接报 WRONGTYPE。
/// 使用 ctx.get_meta 以兼容旧格式（无 namespace）数据。
fn load_meta(ctx: &CommandContext, user_key: &[u8]) -> KvdbResult<Option<Metadata>> {
    match ctx.get_meta(user_key)? {
        Some(v) => {
            if v.is_empty() {
                return Ok(None);
            }
            let data_type = DataType::from_code(v[0] & 0x0F)
                .ok_or_else(|| KvdbError::Protocol("invalid data type".to_string()))?;
            if data_type == DataType::String {
                // String 不使用 Metadata 编码，直接报类型错误。
                return Err(KvdbError::Command(WRONGTYPE.to_string()));
            }
            let meta = decode_metadata(&v)
                .ok_or_else(|| KvdbError::Protocol("invalid metadata encoding".to_string()))?;
            if meta.is_expired(now_ms()) {
                Ok(None)
            } else {
                Ok(Some(meta))
            }
        }
        None => Ok(None),
    }
}

fn ensure_zset_meta(ctx: &CommandContext, user_key: &[u8]) -> KvdbResult<Metadata> {
    match load_meta(ctx, user_key)? {
        Some(meta) => {
            if meta.data_type() != Some(DataType::ZSet) {
                return Err(KvdbError::Command(WRONGTYPE.to_string()));
            }
            Ok(meta)
        }
        None => Ok(Metadata::new(DataType::ZSet, generate_version())),
    }
}

fn read_zset_meta(ctx: &CommandContext, user_key: &[u8]) -> KvdbResult<Option<Metadata>> {
    match load_meta(ctx, user_key)? {
        Some(meta) => {
            if meta.data_type() != Some(DataType::ZSet) {
                return Err(KvdbError::Command(WRONGTYPE.to_string()));
            }
            Ok(Some(meta))
        }
        None => Ok(None),
    }
}

fn member_subkey(ctx: &CommandContext, user_key: &[u8], version: u64, member: &[u8]) -> Vec<u8> {
    ctx.sub_key(user_key, version, member)
}

fn score_subkey(
    ctx: &CommandContext,
    user_key: &[u8],
    version: u64,
    score: f64,
    member: &[u8],
) -> Vec<u8> {
    let score_bytes = encode_score(score);
    let mut sub = Vec::with_capacity(8 + member.len());
    sub.extend_from_slice(&score_bytes);
    sub.extend_from_slice(member);
    ctx.sub_key(user_key, version, &sub)
}

fn parse_score_subkey<'a>(ctx: &'a CommandContext, key: &'a [u8]) -> Option<(f64, &'a [u8])> {
    let (_, _, sub) = ctx.parse_subkey(key)?;
    if sub.len() < 8 {
        return None;
    }
    let score = decode_score(sub[..8].try_into().ok()?);
    Some((score, &sub[8..]))
}

fn zadd(ctx: &CommandContext, args: &[Bytes]) -> KvdbResult<RespValue> {
    expect_min_arg_count("ZADD", args, 3)?;
    if (args.len() - 1) % 2 != 0 {
        return Err(KvdbError::WrongArgCount("ZADD"));
    }

    let user_key = &args[0];
    let meta_key = ctx.meta_key(user_key);
    let mut meta = ensure_zset_meta(ctx, user_key)?;
    let version = meta.version;

    // 同一命令内对同一 member 多次设置时，以最后一次 score 为准，仅计数一次。
    let mut updates: HashMap<Bytes, f64> = HashMap::new();
    for pair in args[1..].chunks_exact(2) {
        let score = parse_score(&pair[0])?;
        updates.insert(pair[1].clone(), score);
    }

    let mut batch = WriteBatch::default();
    let mut added: i64 = 0;

    for (member, score) in updates {
        let mkey = member_subkey(ctx, user_key, version, &member);
        let existing = ctx.storage.get(CF_SUBKEY, &mkey)?;

        if let Some(old_bytes) = existing {
            let old_score = decode_score(
                old_bytes[..8]
                    .try_into()
                    .map_err(|_| KvdbError::Protocol("invalid score encoding".to_string()))?,
            );
            if old_score == score {
                continue;
            }
            // 删除旧 score -> member 映射
            let old_skey = score_subkey(ctx, user_key, version, old_score, &member);
            batch.delete_cf(ctx.storage.cf_handle(CF_ZSET_SCORE)?, &old_skey);
        } else {
            added += 1;
            meta.size += 1;
        }

        // 写入/更新双 subkey
        batch.put_cf(
            ctx.storage.cf_handle(CF_SUBKEY)?,
            &mkey,
            encode_score(score),
        );
        let skey = score_subkey(ctx, user_key, version, score, &member);
        batch.put_cf(ctx.storage.cf_handle(CF_ZSET_SCORE)?, &skey, b"");
    }

    batch.put_cf(
        ctx.storage.cf_handle(CF_METADATA)?,
        &meta_key,
        encode_metadata(&meta),
    );
    ctx.storage.write(batch)?;
    Ok(RespValue::Integer(added))
}

fn zrange(ctx: &CommandContext, args: &[Bytes]) -> KvdbResult<RespValue> {
    if args.len() != 3 && args.len() != 4 {
        return Err(KvdbError::WrongArgCount("ZRANGE"));
    }
    let with_scores = if args.len() == 4 {
        if args[3].eq_ignore_ascii_case(b"WITHSCORES") {
            true
        } else {
            return Err(KvdbError::Command("syntax error".to_string()));
        }
    } else {
        false
    };

    let user_key = &args[0];
    let meta = match read_zset_meta(ctx, user_key)? {
        Some(m) => m,
        None => return Ok(RespValue::Array(vec![])),
    };

    let start = parse_index(&args[1])?;
    let stop = parse_index(&args[2])?;

    let mut items = collect_sorted_items(ctx, user_key, meta.version)?;
    apply_range(&mut items, start, stop);

    let mut result = Vec::with_capacity(if with_scores {
        items.len() * 2
    } else {
        items.len()
    });
    for (score, member) in items {
        result.push(RespValue::BulkString(Some(Bytes::copy_from_slice(&member))));
        if with_scores {
            result.push(RespValue::BulkString(Some(Bytes::from(format_score(
                score,
            )))));
        }
    }
    Ok(RespValue::Array(result))
}

fn zrangebyscore(ctx: &CommandContext, args: &[Bytes]) -> KvdbResult<RespValue> {
    if args.len() != 3 && args.len() != 4 {
        return Err(KvdbError::WrongArgCount("ZRANGEBYSCORE"));
    }
    let with_scores = if args.len() == 4 {
        if args[3].eq_ignore_ascii_case(b"WITHSCORES") {
            true
        } else {
            return Err(KvdbError::Command("syntax error".to_string()));
        }
    } else {
        false
    };

    let user_key = &args[0];
    let meta = match read_zset_meta(ctx, user_key)? {
        Some(m) => m,
        None => return Ok(RespValue::Array(vec![])),
    };

    let min = parse_score(&args[1])?;
    let max = parse_score(&args[2])?;

    let prefix = ctx.sub_key(user_key, meta.version, &[]);
    let pairs: Vec<(f64, Vec<u8>)> = ctx
        .storage
        .prefix_scan(CF_ZSET_SCORE, &prefix)?
        .into_iter()
        .filter_map(|(k, _)| {
            let (score, member) = parse_score_subkey(ctx, &k)?;
            if score >= min && score <= max {
                Some((score, member.to_vec()))
            } else {
                None
            }
        })
        .collect();

    let mut result = Vec::with_capacity(if with_scores {
        pairs.len() * 2
    } else {
        pairs.len()
    });
    for (score, member) in pairs {
        result.push(RespValue::BulkString(Some(Bytes::from(member))));
        if with_scores {
            result.push(RespValue::BulkString(Some(Bytes::from(format_score(
                score,
            )))));
        }
    }
    Ok(RespValue::Array(result))
}

fn zrevrange(ctx: &CommandContext, args: &[Bytes]) -> KvdbResult<RespValue> {
    if args.len() != 3 && args.len() != 4 {
        return Err(KvdbError::WrongArgCount("ZREVRANGE"));
    }
    let with_scores = if args.len() == 4 {
        if args[3].eq_ignore_ascii_case(b"WITHSCORES") {
            true
        } else {
            return Err(KvdbError::Command("syntax error".to_string()));
        }
    } else {
        false
    };

    let user_key = &args[0];
    let meta = match read_zset_meta(ctx, user_key)? {
        Some(m) => m,
        None => return Ok(RespValue::Array(vec![])),
    };

    let start = parse_index(&args[1])?;
    let stop = parse_index(&args[2])?;

    let mut items = collect_sorted_items(ctx, user_key, meta.version)?;
    items.reverse();
    apply_range(&mut items, start, stop);

    let mut result = Vec::with_capacity(if with_scores {
        items.len() * 2
    } else {
        items.len()
    });
    for (score, member) in items {
        result.push(RespValue::BulkString(Some(Bytes::copy_from_slice(&member))));
        if with_scores {
            result.push(RespValue::BulkString(Some(Bytes::from(format_score(
                score,
            )))));
        }
    }
    Ok(RespValue::Array(result))
}

fn zrevrangebyscore(ctx: &CommandContext, args: &[Bytes]) -> KvdbResult<RespValue> {
    if args.len() != 3 && args.len() != 4 {
        return Err(KvdbError::WrongArgCount("ZREVRANGEBYSCORE"));
    }
    let with_scores = if args.len() == 4 {
        if args[3].eq_ignore_ascii_case(b"WITHSCORES") {
            true
        } else {
            return Err(KvdbError::Command("syntax error".to_string()));
        }
    } else {
        false
    };

    let user_key = &args[0];
    let meta = match read_zset_meta(ctx, user_key)? {
        Some(m) => m,
        None => return Ok(RespValue::Array(vec![])),
    };

    // ZREVRANGEBYSCORE 参数顺序为 max min（与 ZRANGEBYSCORE 相反）
    let max = parse_score(&args[1])?;
    let min = parse_score(&args[2])?;

    let prefix = ctx.sub_key(user_key, meta.version, &[]);
    let mut pairs: Vec<(f64, Vec<u8>)> = ctx
        .storage
        .prefix_scan(CF_ZSET_SCORE, &prefix)?
        .into_iter()
        .filter_map(|(k, _)| {
            let (score, member) = parse_score_subkey(ctx, &k)?;
            if score >= min && score <= max {
                Some((score, member.to_vec()))
            } else {
                None
            }
        })
        .collect();
    pairs.reverse();

    let mut result = Vec::with_capacity(if with_scores {
        pairs.len() * 2
    } else {
        pairs.len()
    });
    for (score, member) in pairs {
        result.push(RespValue::BulkString(Some(Bytes::from(member))));
        if with_scores {
            result.push(RespValue::BulkString(Some(Bytes::from(format_score(
                score,
            )))));
        }
    }
    Ok(RespValue::Array(result))
}

fn zrem(ctx: &CommandContext, args: &[Bytes]) -> KvdbResult<RespValue> {
    expect_min_arg_count("ZREM", args, 2)?;

    let user_key = &args[0];
    let meta_key = ctx.meta_key(user_key);
    let mut meta = match read_zset_meta(ctx, user_key)? {
        Some(m) => m,
        None => return Ok(RespValue::Integer(0)),
    };
    let version = meta.version;

    let mut batch = WriteBatch::default();
    let mut removed: i64 = 0;

    for member in &args[1..] {
        let mkey = member_subkey(ctx, user_key, version, member);
        if let Some(old_bytes) = ctx.storage.get(CF_SUBKEY, &mkey)? {
            let old_score = decode_score(
                old_bytes[..8]
                    .try_into()
                    .map_err(|_| KvdbError::Protocol("invalid score encoding".to_string()))?,
            );
            let skey = score_subkey(ctx, user_key, version, old_score, member);
            batch.delete_cf(ctx.storage.cf_handle(CF_SUBKEY)?, &mkey);
            batch.delete_cf(ctx.storage.cf_handle(CF_ZSET_SCORE)?, &skey);
            removed += 1;
            meta.size = meta.size.saturating_sub(1);
        }
    }

    if removed > 0 {
        batch.put_cf(
            ctx.storage.cf_handle(CF_METADATA)?,
            &meta_key,
            encode_metadata(&meta),
        );
        ctx.storage.write(batch)?;
    }
    Ok(RespValue::Integer(removed))
}

fn zrank(ctx: &CommandContext, args: &[Bytes]) -> KvdbResult<RespValue> {
    expect_arg_count("ZRANK", args, 2)?;

    let user_key = &args[0];
    let meta = match read_zset_meta(ctx, user_key)? {
        Some(m) => m,
        None => return Ok(RespValue::BulkString(None)),
    };

    let target = &args[1];
    let target_score = match ctx.storage.get(
        CF_SUBKEY,
        &member_subkey(ctx, user_key, meta.version, target),
    )? {
        Some(bytes) => decode_score(
            bytes[..8]
                .try_into()
                .map_err(|_| KvdbError::Protocol("invalid score encoding".to_string()))?,
        ),
        None => return Ok(RespValue::BulkString(None)),
    };

    let items = collect_sorted_items(ctx, user_key, meta.version)?;
    let rank = items
        .iter()
        .position(|(score, member)| *score == target_score && member.as_slice() == target.as_ref())
        .map(|i| i as i64)
        .ok_or_else(|| KvdbError::Command("member disappeared".to_string()))?;

    Ok(RespValue::Integer(rank))
}

fn zrevrank(ctx: &CommandContext, args: &[Bytes]) -> KvdbResult<RespValue> {
    expect_arg_count("ZREVRANK", args, 2)?;

    let user_key = &args[0];
    let meta = match read_zset_meta(ctx, user_key)? {
        Some(m) => m,
        None => return Ok(RespValue::BulkString(None)),
    };

    let target = &args[1];
    let target_score = match ctx.storage.get(
        CF_SUBKEY,
        &member_subkey(ctx, user_key, meta.version, target),
    )? {
        Some(bytes) => decode_score(
            bytes[..8]
                .try_into()
                .map_err(|_| KvdbError::Protocol("invalid score encoding".to_string()))?,
        ),
        None => return Ok(RespValue::BulkString(None)),
    };

    let items = collect_sorted_items(ctx, user_key, meta.version)?;
    let rank = items
        .iter()
        .rposition(|(score, member)| *score == target_score && member.as_slice() == target.as_ref())
        .map(|i| (items.len() - 1 - i) as i64)
        .ok_or_else(|| KvdbError::Command("member disappeared".to_string()))?;

    Ok(RespValue::Integer(rank))
}

fn zscore(ctx: &CommandContext, args: &[Bytes]) -> KvdbResult<RespValue> {
    expect_arg_count("ZSCORE", args, 2)?;

    let user_key = &args[0];
    let meta = match read_zset_meta(ctx, user_key)? {
        Some(m) => m,
        None => return Ok(RespValue::BulkString(None)),
    };

    match ctx.storage.get(
        CF_SUBKEY,
        &member_subkey(ctx, user_key, meta.version, &args[1]),
    )? {
        Some(bytes) => {
            let score = decode_score(
                bytes[..8]
                    .try_into()
                    .map_err(|_| KvdbError::Protocol("invalid score encoding".to_string()))?,
            );
            Ok(RespValue::BulkString(Some(Bytes::from(format_score(
                score,
            )))))
        }
        None => Ok(RespValue::BulkString(None)),
    }
}

fn zcard(ctx: &CommandContext, args: &[Bytes]) -> KvdbResult<RespValue> {
    expect_arg_count("ZCARD", args, 1)?;

    let user_key = &args[0];
    let meta = match read_zset_meta(ctx, user_key)? {
        Some(m) => m,
        None => return Ok(RespValue::Integer(0)),
    };

    Ok(RespValue::Integer(meta.size as i64))
}

fn zincrby(ctx: &CommandContext, args: &[Bytes]) -> KvdbResult<RespValue> {
    expect_arg_count("ZINCRBY", args, 3)?;

    let user_key = &args[0];
    let increment = parse_score(&args[1])?;
    let member = &args[2];
    let meta_key = ctx.meta_key(user_key);
    let mut meta = ensure_zset_meta(ctx, user_key)?;
    let version = meta.version;

    let mkey = member_subkey(ctx, user_key, version, member);
    let mut batch = WriteBatch::default();

    let new_score = if let Some(old_bytes) = ctx.storage.get(CF_SUBKEY, &mkey)? {
        let old_score = decode_score(
            old_bytes[..8]
                .try_into()
                .map_err(|_| KvdbError::Protocol("invalid score encoding".to_string()))?,
        );
        let new_score = old_score + increment;
        if !new_score.is_finite() {
            return Err(KvdbError::Command(
                "increment would produce NaN or Infinity".to_string(),
            ));
        }
        // 删除旧 score -> member 映射
        let old_skey = score_subkey(ctx, user_key, version, old_score, member);
        batch.delete_cf(ctx.storage.cf_handle(CF_ZSET_SCORE)?, &old_skey);
        new_score
    } else {
        meta.size += 1;
        increment
    };

    batch.put_cf(
        ctx.storage.cf_handle(CF_SUBKEY)?,
        &mkey,
        encode_score(new_score),
    );
    let skey = score_subkey(ctx, user_key, version, new_score, member);
    batch.put_cf(ctx.storage.cf_handle(CF_ZSET_SCORE)?, &skey, b"");
    batch.put_cf(
        ctx.storage.cf_handle(CF_METADATA)?,
        &meta_key,
        encode_metadata(&meta),
    );
    ctx.storage.write(batch)?;

    Ok(RespValue::BulkString(Some(Bytes::from(format_score(
        new_score,
    )))))
}

fn parse_index(s: &[u8]) -> KvdbResult<i64> {
    std::str::from_utf8(s)
        .map_err(|_| KvdbError::NotInteger)?
        .parse::<i64>()
        .map_err(|_| KvdbError::NotInteger)
}

fn collect_sorted_items(
    ctx: &CommandContext,
    user_key: &[u8],
    version: u64,
) -> KvdbResult<Vec<(f64, Vec<u8>)>> {
    let prefix = ctx.sub_key(user_key, version, &[]);
    let mut items: Vec<(f64, Vec<u8>)> = ctx
        .storage
        .prefix_scan(CF_ZSET_SCORE, &prefix)?
        .into_iter()
        .filter_map(|(k, _)| {
            let (score, member) = parse_score_subkey(ctx, &k)?;
            Some((score, member.to_vec()))
        })
        .collect();
    // 稳定排序：先 score，再 member 字典序
    items.sort_by(|a, b| {
        a.0.partial_cmp(&b.0)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.1.cmp(&b.1))
    });
    Ok(items)
}

fn apply_range(items: &mut Vec<(f64, Vec<u8>)>, start: i64, stop: i64) {
    let len = items.len() as i64;
    let s = normalize_index(start, len);
    let e = normalize_index(stop, len);
    if s > e || s >= len {
        items.clear();
        return;
    }
    let e = e.min(len - 1);
    *items = items.drain(s as usize..=e as usize).collect();
}

fn normalize_index(idx: i64, len: i64) -> i64 {
    if idx < 0 { (len + idx).max(0) } else { idx }
}
