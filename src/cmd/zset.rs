use bytes::Bytes;
use rocksdb::WriteBatch;
use std::collections::HashMap;

use super::{
    CommandContext, CommandTable, expect_arg_count, expect_min_arg_count, wrong_type_error,
};
use crate::encoding::{decode_metadata, encode_metadata, generate_version};
use crate::error::{KvdbError, KvdbResult};
use crate::protocol::RespValue;
use crate::storage::{CF_METADATA, CF_SUBKEY, CF_ZSET_SCORE, DataType, Metadata};

const ZSET_PAGE_SIZE: usize = 1024;

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
                return Err(wrong_type_error());
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
                return Err(wrong_type_error());
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
                return Err(wrong_type_error());
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

/// 构造 score 范围查询的下界 seek key（member 为空，字典序最小）。
fn score_seek_key(ctx: &CommandContext, user_key: &[u8], version: u64, score: f64) -> Vec<u8> {
    score_subkey(ctx, user_key, version, score, &[])
}

fn parse_score_subkey<'a>(ctx: &'a CommandContext, key: &'a [u8]) -> Option<(f64, &'a [u8])> {
    let (_, _, sub) = ctx.parse_subkey(key)?;
    if sub.len() < 8 {
        return None;
    }
    let score = decode_score(sub[..8].try_into().ok()?);
    Some((score, &sub[8..]))
}

/// (score, member) 分页结果与下一页起始 key。
type ScorePage = (Vec<(f64, Vec<u8>)>, Option<Vec<u8>>);

/// 分页正向迭代 zset_score 列族，返回 (score, member) 列表与下一页起始 key。
fn iter_score_page(
    ctx: &CommandContext,
    user_key: &[u8],
    version: u64,
    start_key: &[u8],
) -> KvdbResult<ScorePage> {
    let prefix = ctx.sub_key(user_key, version, &[]);
    let (items, next_key) =
        ctx.storage
            .prefix_scan_page(CF_ZSET_SCORE, &prefix, start_key, ZSET_PAGE_SIZE)?;
    let mut pairs = Vec::with_capacity(items.len());
    for (k, _) in items {
        let (score, member) = parse_score_subkey(ctx, &k)
            .ok_or_else(|| KvdbError::Protocol("invalid zset score subkey".to_string()))?;
        pairs.push((score, member.to_vec()));
    }
    Ok((pairs, next_key))
}

/// 分页反向迭代 zset_score 列族，返回 (score, member) 列表与下一页起始 key。
fn iter_score_page_reverse(
    ctx: &CommandContext,
    user_key: &[u8],
    version: u64,
    start_key: &[u8],
) -> KvdbResult<ScorePage> {
    let prefix = ctx.sub_key(user_key, version, &[]);
    let (items, next_key) =
        ctx.storage
            .prefix_scan_page_reverse(CF_ZSET_SCORE, &prefix, start_key, ZSET_PAGE_SIZE)?;
    let mut pairs = Vec::with_capacity(items.len());
    for (k, _) in items {
        let (score, member) = parse_score_subkey(ctx, &k)
            .ok_or_else(|| KvdbError::Protocol("invalid zset score subkey".to_string()))?;
        pairs.push((score, member.to_vec()));
    }
    Ok((pairs, next_key))
}

/// ZADD 选项标志：NX/XX/GT/LT/CH/INCR。
/// NX: 仅新增；XX: 仅更新；GT: 仅当新 score 大于现值时更新；
/// LT: 仅当新 score 小于现值时更新；CH: 返回变更数（新增+更新）；
/// INCR: 增量更新，返回新 score，仅允许单个 score-member。
#[derive(Default, Clone, Copy)]
struct ZaddFlags {
    nx: bool,
    xx: bool,
    gt: bool,
    lt: bool,
    ch: bool,
    incr: bool,
}

/// 解析 ZADD 选项并校验组合合法性。
/// NX+XX / GT+LT / NX+GT / NX+LT 均互斥；INCR 仅允许单个 score-member。
fn parse_zadd_flags(args: &[Bytes]) -> KvdbResult<(ZaddFlags, usize)> {
    let mut flags = ZaddFlags::default();
    let mut i = 0;
    while i < args.len() {
        let upper = args[i].to_ascii_uppercase();
        match upper.as_slice() {
            b"NX" => flags.nx = true,
            b"XX" => flags.xx = true,
            b"GT" => flags.gt = true,
            b"LT" => flags.lt = true,
            b"CH" => flags.ch = true,
            b"INCR" => flags.incr = true,
            _ => break,
        }
        i += 1;
    }
    // NX/XX 互斥：分别表示仅新增 / 仅更新
    if flags.nx && flags.xx {
        return Err(KvdbError::Command(
            "XX and NX options at the same time are not compatible".to_string(),
        ));
    }
    // GT/LT 互斥：分别表示仅当更大 / 更小时更新
    if flags.gt && flags.lt {
        return Err(KvdbError::Command(
            "GT, LT, and/or NX options at the same time are not compatible".to_string(),
        ));
    }
    // NX 与 GT/LT 互斥：NX 不允许更新，GT/LT 仅用于约束更新
    if flags.nx && (flags.gt || flags.lt) {
        return Err(KvdbError::Command(
            "GT, LT, and/or NX options at the same time are not compatible".to_string(),
        ));
    }
    Ok((flags, i))
}

fn zadd(ctx: &CommandContext, args: &[Bytes]) -> KvdbResult<RespValue> {
    expect_min_arg_count("ZADD", args, 3)?;
    let user_key = &args[0];
    let (flags, opt_count) = parse_zadd_flags(&args[1..])?;
    let pairs = &args[1 + opt_count..];
    if pairs.is_empty() || pairs.len() % 2 != 0 {
        return Err(KvdbError::WrongArgCount("ZADD"));
    }
    // INCR 仅允许单个 score-member；多对在 INCR 模式下无意义（无法返回多个新 score）
    if flags.incr && pairs.len() != 2 {
        return Err(KvdbError::Command(
            "INCR option supports a single increment-element pair".to_string(),
        ));
    }

    let meta_key = ctx.meta_key(user_key);
    let mut meta = ensure_zset_meta(ctx, user_key)?;
    let version = meta.version;

    // INCR 模式走单独路径，返回新 score（或 nil 表示未应用）
    if flags.incr {
        let score = parse_score(&pairs[0])?;
        let member = &pairs[1];
        let mkey = member_subkey(ctx, user_key, version, member);
        let existing = ctx.storage.get(CF_SUBKEY, &mkey)?;

        // NX: 已存在则跳过
        if flags.nx && existing.is_some() {
            return Ok(RespValue::BulkString(None));
        }
        // XX: 不存在则跳过
        if flags.xx && existing.is_none() {
            return Ok(RespValue::BulkString(None));
        }

        let (new_score, is_new) = if let Some(old_bytes) = existing {
            let old_score = decode_score(
                old_bytes[..8]
                    .try_into()
                    .map_err(|_| KvdbError::Protocol("invalid score encoding".to_string()))?,
            );
            let new_score = old_score + score;
            if !new_score.is_finite() {
                return Err(KvdbError::Command(
                    "increment would produce NaN or Infinity".to_string(),
                ));
            }
            // GT/LT 约束：新 score 必须更大/更小才更新
            if flags.gt && new_score <= old_score {
                return Ok(RespValue::BulkString(None));
            }
            if flags.lt && new_score >= old_score {
                return Ok(RespValue::BulkString(None));
            }
            // 删除旧 score→member 映射
            let old_skey = score_subkey(ctx, user_key, version, old_score, member);
            let mut batch = WriteBatch::default();
            batch.delete_cf(ctx.storage.cf_handle(CF_ZSET_SCORE)?, &old_skey);
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
            (new_score, false)
        } else {
            // 新 member：直接以 increment 作为 score
            let mut batch = WriteBatch::default();
            batch.put_cf(
                ctx.storage.cf_handle(CF_SUBKEY)?,
                &mkey,
                encode_score(score),
            );
            let skey = score_subkey(ctx, user_key, version, score, member);
            batch.put_cf(ctx.storage.cf_handle(CF_ZSET_SCORE)?, &skey, b"");
            meta.size += 1;
            batch.put_cf(
                ctx.storage.cf_handle(CF_METADATA)?,
                &meta_key,
                encode_metadata(&meta),
            );
            ctx.storage.write(batch)?;
            (score, true)
        };

        let _ = is_new;
        return Ok(RespValue::BulkString(Some(Bytes::from(format_score(
            new_score,
        )))));
    }

    // 常规 ZADD 模式：同一命令内对同一 member 多次设置，以最后一次 score 为准。
    let mut updates: HashMap<Bytes, f64> = HashMap::new();
    for pair in pairs.chunks_exact(2) {
        let score = parse_score(&pair[0])?;
        updates.insert(pair[1].clone(), score);
    }

    let mut batch = WriteBatch::default();
    let mut added: i64 = 0;
    let mut changed: i64 = 0;

    for (member, score) in updates {
        let mkey = member_subkey(ctx, user_key, version, &member);
        let existing = ctx.storage.get(CF_SUBKEY, &mkey)?;

        if let Some(old_bytes) = existing {
            // 已存在 member
            let old_score = decode_score(
                old_bytes[..8]
                    .try_into()
                    .map_err(|_| KvdbError::Protocol("invalid score encoding".to_string()))?,
            );
            // NX: 仅新增，跳过已存在
            if flags.nx {
                continue;
            }
            // GT: 仅当更大时更新
            if flags.gt && score <= old_score {
                continue;
            }
            // LT: 仅当更小时更新
            if flags.lt && score >= old_score {
                continue;
            }
            if old_score == score {
                continue;
            }
            // 删除旧 score→member 映射
            let old_skey = score_subkey(ctx, user_key, version, old_score, &member);
            batch.delete_cf(ctx.storage.cf_handle(CF_ZSET_SCORE)?, &old_skey);
            changed += 1;
        } else {
            // 新 member
            // XX: 仅更新，跳过新增
            if flags.xx {
                continue;
            }
            added += 1;
            changed += 1;
            meta.size += 1;
        }

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
    // CH: 返回变更数（新增+更新）；默认只返回新增数
    Ok(RespValue::Integer(if flags.ch { changed } else { added }))
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

    let (start_rank, count) = rank_range(meta.size as i64, start, stop);
    let items = collect_rank_range_items(ctx, user_key, meta.version, start_rank, count, false)?;

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
    if args.len() < 3 {
        return Err(KvdbError::WrongArgCount("ZRANGEBYSCORE"));
    }
    let user_key = &args[0];
    let meta = match read_zset_meta(ctx, user_key)? {
        Some(m) => m,
        None => return Ok(RespValue::Array(vec![])),
    };

    let min = parse_score(&args[1])?;
    let max = parse_score(&args[2])?;
    let (with_scores, limit) = parse_zrangebyscore_options(&args[3..])?;

    // 从 min score 的下界开始正向迭代，确保覆盖该 score 下的所有 member。
    let start_key = score_seek_key(ctx, user_key, meta.version, min);
    let mut result = Vec::new();
    let mut current_key = start_key;
    let mut seen = 0usize;
    let mut matched = 0usize;
    let mut done = false;

    loop {
        let (items, next_key) = iter_score_page(ctx, user_key, meta.version, &current_key)?;
        if items.is_empty() {
            break;
        }
        for (score, member) in items {
            if score > max {
                done = true;
                break;
            }
            seen += 1;
            if seen <= limit.offset {
                continue;
            }
            result.push(RespValue::BulkString(Some(Bytes::from(member))));
            if with_scores {
                result.push(RespValue::BulkString(Some(Bytes::from(format_score(
                    score,
                )))));
            }
            matched += 1;
            if limit.count > 0 && matched >= limit.count {
                done = true;
                break;
            }
        }
        if done {
            break;
        }
        match next_key {
            Some(k) => current_key = k,
            None => break,
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

    let (start_rank, count) = rank_range(meta.size as i64, start, stop);
    let items = collect_rank_range_items(ctx, user_key, meta.version, start_rank, count, true)?;

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
    if args.len() < 3 {
        return Err(KvdbError::WrongArgCount("ZREVRANGEBYSCORE"));
    }
    let user_key = &args[0];
    let meta = match read_zset_meta(ctx, user_key)? {
        Some(m) => m,
        None => return Ok(RespValue::Array(vec![])),
    };

    // ZREVRANGEBYSCORE 参数顺序为 max min（与 ZRANGEBYSCORE 相反）
    let max = parse_score(&args[1])?;
    let min = parse_score(&args[2])?;
    let (with_scores, limit) = parse_zrangebyscore_options(&args[3..])?;

    // 从 prefix 末尾（最高 score）开始反向迭代，跳过 score > max 的项。
    // 不使用 score_seek_upper 避免 member 长度截断导致的漏取。
    let mut result = Vec::new();
    let mut current_key = Vec::new();
    let mut seen = 0usize;
    let mut matched = 0usize;
    let mut done = false;

    loop {
        let (items, next_key) = iter_score_page_reverse(ctx, user_key, meta.version, &current_key)?;
        if items.is_empty() {
            break;
        }
        for (score, member) in items {
            if score > max {
                continue;
            }
            if score < min {
                done = true;
                break;
            }
            seen += 1;
            if seen <= limit.offset {
                continue;
            }
            result.push(RespValue::BulkString(Some(Bytes::from(member))));
            if with_scores {
                result.push(RespValue::BulkString(Some(Bytes::from(format_score(
                    score,
                )))));
            }
            matched += 1;
            if limit.count > 0 && matched >= limit.count {
                done = true;
                break;
            }
        }
        if done {
            break;
        }
        match next_key {
            Some(k) => current_key = k,
            None => break,
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

    let prefix = ctx.sub_key(user_key, meta.version, &[]);
    let mut current_key = Vec::new();
    let mut rank = 0i64;
    loop {
        let (items, next_key) =
            ctx.storage
                .prefix_scan_page(CF_ZSET_SCORE, &prefix, &current_key, ZSET_PAGE_SIZE)?;
        if items.is_empty() {
            break;
        }
        for (k, _) in items {
            let (score, member) = parse_score_subkey(ctx, &k)
                .ok_or_else(|| KvdbError::Protocol("invalid zset score subkey".to_string()))?;
            if score == target_score && member == target.as_ref() {
                return Ok(RespValue::Integer(rank));
            }
            rank += 1;
        }
        match next_key {
            Some(k) => current_key = k,
            None => break,
        }
    }
    Ok(RespValue::BulkString(None))
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

    let prefix = ctx.sub_key(user_key, meta.version, &[]);
    let mut current_key = Vec::new();
    let mut rank = 0i64;
    let mut found = false;
    loop {
        let (items, next_key) = if current_key.is_empty() {
            let mut upper = prefix.clone();
            upper.extend_from_slice(&[0xFF; 8]);
            ctx.storage
                .prefix_scan_page_reverse(CF_ZSET_SCORE, &prefix, &upper, ZSET_PAGE_SIZE)?
        } else {
            ctx.storage.prefix_scan_page_reverse(
                CF_ZSET_SCORE,
                &prefix,
                &current_key,
                ZSET_PAGE_SIZE,
            )?
        };
        if items.is_empty() {
            break;
        }
        for (k, _) in items {
            let (score, member) = parse_score_subkey(ctx, &k)
                .ok_or_else(|| KvdbError::Protocol("invalid zset score subkey".to_string()))?;
            if score == target_score && member == target.as_ref() {
                found = true;
                break;
            }
            rank += 1;
        }
        if found {
            break;
        }
        match next_key {
            Some(k) => current_key = k,
            None => break,
        }
    }
    if found {
        Ok(RespValue::Integer(rank))
    } else {
        Ok(RespValue::BulkString(None))
    }
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

fn parse_usize(s: &[u8]) -> KvdbResult<usize> {
    std::str::from_utf8(s)
        .map_err(|_| KvdbError::NotInteger)?
        .parse::<usize>()
        .map_err(|_| KvdbError::NotInteger)
}

#[derive(Debug, Clone, Copy, Default)]
struct Limit {
    offset: usize,
    count: usize,
}

/// 解析 ZRANGEBYSCORE / ZREVRANGEBYSCORE 的可选参数：WITHSCORES 与 LIMIT offset count。
fn parse_zrangebyscore_options(args: &[Bytes]) -> KvdbResult<(bool, Limit)> {
    let mut with_scores = false;
    let mut limit = Limit::default();
    let mut i = 0;
    while i < args.len() {
        let arg = &args[i];
        if arg.eq_ignore_ascii_case(b"WITHSCORES") {
            with_scores = true;
            i += 1;
        } else if arg.eq_ignore_ascii_case(b"LIMIT") {
            if i + 2 >= args.len() {
                return Err(KvdbError::Command("syntax error".to_string()));
            }
            limit.offset = parse_usize(&args[i + 1])?;
            limit.count = parse_usize(&args[i + 2])?;
            i += 3;
        } else {
            return Err(KvdbError::Command("syntax error".to_string()));
        }
    }
    Ok((with_scores, limit))
}

/// 将 ZRANGE/ZREVRANGE 的 start/stop 索引转换为 (start_rank, count)。
fn rank_range(len: i64, start: i64, stop: i64) -> (usize, usize) {
    let s = normalize_index(start, len);
    let e = normalize_index(stop, len);
    if s > e || s >= len {
        return (0, 0);
    }
    let e = e.min(len - 1);
    ((s as usize), ((e - s + 1) as usize))
}

fn normalize_index(idx: i64, len: i64) -> i64 {
    if idx < 0 { (len + idx).max(0) } else { idx }
}

/// 按需读取指定 rank 范围的 (score, member) 条目，避免全量加载。
/// reverse=true 时从末尾反向读取，结果已按反向顺序排列。
fn collect_rank_range_items(
    ctx: &CommandContext,
    user_key: &[u8],
    version: u64,
    start_rank: usize,
    count: usize,
    reverse: bool,
) -> KvdbResult<Vec<(f64, Vec<u8>)>> {
    if count == 0 {
        return Ok(Vec::new());
    }
    let prefix = ctx.sub_key(user_key, version, &[]);
    let mut result = Vec::with_capacity(count);
    let mut current_key = Vec::new();
    let mut skipped = 0usize;

    loop {
        let (items, next_key) = if reverse {
            if current_key.is_empty() {
                let mut upper = prefix.clone();
                upper.extend_from_slice(&[0xFF; 8]);
                ctx.storage.prefix_scan_page_reverse(
                    CF_ZSET_SCORE,
                    &prefix,
                    &upper,
                    ZSET_PAGE_SIZE,
                )?
            } else {
                ctx.storage.prefix_scan_page_reverse(
                    CF_ZSET_SCORE,
                    &prefix,
                    &current_key,
                    ZSET_PAGE_SIZE,
                )?
            }
        } else {
            ctx.storage
                .prefix_scan_page(CF_ZSET_SCORE, &prefix, &current_key, ZSET_PAGE_SIZE)?
        };
        if items.is_empty() {
            break;
        }
        for (k, _) in items {
            let (score, member) = parse_score_subkey(ctx, &k)
                .ok_or_else(|| KvdbError::Protocol("invalid zset score subkey".to_string()))?;
            if skipped < start_rank {
                skipped += 1;
                continue;
            }
            result.push((score, member.to_vec()));
            if result.len() >= count {
                return Ok(result);
            }
        }
        match next_key {
            Some(k) => current_key = k,
            None => break,
        }
    }
    Ok(result)
}
