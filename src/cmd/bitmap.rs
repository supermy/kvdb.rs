use bytes::Bytes;
use rocksdb::WriteBatch;

use super::{CommandContext, CommandTable, expect_arg_count, expect_min_arg_count};
use crate::encoding::{decode_metadata, encode_metadata, generate_version, metadata_key, subkey};
use crate::error::{KvdbError, KvdbResult};
use crate::protocol::RespValue;
use crate::storage::{CF_METADATA, CF_SUBKEY};
use crate::types::{DataType, Metadata};

const CF_META: &str = CF_METADATA;
const CF_SUB: &str = CF_SUBKEY;
const FRAGMENT_BYTES: usize = 1024;
const FRAGMENT_BITS: u64 = (FRAGMENT_BYTES * 8) as u64;

pub fn register(table: &mut CommandTable) {
    table.register("SETBIT", setbit);
    table.register("GETBIT", getbit);
    table.register("BITCOUNT", bitcount);
    table.register("BITOP", bitop);
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

fn read_bitmap_meta(ctx: &CommandContext, key: &[u8]) -> KvdbResult<Option<Metadata>> {
    match ctx.storage.get(CF_META, key)? {
        Some(v) => {
            let meta = decode_metadata(&v)
                .ok_or(KvdbError::Protocol("invalid metadata encoding".to_string()))?;
            if meta.data_type() != Some(DataType::Bitmap) {
                return Err(KvdbError::Command(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
                ));
            }
            if meta.is_expired(now_ms()) {
                return Ok(None);
            }
            Ok(Some(meta))
        }
        None => Ok(None),
    }
}

fn get_or_create_metadata(ctx: &CommandContext, key: &[u8]) -> KvdbResult<Metadata> {
    match read_bitmap_meta(ctx, key)? {
        Some(meta) => Ok(meta),
        None => Ok(Metadata::new(DataType::Bitmap, generate_version())),
    }
}

fn read_fragment(
    ctx: &CommandContext,
    meta: &Metadata,
    user_key: &[u8],
    index: u64,
) -> KvdbResult<Vec<u8>> {
    let sk = subkey(user_key, meta.version, &index.to_be_bytes());
    match ctx.storage.get(CF_SUB, &sk)? {
        Some(v) => Ok(v),
        None => Ok(Vec::new()),
    }
}

fn parse_offset(data: &[u8]) -> KvdbResult<u64> {
    std::str::from_utf8(data)
        .map_err(|_| {
            KvdbError::Command("bit offset is not an integer or out of range".to_string())
        })?
        .parse::<u64>()
        .map_err(|_| KvdbError::Command("bit offset is not an integer or out of range".to_string()))
}

fn parse_bit(data: &[u8]) -> KvdbResult<u8> {
    if data == b"0" {
        Ok(0)
    } else if data == b"1" {
        Ok(1)
    } else {
        Err(KvdbError::Command(
            "bit is not an integer or out of range".to_string(),
        ))
    }
}

fn parse_i64(data: &[u8]) -> KvdbResult<i64> {
    std::str::from_utf8(data)
        .map_err(|_| KvdbError::NotInteger)?
        .parse::<i64>()
        .map_err(|_| KvdbError::NotInteger)
}

fn normalize_index(idx: i64, size: i64) -> usize {
    let mut idx = if idx < 0 { size + idx } else { idx };
    if idx < 0 {
        idx = 0;
    }
    if idx >= size {
        idx = size - 1;
    }
    idx as usize
}

fn setbit(ctx: &CommandContext, args: &[Bytes]) -> KvdbResult<RespValue> {
    expect_arg_count("SETBIT", args, 3)?;
    let user_key = &args[0];
    let offset = parse_offset(&args[1])?;
    let value = parse_bit(&args[2])?;

    let meta_key = metadata_key(user_key);
    let mut meta = get_or_create_metadata(ctx, &meta_key)?;

    let index = offset / FRAGMENT_BITS;
    let frag_off = ((offset % FRAGMENT_BITS) / 8) as usize;
    let bit_off = (offset % 8) as u8;

    let mut frag = read_fragment(ctx, &meta, user_key, index)?;
    if frag.len() <= frag_off {
        frag.resize(frag_off + 1, 0);
    }

    let old_bit = ((frag[frag_off] >> bit_off) & 1) as i64;
    if value == 1 {
        frag[frag_off] |= 1 << bit_off;
    } else {
        frag[frag_off] &= !(1 << bit_off);
    }

    let byte_pos = (offset / 8 + 1) as u64;
    if byte_pos > meta.size {
        meta.size = byte_pos;
    }

    let mut batch = WriteBatch::default();
    ctx.storage
        .batch_put(&mut batch, CF_META, &meta_key, &encode_metadata(&meta))?;
    let sk = subkey(user_key, meta.version, &index.to_be_bytes());
    ctx.storage.batch_put(&mut batch, CF_SUB, &sk, &frag)?;
    ctx.storage.write(batch)?;

    Ok(RespValue::Integer(old_bit))
}

fn getbit(ctx: &CommandContext, args: &[Bytes]) -> KvdbResult<RespValue> {
    expect_arg_count("GETBIT", args, 2)?;
    let user_key = &args[0];
    let offset = parse_offset(&args[1])?;

    let meta_key = metadata_key(user_key);
    let meta = match read_bitmap_meta(ctx, &meta_key)? {
        Some(m) => m,
        None => return Ok(RespValue::Integer(0)),
    };

    let index = offset / FRAGMENT_BITS;
    let frag_off = ((offset % FRAGMENT_BITS) / 8) as usize;
    let bit_off = (offset % 8) as u8;

    let frag = read_fragment(ctx, &meta, user_key, index)?;
    if frag_off >= frag.len() {
        return Ok(RespValue::Integer(0));
    }
    let bit = ((frag[frag_off] >> bit_off) & 1) as i64;
    Ok(RespValue::Integer(bit))
}

fn bitcount(ctx: &CommandContext, args: &[Bytes]) -> KvdbResult<RespValue> {
    if args.len() != 1 && args.len() != 3 {
        return Err(KvdbError::WrongArgCount("BITCOUNT"));
    }
    let user_key = &args[0];
    let (start, end) = if args.len() == 1 {
        (0i64, -1i64)
    } else {
        (parse_i64(&args[1])?, parse_i64(&args[2])?)
    };

    let meta_key = metadata_key(user_key);
    let meta = match read_bitmap_meta(ctx, &meta_key)? {
        Some(m) => m,
        None => return Ok(RespValue::Integer(0)),
    };

    let size = meta.size as i64;
    if size == 0 {
        return Ok(RespValue::Integer(0));
    }

    let start = normalize_index(start, size);
    let end = normalize_index(end, size);
    if start > end {
        return Ok(RespValue::Integer(0));
    }

    let start_frag = (start / FRAGMENT_BYTES) as u64;
    let end_frag = (end / FRAGMENT_BYTES) as u64;
    let mut count: u64 = 0;

    for frag_idx in start_frag..=end_frag {
        let frag = read_fragment(ctx, &meta, user_key, frag_idx)?;
        if frag.is_empty() {
            continue;
        }
        let frag_start_byte = frag_idx as usize * FRAGMENT_BYTES;
        let local_start = start.saturating_sub(frag_start_byte);
        let local_end = (end - frag_start_byte).min(FRAGMENT_BYTES - 1);
        let local_end = local_end.min(frag.len() - 1);
        if local_start > local_end {
            continue;
        }
        for b in &frag[local_start..=local_end] {
            count += b.count_ones() as u64;
        }
    }

    Ok(RespValue::Integer(count as i64))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BitOp {
    And,
    Or,
    Xor,
    Not,
}

fn parse_operation(data: &[u8]) -> KvdbResult<BitOp> {
    if data.eq_ignore_ascii_case(b"AND") {
        Ok(BitOp::And)
    } else if data.eq_ignore_ascii_case(b"OR") {
        Ok(BitOp::Or)
    } else if data.eq_ignore_ascii_case(b"XOR") {
        Ok(BitOp::Xor)
    } else if data.eq_ignore_ascii_case(b"NOT") {
        Ok(BitOp::Not)
    } else {
        Err(KvdbError::Command("syntax error".to_string()))
    }
}

fn bitop(ctx: &CommandContext, args: &[Bytes]) -> KvdbResult<RespValue> {
    expect_min_arg_count("BITOP", args, 3)?;
    let op = parse_operation(&args[0])?;
    let dest_key = &args[1];
    let src_keys = &args[2..];

    if op == BitOp::Not && src_keys.len() != 1 {
        return Err(KvdbError::Command(
            "BITOP NOT must be called with a single source key".to_string(),
        ));
    }

    let mut src_metas = Vec::with_capacity(src_keys.len());
    let mut max_size: u64 = 0;
    for key in src_keys {
        let meta_key = metadata_key(key);
        match read_bitmap_meta(ctx, &meta_key)? {
            Some(m) => {
                if m.size > max_size {
                    max_size = m.size;
                }
                src_metas.push(Some(m));
            }
            None => src_metas.push(None),
        }
    }

    let dest_meta_key = metadata_key(dest_key);
    let dest_version = generate_version();
    let mut dest_meta = Metadata::new(DataType::Bitmap, dest_version);
    dest_meta.size = max_size;

    let mut batch = WriteBatch::default();
    ctx.storage.batch_put(
        &mut batch,
        CF_META,
        &dest_meta_key,
        &encode_metadata(&dest_meta),
    )?;

    if max_size == 0 {
        ctx.storage.write(batch)?;
        return Ok(RespValue::Integer(0));
    }

    let max_frag_idx = (max_size - 1) / FRAGMENT_BYTES as u64;

    for frag_idx in 0..=max_frag_idx {
        let frag_start_byte = frag_idx as usize * FRAGMENT_BYTES;
        let frag_len = FRAGMENT_BYTES.min((max_size as usize).saturating_sub(frag_start_byte));
        if frag_len == 0 {
            continue;
        }

        let mut result = vec![0u8; frag_len];

        match op {
            BitOp::Not => {
                let src_meta = src_metas[0].as_ref().unwrap();
                let frag = read_fragment(ctx, src_meta, &src_keys[0], frag_idx)?;
                for i in 0..frag_len {
                    result[i] = if i < frag.len() { !frag[i] } else { !0u8 };
                }
            }
            _ => {
                let mut first = true;
                for (i, src_meta_opt) in src_metas.iter().enumerate() {
                    let frag = match src_meta_opt {
                        Some(m) => read_fragment(ctx, m, &src_keys[i], frag_idx)?,
                        None => Vec::new(),
                    };
                    if first {
                        for j in 0..frag_len {
                            result[j] = if j < frag.len() { frag[j] } else { 0 };
                        }
                        first = false;
                    } else {
                        for j in 0..frag_len {
                            let b = if j < frag.len() { frag[j] } else { 0 };
                            result[j] = match op {
                                BitOp::And => result[j] & b,
                                BitOp::Or => result[j] | b,
                                BitOp::Xor => result[j] ^ b,
                                BitOp::Not => unreachable!(),
                            };
                        }
                    }
                }
            }
        }

        let sk = subkey(dest_key, dest_version, &frag_idx.to_be_bytes());
        ctx.storage.batch_put(&mut batch, CF_SUB, &sk, &result)?;
    }

    ctx.storage.write(batch)?;
    Ok(RespValue::Integer(max_size as i64))
}
