use super::{CommandContext, CommandTable, expect_min_arg_count};
use crate::encoding::{decode_metadata, generate_version};
use crate::error::{KvdbError, KvdbResult};
use crate::protocol::RespValue;
use crate::storage::{CF_METADATA, CF_SUBKEY, DataType};
use bytes::Bytes;
use rocksdb::WriteBatch;

const WRONGTYPE: &str = "WRONGTYPE Operation against a key holding the wrong kind of value";

/// Stream 命令分页上限，防止 XRANGE/XREAD 单次返回过大结果导致 OOM。
const STREAM_PAGE_LIMIT: usize = 1000;

type EntryPage = (Vec<(EntryId, Vec<(Bytes, Bytes)>)>, Option<Vec<u8>>);

pub fn register(table: &mut CommandTable) {
    table.register("XADD", xadd);
    table.register("XLEN", xlen);
    table.register("XRANGE", xrange);
    table.register("XREAD", xread);
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Stream 专用 metadata：flags + expire + version + size + last_ms + last_seq。
/// 使用自定义编码而非复用 Metadata 结构，避免扩展公共类型影响其他数据类型。
fn encode_stream_meta(
    flags: u8,
    expire: i64,
    version: u64,
    size: u64,
    last_ms: u64,
    last_seq: u64,
) -> Vec<u8> {
    let mut buf = Vec::with_capacity(1 + 8 + 8 + 8 + 8 + 8);
    buf.push(flags);
    buf.extend_from_slice(&expire.to_be_bytes());
    buf.extend_from_slice(&version.to_be_bytes());
    buf.extend_from_slice(&size.to_be_bytes());
    buf.extend_from_slice(&last_ms.to_be_bytes());
    buf.extend_from_slice(&last_seq.to_be_bytes());
    buf
}

fn decode_stream_meta(data: &[u8]) -> Option<(u8, i64, u64, u64, u64, u64)> {
    if data.len() < 1 + 8 + 8 + 8 + 8 + 8 {
        return None;
    }
    let flags = data[0];
    let expire = i64::from_be_bytes([
        data[1], data[2], data[3], data[4], data[5], data[6], data[7], data[8],
    ]);
    let version = u64::from_be_bytes([
        data[9], data[10], data[11], data[12], data[13], data[14], data[15], data[16],
    ]);
    let size = u64::from_be_bytes([
        data[17], data[18], data[19], data[20], data[21], data[22], data[23], data[24],
    ]);
    let last_ms = u64::from_be_bytes([
        data[25], data[26], data[27], data[28], data[29], data[30], data[31], data[32],
    ]);
    let last_seq = u64::from_be_bytes([
        data[33], data[34], data[35], data[36], data[37], data[38], data[39], data[40],
    ]);
    Some((flags, expire, version, size, last_ms, last_seq))
}

fn read_stream_meta(ctx: &CommandContext, user_key: &[u8]) -> KvdbResult<Option<StreamMeta>> {
    match ctx.get_meta(user_key)? {
        Some(v) => {
            // String 类型使用独立编码，直接判定为类型错误。
            if DataType::from_code(v[0] & 0x0F) == Some(DataType::String) {
                return Err(KvdbError::Command(WRONGTYPE.to_string()));
            }
            // 先尝试 Stream 自定义编码；失败则回退到通用 Metadata 编码（兼容未迁移数据）。
            if let Some((flags, expire, version, size, last_ms, last_seq)) = decode_stream_meta(&v)
            {
                if DataType::from_code(flags & 0x0F) != Some(DataType::Stream) {
                    return Err(KvdbError::Command(WRONGTYPE.to_string()));
                }
                return Ok(Some(StreamMeta {
                    flags,
                    expire,
                    version,
                    size,
                    last_ms,
                    last_seq,
                }));
            }
            let meta = decode_metadata(&v).ok_or_else(|| {
                KvdbError::Protocol("invalid stream metadata encoding".to_string())
            })?;
            if meta.data_type() != Some(DataType::Stream) {
                return Err(KvdbError::Command(WRONGTYPE.to_string()));
            }
            if meta.is_expired(now_ms() as i64) {
                return Ok(None);
            }
            Ok(Some(StreamMeta {
                flags: meta.flags,
                expire: meta.expire,
                version: meta.version,
                size: meta.size,
                last_ms: 0,
                last_seq: 0,
            }))
        }
        None => Ok(None),
    }
}

#[derive(Debug, Clone, Copy)]
struct StreamMeta {
    flags: u8,
    expire: i64,
    version: u64,
    size: u64,
    last_ms: u64,
    last_seq: u64,
}

impl StreamMeta {
    fn new(version: u64) -> Self {
        Self {
            flags: crate::types::build_flags(DataType::Stream),
            expire: 0,
            version,
            size: 0,
            last_ms: 0,
            last_seq: 0,
        }
    }

    fn is_expired(&self) -> bool {
        self.expire > 0 && self.expire <= now_ms() as i64
    }

    fn encode(&self) -> Vec<u8> {
        encode_stream_meta(
            self.flags,
            self.expire,
            self.version,
            self.size,
            self.last_ms,
            self.last_seq,
        )
    }
}

/// Entry ID：毫秒时间戳 + 序列号，均使用大端序编码以保证字典序与数值序一致。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct EntryId(u64, u64);

impl EntryId {
    fn encode(&self) -> [u8; 16] {
        let mut buf = [0u8; 16];
        buf[..8].copy_from_slice(&self.0.to_be_bytes());
        buf[8..].copy_from_slice(&self.1.to_be_bytes());
        buf
    }

    fn format_id(&self) -> String {
        format!("{}-{}", self.0, self.1)
    }

    fn parse(s: &[u8]) -> KvdbResult<Option<Self>> {
        let text = std::str::from_utf8(s)
            .map_err(|_| KvdbError::Command("invalid stream ID".to_string()))?;
        if text == "-" {
            return Ok(Some(EntryId(0, 0)));
        }
        if text == "+" {
            return Ok(Some(EntryId(u64::MAX, u64::MAX)));
        }
        let parts: Vec<&str> = text.splitn(2, '-').collect();
        if parts.len() != 2 {
            return Err(KvdbError::Command("invalid stream ID".to_string()));
        }
        let ms = parts[0]
            .parse::<u64>()
            .map_err(|_| KvdbError::Command("invalid stream ID".to_string()))?;
        let seq = parts[1]
            .parse::<u64>()
            .map_err(|_| KvdbError::Command("invalid stream ID".to_string()))?;
        Ok(Some(EntryId(ms, seq)))
    }
}

/// 将字段列表编码为 value：偶数个字符串，每串前 4 字节长度前缀。
fn encode_fields(fields: &[(Bytes, Bytes)]) -> Vec<u8> {
    let mut buf = Vec::new();
    for (k, v) in fields {
        buf.extend_from_slice(&(k.len() as u32).to_be_bytes());
        buf.extend_from_slice(k);
        buf.extend_from_slice(&(v.len() as u32).to_be_bytes());
        buf.extend_from_slice(v);
    }
    buf
}

fn decode_fields(data: &[u8]) -> Option<Vec<(Bytes, Bytes)>> {
    let mut result = Vec::new();
    let mut pos = 0;
    while pos < data.len() {
        if pos + 4 > data.len() {
            return None;
        }
        let k_len =
            u32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]) as usize;
        pos += 4;
        if pos + k_len + 4 > data.len() {
            return None;
        }
        let k = Bytes::copy_from_slice(&data[pos..pos + k_len]);
        pos += k_len;
        let v_len =
            u32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]) as usize;
        pos += 4;
        if pos + v_len > data.len() {
            return None;
        }
        let v = Bytes::copy_from_slice(&data[pos..pos + v_len]);
        pos += v_len;
        result.push((k, v));
    }
    Some(result)
}

/// 生成新的 Entry ID。规则：
/// - "*"：取当前毫秒，序列号在该毫秒内递增；若毫秒变化则序列号归零。
/// - "ms-*"：取指定毫秒，序列号在该毫秒内递增。
fn generate_id(spec: &[u8], meta: &StreamMeta) -> KvdbResult<EntryId> {
    let text = std::str::from_utf8(spec)
        .map_err(|_| KvdbError::Command("invalid stream ID".to_string()))?;
    if text == "*" {
        let ms = now_ms();
        let seq = if meta.last_ms == ms {
            meta.last_seq + 1
        } else {
            0
        };
        return Ok(EntryId(ms, seq));
    }
    if let Some(prefix) = text.strip_suffix("-*") {
        let ms = prefix
            .parse::<u64>()
            .map_err(|_| KvdbError::Command("invalid stream ID".to_string()))?;
        let seq = if meta.last_ms == ms {
            meta.last_seq + 1
        } else {
            0
        };
        return Ok(EntryId(ms, seq));
    }
    let id =
        EntryId::parse(spec)?.ok_or_else(|| KvdbError::Command("invalid stream ID".to_string()))?;
    // 显式指定 ID 时，若小于等于最后生成的 ID 则拒绝，保证单调递增。
    if meta.size > 0 && id <= EntryId(meta.last_ms, meta.last_seq) {
        return Err(KvdbError::Command(
            "The ID specified in XADD must be greater than 0-0".to_string(),
        ));
    }
    Ok(id)
}

fn xadd(ctx: &CommandContext, args: &[Bytes]) -> KvdbResult<RespValue> {
    expect_min_arg_count("XADD", args, 3)?;
    // 字段数量 = args.len() - key - id = 偶数
    if (args.len() - 2) % 2 != 0 {
        return Err(KvdbError::WrongArgCount("XADD"));
    }

    let user_key = &args[0];
    let id_spec = &args[1];
    let meta_key = ctx.meta_key(user_key);

    let mut meta = match read_stream_meta(ctx, user_key)? {
        Some(m) => m,
        None => StreamMeta::new(generate_version()),
    };
    if meta.is_expired() {
        meta = StreamMeta::new(generate_version());
    }

    let id = generate_id(id_spec, &meta)?;

    let mut fields = Vec::with_capacity((args.len() - 2) / 2);
    for pair in args[2..].chunks_exact(2) {
        fields.push((pair[0].clone(), pair[1].clone()));
    }

    let eid_bytes = id.encode();
    let sub_key = ctx.sub_key(user_key, meta.version, &eid_bytes);
    let value = encode_fields(&fields);

    let mut batch = WriteBatch::default();
    batch.put_cf(ctx.storage.cf_handle(CF_SUBKEY)?, &sub_key, &value);

    meta.size += 1;
    meta.last_ms = id.0;
    meta.last_seq = id.1;
    batch.put_cf(
        ctx.storage.cf_handle(CF_METADATA)?,
        &meta_key,
        meta.encode(),
    );

    ctx.storage.write(batch)?;
    Ok(RespValue::BulkString(Some(Bytes::from(id.format_id()))))
}

fn xlen(ctx: &CommandContext, args: &[Bytes]) -> KvdbResult<RespValue> {
    if args.len() != 1 {
        return Err(KvdbError::WrongArgCount("XLEN"));
    }
    let user_key = &args[0];
    let meta = match read_stream_meta(ctx, user_key)? {
        Some(m) => m,
        None => return Ok(RespValue::Integer(0)),
    };
    if meta.is_expired() {
        return Ok(RespValue::Integer(0));
    }
    Ok(RespValue::Integer(meta.size as i64))
}

/// 解析 XRANGE 边界；"-" 返回最小 ID，"+" 返回最大 ID，"ms-" 返回该毫秒 seq=0。
fn parse_range_bound(s: &[u8], _is_start: bool) -> KvdbResult<EntryId> {
    let text =
        std::str::from_utf8(s).map_err(|_| KvdbError::Command("invalid stream ID".to_string()))?;
    if text == "-" {
        return Ok(EntryId(0, 0));
    }
    if text == "+" {
        return Ok(EntryId(u64::MAX, u64::MAX));
    }
    if text.ends_with('-') && text.len() > 1 {
        let ms = text[..text.len() - 1]
            .parse::<u64>()
            .map_err(|_| KvdbError::Command("invalid stream ID".to_string()))?;
        return Ok(EntryId(ms, 0));
    }
    EntryId::parse(s)?.ok_or_else(|| KvdbError::Command("invalid stream ID".to_string()))
}

fn build_entry_resp(id: EntryId, fields: &[(Bytes, Bytes)]) -> RespValue {
    let mut field_arr = Vec::with_capacity(fields.len() * 2);
    for (k, v) in fields {
        field_arr.push(RespValue::BulkString(Some(k.clone())));
        field_arr.push(RespValue::BulkString(Some(v.clone())));
    }
    RespValue::Array(vec![
        RespValue::BulkString(Some(Bytes::from(id.format_id()))),
        RespValue::Array(field_arr),
    ])
}

fn entry_subkey_prefix(ctx: &CommandContext, user_key: &[u8], version: u64) -> Vec<u8> {
    ctx.sub_key(user_key, version, &[])
}

/// 从 subkey 解析 EntryId；subkey 格式为 metadata_key|version|eid_ms(8)|eid_seq(8)。
fn parse_entry_id_from_subkey(ctx: &CommandContext, key: &[u8]) -> Option<EntryId> {
    let (_, _, sub) = ctx.parse_subkey(key)?;
    if sub.len() < 16 {
        return None;
    }
    let ms = u64::from_be_bytes(sub[..8].try_into().ok()?);
    let seq = u64::from_be_bytes(sub[8..16].try_into().ok()?);
    Some(EntryId(ms, seq))
}

fn iter_entries_page(
    ctx: &CommandContext,
    user_key: &[u8],
    version: u64,
    start_key: &[u8],
) -> KvdbResult<EntryPage> {
    let prefix = entry_subkey_prefix(ctx, user_key, version);
    let (items, next_key) =
        ctx.storage
            .prefix_scan_page(CF_SUBKEY, &prefix, start_key, STREAM_PAGE_LIMIT)?;
    let mut entries = Vec::with_capacity(items.len());
    for (k, v) in items {
        let id = parse_entry_id_from_subkey(ctx, &k)
            .ok_or_else(|| KvdbError::Protocol("invalid stream subkey encoding".to_string()))?;
        let fields = decode_fields(&v)
            .ok_or_else(|| KvdbError::Protocol("invalid stream entry encoding".to_string()))?;
        entries.push((id, fields));
    }
    Ok((entries, next_key))
}

fn xrange(ctx: &CommandContext, args: &[Bytes]) -> KvdbResult<RespValue> {
    // XRANGE key start end [COUNT count]
    if args.len() < 3 || args.len() > 5 {
        return Err(KvdbError::WrongArgCount("XRANGE"));
    }
    let user_key = &args[0];
    let start = parse_range_bound(&args[1], true)?;
    let end = parse_range_bound(&args[2], false)?;

    let mut count = None;
    if args.len() == 5 {
        let opt = String::from_utf8_lossy(&args[3]).to_ascii_uppercase();
        if opt != "COUNT" {
            return Err(KvdbError::Command("syntax error".to_string()));
        }
        let n = std::str::from_utf8(&args[4])
            .map_err(|_| KvdbError::NotInteger)?
            .parse::<usize>()
            .map_err(|_| KvdbError::NotInteger)?;
        count = Some(n);
    }

    let meta = match read_stream_meta(ctx, user_key)? {
        Some(m) => m,
        None => return Ok(RespValue::Array(vec![])),
    };
    if meta.is_expired() {
        return Ok(RespValue::Array(vec![]));
    }

    let mut result = Vec::new();
    let mut start_key = Vec::new();
    let mut remaining = count;
    loop {
        let (entries, next_key) = iter_entries_page(ctx, user_key, meta.version, &start_key)?;
        if entries.is_empty() {
            break;
        }
        for (id, fields) in entries {
            if id < start {
                continue;
            }
            if id > end {
                return Ok(RespValue::Array(result));
            }
            result.push(build_entry_resp(id, &fields));
            if let Some(ref mut n) = remaining {
                *n -= 1;
                if *n == 0 {
                    return Ok(RespValue::Array(result));
                }
            }
        }
        match next_key {
            Some(k) => start_key = k,
            None => break,
        }
    }
    Ok(RespValue::Array(result))
}

fn xread(ctx: &CommandContext, args: &[Bytes]) -> KvdbResult<RespValue> {
    // XREAD STREAMS key [key ...] id [id ...]
    // 本实现为阻塞无关版本：仅返回指定流中 ID 大于给定 ID 的条目。
    if args.len() < 3 {
        return Err(KvdbError::WrongArgCount("XREAD"));
    }
    if !args[0].eq_ignore_ascii_case(b"STREAMS") {
        return Err(KvdbError::Command("syntax error".to_string()));
    }
    let remaining = args.len() - 1;
    // key 与 id 数量必须相等且至少为 1
    if remaining % 2 != 0 {
        return Err(KvdbError::WrongArgCount("XREAD"));
    }
    let num_streams = remaining / 2;
    let keys = &args[1..1 + num_streams];
    let ids = &args[1 + num_streams..];

    let mut streams = Vec::with_capacity(num_streams);
    for (key, id_spec) in keys.iter().zip(ids.iter()) {
        let start = parse_xread_id(id_spec)?;
        let meta = match read_stream_meta(ctx, key)? {
            Some(m) => m,
            None => continue,
        };
        if meta.is_expired() {
            continue;
        }
        let mut entries = Vec::new();
        let mut start_key = Vec::new();
        loop {
            let (page, next_key) = iter_entries_page(ctx, key, meta.version, &start_key)?;
            if page.is_empty() {
                break;
            }
            for (id, fields) in page {
                if id <= start {
                    continue;
                }
                entries.push(build_entry_resp(id, &fields));
            }
            match next_key {
                Some(k) => start_key = k,
                None => break,
            }
        }
        if !entries.is_empty() {
            streams.push(RespValue::Array(vec![
                RespValue::BulkString(Some(key.clone())),
                RespValue::Array(entries),
            ]));
        }
    }
    Ok(RespValue::Array(streams))
}

/// XREAD 的 ID 参数：不支持 "-" / "+"，可为 "0-0" 等具体值。
fn parse_xread_id(s: &[u8]) -> KvdbResult<EntryId> {
    EntryId::parse(s)?.ok_or_else(|| KvdbError::Command("invalid stream ID".to_string()))
}
