use crate::types::{DataType, FLAGS_TYPE_MASK, Metadata};

/// 生成 8 字节版本号：高 48 位为毫秒时间戳，低 16 位为随机数，保证单调递增且唯一。
pub fn generate_version() -> u64 {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    let rnd = rand::random::<u16>() as u64;
    (now << 16) | (rnd & 0xFFFF)
}

/// 构造 metadata 列族的键：当前简化设计为 4 字节长度前缀 + 用户原始键。
/// 未来可扩展 namespace 与 cluster slot 前缀，保持兼容性。
pub fn metadata_key(user_key: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(4 + user_key.len());
    buf.extend_from_slice(&(user_key.len() as u32).to_be_bytes());
    buf.extend_from_slice(user_key);
    buf
}

/// 解析 metadata 键，返回用户原始键。
pub fn parse_metadata_key(data: &[u8]) -> Option<&[u8]> {
    if data.len() < 4 {
        return None;
    }
    let len = u32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;
    if data.len() < 4 + len {
        return None;
    }
    Some(&data[4..4 + len])
}

/// 构造 subkey：metadata_key + version(8) + sub，用于 Hash/List/Set/ZSet/Bitmap 子键。
pub fn subkey(user_key: &[u8], version: u64, sub: &[u8]) -> Vec<u8> {
    let mut buf = metadata_key(user_key);
    buf.extend_from_slice(&version.to_be_bytes());
    buf.extend_from_slice(sub);
    buf
}

/// 解析 subkey，返回 (user_key, version, sub)。
pub fn parse_subkey(data: &[u8]) -> Option<(&[u8], u64, &[u8])> {
    if data.len() < 4 {
        return None;
    }
    let key_len = u32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;
    if data.len() < 4 + key_len + 8 {
        return None;
    }
    let user_key = &data[4..4 + key_len];
    let version = u64::from_be_bytes([
        data[4 + key_len],
        data[4 + key_len + 1],
        data[4 + key_len + 2],
        data[4 + key_len + 3],
        data[4 + key_len + 4],
        data[4 + key_len + 5],
        data[4 + key_len + 6],
        data[4 + key_len + 7],
    ]);
    let sub = &data[4 + key_len + 8..];
    Some((user_key, version, sub))
}

/// 编码复合类型 metadata value。
/// List 额外包含 head/tail 字段，其他类型仅 flags + expire + version + size。
pub fn encode_metadata(meta: &Metadata) -> Vec<u8> {
    let is_list = meta.data_type() == Some(DataType::List);
    let capacity = if is_list {
        1 + 8 + 8 + 8 + 8 + 8
    } else {
        1 + 8 + 8 + 8
    };
    let mut buf = Vec::with_capacity(capacity);
    buf.push(meta.flags);
    buf.extend_from_slice(&meta.expire.to_be_bytes());
    buf.extend_from_slice(&meta.version.to_be_bytes());
    buf.extend_from_slice(&meta.size.to_be_bytes());
    if is_list {
        buf.extend_from_slice(&meta.head.to_be_bytes());
        buf.extend_from_slice(&meta.tail.to_be_bytes());
    }
    buf
}

/// 解码复合类型 metadata value。
pub fn decode_metadata(data: &[u8]) -> Option<Metadata> {
    if data.len() < 1 + 8 + 8 + 8 {
        return None;
    }
    let flags = data[0];
    let data_type = DataType::from_code(flags & FLAGS_TYPE_MASK)?;
    let expire = i64::from_be_bytes([
        data[1], data[2], data[3], data[4], data[5], data[6], data[7], data[8],
    ]);
    let version = u64::from_be_bytes([
        data[9], data[10], data[11], data[12], data[13], data[14], data[15], data[16],
    ]);
    let size = u64::from_be_bytes([
        data[17], data[18], data[19], data[20], data[21], data[22], data[23], data[24],
    ]);
    let (head, tail) = if data_type == DataType::List {
        if data.len() < 1 + 8 + 8 + 8 + 8 + 8 {
            return None;
        }
        let head = i64::from_be_bytes([
            data[25], data[26], data[27], data[28], data[29], data[30], data[31], data[32],
        ]);
        let tail = i64::from_be_bytes([
            data[33], data[34], data[35], data[36], data[37], data[38], data[39], data[40],
        ]);
        (head, tail)
    } else {
        (0, -1)
    };
    Some(Metadata {
        flags,
        expire,
        version,
        size,
        head,
        tail,
    })
}

/// 编码 String 值：flags + expire(8) + payload。
pub fn encode_string(flags: u8, expire: i64, payload: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(1 + 8 + payload.len());
    buf.push(flags);
    buf.extend_from_slice(&expire.to_be_bytes());
    buf.extend_from_slice(payload);
    buf
}

/// 解码 String 值，返回 (flags, expire, payload)。
pub fn decode_string(data: &[u8]) -> Option<(u8, i64, &[u8])> {
    if data.len() < 1 + 8 {
        return None;
    }
    let flags = data[0];
    let expire = i64::from_be_bytes([
        data[1], data[2], data[3], data[4], data[5], data[6], data[7], data[8],
    ]);
    Some((flags, expire, &data[9..]))
}
