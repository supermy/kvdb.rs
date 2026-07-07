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

/// 构造 metadata 列族的键。
/// namespace 为空时使用旧格式（4 字节长度前缀 + 用户原始键），保持与历史数据兼容；
/// namespace 非空时使用新格式（1 字节 ns 长度 + namespace + 4 字节 key 长度 + 用户原始键），
/// 实现多租户键空间隔离。
pub fn metadata_key(user_key: &[u8], namespace: &[u8]) -> Vec<u8> {
    if namespace.is_empty() {
        let mut buf = Vec::with_capacity(4 + user_key.len());
        buf.extend_from_slice(&(user_key.len() as u32).to_be_bytes());
        buf.extend_from_slice(user_key);
        buf
    } else {
        let mut buf = Vec::with_capacity(1 + namespace.len() + 4 + user_key.len());
        buf.push(namespace.len() as u8);
        buf.extend_from_slice(namespace);
        buf.extend_from_slice(&(user_key.len() as u32).to_be_bytes());
        buf.extend_from_slice(user_key);
        buf
    }
}

/// 解析 metadata 键，返回用户原始键（不含 namespace）。
/// 根据传入的 namespace 选择解析格式，namespace 为空时解析旧格式。
pub fn parse_metadata_key<'a>(data: &'a [u8], namespace: &'a [u8]) -> Option<&'a [u8]> {
    if namespace.is_empty() {
        if data.len() < 4 {
            return None;
        }
        let len = u32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;
        if data.len() < 4 + len {
            return None;
        }
        Some(&data[4..4 + len])
    } else {
        let ns_len = namespace.len();
        if data.len() < 1 + ns_len + 4 {
            return None;
        }
        if data[0] as usize != ns_len || &data[1..1 + ns_len] != namespace {
            return None;
        }
        let key_start = 1 + ns_len;
        let key_len = u32::from_be_bytes([
            data[key_start],
            data[key_start + 1],
            data[key_start + 2],
            data[key_start + 3],
        ]) as usize;
        if data.len() < key_start + 4 + key_len {
            return None;
        }
        Some(&data[key_start + 4..key_start + 4 + key_len])
    }
}

/// 构造 subkey：metadata_key + version(8) + sub，用于 Hash/List/Set/ZSet/Bitmap/Stream 子键。
pub fn subkey(user_key: &[u8], version: u64, sub: &[u8], namespace: &[u8]) -> Vec<u8> {
    let mut buf = metadata_key(user_key, namespace);
    buf.extend_from_slice(&version.to_be_bytes());
    buf.extend_from_slice(sub);
    buf
}

/// 解析 subkey，返回 (user_key, version, sub)。
/// namespace 为空时解析旧格式，否则跳过 namespace 前缀后解析。
pub fn parse_subkey<'a>(data: &'a [u8], namespace: &'a [u8]) -> Option<(&'a [u8], u64, &'a [u8])> {
    let ns_len = namespace.len();
    let key_offset = if namespace.is_empty() {
        0
    } else {
        if data.len() < 1 + ns_len + 4 {
            return None;
        }
        if data[0] as usize != ns_len || &data[1..1 + ns_len] != namespace {
            return None;
        }
        1 + ns_len
    };
    if data.len() < key_offset + 4 {
        return None;
    }
    let key_len = u32::from_be_bytes([
        data[key_offset],
        data[key_offset + 1],
        data[key_offset + 2],
        data[key_offset + 3],
    ]) as usize;
    if data.len() < key_offset + 4 + key_len + 8 {
        return None;
    }
    let user_key = &data[key_offset + 4..key_offset + 4 + key_len];
    let version_pos = key_offset + 4 + key_len;
    let version = u64::from_be_bytes([
        data[version_pos],
        data[version_pos + 1],
        data[version_pos + 2],
        data[version_pos + 3],
        data[version_pos + 4],
        data[version_pos + 5],
        data[version_pos + 6],
        data[version_pos + 7],
    ]);
    let sub = &data[version_pos + 8..];
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
