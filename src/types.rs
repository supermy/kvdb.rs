/// 编码版本：当前固定为 1，使用 8 字节毫秒过期时间与 8 字节 size。
pub const ENCODING_VERSION: u8 = 1;

/// flags 高 4 位掩码：编码版本。
pub const FLAGS_VERSION_MASK: u8 = 0xF0;
/// flags 低 4 位掩码：数据类型。
pub const FLAGS_TYPE_MASK: u8 = 0x0F;

/// 数据类型枚举值（占 flags 低 4 位）。
pub const DATA_TYPE_STRING: u8 = 1;
pub const DATA_TYPE_HASH: u8 = 2;
pub const DATA_TYPE_LIST: u8 = 3;
pub const DATA_TYPE_SET: u8 = 4;
pub const DATA_TYPE_ZSET: u8 = 5;
pub const DATA_TYPE_BITMAP: u8 = 6;
pub const DATA_TYPE_STREAM: u8 = 8;

/// Redis 兼容数据类型。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataType {
    String,
    Hash,
    List,
    Set,
    ZSet,
    Stream,
    Bitmap,
}

impl DataType {
    pub const fn code(&self) -> u8 {
        match self {
            DataType::String => DATA_TYPE_STRING,
            DataType::Hash => DATA_TYPE_HASH,
            DataType::List => DATA_TYPE_LIST,
            DataType::Set => DATA_TYPE_SET,
            DataType::ZSet => DATA_TYPE_ZSET,
            DataType::Stream => DATA_TYPE_STREAM,
            DataType::Bitmap => DATA_TYPE_BITMAP,
        }
    }

    pub const fn from_code(code: u8) -> Option<Self> {
        match code {
            DATA_TYPE_STRING => Some(DataType::String),
            DATA_TYPE_HASH => Some(DataType::Hash),
            DATA_TYPE_LIST => Some(DataType::List),
            DATA_TYPE_SET => Some(DataType::Set),
            DATA_TYPE_ZSET => Some(DataType::ZSet),
            DATA_TYPE_STREAM => Some(DataType::Stream),
            DATA_TYPE_BITMAP => Some(DataType::Bitmap),
            _ => None,
        }
    }
}

/// 构造 flags 字段：高 4 位为编码版本，低 4 位为数据类型。
pub const fn build_flags(data_type: DataType) -> u8 {
    (ENCODING_VERSION << 4) | (data_type.code() & FLAGS_TYPE_MASK)
}

/// 从 flags 解析数据类型。
pub const fn data_type_from_flags(flags: u8) -> Option<DataType> {
    DataType::from_code(flags & FLAGS_TYPE_MASK)
}

/// 从 flags 解析编码版本。
pub const fn version_from_flags(flags: u8) -> u8 {
    (flags & FLAGS_VERSION_MASK) >> 4
}

/// 复合类型的元数据（metadata 列族）。
/// 对于 String，metadata value 即为 flags + expire + payload，不使用此结构。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Metadata {
    pub flags: u8,
    /// 过期时间，毫秒级时间戳；0 表示永不过期。
    pub expire: i64,
    /// 版本号，用于快速删除（更新版本后旧 subkey 由 Compaction 回收）。
    pub version: u64,
    /// 元素数量。
    pub size: u64,
    /// List 头索引（仅 List 使用）。
    pub head: i64,
    /// List 尾索引（仅 List 使用）。
    pub tail: i64,
}

impl Metadata {
    /// 创建复合类型元数据；head/tail 对非 List 类型无意义。
    pub fn new(data_type: DataType, version: u64) -> Self {
        Self {
            flags: build_flags(data_type),
            expire: 0,
            version,
            size: 0,
            head: 0,
            tail: -1,
        }
    }

    pub fn data_type(&self) -> Option<DataType> {
        data_type_from_flags(self.flags)
    }

    pub fn encoding_version(&self) -> u8 {
        version_from_flags(self.flags)
    }

    pub fn is_expired(&self, now_ms: i64) -> bool {
        self.expire > 0 && self.expire <= now_ms
    }
}
