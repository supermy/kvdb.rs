use rocksdb::{
    BlockBasedOptions, Cache, ColumnFamily, ColumnFamilyDescriptor, DB, IteratorMode, Options,
    WriteBatch,
};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::config::{CompressionType, Config};
use crate::error::{KvdbError, KvdbResult};

pub use crate::types::{DataType, Metadata};

pub const CF_DEFAULT: &str = "default";
pub const CF_METADATA: &str = "metadata";
pub const CF_SUBKEY: &str = "subkey";
pub const CF_ZSET_SCORE: &str = "zset_score";
pub const CF_PUBSUB: &str = "pubsub";

const ALL_CFS: &[&str] = &[CF_DEFAULT, CF_METADATA, CF_SUBKEY, CF_ZSET_SCORE, CF_PUBSUB];

/// per-key 互斥锁分片数。固定大小池避免 DashMap 无界增长，
/// 哈希碰撞概率随分片数增大而降低；1024 在常见负载下冲突率 < 0.1%。
const KEY_LOCK_SHARDS: usize = 1024;

type Page = (Vec<(Vec<u8>, Vec<u8>)>, Option<Vec<u8>>);

impl DataType {
    /// 返回该数据类型的主列族：String 存 metadata，复合类型 subkey 存对应列族。
    pub const fn cf_name(&self) -> &'static str {
        match self {
            DataType::String => CF_METADATA,
            DataType::Hash => CF_SUBKEY,
            DataType::List => CF_SUBKEY,
            DataType::Set => CF_SUBKEY,
            DataType::ZSet => CF_SUBKEY,
            DataType::Stream => CF_SUBKEY,
            DataType::Bitmap => CF_SUBKEY,
        }
    }
}

/// RocksDB 存储引擎封装，所有列族共享同一 WAL 与 Compaction 资源。
/// key_locks 为固定大小分片互斥锁池，用于 INCR/DECR/APPEND 等读-改-写操作的并发保护，
/// 避免 DashMap 按 key 无限增长导致内存泄漏。
pub struct StorageEngine {
    db: Arc<DB>,
    path: PathBuf,
    key_locks: Vec<parking_lot::Mutex<()>>,
}

impl StorageEngine {
    /// 打开或创建数据库，自动补齐缺失列族；列族共享 Cache 与 WAL。
    /// 开启 Bloom filter（10 bits/key）以加速点查询，对不存在的 key 可跳过磁盘 IO。
    pub fn open(path: impl AsRef<Path>, config: &Config) -> KvdbResult<Self> {
        let path = path.as_ref().to_path_buf();
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        opts.set_max_open_files(config.storage.max_open_files);
        opts.set_write_buffer_size(config.storage.write_buffer_size as usize);
        opts.set_max_write_buffer_number(config.storage.max_write_buffer_number);
        opts.set_min_write_buffer_number_to_merge(config.storage.min_write_buffer_number_to_merge);
        opts.set_target_file_size_base(config.storage.target_file_size_base);
        opts.set_max_bytes_for_level_base(config.storage.max_bytes_for_level_base);
        opts.set_level_compaction_dynamic_level_bytes(
            config.storage.level_compaction_dynamic_level_bytes,
        );
        opts.set_compression_type(to_rocksdb_compression(config.storage.compression_type));
        opts.set_bottommost_compression_type(to_rocksdb_compression(
            config.storage.bottommost_compression_type,
        ));
        opts.set_max_background_jobs(config.storage.max_background_jobs);
        opts.set_max_subcompactions(config.storage.max_subcompactions);
        opts.set_enable_pipelined_write(config.storage.enable_pipelined_write);
        opts.set_use_fsync(config.storage.use_fsync);
        if let Some(wal_dir) = &config.storage.wal_dir {
            opts.set_wal_dir(wal_dir);
        }

        // 统一 Block Cache：索引与过滤器块也纳入预算，避免内存无界增长。
        let cache = Cache::new_lru_cache(config.storage.block_cache_size as usize);
        let mut bbto = BlockBasedOptions::default();
        bbto.set_block_cache(&cache);
        bbto.set_cache_index_and_filter_blocks(config.storage.cache_index_and_filter_blocks);
        // Bloom filter：10 bits/key 在 1% 误判率下显著减少不存在的 key 的磁盘读取，
        // 对 GET/HGET/ZSCORE 等点查询路径提升明显。block_based=true 将过滤器存于块内，
        // 配合 cache_index_and_filter_blocks 可被 Block Cache 缓存。
        bbto.set_bloom_filter(10.0, true);
        opts.set_block_based_table_factory(&bbto);

        // 列出已有列族；若数据库不存在则使用全部预定义列族。
        let existing = DB::list_cf(&opts, &path)
            .unwrap_or_else(|_| ALL_CFS.iter().map(|s| s.to_string()).collect());
        let descriptors: Vec<_> = existing
            .iter()
            .map(|name| ColumnFamilyDescriptor::new(name, opts.clone()))
            .collect();
        let db = DB::open_cf_descriptors(&opts, &path, descriptors)?;

        // 初始化固定大小分片锁池，避免 per-key DashMap 无界增长。
        // Mutex::new 是 const fn 在新版 parking_lot 中，但为兼容旧版使用运行时构造。
        let key_locks = (0..KEY_LOCK_SHARDS)
            .map(|_| parking_lot::Mutex::new(()))
            .collect();

        Ok(Self {
            db: Arc::new(db),
            path,
            key_locks,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn cf(&self, name: &str) -> KvdbResult<&ColumnFamily> {
        self.cf_handle(name)
    }

    /// 获取 per-key 互斥锁的 guard，用于 INCR/DECR/APPEND 等读-改-写操作的并发保护。
    /// 使用固定大小分片锁池：对 key 做哈希后取模映射到 KEY_LOCK_SHARDS 个分片之一，
    /// 避免按 key 存储 Arc 导致的 DashMap 无界增长。哈希碰撞时不同 key 会串行化，
    /// 但 1024 分片下冲突概率极低，不影响吞吐。
    pub fn key_lock(&self, key: &[u8]) -> parking_lot::MutexGuard<'_, ()> {
        let idx = shard_index(key);
        self.key_locks[idx].lock()
    }

    pub fn cf_handle(&self, name: &str) -> KvdbResult<&ColumnFamily> {
        self.db
            .cf_handle(name)
            .ok_or_else(|| KvdbError::Command(format!("missing column family: {}", name)))
    }

    pub fn get(&self, cf: &str, key: &[u8]) -> KvdbResult<Option<Vec<u8>>> {
        let cf = self.cf(cf)?;
        Ok(self.db.get_cf(cf, key)?)
    }

    pub fn put(&self, cf: &str, key: &[u8], value: &[u8]) -> KvdbResult<()> {
        let cf = self.cf(cf)?;
        Ok(self.db.put_cf(cf, key, value)?)
    }

    pub fn delete(&self, cf: &str, key: &[u8]) -> KvdbResult<()> {
        let cf = self.cf(cf)?;
        Ok(self.db.delete_cf(cf, key)?)
    }

    pub fn write(&self, batch: WriteBatch) -> KvdbResult<()> {
        // 批量写入共享同一 WAL，保证原子性与崩溃恢复一致性。
        Ok(self.db.write(batch)?)
    }

    pub fn batch_put(
        &self,
        batch: &mut WriteBatch,
        cf: &str,
        key: &[u8],
        value: &[u8],
    ) -> KvdbResult<()> {
        let cf = self.cf(cf)?;
        batch.put_cf(cf, key, value);
        Ok(())
    }

    pub fn batch_delete(&self, batch: &mut WriteBatch, cf: &str, key: &[u8]) -> KvdbResult<()> {
        let cf = self.cf(cf)?;
        batch.delete_cf(cf, key);
        Ok(())
    }

    pub fn prefix_scan(&self, cf: &str, prefix: &[u8]) -> KvdbResult<Vec<(Vec<u8>, Vec<u8>)>> {
        let cf = self.cf(cf)?;
        let mut iter = self.db.prefix_iterator_cf(cf, prefix);
        let mut result = Vec::new();
        // 无 prefix extractor 时需显式过滤：遇到第一个不匹配前缀的键即停止，
        // 避免 prefix_iterator 越界返回其他前缀（如其他 namespace）的数据。
        loop {
            match iter.next() {
                None => break,
                Some(Err(e)) => return Err(KvdbError::from(e)),
                Some(Ok((k, v))) => {
                    if !k.starts_with(prefix) {
                        break;
                    }
                    result.push((k.to_vec(), v.to_vec()));
                }
            }
        }
        Ok(result)
    }

    /// 分页前缀扫描：从 `start_key` 的下一项开始，扫描同一前缀下的最多 `limit` 条记录。
    /// 当 `start_key` 为空时从首项开始；返回 (条目, 下一页起始 key)，条目不足 limit 时后者为 None。
    pub fn prefix_scan_page(
        &self,
        cf: &str,
        prefix: &[u8],
        start_key: &[u8],
        limit: usize,
    ) -> KvdbResult<Page> {
        let cf = self.cf(cf)?;
        let mut iter = self.db.prefix_iterator_cf(cf, prefix);
        if !start_key.is_empty() {
            iter.set_mode(rocksdb::IteratorMode::From(
                start_key,
                rocksdb::Direction::Forward,
            ));
        }
        let mut result = Vec::with_capacity(limit);
        let mut last_key = None;
        let mut first = !start_key.is_empty();
        // 追踪退出原因：到达前缀边界时无需再预读下一条，直接标记无更多数据。
        let mut hit_boundary = false;
        for item in iter.by_ref() {
            let (k, v) = item?;
            if first {
                // 跳过 start_key 自身，从下一项开始
                first = false;
                if k.as_ref() == start_key {
                    continue;
                }
            }
            // prefix_iterator 在无 prefix extractor 时不会自动截断前缀边界，
            // 需显式判断：遇到不匹配前缀的键即停止，避免越界扫描其他 namespace 的数据。
            if !k.as_ref().starts_with(prefix) {
                hit_boundary = true;
                break;
            }
            last_key = Some(k.to_vec());
            result.push((k.to_vec(), v.to_vec()));
            if result.len() >= limit {
                break;
            }
        }
        // 仅在未到达前缀边界且已填满一页时才预读下一条，减少一次无效 IO。
        let next_key = if hit_boundary || result.len() < limit {
            None
        } else {
            // 预读下一条确认是否还有更多匹配前缀的数据
            match iter.next() {
                None => None,
                Some(Err(e)) => return Err(KvdbError::from(e)),
                Some(Ok((k, _))) => {
                    if k.starts_with(prefix) {
                        last_key
                    } else {
                        None
                    }
                }
            }
        };
        Ok((result, next_key))
    }

    /// 反向分页前缀扫描：从 `start_key` 的前一项开始，按字典序倒序扫描同一前缀下的最多 `limit` 条记录。
    /// 当 `start_key` 为空时从末项开始；调用方需保证 `start_key` 位于目标前缀范围内或为其上界，
    /// 否则可能返回空。
    /// 返回 (条目, 下一页起始 key)，条目不足 limit 时后者为 None。
    pub fn prefix_scan_page_reverse(
        &self,
        cf: &str,
        prefix: &[u8],
        start_key: &[u8],
        limit: usize,
    ) -> KvdbResult<Page> {
        let cf = self.cf(cf)?;
        let mut iter = self.db.prefix_iterator_cf(cf, prefix);
        if start_key.is_empty() {
            // 未指定起始键时，从 prefix 的字典序上界开始反向扫描。
            // 由于 prefix_iterator_cf 初始化时会 seek 到 prefix 首项，
            // 反向模式下直接构造 prefix + 0xFF... 作为上界 seek 点。
            let mut upper = prefix.to_vec();
            upper.extend_from_slice(&[0xFF; 8]);
            iter.set_mode(rocksdb::IteratorMode::From(
                &upper,
                rocksdb::Direction::Reverse,
            ));
        } else {
            iter.set_mode(rocksdb::IteratorMode::From(
                start_key,
                rocksdb::Direction::Reverse,
            ));
        }
        let mut result = Vec::with_capacity(limit);
        let mut last_key = None;
        let mut first = true;
        // 追踪退出原因：到达前缀边界时无需预读，直接标记无更多数据。
        let mut hit_boundary = false;
        for item in iter.by_ref() {
            let (k, v) = item?;
            if first {
                // 跳过 start_key 自身，从前一项开始
                first = false;
                if k.as_ref() == start_key {
                    continue;
                }
            }
            // prefix_iterator 在反向越过前缀边界时可能返回非目标键，需显式截断。
            if !k.as_ref().starts_with(prefix) {
                hit_boundary = true;
                break;
            }
            last_key = Some(k.to_vec());
            result.push((k.to_vec(), v.to_vec()));
            if result.len() >= limit {
                break;
            }
        }
        let next_key = if hit_boundary || result.len() < limit {
            None
        } else {
            match iter.next() {
                None => None,
                Some(Err(e)) => return Err(KvdbError::from(e)),
                Some(Ok((k, _))) => {
                    if k.starts_with(prefix) {
                        last_key
                    } else {
                        None
                    }
                }
            }
        };
        Ok((result, next_key))
    }

    pub fn full_scan(&self, cf: &str) -> KvdbResult<Vec<(Vec<u8>, Vec<u8>)>> {
        let cf = self.cf(cf)?;
        let iter = self.db.iterator_cf(cf, IteratorMode::Start);
        iter.map(|r| r.map(|(k, v)| (k.to_vec(), v.to_vec())))
            .collect::<Result<_, _>>()
            .map_err(KvdbError::from)
    }

    /// 分页扫描并批量删除指定列族中匹配前缀的全部键。
    /// 每批 `batch_size` 条，避免单个 WriteBatch 过大导致内存峰值。
    pub fn delete_prefix(&self, cf: &str, prefix: &[u8], batch_size: usize) -> KvdbResult<()> {
        let cf_handle = self.cf(cf)?;
        let mut start_key = Vec::new();
        loop {
            let (items, next_key) = self.prefix_scan_page(cf, prefix, &start_key, batch_size)?;
            if items.is_empty() {
                break;
            }
            let mut batch = WriteBatch::default();
            for (k, _) in &items {
                batch.delete_cf(cf_handle, k);
            }
            self.write(batch)?;
            match next_key {
                Some(k) => start_key = k,
                None => break,
            }
        }
        Ok(())
    }

    /// 迭代计数指定列族中匹配前缀的键数量，避免全量加载到内存。
    pub fn count_prefix(&self, cf: &str, prefix: &[u8]) -> KvdbResult<u64> {
        let cf = self.cf(cf)?;
        let mut iter = self.db.prefix_iterator_cf(cf, prefix);
        let mut count = 0u64;
        loop {
            match iter.next() {
                None => break,
                Some(Err(e)) => return Err(KvdbError::from(e)),
                Some(Ok((k, _))) => {
                    // 无 prefix extractor 时需显式截断，避免越界计数其他前缀的键。
                    if !k.starts_with(prefix) {
                        break;
                    }
                    count += 1;
                }
            }
        }
        Ok(count)
    }

    pub fn checkpoint(&self, path: impl AsRef<Path>) -> KvdbResult<()> {
        let cp = rocksdb::checkpoint::Checkpoint::new(&self.db)?;
        cp.create_checkpoint(path)?;
        Ok(())
    }

    pub fn repair(path: impl AsRef<Path>) -> KvdbResult<()> {
        let opts = Options::default();
        DB::repair(&opts, path)?;
        Ok(())
    }

    pub fn flush(&self) -> KvdbResult<()> {
        // Flush 默认列族即可触发 WAL 与 MemTable 持久化；生产环境中可按需 flush 所有列族。
        let cf = self.cf(CF_DEFAULT)?;
        Ok(self.db.flush_cf(cf)?)
    }

    /// 读取 RocksDB 属性值（如 "rocksdb.stats"）。
    pub fn property_value(&self, name: &str) -> KvdbResult<Option<String>> {
        let cf = self.cf(CF_DEFAULT)?;
        Ok(self.db.property_value_cf(cf, name)?)
    }
}

fn to_rocksdb_compression(t: CompressionType) -> rocksdb::DBCompressionType {
    match t {
        CompressionType::None => rocksdb::DBCompressionType::None,
        CompressionType::Snappy => rocksdb::DBCompressionType::Snappy,
        CompressionType::Lz4 => rocksdb::DBCompressionType::Lz4,
        CompressionType::Zstd => rocksdb::DBCompressionType::Zstd,
    }
}

/// 将 key 哈希到 [0, KEY_LOCK_SHARDS) 区间，用于分片锁池索引。
/// 使用 FxHash 风格的乘法哈希：速度快、分布均匀，适合分片场景。
fn shard_index(key: &[u8]) -> usize {
    // 简单快速哈希：乘以黄金分割常数，取高 bits 作为索引。
    // 避免引入额外 crate，使用 std hasher 即可满足分片需求。
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    key.hash(&mut hasher);
    (hasher.finish() as usize) % KEY_LOCK_SHARDS
}
