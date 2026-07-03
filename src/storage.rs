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
pub struct StorageEngine {
    db: Arc<DB>,
    path: PathBuf,
}

impl StorageEngine {
    /// 打开或创建数据库，自动补齐缺失列族；列族共享 Cache 与 WAL。
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
        opts.set_block_based_table_factory(&bbto);

        // 列出已有列族；若数据库不存在则使用全部预定义列族。
        let existing = DB::list_cf(&opts, &path)
            .unwrap_or_else(|_| ALL_CFS.iter().map(|s| s.to_string()).collect());
        let descriptors: Vec<_> = existing
            .iter()
            .map(|name| ColumnFamilyDescriptor::new(name, opts.clone()))
            .collect();
        let db = DB::open_cf_descriptors(&opts, &path, descriptors)?;
        Ok(Self {
            db: Arc::new(db),
            path,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn cf(&self, name: &str) -> KvdbResult<&ColumnFamily> {
        self.cf_handle(name)
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
        let iter = self.db.prefix_iterator_cf(cf, prefix);
        iter.map(|r| r.map(|(k, v)| (k.to_vec(), v.to_vec())))
            .collect::<Result<_, _>>()
            .map_err(KvdbError::from)
    }

    pub fn full_scan(&self, cf: &str) -> KvdbResult<Vec<(Vec<u8>, Vec<u8>)>> {
        let cf = self.cf(cf)?;
        let iter = self.db.iterator_cf(cf, IteratorMode::Start);
        iter.map(|r| r.map(|(k, v)| (k.to_vec(), v.to_vec())))
            .collect::<Result<_, _>>()
            .map_err(KvdbError::from)
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
