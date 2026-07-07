use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use sysinfo::System;

use crate::error::{KvdbError, KvdbResult};

const MIB: u64 = 1024 * 1024;
const GIB: u64 = 1024 * MIB;
const DEFAULT_BLOCK_CACHE: u64 = 512 * MIB;
const LOW_MEM_BLOCK_CACHE: u64 = 256 * MIB;
const LOW_MEM_THRESHOLD: u64 = 8 * GIB;

#[derive(Debug, Default, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CompressionType {
    None,
    Snappy,
    #[default]
    Lz4,
    Zstd,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    pub bind: String,
    pub unix_socket: Option<String>,
    pub worker_threads: Option<usize>,
    pub maxclients: i64,
    pub tcp_keepalive: u64,
    pub timeout: u64,
    pub http_bind: String,
    /// 全局命名空间前缀，默认空字符串表示不启用；非空时所有键名前增加该前缀，
    /// 但读取时仍兼容旧格式数据（未带前缀）。
    pub namespace: String,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind: "127.0.0.1:6379".to_string(),
            unix_socket: None,
            worker_threads: None,
            maxclients: 10000,
            tcp_keepalive: 300,
            timeout: 0,
            http_bind: "127.0.0.1:8080".to_string(),
            namespace: String::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    pub db_path: String,
    pub wal_dir: Option<String>,
    pub max_open_files: i32,
    pub write_buffer_size: u64,
    pub max_write_buffer_number: i32,
    pub min_write_buffer_number_to_merge: i32,
    pub target_file_size_base: u64,
    pub max_bytes_for_level_base: u64,
    pub level_compaction_dynamic_level_bytes: bool,
    pub compression_type: CompressionType,
    pub bottommost_compression_type: CompressionType,
    pub block_cache_size: u64,
    pub cache_index_and_filter_blocks: bool,
    pub wal_bytes_limit: u64,
    pub max_background_jobs: i32,
    pub max_subcompactions: u32,
    pub enable_pipelined_write: bool,
    pub use_fsync: bool,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            db_path: "./data".to_string(),
            wal_dir: None,
            max_open_files: -1,
            write_buffer_size: 64 * MIB,
            max_write_buffer_number: 6,
            min_write_buffer_number_to_merge: 2,
            target_file_size_base: 64 * MIB,
            max_bytes_for_level_base: 256 * MIB,
            level_compaction_dynamic_level_bytes: true,
            compression_type: CompressionType::Lz4,
            bottommost_compression_type: CompressionType::Zstd,
            block_cache_size: DEFAULT_BLOCK_CACHE,
            cache_index_and_filter_blocks: true,
            wal_bytes_limit: 64 * MIB,
            max_background_jobs: 4,
            max_subcompactions: 2,
            enable_pipelined_write: true,
            use_fsync: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub server: ServerConfig,
    pub storage: StorageConfig,
    pub log_level: String,
    pub dynamic_config: bool,
    #[serde(skip)]
    pub config_file: Option<PathBuf>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            server: ServerConfig::default(),
            storage: StorageConfig::default(),
            log_level: "info".to_string(),
            dynamic_config: true,
            config_file: None,
        }
    }
}

impl Config {
    fn merge(&mut self, other: Config) {
        self.server = other.server;
        self.storage = other.storage;
        self.log_level = other.log_level;
        self.dynamic_config = other.dynamic_config;
    }

    /// 资源上限校验：拒绝可能导致 OOM 或资源枯竭的参数组合。
    pub fn validate(&self) -> KvdbResult<()> {
        if self.storage.write_buffer_size == 0 {
            return Err(KvdbError::Config(
                "write_buffer_size must be > 0".to_string(),
            ));
        }
        if self.storage.max_write_buffer_number <= 0 {
            return Err(KvdbError::Config(
                "max_write_buffer_number must be > 0".to_string(),
            ));
        }
        let mut sys = System::new_all();
        sys.refresh_memory();
        // sysinfo 返回 KB，转换为字节。
        let total_mem = sys.total_memory() * 1024;
        let write_buffer_total =
            self.storage.write_buffer_size * self.storage.max_write_buffer_number as u64;
        let budget = self.storage.block_cache_size + write_buffer_total;
        if budget > total_mem / 2 {
            return Err(KvdbError::Config(format!(
                "memory budget {} exceeds 50% of physical memory {}",
                budget, total_mem
            )));
        }
        if self.server.maxclients <= 0 {
            return Err(KvdbError::Config("maxclients must be > 0".to_string()));
        }
        Ok(())
    }

    /// 根据物理内存自动下调 Block Cache（在 < 8GB 设备上降至 256MB）。
    pub fn adjust_for_memory(&mut self) {
        let mut sys = System::new_all();
        sys.refresh_memory();
        let total_mem = sys.total_memory() * 1024;
        if total_mem < LOW_MEM_THRESHOLD && self.storage.block_cache_size > LOW_MEM_BLOCK_CACHE {
            self.storage.block_cache_size = LOW_MEM_BLOCK_CACHE;
        }
    }
}

/// 分层配置管理器：Hardcoded Default → 配置文件 → 运行时 API。
pub struct ConfigManager {
    current: Arc<RwLock<Config>>,
}

impl ConfigManager {
    pub fn new(config: Config) -> Self {
        Self {
            current: Arc::new(RwLock::new(config)),
        }
    }

    pub fn load(path: Option<&Path>) -> KvdbResult<Self> {
        let mut config = Config::default();
        if let Some(p) = path {
            config.config_file = Some(p.to_path_buf());
            let content = std::fs::read_to_string(p)?;
            let file_config: Config = toml::from_str(&content).or_else(|_| {
                serde_yaml::from_str(&content)
                    .map_err(|e| KvdbError::Config(format!("failed to parse config file: {}", e)))
            })?;
            config.merge(file_config);
        }
        // 环境变量层（KVDB_LOG_LEVEL 等）可后续扩展，此处保持默认值。
        config.adjust_for_memory();
        config.validate()?;
        Ok(Self::new(config))
    }

    pub fn get(&self) -> Config {
        self.current.read().clone()
    }

    /// 运行时热更新：校验失败时回滚并返回错误，原配置继续运行。
    pub fn update(&self, mut config: Config) -> KvdbResult<()> {
        config.adjust_for_memory();
        config.validate()?;
        *self.current.write() = config;
        Ok(())
    }
}
