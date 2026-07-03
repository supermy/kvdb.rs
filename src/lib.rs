use std::path::Path;

pub mod benchmark;
pub mod cli;
pub mod cluster;
pub mod cmd;
pub mod config;
pub mod encoding;
pub mod error;
pub mod http_server;
pub mod lua;
pub mod protocol;
pub mod pubsub;
pub mod replication;
pub mod server;
pub mod storage;
pub mod thread_pool;
pub mod types;

pub use config::{Config, ConfigManager};
pub use error::{KvdbError, KvdbResult};
pub use server::Server;
pub use storage::{DataType, StorageEngine};

/// 打开嵌入式数据库实例，Server 与嵌入式模式共享同一 StorageEngine。
pub fn open_embedded(path: impl AsRef<Path>, config: Config) -> KvdbResult<StorageEngine> {
    StorageEngine::open(path, &config)
}
