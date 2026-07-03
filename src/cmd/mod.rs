use bytes::Bytes;
use std::collections::HashMap;
use std::sync::Arc;

use crate::cluster::ClusterState;
use crate::config::ConfigManager;
use crate::error::{KvdbError, KvdbResult};
use crate::lua::LuaEngine;
use crate::protocol::RespValue;
use crate::pubsub::{PubSubHub, PublishEvent};
use crate::replication::ReplicationState;
use crate::storage::StorageEngine;
use crate::thread_pool::ThreadPool;
use tokio::sync::mpsc;

pub mod admin;
pub mod bitmap;
pub mod cluster;
pub mod hash;
pub mod list;
pub mod lua;
pub mod pubsub;
pub mod set;
pub mod string;
pub mod zset;

#[derive(Debug, Default, Clone)]
pub struct ClientState {
    pub db_index: usize,
    pub subscribed_channels: Vec<String>,
}

#[derive(Clone)]
pub struct CommandContext {
    pub storage: Arc<StorageEngine>,
    pub config: Arc<ConfigManager>,
    pub tx_pool: ThreadPool,
    pub client: ClientState,
    pub pubsub: Arc<PubSubHub>,
    pub pubsub_tx: mpsc::UnboundedSender<PublishEvent>,
    pub client_id: u64,
    pub lua: Arc<LuaEngine>,
    pub replication: Arc<ReplicationState>,
    pub cluster: Arc<ClusterState>,
}

pub type CommandFn = fn(&CommandContext, &[Bytes]) -> KvdbResult<RespValue>;

pub struct CommandTable {
    table: HashMap<String, CommandFn>,
}

impl CommandTable {
    pub fn new() -> Self {
        let mut table = Self {
            table: HashMap::new(),
        };
        string::register(&mut table);
        admin::register(&mut table);
        hash::register(&mut table);
        list::register(&mut table);
        set::register(&mut table);
        zset::register(&mut table);
        bitmap::register(&mut table);
        pubsub::register(&mut table);
        lua::register(&mut table);
        cluster::register(&mut table);
        table
    }

    pub fn register(&mut self, name: &str, func: CommandFn) {
        self.table.insert(name.to_ascii_uppercase(), func);
    }

    /// 命令分发：执行顺序为解析 → 查表 → 执行 → 返回 RespValue。
    /// 每个命令拥有传入的 Bytes 切片的所有权视图，StorageEngine 负责复制需要持久化的数据。
    pub fn dispatch(&self, ctx: &CommandContext, cmd: &[u8], args: &[Bytes]) -> RespValue {
        let name = String::from_utf8_lossy(cmd).to_ascii_uppercase();
        match self.table.get(&name) {
            Some(func) => match func(ctx, args) {
                Ok(v) => v,
                Err(e) => RespValue::Error(format!("ERR {}", e)),
            },
            None => RespValue::Error(format!("ERR unknown command '{}'", name)),
        }
    }
}

impl Default for CommandTable {
    fn default() -> Self {
        Self::new()
    }
}

pub(crate) fn expect_arg_count(
    name: &'static str,
    args: &[Bytes],
    expected: usize,
) -> KvdbResult<()> {
    if args.len() != expected {
        return Err(KvdbError::WrongArgCount(name));
    }
    Ok(())
}

pub(crate) fn expect_min_arg_count(
    name: &'static str,
    args: &[Bytes],
    min: usize,
) -> KvdbResult<()> {
    if args.len() < min {
        return Err(KvdbError::WrongArgCount(name));
    }
    Ok(())
}
