use bytes::Bytes;
use std::collections::HashMap;
use std::sync::Arc;

use crate::cluster::ClusterState;
use crate::config::ConfigManager;
use crate::encoding::{metadata_key, parse_metadata_key, parse_subkey, subkey};
use crate::error::{KvdbError, KvdbResult};
use crate::lua::LuaEngine;
use crate::protocol::RespValue;
use crate::pubsub::{PubSubHub, PublishEvent};
use crate::replication::ReplicationState;
use crate::storage::{CF_METADATA, StorageEngine};
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
pub mod stream;
pub mod string;
pub mod zset;

/// 统一的 WRONGTYPE 错误消息：所有命令在遇到类型不匹配时必须返回该消息，
/// 保证客户端可凭字符串匹配识别类型错误，与 Redis 协议兼容。
pub const WRONGTYPE: &str = "WRONGTYPE Operation against a key holding the wrong kind of value";

/// 构造 WRONGTYPE 错误，避免各模块重复字面量导致消息漂移。
pub fn wrong_type_error() -> KvdbError {
    KvdbError::Command(WRONGTYPE.to_string())
}

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
    /// 全局命名空间前缀，来自 server.namespace 配置；空表示不启用。
    pub namespace: Bytes,
}

impl CommandContext {
    /// 构造当前 namespace 下的 metadata 键。
    pub fn meta_key(&self, user_key: &[u8]) -> Vec<u8> {
        metadata_key(user_key, &self.namespace)
    }

    /// 构造当前 namespace 下的 subkey。
    pub fn sub_key(&self, user_key: &[u8], version: u64, sub: &[u8]) -> Vec<u8> {
        subkey(user_key, version, sub, &self.namespace)
    }

    /// 解析当前 namespace 下的 metadata 键，返回用户原始键。
    pub fn parse_meta_key<'a>(&'a self, data: &'a [u8]) -> Option<&'a [u8]> {
        parse_metadata_key(data, &self.namespace)
    }

    /// 解析当前 namespace 下的 subkey，返回 (user_key, version, sub)。
    pub fn parse_subkey<'a>(&'a self, data: &'a [u8]) -> Option<(&'a [u8], u64, &'a [u8])> {
        parse_subkey(data, &self.namespace)
    }

    /// 读取 metadata 列族，优先使用当前 namespace 键；若未命中且 namespace 非空，
    /// 则回退到旧格式（无 namespace）以兼容历史数据。
    pub fn get_meta(&self, user_key: &[u8]) -> KvdbResult<Option<Vec<u8>>> {
        let key = self.meta_key(user_key);
        match self.storage.get(CF_METADATA, &key)? {
            Some(v) => Ok(Some(v)),
            None => {
                if self.namespace.is_empty() {
                    Ok(None)
                } else {
                    let legacy = metadata_key(user_key, &[]);
                    self.storage.get(CF_METADATA, &legacy)
                }
            }
        }
    }
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
        stream::register(&mut table);
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
    /// 错误格式遵循 Redis 协议：WRONGTYPE 等已知错误码不带 "ERR " 前缀，
    /// 其他通用错误统一加 "ERR " 前缀。
    pub fn dispatch(&self, ctx: &CommandContext, cmd: &[u8], args: &[Bytes]) -> RespValue {
        let name = String::from_utf8_lossy(cmd).to_ascii_uppercase();
        match self.table.get(&name) {
            Some(func) => match func(ctx, args) {
                Ok(v) => v,
                Err(e) => RespValue::Error(format_error(e)),
            },
            None => RespValue::Error(format!("ERR unknown command '{}'", name)),
        }
    }
}

/// 将 KvdbError 格式化为 Redis 协议错误字符串。
/// 已知 Redis 错误码（WRONGTYPE/NOAUTH/LOADING 等）直接输出，不加 "ERR " 前缀；
/// 其他错误统一加 "ERR " 前缀，与 Redis 行为一致。
fn format_error(e: KvdbError) -> String {
    let msg = e.to_string();
    // Redis 错误码为全大写单词 + 空格；若消息以此开头则视为已带错误码。
    if has_redis_error_code(&msg) {
        msg
    } else {
        format!("ERR {}", msg)
    }
}

/// 判断消息是否已以 Redis 错误码开头（如 "WRONGTYPE "、"NOAUTH "）。
/// 规则：首单词全大写字母且长度 ≥ 2，后接空格。
fn has_redis_error_code(msg: &str) -> bool {
    let mut upper_count = 0usize;
    for c in msg.chars() {
        if c.is_ascii_uppercase() {
            upper_count += 1;
        } else {
            return c == ' ' && upper_count >= 2;
        }
    }
    false
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
