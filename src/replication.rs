use parking_lot::RwLock;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// 主从复制角色：主节点或指定主地址的从节点。
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub enum ReplicationRole {
    #[default]
    Master,
    Replica {
        host: String,
        port: u16,
    },
}

/// 复制状态：维护当前角色、主节点复制 ID/偏移量与本地处理偏移量。
/// 目前为骨架实现，仅记录角色切换与偏移量；全量/增量同步后续补充。
#[derive(Default)]
pub struct ReplicationState {
    role: RwLock<ReplicationRole>,
    master_replid: RwLock<String>,
    master_repl_offset: AtomicU64,
    local_offset: AtomicU64,
}

impl ReplicationState {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn role(&self) -> ReplicationRole {
        self.role.read().clone()
    }

    pub fn is_master(&self) -> bool {
        matches!(self.role(), ReplicationRole::Master)
    }

    /// 切换为指定主节点的从节点；骨架阶段不建立真实连接。
    pub fn set_replica(&self, host: String, port: u16) {
        *self.role.write() = ReplicationRole::Replica { host, port };
    }

    /// 提升为主节点（REPLICAOF NO ONE）。
    pub fn set_master(&self) {
        *self.role.write() = ReplicationRole::Master;
    }

    pub fn master_replid(&self) -> String {
        self.master_replid.read().clone()
    }

    pub fn set_master_replid(&self, id: String) {
        *self.master_replid.write() = id;
    }

    pub fn master_repl_offset(&self) -> u64 {
        self.master_repl_offset.load(Ordering::Relaxed)
    }

    pub fn set_master_repl_offset(&self, offset: u64) {
        self.master_repl_offset.store(offset, Ordering::Relaxed);
    }

    pub fn local_offset(&self) -> u64 {
        self.local_offset.load(Ordering::Relaxed)
    }

    /// 累计本地处理偏移量；骨架阶段暂不绑定实际命令字节数。
    pub fn add_local_offset(&self, delta: u64) {
        self.local_offset.fetch_add(delta, Ordering::Relaxed);
    }
}
