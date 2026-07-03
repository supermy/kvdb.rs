use bytes::Bytes;
use std::collections::HashMap;
use std::sync::Arc;

pub const CLUSTER_SLOT_COUNT: u16 = 16384;

/// 集群节点信息（骨架阶段仅保存地址与角色）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterNode {
    pub id: String,
    pub addr: String,
    pub role: ClusterNodeRole,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClusterNodeRole {
    Master,
    Replica,
}

/// 集群状态：槽位到主节点地址的映射。
/// 目前为骨架实现，所有槽位默认归属本节点；迁移与故障转移后续补充。
#[derive(Default)]
pub struct ClusterState {
    slots: parking_lot::RwLock<HashMap<u16, String>>,
    nodes: parking_lot::RwLock<HashMap<String, ClusterNode>>,
}

impl ClusterState {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// 计算键所属的哈希槽（兼容 Redis Cluster CRC16 mod 16384）。
    pub fn key_slot(key: &[u8]) -> u16 {
        let (start, end) = match key.iter().position(|&b| b == b'{') {
            Some(start) => match key[start..].iter().position(|&b| b == b'}') {
                Some(len) if len > 1 => (start + 1, start + len),
                _ => (0, key.len()),
            },
            None => (0, key.len()),
        };
        let tag = &key[start..end];
        let crc = crc16::State::<crc16::XMODEM>::calculate(tag);
        crc % CLUSTER_SLOT_COUNT
    }

    pub fn assign_slot(&self, slot: u16, node_id: String) {
        self.slots.write().insert(slot, node_id.clone());
    }

    pub fn slot_node(&self, slot: u16) -> Option<String> {
        self.slots.read().get(&slot).cloned()
    }

    pub fn add_node(&self, node: ClusterNode) {
        self.nodes.write().insert(node.id.clone(), node);
    }

    pub fn nodes(&self) -> Vec<ClusterNode> {
        self.nodes.read().values().cloned().collect()
    }

    /// 返回 CLUSTER SLOTS 骨架所需的槽位范围列表：单个范围覆盖全部 16384 槽。
    pub fn slots_ranges(&self) -> Vec<(u16, u16, String)> {
        let guard = self.slots.read();
        if guard.is_empty() {
            // 默认所有槽位由本地主节点负责。
            return vec![(0, CLUSTER_SLOT_COUNT - 1, "127.0.0.1:6379".to_string())];
        }
        // 简化：按节点 ID 聚合连续槽位，实际集群需要维护精确范围。
        let mut ranges = Vec::new();
        let mut slots: Vec<(u16, String)> = guard.iter().map(|(&k, v)| (k, v.clone())).collect();
        slots.sort_by_key(|(s, _)| *s);
        if slots.is_empty() {
            return ranges;
        }
        let mut start = slots[0].0;
        let mut end = start;
        let mut current_node = slots[0].1.clone();
        for (slot, node) in slots.into_iter().skip(1) {
            if slot == end + 1 && node == current_node {
                end = slot;
            } else {
                ranges.push((start, end, current_node.clone()));
                start = slot;
                end = slot;
                current_node = node;
            }
        }
        ranges.push((start, end, current_node));
        ranges
    }
}

/// 将槽位范围转换为 RESP 数组条目：
/// [start, end, [master_addr, port, node_id], ...replicas]
pub fn slots_range_to_resp(start: u16, end: u16, addr: &str) -> crate::protocol::RespValue {
    let parts: Vec<&str> = addr.split(':').collect();
    let host = parts.first().copied().unwrap_or("127.0.0.1").to_string();
    let port = parts
        .get(1)
        .and_then(|s| s.parse::<i64>().ok())
        .unwrap_or(6379);
    crate::protocol::RespValue::Array(vec![
        crate::protocol::RespValue::Integer(start as i64),
        crate::protocol::RespValue::Integer(end as i64),
        crate::protocol::RespValue::Array(vec![
            crate::protocol::RespValue::BulkString(Some(Bytes::from(host))),
            crate::protocol::RespValue::Integer(port),
            crate::protocol::RespValue::BulkString(Some(Bytes::from_static(
                b"0000000000000000000000000000000000000000",
            ))),
        ]),
    ])
}
