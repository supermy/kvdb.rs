use bytes::Bytes;

use super::{CommandContext, CommandTable, expect_arg_count};
use crate::cluster::slots_range_to_resp;
use crate::error::KvdbResult;
use crate::protocol::RespValue;

pub fn register(table: &mut CommandTable) {
    table.register("CLUSTER", cluster);
}

/// 处理 CLUSTER 子命令：SLOTS / KEYSLOT / NODES（骨架实现）。
fn cluster(ctx: &CommandContext, args: &[Bytes]) -> KvdbResult<RespValue> {
    if args.is_empty() {
        return Ok(RespValue::Error(
            "ERR wrong number of arguments for 'cluster' command".to_string(),
        ));
    }
    let sub = String::from_utf8_lossy(&args[0]).to_ascii_uppercase();
    match sub.as_str() {
        "SLOTS" => cluster_slots(ctx),
        "KEYSLOT" => cluster_keyslot(ctx, &args[1..]),
        "NODES" => cluster_nodes(ctx),
        _ => Ok(RespValue::Error(format!(
            "ERR unknown subcommand '{}'",
            sub
        ))),
    }
}

fn cluster_slots(ctx: &CommandContext) -> KvdbResult<RespValue> {
    let ranges = ctx.cluster.slots_ranges();
    let items: Vec<RespValue> = ranges
        .into_iter()
        .map(|(start, end, addr)| slots_range_to_resp(start, end, &addr))
        .collect();
    Ok(RespValue::Array(items))
}

fn cluster_keyslot(_ctx: &CommandContext, args: &[Bytes]) -> KvdbResult<RespValue> {
    expect_arg_count("CLUSTER KEYSLOT", args, 1)?;
    let slot = crate::cluster::ClusterState::key_slot(&args[0]);
    Ok(RespValue::Integer(slot as i64))
}

fn cluster_nodes(ctx: &CommandContext) -> KvdbResult<RespValue> {
    let nodes = ctx.cluster.nodes();
    if nodes.is_empty() {
        // 骨架阶段返回本节点信息。
        return Ok(RespValue::BulkString(Some(Bytes::from_static(
            b"0000000000000000000000000000000000000000 127.0.0.1:6379 myself,master - 0 0 0 connected 0-16383\n",
        ))));
    }
    let text: String = nodes
        .into_iter()
        .map(|n| {
            format!(
                "{} {} {} - 0 0 0 connected\n",
                n.id,
                n.addr,
                n.role.role_str()
            )
        })
        .collect();
    Ok(RespValue::BulkString(Some(Bytes::from(text))))
}

trait RoleStr {
    fn role_str(&self) -> &'static str;
}

impl RoleStr for crate::cluster::ClusterNodeRole {
    fn role_str(&self) -> &'static str {
        match self {
            crate::cluster::ClusterNodeRole::Master => "master",
            crate::cluster::ClusterNodeRole::Replica => "replica",
        }
    }
}
