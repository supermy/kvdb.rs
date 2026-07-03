use bytes::Bytes;

use super::{CommandContext, CommandTable, expect_min_arg_count};
use crate::error::KvdbResult;
use crate::protocol::RespValue;

pub fn register(table: &mut CommandTable) {
    table.register("SUBSCRIBE", subscribe);
    table.register("UNSUBSCRIBE", unsubscribe);
    table.register("PUBLISH", publish);
}

/// SUBSCRIBE channel [channel ...]
/// 返回一个数组，每个元素是一次确认的 ["subscribe", channel, total_subscriptions]。
fn subscribe(ctx: &CommandContext, args: &[Bytes]) -> KvdbResult<RespValue> {
    expect_min_arg_count("SUBSCRIBE", args, 1)?;
    let channels: Vec<String> = args
        .iter()
        .map(|b| String::from_utf8_lossy(b).into_owned())
        .collect();

    let added = ctx
        .pubsub
        .subscribe(ctx.client_id, channels, ctx.pubsub_tx.clone());

    let mut replies = Vec::with_capacity(added.len());
    for ch in added {
        let count = ctx.pubsub.subscription_count(ctx.client_id) as i64;
        replies.push(RespValue::Array(vec![
            RespValue::BulkString(Some(Bytes::from("subscribe"))),
            RespValue::BulkString(Some(Bytes::from(ch))),
            RespValue::Integer(count),
        ]));
    }

    Ok(RespValue::Array(replies))
}

/// UNSUBSCRIBE [channel [channel ...]]
/// 返回一个数组，每个元素是一次确认的 ["unsubscribe", channel, total_subscriptions]。
fn unsubscribe(ctx: &CommandContext, args: &[Bytes]) -> KvdbResult<RespValue> {
    let channels: Option<Vec<String>> = if args.is_empty() {
        None
    } else {
        Some(
            args.iter()
                .map(|b| String::from_utf8_lossy(b).into_owned())
                .collect(),
        )
    };

    let removed = match &channels {
        Some(list) => ctx.pubsub.unsubscribe(ctx.client_id, Some(list)),
        None => ctx.pubsub.unsubscribe(ctx.client_id, None),
    };

    let mut replies = Vec::with_capacity(removed.len());
    for ch in removed {
        let count = ctx.pubsub.subscription_count(ctx.client_id) as i64;
        replies.push(RespValue::Array(vec![
            RespValue::BulkString(Some(Bytes::from("unsubscribe"))),
            RespValue::BulkString(Some(Bytes::from(ch))),
            RespValue::Integer(count),
        ]));
    }

    Ok(RespValue::Array(replies))
}

/// PUBLISH channel message
/// 返回收到消息的订阅者数量。
fn publish(ctx: &CommandContext, args: &[Bytes]) -> KvdbResult<RespValue> {
    expect_min_arg_count("PUBLISH", args, 2)?;
    let channel = String::from_utf8_lossy(&args[0]);
    let message = args[1].clone();
    let count = ctx.pubsub.publish(&channel, message);
    Ok(RespValue::Integer(count as i64))
}
