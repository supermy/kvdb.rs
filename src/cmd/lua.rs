use bytes::Bytes;

use super::{CommandContext, CommandTable, expect_min_arg_count};
use crate::error::{KvdbError, KvdbResult};
use crate::protocol::RespValue;

pub fn register(table: &mut CommandTable) {
    table.register("EVAL", eval);
    table.register("EVALSHA", evalsha);
}

/// EVAL script numkeys key [key ...] arg [arg ...]
fn eval(ctx: &CommandContext, args: &[Bytes]) -> KvdbResult<RespValue> {
    expect_min_arg_count("EVAL", args, 2)?;
    let script = String::from_utf8_lossy(&args[0]);
    let numkeys = parse_numkeys(&args[1])?;
    if args.len() < 2 + numkeys {
        return Err(KvdbError::WrongArgCount("EVAL"));
    }
    let keys = args[2..2 + numkeys].to_vec();
    let argv = args[2 + numkeys..].to_vec();
    ctx.lua.eval(&script, keys, argv, ctx)
}

/// EVALSHA sha1 numkeys key [key ...] arg [arg ...]
fn evalsha(ctx: &CommandContext, args: &[Bytes]) -> KvdbResult<RespValue> {
    expect_min_arg_count("EVALSHA", args, 2)?;
    let sha1 = String::from_utf8_lossy(&args[0]);
    let numkeys = parse_numkeys(&args[1])?;
    if args.len() < 2 + numkeys {
        return Err(KvdbError::WrongArgCount("EVALSHA"));
    }
    let keys = args[2..2 + numkeys].to_vec();
    let argv = args[2 + numkeys..].to_vec();
    ctx.lua.evalsha(&sha1, keys, argv, ctx)
}

fn parse_numkeys(data: &[u8]) -> KvdbResult<usize> {
    std::str::from_utf8(data)
        .map_err(|_| KvdbError::Command("numkeys must be an integer".to_string()))?
        .parse::<usize>()
        .map_err(|_| KvdbError::Command("numkeys must be an integer".to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::ClusterState;
    use crate::cmd::{ClientState, CommandContext, CommandTable};
    use crate::config::{Config, ConfigManager};
    use crate::protocol::RespValue;
    use crate::pubsub::PubSubHub;
    use crate::replication::ReplicationState;
    use crate::storage::StorageEngine;
    use crate::thread_pool::ThreadPool;
    use bytes::Bytes;
    use std::sync::Arc;
    use tokio::sync::mpsc;

    fn setup() -> (CommandContext, CommandTable, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.storage.db_path = dir.path().join("db").to_string_lossy().to_string();
        let config = Arc::new(ConfigManager::new(config));
        let storage =
            Arc::new(StorageEngine::open(&config.get().storage.db_path, &config.get()).unwrap());
        let table = Arc::new(CommandTable::new());
        let lua = Arc::new(crate::lua::LuaEngine::new(Arc::clone(&table)).unwrap());
        let pubsub = Arc::new(PubSubHub::new());
        let (tx, _rx) = mpsc::unbounded_channel();
        let ctx = CommandContext {
            storage,
            config,
            tx_pool: ThreadPool::new(1),
            client: ClientState::default(),
            pubsub,
            pubsub_tx: tx,
            client_id: 0,
            lua,
            replication: ReplicationState::new(),
            cluster: ClusterState::new(),
            namespace: bytes::Bytes::new(),
        };
        (ctx, CommandTable::new(), dir)
    }

    #[test]
    fn eval_set_get() {
        let (ctx, _table, _dir) = setup();
        let reply = eval(
            &ctx,
            &[
                Bytes::from("return redis.call('GET', KEYS[1])"),
                Bytes::from("1"),
                Bytes::from("k"),
            ],
        )
        .unwrap();
        assert_eq!(reply, RespValue::Null);
    }
}
