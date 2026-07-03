use bytes::Bytes;
use dashmap::DashMap;
use mlua::{AnyUserData, Lua, Table, UserData, Value};
use sha1::{Digest, Sha1};
use std::sync::Arc;

use crate::cmd::{CommandContext, CommandTable};
use crate::error::{KvdbError, KvdbResult};
use crate::protocol::RespValue;

/// Lua 脚本引擎：维护脚本缓存，并在脚本内提供 `redis.call` / `redis.pcall`。
pub struct LuaEngine {
    lua: Lua,
    scripts: Arc<DashMap<String, String>>,
}

impl LuaEngine {
    pub fn new(table: Arc<CommandTable>) -> KvdbResult<Self> {
        let lua = Lua::new();
        let scripts = Arc::new(DashMap::new());
        register_redis_api(&lua, table)?;
        Ok(Self { lua, scripts })
    }

    /// 执行 Lua 脚本。keys 与 args 通过 KEYS / ARGV 表暴露给脚本。
    pub fn eval(
        &self,
        script: &str,
        keys: Vec<Bytes>,
        args: Vec<Bytes>,
        ctx: &CommandContext,
    ) -> KvdbResult<RespValue> {
        let sha1 = sha1_hex(script);
        self.scripts.insert(sha1, script.to_string());
        self.run_script(script, keys, args, ctx)
    }

    /// 通过 SHA1 执行已缓存脚本；未缓存时返回错误。
    pub fn evalsha(
        &self,
        sha1: &str,
        keys: Vec<Bytes>,
        args: Vec<Bytes>,
        ctx: &CommandContext,
    ) -> KvdbResult<RespValue> {
        let script = self
            .scripts
            .get(sha1)
            .map(|entry| entry.clone())
            .ok_or_else(|| {
                KvdbError::Command("NOSCRIPT No matching script. Please use EVAL.".to_string())
            })?;
        self.run_script(&script, keys, args, ctx)
    }

    fn run_script(
        &self,
        script: &str,
        keys: Vec<Bytes>,
        args: Vec<Bytes>,
        ctx: &CommandContext,
    ) -> KvdbResult<RespValue> {
        let globals = self.lua.globals();
        globals.set("_KVDB_CTX", ContextHandle::new(ctx.clone()))?;
        globals.set("KEYS", bytes_vec_to_table(&self.lua, keys)?)?;
        globals.set("ARGV", bytes_vec_to_table(&self.lua, args)?)?;

        let chunk = self.lua.load(script);
        let value: Value = chunk.eval()?;
        resp_value_from_lua(value)
    }
}

fn sha1_hex(script: &str) -> String {
    let mut hasher = Sha1::new();
    hasher.update(script.as_bytes());
    hasher
        .finalize()
        .iter()
        .map(|b| format!("{:02x}", b))
        .collect()
}

fn bytes_vec_to_table(lua: &Lua, vec: Vec<Bytes>) -> mlua::Result<Table> {
    let table = lua.create_table()?;
    for (i, b) in vec.into_iter().enumerate() {
        table.set(i + 1, String::from_utf8_lossy(&b).into_owned())?;
    }
    Ok(table)
}

/// 包装 CommandContext，使其可作为 Lua 全局变量传递。
struct ContextHandle {
    ctx: CommandContext,
}

impl ContextHandle {
    fn new(ctx: CommandContext) -> Self {
        Self { ctx }
    }
}

impl UserData for ContextHandle {}

fn register_redis_api(lua: &Lua, table: Arc<CommandTable>) -> mlua::Result<()> {
    let redis = lua.create_table()?;

    let call_table = table.clone();
    let call: mlua::Function = lua.create_function(move |lua, params: mlua::Variadic<Value>| {
        let result = exec_redis_command(lua, &call_table, params);
        match result {
            Ok(value) => Ok(value),
            Err(e) => Err(mlua::Error::RuntimeError(format!("{e}"))),
        }
    })?;

    let pcall_table = table;
    let pcall: mlua::Function =
        lua.create_function(move |lua, params: mlua::Variadic<Value>| {
            match exec_redis_command(lua, &pcall_table, params) {
                Ok(value) => Ok(value),
                Err(e) => {
                    let err_table = lua.create_table()?;
                    err_table.set("err", format!("{e}"))?;
                    Ok(Value::Table(err_table))
                }
            }
        })?;

    redis.set("call", call)?;
    redis.set("pcall", pcall)?;
    lua.globals().set("redis", redis)?;
    Ok(())
}

fn exec_redis_command(
    lua: &Lua,
    table: &Arc<CommandTable>,
    params: mlua::Variadic<Value>,
) -> KvdbResult<Value> {
    let mut iter = params.into_iter();
    let cmd = match iter.next() {
        Some(Value::String(s)) => s.to_string_lossy().into_bytes(),
        Some(other) => {
            return Err(KvdbError::Command(format!(
                "Lua redis() command argument must be a string, got {:?}",
                other
            )));
        }
        None => return Err(KvdbError::WrongArgCount("redis")),
    };

    let args: Vec<Bytes> = iter
        .map(|v| match v {
            Value::String(s) => Bytes::from(s.to_string_lossy()),
            Value::Integer(n) => Bytes::from(n.to_string()),
            Value::Number(n) => Bytes::from(n.to_string()),
            _ => Bytes::new(),
        })
        .collect();

    let handle: AnyUserData = lua.globals().get("_KVDB_CTX")?;
    let ctx = handle.borrow::<ContextHandle>()?;
    let reply = table.dispatch(&ctx.ctx, &cmd, &args);
    lua_value_from_resp(lua, reply)
}

fn lua_value_from_resp(lua: &Lua, value: RespValue) -> KvdbResult<Value> {
    Ok(match value {
        RespValue::SimpleString(s) | RespValue::Error(s) => Value::String(lua.create_string(&s)?),
        RespValue::Integer(n) => Value::Integer(n),
        RespValue::BulkString(None) => Value::Nil,
        RespValue::BulkString(Some(b)) => Value::String(lua.create_string(&b)?),
        RespValue::Null => Value::Nil,
        RespValue::Boolean(b) => Value::Boolean(b),
        RespValue::Double(f) => Value::Number(f),
        RespValue::Array(items) => {
            let table = lua.create_table()?;
            for (i, item) in items.into_iter().enumerate() {
                table.set(i + 1, lua_value_from_resp(lua, item)?)?;
            }
            Value::Table(table)
        }
        RespValue::Map(map) => {
            let table = lua.create_table()?;
            for (k, v) in map {
                table.set(lua_value_from_resp(lua, k)?, lua_value_from_resp(lua, v)?)?;
            }
            Value::Table(table)
        }
        RespValue::Set(items) => {
            let table = lua.create_table()?;
            for (i, item) in items.into_iter().enumerate() {
                table.set(i + 1, lua_value_from_resp(lua, item)?)?;
            }
            Value::Table(table)
        }
    })
}

fn resp_value_from_lua(value: Value) -> KvdbResult<RespValue> {
    Ok(match value {
        Value::Nil => RespValue::Null,
        Value::Boolean(b) => RespValue::Boolean(b),
        Value::Integer(n) => RespValue::Integer(n),
        Value::Number(n) => RespValue::Double(n),
        Value::String(s) => RespValue::BulkString(Some(Bytes::from(s.to_string_lossy()))),
        Value::Table(table) => {
            // 若表存在整数键 1..n，则视为数组；否则视为 map。
            let mut map = Vec::new();
            let mut arr = Vec::new();
            let mut max_index = 0i64;
            for pair in table.pairs::<Value, Value>() {
                let (k, v) = pair?;
                if let Value::Integer(idx) = k {
                    if idx > 0 {
                        max_index = max_index.max(idx);
                        if arr.len() < idx as usize {
                            arr.resize(idx as usize, RespValue::Null);
                        }
                        arr[idx as usize - 1] = resp_value_from_lua(v)?;
                        continue;
                    }
                }
                map.push((resp_value_from_lua(k)?, resp_value_from_lua(v)?));
            }
            if map.is_empty() && max_index > 0 {
                RespValue::Array(arr)
            } else if !map.is_empty() {
                RespValue::Map(map)
            } else {
                RespValue::Array(vec![])
            }
        }
        _ => RespValue::Null,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::ClusterState;
    use crate::cmd::{ClientState, CommandTable};
    use crate::config::{Config, ConfigManager};
    use crate::protocol::RespValue;
    use crate::pubsub::PubSubHub;
    use crate::replication::ReplicationState;
    use crate::storage::StorageEngine;
    use crate::thread_pool::ThreadPool;
    use std::sync::Arc;
    use tokio::sync::mpsc;

    fn setup() -> (CommandContext, Arc<CommandTable>, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.storage.db_path = dir.path().join("db").to_string_lossy().to_string();
        let config = Arc::new(ConfigManager::new(config));
        let storage =
            Arc::new(StorageEngine::open(&config.get().storage.db_path, &config.get()).unwrap());
        let table = Arc::new(CommandTable::new());
        let lua = Arc::new(LuaEngine::new(Arc::clone(&table)).unwrap());
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
        };
        (ctx, table, dir)
    }

    #[test]
    fn lua_eval_set_get() {
        let (ctx, table, _dir) = setup();
        let engine = LuaEngine::new(table).unwrap();
        let script = "redis.call('SET', KEYS[1], ARGV[1]); return redis.call('GET', KEYS[1])";
        let result = engine
            .eval(script, vec![Bytes::from("k")], vec![Bytes::from("v")], &ctx)
            .unwrap();
        assert_eq!(result, RespValue::BulkString(Some(Bytes::from("v"))));
    }

    #[test]
    fn lua_evalsha_cache() {
        let (ctx, table, _dir) = setup();
        let engine = LuaEngine::new(table).unwrap();
        let script = "return ARGV[1]";
        let sha1 = sha1_hex(script);
        let _ = engine
            .eval(script, vec![], vec![Bytes::from("hello")], &ctx)
            .unwrap();
        let result = engine
            .evalsha(&sha1, vec![], vec![Bytes::from("hello")], &ctx)
            .unwrap();
        assert_eq!(result, RespValue::BulkString(Some(Bytes::from("hello"))));
    }
}
