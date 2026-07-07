use bytes::{Buf, Bytes, BytesMut};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use crate::cluster::ClusterState;
use crate::cmd::{ClientState, CommandContext, CommandTable};
use crate::config::ConfigManager;

use crate::lua::LuaEngine;
use crate::protocol::{RespParser, RespSerializer, RespValue};
use crate::pubsub::PubSubHub;
use crate::replication::ReplicationState;
use crate::storage::StorageEngine;
use crate::thread_pool::ThreadPool;

/// TCP/RESP 服务器：每个连接独立任务，支持流水线批量解析、Pub/Sub 推送与事务队列。
pub struct Server {
    config: Arc<ConfigManager>,
    storage: Arc<StorageEngine>,
    listener: TcpListener,
    table: Arc<CommandTable>,
    thread_pool: ThreadPool,
    pubsub: Arc<PubSubHub>,
    lua: Arc<LuaEngine>,
    replication: Arc<ReplicationState>,
    cluster: Arc<ClusterState>,
}

impl Server {
    pub async fn bind(
        config: Arc<ConfigManager>,
        storage: Arc<StorageEngine>,
    ) -> crate::error::KvdbResult<Self> {
        let cfg = config.get();
        let listener = TcpListener::bind(&cfg.server.bind).await?;
        let table = Arc::new(CommandTable::new());
        let lua = Arc::new(LuaEngine::new(Arc::clone(&table))?);
        Ok(Self {
            config,
            storage,
            listener,
            table,
            thread_pool: ThreadPool::new(
                std::thread::available_parallelism()
                    .map(|n| n.get())
                    .unwrap_or(2),
            ),
            pubsub: Arc::new(PubSubHub::new()),
            lua,
            replication: ReplicationState::new(),
            cluster: ClusterState::new(),
        })
    }

    pub fn local_addr(&self) -> std::io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    pub async fn run(self) -> anyhow::Result<()> {
        let addr = self.listener.local_addr()?;
        tracing::info!("kvdb listening on {}", addr);
        loop {
            let (stream, peer) = self.listener.accept().await?;
            let (pubsub_tx, pubsub_rx) = tokio::sync::mpsc::unbounded_channel();
            let client_id = self.pubsub.alloc_client_id();
            // 每个连接拥有独立的 CommandContext，但共享线程池、命令表与 Pub/Sub 中心。
            let namespace = Bytes::from(self.config.get().server.namespace);
            let ctx = CommandContext {
                storage: Arc::clone(&self.storage),
                config: Arc::clone(&self.config),
                tx_pool: self.thread_pool.clone(),
                client: ClientState::default(),
                pubsub: Arc::clone(&self.pubsub),
                pubsub_tx,
                client_id,
                lua: Arc::clone(&self.lua),
                replication: Arc::clone(&self.replication),
                cluster: Arc::clone(&self.cluster),
                namespace,
            };
            let table = Arc::clone(&self.table);
            tokio::spawn(async move {
                if let Err(e) = handle_connection(stream, peer, ctx, table, pubsub_rx).await {
                    tracing::debug!("connection {} closed: {}", peer, e);
                }
            });
        }
    }
}

/// 单个连接的事务状态：MULTI 命令队列与被监控键快照。
#[derive(Default)]
struct TransactionState {
    in_multi: bool,
    queued: Vec<(Bytes, Vec<Bytes>)>,
    watched: Vec<(Bytes, Option<Vec<u8>>)>,
}

impl TransactionState {
    fn reset(&mut self) {
        self.in_multi = false;
        self.queued.clear();
        // WATCH 快照在 EXEC/DISCARD 后清除（Redis 语义）。
        self.watched.clear();
    }
}

async fn handle_connection(
    mut stream: TcpStream,
    _peer: SocketAddr,
    ctx: CommandContext,
    table: Arc<CommandTable>,
    mut pubsub_rx: tokio::sync::mpsc::UnboundedReceiver<crate::pubsub::PublishEvent>,
) -> anyhow::Result<()> {
    let (reader, mut writer) = stream.split();
    let mut reader = tokio::io::BufReader::new(reader);
    let mut buf = BytesMut::with_capacity(4096);
    let mut tx_state = TransactionState::default();

    loop {
        // 先尝试从缓冲区解析完整命令；若数据不足则继续读取。
        if let Some((mut args, consumed)) = RespParser::parse_cmd(&mut buf) {
            buf.advance(consumed);
            if args.is_empty() {
                write_reply(
                    &mut writer,
                    RespValue::Error("ERR empty command".to_string()),
                )
                .await?;
                continue;
            }
            let cmd = args.remove(0);
            let cmd_upper = String::from_utf8_lossy(&cmd).to_ascii_uppercase();

            // 事务控制命令直接在 I/O 任务中处理，避免入队与监控状态竞争。
            match cmd_upper.as_str() {
                "MULTI" => {
                    if tx_state.in_multi {
                        write_reply(
                            &mut writer,
                            RespValue::Error("ERR MULTI calls can not be nested".to_string()),
                        )
                        .await?;
                    } else {
                        tx_state.in_multi = true;
                        write_reply(&mut writer, RespValue::SimpleString("OK".to_string())).await?;
                    }
                    continue;
                }
                "DISCARD" => {
                    if !tx_state.in_multi {
                        write_reply(
                            &mut writer,
                            RespValue::Error("ERR DISCARD without MULTI".to_string()),
                        )
                        .await?;
                    } else {
                        tx_state.reset();
                        write_reply(&mut writer, RespValue::SimpleString("OK".to_string())).await?;
                    }
                    continue;
                }
                "WATCH" => {
                    if tx_state.in_multi {
                        write_reply(
                            &mut writer,
                            RespValue::Error("ERR WATCH inside MULTI is not allowed".to_string()),
                        )
                        .await?;
                    } else {
                        let keys: Vec<Bytes> = args.to_vec();
                        tx_state.watched = watch_keys(&ctx, &keys).await?;
                        write_reply(&mut writer, RespValue::SimpleString("OK".to_string())).await?;
                    }
                    continue;
                }
                "EXEC" => {
                    if !tx_state.in_multi {
                        write_reply(
                            &mut writer,
                            RespValue::Error("ERR EXEC without MULTI".to_string()),
                        )
                        .await?;
                        continue;
                    }
                    if watched_keys_changed(&ctx, &tx_state.watched).await? {
                        tx_state.reset();
                        write_reply(&mut writer, RespValue::Null).await?;
                        continue;
                    }
                    let queued = std::mem::take(&mut tx_state.queued);
                    tx_state.reset();
                    let replies = execute_queue(&ctx, &table, queued).await?;
                    write_reply(&mut writer, RespValue::Array(replies)).await?;
                    continue;
                }
                _ => {}
            }

            // 在 MULTI 状态下，普通命令入队并返回 QUEUED。
            if tx_state.in_multi {
                tx_state.queued.push((cmd, args));
                write_reply(&mut writer, RespValue::SimpleString("QUEUED".to_string())).await?;
                continue;
            }

            // 在独立线程中执行阻塞型命令，避免阻塞 I/O 任务。
            let table = Arc::clone(&table);
            let ctx = ctx.clone();
            let tx_pool = ctx.tx_pool.clone();
            let mut reply_fut = tx_pool.spawn(move || table.dispatch(&ctx, &cmd, &args));

            // 等待命令回复，期间仍可接收并推送 Pub/Sub 消息。
            loop {
                tokio::select! {
                    reply = &mut reply_fut => {
                        let reply = reply.unwrap_or_else(|_| {
                            RespValue::Error("ERR command aborted".to_string())
                        });
                        write_reply(&mut writer, reply).await?;
                        break;
                    }
                    Some(event) = pubsub_rx.recv() => {
                        let msg = pubsub_message(&event.channel, event.message);
                        write_reply(&mut writer, msg).await?;
                    }
                }
            }
        } else {
            tokio::select! {
                n = reader.read_buf(&mut buf) => {
                    if n? == 0 {
                        break; // 客户端断开
                    }
                }
                Some(event) = pubsub_rx.recv() => {
                    let msg = pubsub_message(&event.channel, event.message);
                    write_reply(&mut writer, msg).await?;
                }
            }
        }
    }
    Ok(())
}

/// 对给定用户键列表建立 WATCH 快照：存储 metadata 列族当前值（或 None）。
/// 使用 get_meta 以兼容旧格式（无 namespace）数据。
async fn watch_keys(
    ctx: &CommandContext,
    keys: &[Bytes],
) -> anyhow::Result<Vec<(Bytes, Option<Vec<u8>>)>> {
    let keys = keys.to_vec();
    let ctx = ctx.clone();
    let pool = ctx.tx_pool.clone();
    pool.spawn(move || {
        keys.into_iter()
            .map(|key| {
                let value = ctx.get_meta(&key)?;
                Ok::<_, crate::error::KvdbError>((key, value))
            })
            .collect::<Result<Vec<_>, _>>()
    })
    .await
    .map_err(|e| anyhow::anyhow!("watch task aborted: {e}"))?
    .map_err(|e| anyhow::anyhow!("watch failed: {e}"))
}

/// 检查 WATCH 快照是否失效：任一被监控键的当前值与快照不一致即返回 true。
async fn watched_keys_changed(
    ctx: &CommandContext,
    watched: &[(Bytes, Option<Vec<u8>>)],
) -> anyhow::Result<bool> {
    if watched.is_empty() {
        return Ok(false);
    }
    let watched: Vec<(Bytes, Option<Vec<u8>>)> = watched.to_vec();
    let ctx = ctx.clone();
    let pool = ctx.tx_pool.clone();
    pool.spawn(move || {
        for (key, snapshot) in watched {
            let current = ctx.get_meta(&key)?;
            if current != snapshot {
                return Ok::<_, crate::error::KvdbError>(true);
            }
        }
        Ok(false)
    })
    .await
    .map_err(|e| anyhow::anyhow!("watch check task aborted: {e}"))?
    .map_err(|e| anyhow::anyhow!("watch check failed: {e}"))
}

/// 顺序执行事务队列中的命令，返回每条命令的回复。
async fn execute_queue(
    ctx: &CommandContext,
    table: &Arc<CommandTable>,
    queued: Vec<(Bytes, Vec<Bytes>)>,
) -> anyhow::Result<Vec<RespValue>> {
    let mut replies = Vec::with_capacity(queued.len());
    for (cmd, args) in queued {
        let table = Arc::clone(table);
        let ctx = ctx.clone();
        let tx_pool = ctx.tx_pool.clone();
        let reply = tx_pool
            .spawn(move || table.dispatch(&ctx, &cmd, &args))
            .await
            .unwrap_or_else(|_| RespValue::Error("ERR command aborted".to_string()));
        replies.push(reply);
    }
    Ok(replies)
}

fn pubsub_message(channel: &str, message: Bytes) -> RespValue {
    RespValue::Array(vec![
        RespValue::BulkString(Some(Bytes::from("message"))),
        RespValue::BulkString(Some(Bytes::from(channel.to_string()))),
        RespValue::BulkString(Some(message)),
    ])
}

async fn write_reply<W>(writer: &mut W, reply: RespValue) -> anyhow::Result<()>
where
    W: AsyncWriteExt + Unpin,
{
    writer.write_all(&RespSerializer::serialize(&reply)).await?;
    writer.flush().await?;
    Ok(())
}
