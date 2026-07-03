# kvdb.rs 引擎实现计划

## 1. 摘要

本计划按 `agents.md` 的完整规格，从零开始实现 `kvdb.rs`：一个基于 RocksDB、兼容 Redis 协议（RESP2/RESP3）、支持动态配置热更新、主从复制与集群模式的 Rust 键值数据库。交付形态同时包含 **Server 模式**（TCP/Unix Socket/TLS + HTTP 管理接口）与 **嵌入式模式**（Rust Library API），两者共享同一存储格式与配置系统。

实施顺序遵循“存储与配置 → 协议与命令 → 网络与运行时 → 高级特性 → 测试与 CI”的依赖关系，优先保证核心读写路径可用，再逐步叠加事务、Lua、Pub/Sub、复制、集群等能力。

## 2. 当前状态分析

- 仓库仅包含两个文档文件：
  - `agents.md`：完整的产品与架构规格。
  - `todos.md`：当前唯一待办 `生成kvdb引擎；`。
- 没有 `Cargo.toml`、源代码、测试或 CI 配置。
- 所有模块均需新建，且模块间存在明显依赖：
  - `cmd/` 依赖 `storage` + `config` + `protocol` + `transaction`。
  - `server` 依赖 `protocol` + `cmd` + `thread_pool` + `metrics`。
  - `replication` / `cluster` 依赖 `storage` + `server` 的底层原语。
- 因此第一阶段必须先建立可独立验证的存储与配置核心。

## 3. 项目布局

采用单 crate + 多 binary 结构：

```text
kvdb.rs/
├── Cargo.toml
├── .github/workflows/ci.yml
├── README.md
├── AGENT.md（随实现同步更新）
├── src/
│   ├── lib.rs                 # 嵌入式公共 API 入口
│   ├── main.rs                # Server / CLI 入口（kvdb）
│   ├── cli.rs                 # 命令行解析、配置加载、子命令分发
│   ├── config.rs              # 分层配置 + 热更新 + 校验 + 回滚
│   ├── storage.rs             # RocksDB 封装、列族、WAL、Checkpoint、迭代器
│   ├── protocol.rs            # RESP2/RESP3 解析与序列化
│   ├── server.rs              # TCP/UDS/TLS 服务、连接管理、流水线
│   ├── thread_pool.rs         # 固定工作线程池
│   ├── transaction.rs         # MULTI/EXEC/DISCARD/WATCH
│   ├── lua.rs                 # Lua 脚本引擎（mlua 沙箱）
│   ├── pubsub.rs              # 发布订阅内存频道
│   ├── replication.rs         # 主从复制（Checkpoint + WAL tailing）
│   ├── cluster.rs             # 16384 槽位集群
│   ├── metrics.rs             # Prometheus 指标
│   ├── http_server.rs         # HTTP 管理接口
│   ├── benchmark.rs           # 内置基准框架（lib）
│   ├── bin/
│   │   └── benchmark.rs       # kvdb-benchmark binary
│   ├── cmd/                   # Redis 命令实现
│   │   ├── mod.rs
│   │   ├── string.rs
│   │   ├── hash.rs
│   │   ├── list.rs
│   │   ├── set.rs
│   │   ├── zset.rs
│   │   ├── stream.rs
│   │   ├── bitmap.rs
│   │   └── admin.rs
│   └── web/                   # 管理页面静态资源
│       ├── index.html
│       ├── app.js
│       └── style.css
└── tests/
    ├── unit.rs
    ├── integration.rs
    ├── smoke.rs
    ├── regression.rs
    ├── acceptance.rs
    ├── system.rs
    ├── e2e.rs
    └── server.rs
```

## 4. 详细实施步骤

### 4.1 脚手架与依赖

**目标文件**：
- `Cargo.toml`
- `src/lib.rs`
- `src/main.rs`
- `src/bin/benchmark.rs`
- `.github/workflows/ci.yml`

**内容**：
- 包名使用 `kvdb-rs`（crate 名 `kvdb_rs`），Rust edition 2024，最低 Rust 版本 1.85。
- 提供 `lib`（嵌入式 API）与两个 `bin`：`kvdb`（server/cli）、`kvdb-benchmark`。
- 核心依赖：
  - `rocksdb = "0.23"`：RocksDB FFI。
  - `mlua = { version = "0.10", features = ["lua54", "send"] }`：Lua 脚本。
  - `tokio = { version = "1", features = ["full"] }`：异步运行时。
  - `bytes = "1"`：零拷贝网络缓冲。
  - `serde = { version = "1", features = ["derive"] }` + `toml = "0.8"` + `serde_yaml = "0.9"`：配置序列化。
  - `clap = { version = "4", features = ["derive"] }`：CLI。
  - `tracing = "0.1"` + `tracing-subscriber = "0.3"`：日志。
  - `thiserror = "2"` + `anyhow = "1"`：错误处理。
  - `parking_lot = "0.12"` + `dashmap = "6"`：并发原语。
  - `axum = "0.7"` + `tower = "0.5"`：HTTP 管理接口。
  - `rustls = "0.23"` + `tokio-rustls = "0.26"` + `rustls-pemfile = "2"`：TLS（可选）。
  - `prometheus = "0.13"`：指标。
  - `crc16 = "0.8"`：Cluster 槽位计算。
  - `rand = "0.8"`、`itertools = "0.13"`、`chrono = "0.4"`、`regex = "1"`、`sha1_smol`/`sha1`：工具。
- `src/lib.rs` 暴露最小公共 API：`KvdbResult`、`KvdbError`、`EmbeddedDb`、`open_embedded`、`Config`。
- `src/main.rs` 调用 `cli::run()`。
- CI 矩阵：Ubuntu / macOS / Windows，执行 `cargo build --bins`、`cargo test`、`cargo fmt --check`、`cargo clippy -- -D warnings`、上传产物。

### 4.2 配置系统

**目标文件**：`src/config.rs`

**核心结构**：

```rust
pub struct Config {
    pub server: ServerConfig,
    pub storage: StorageConfig,
    pub log_level: String,
    pub dynamic_config: bool,
}

pub struct ConfigManager {
    current: Arc<RwLock<Config>>,
    tx: watch::Sender<Config>,
    validators: Vec<Box<dyn ConfigValidator>>,
}
```

**实现要点**：
- 默认值硬编码（与 `agents.md` 一致）。
- 加载顺序：Hardcoded → 配置文件（TOML/YAML）→ 环境变量（`KVDB_` 前缀）→ 运行时 API。
- 热更新使用 `tokio::sync::watch`，变更前通过校验器；校验失败时回滚并返回错误，原配置继续运行。
- 提供 `get()`、`update(new: Config)`、`subscribe()` 三个公共方法。
- 资源安全校验：
  - `block_cache_size + write_buffer_size * max_write_buffer_number <= 物理内存 50%`。
  - 内存 < 8GB 时自动下调 `block_cache_size` 到 256MB。
  - `maxclients` 不超过系统 fd 限制。
  - key/value 大小上限 512MB。

### 4.3 存储引擎

**目标文件**：`src/storage.rs`

**核心结构**：

```rust
pub struct StorageEngine {
    db: Arc<DB>,
    cf_handles: HashMap<String, ColumnFamily>,
    options: Options,
    path: PathBuf,
}

pub enum DataType {
    String, Hash, List, Set, ZSet, Stream, Bitmap,
}
```

**实现要点**：
- 列族列表：`default`、`string`、`hash`、`list`、`set`、`zset`、`stream`、`bitmap`、`metadata`。
- 打开时若列族不存在则创建；使用共享 `Cache` 与统一 `BlockBasedOptions`。
- 提供 `get(cf, key)`、`put(cf, key, value)`、`delete(cf, key)`、`write(batch)`、`prefix_scan(cf, prefix)`、`checkpoint(path)`、`repair(path)`。
- 封装 `Iterator` 为 `KvdbIterator`，处理生命周期与列族引用。
- 在列族创建/删除、WAL 写入、Flush、Compaction 策略选择处加中文注释说明一致性边界与持久化保证。
- 内存预算控制：计算 `block_cache_size + write_buffer_size * max_write_buffer_number` 并在启动时告警/拒绝。

### 4.4 RESP 协议

**目标文件**：`src/protocol.rs`

**核心类型**：

```rust
pub enum RespValue {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(Option<Bytes>),
    Array(Vec<RespValue>),
    Null,
    Boolean(bool),
    Double(f64),
    Map(Vec<(RespValue, RespValue)>),
    Set(Vec<RespValue>),          // RESP3
}

pub struct RespParser;
pub struct RespSerializer;
```

**实现要点**：
- 支持 RESP2/RESP3 的完整解析与序列化。
- 实现流式/部分解析，使用 `BytesMut` 缓冲，大参数尽量引用原始缓冲区减少拷贝。
- 命令请求统一解析为 `Array of BulkString`；错误统一格式化为 `-ERR message\r\n`。
- 提供 `parse_cmd(buffer) -> Option<(Vec<Bytes>, usize)>` 以支持流水线。

### 4.5 命令层

**目标文件**：`src/cmd/mod.rs` 与 `src/cmd/*.rs`

**核心结构**：

```rust
pub struct CommandContext {
    pub storage: Arc<StorageEngine>,
    pub config: Arc<ConfigManager>,
    pub tx_pool: ThreadPool,
    pub client: ClientState,
}

pub type CommandFn = fn(&CommandContext, &[Bytes]) -> KvdbResult<RespValue>;

pub struct CommandTable {
    table: HashMap<String, CommandFn>,
}
```

**实现要点**：
- `cmd/mod.rs` 注册所有命令，提供 `dispatch(cmd_name, args) -> RespValue`。
- 按数据类型分文件实现命令，先实现 `string` 与 `admin`（优先级最高），再实现 `hash`、`list`、`set`、`zset`、`bitmap`、`stream`。
- 命令签名全部接收 `&[Bytes]` 与 `CommandContext`，返回 `RespValue`。
- 在命令分发与执行路径加中文注释说明执行顺序、原子性边界与内存所有权。
- String 命令：GET/SET/MGET/MSET/DEL/EXISTS/INCR/DECR/APPEND。
- Admin 命令：INFO/CONFIG/FLUSHDB/FLUSHALL/SAVE/BGSAVE/LASTSAVE/DBSIZE/PING/ECHO。

### 4.6 线程池与运行时

**目标文件**：`src/thread_pool.rs`

**核心结构**：

```rust
pub struct ThreadPool {
    sender: Sender<Job>,
    threads: Vec<JoinHandle<()>>,
}
```

**实现要点**：
- 固定大小线程池，执行阻塞型 RocksDB 命令，避免阻塞 Tokio I/O 任务。
- 提供 `spawn<F>(&self, f: F) -> JoinHandle<R>`，其中 `F: FnOnce() -> R + Send + 'static`。
- 命令执行流程：Server 在 I/O 任务中解析请求 → 提交到线程池 → 返回 Future → 序列化响应写回客户端。

### 4.7 Server

**目标文件**：`src/server.rs`

**核心结构**：

```rust
pub struct Server {
    config: Arc<ConfigManager>,
    storage: Arc<StorageEngine>,
    listener: TcpListener,
    uds_listener: Option<UnixListener>,
}

pub struct ClientState {
    addr: SocketAddr,
    db_index: usize,
    transaction: Option<TransactionState>,
    subscribed_channels: Vec<String>,
}
```

**实现要点**：
- 监听 TCP（默认 `127.0.0.1:6379`）与可选 Unix Domain Socket。
- 每个连接一个 Tokio 任务，使用 `BufReader`/`BufWriter`；支持流水线批量解析。
- 大请求（key/value > 512MB）直接返回 OOM 错误并断开连接。
- 支持 `maxclients` 连接限流与空闲超时。
- TLS 可选：通过 `rustls` 动态加载证书/私钥。
- 在协议解析与命令分发路径加中文注释说明执行顺序、原子性边界与内存所有权。

### 4.8 事务

**目标文件**：`src/transaction.rs`

**核心结构**：

```rust
pub struct TransactionState {
    queue: Vec<(String, Vec<Bytes>)>,
    watched: HashSet<Vec<u8>>,
}
```

**实现要点**：
- 支持 MULTI/EXEC/DISCARD/WATCH/UNWATCH。
- 事务期间命令入队，EXEC 时使用 RocksDB `TransactionDB` 或乐观事务批量执行；任一失败回滚全部写操作。
- WATCH 基于 RocksDB Sequence Number 或读取时记录版本号，EXEC 前检查是否被修改。

### 4.9 Lua 脚本

**目标文件**：`src/lua.rs`

**核心结构**：

```rust
pub struct LuaEngine {
    vm: Lua,
    script_cache: Arc<DashMap<String, String>>, // sha -> script
}
```

**实现要点**：
- 使用 `mlua` 创建 Lua 5.4 状态机，禁用危险库（`os`、`io`、`debug`、`loadfile` 等）。
- 注册 `redis.call` 回调，路由到命令表。
- 限制脚本执行时间（通过 `set_hook` 或超时 Future）与内存。
- 提供 `eval(script, keys, args)` 与 `evalsha(sha, keys, args)`。

### 4.10 发布订阅

**目标文件**：`src/pubsub.rs`

**核心结构**：

```rust
pub struct PubSubHub {
    channels: DashMap<String, Vec<Sender<Bytes>>>,
}
```

**实现要点**：
- 基于内存 `tokio::sync::mpsc` 频道，不持久化。
- 支持 SUBSCRIBE/UNSUBSCRIBE/PSUBSCRIBE/PUNSUBSCRIBE/PUBLISH。
- 订阅客户端进入只读推送模式，普通命令返回错误。

### 4.11 主从复制

**目标文件**：`src/replication.rs`

**核心结构**：

```rust
pub struct ReplicationMaster {
    replicas: DashMap<String, ReplicaConn>,
    latest_seq: AtomicU64,
}

pub struct ReplicationSlave {
    master_addr: String,
}
```

**实现要点**：
- 主节点维护复制偏移量（对齐 RocksDB Sequence Number）。
- 全量同步：创建 Checkpoint 并传输；增量同步：通过 `get_updates_since` 或迭代器 tailing WAL。
- 从节点接收快照后持续应用增量写操作。
- 复制路径加中文注释说明偏移量管理与崩溃恢复连续性。

### 4.12 集群

**目标文件**：`src/cluster.rs`

**核心结构**：

```rust
pub struct ClusterState {
    slots: [Slot; 16384],
    nodes: DashMap<String, Node>,
    myself: String,
}
```

**实现要点**：
- 使用 CRC16 mod 16384 计算 key 槽位。
- 实现 CLUSTER NODES/CLUSTER SLOTS/MIGRATE 等基础命令。
- 节点间二进制协议用于槽位迁移与配置传播；先实现手动迁移，自动故障转移标记为后续增强。

### 4.13 指标与 HTTP 管理接口

**目标文件**：`src/metrics.rs`、`src/http_server.rs`

**实现要点**：
- `metrics.rs` 使用 `prometheus` crate 收集 QPS、延迟分位点、RocksDB 属性、配置变更次数。
- `http_server.rs` 使用 `axum` 暴露：
  - `GET /metrics`：Prometheus 文本。
  - `GET /config`：当前配置 JSON。
  - `PUT /config`：热更新配置（校验后应用）。
  - `GET /stats`：RocksDB 内部统计。
  - `GET /health`：健康检查。
  - `GET /`：返回 `src/web/index.html`，JS/CSS 通过 `include_str!` 嵌入。

### 4.14 CLI 与 Benchmark

**目标文件**：`src/cli.rs`、`src/benchmark.rs`、`src/bin/benchmark.rs`

**实现要点**：
- `cli.rs` 提供子命令：
  - `server`：启动服务。
  - `embedded`：进入嵌入式交互（可选）。
  - `compact`、`repair`：RocksDB 工具。
- `benchmark.rs` 实现内部基准框架，覆盖 SET/GET/HSET/HGET/LPUSH/LRANGE/ZADD/ZRANGE，输出 QPS 与 p50/p99/p999 延迟。
- `src/bin/benchmark.rs` 调用该框架并解析命令行参数。

### 4.15 测试体系

**目标文件**：`tests/*.rs`

**实现要点**：
- 按 `agents.md` 八层测试体系覆盖，实际按命令/特性拆分为多个测试文件，便于并行执行与定位失败。
- 每个测试使用临时目录（`tempfile` crate）避免污染工作区。
- Smoke 测试在关键路径打印 `tracing::info!("[SMOKE] ...")`。
- 覆盖率目标：每行生产代码至少被一层测试覆盖；未覆盖路径以 `// untested:` 中文注释说明。
- 当前测试文件：
  1. `smoke.rs`：启动服务器、SET/GET、停止服务器。
  2. `cmd_hash.rs`：Hash 命令正确性。
  3. `cmd_list.rs`：List 命令正确性。
  4. `cmd_set.rs`：Set 命令正确性。
  5. `cmd_zset.rs`：ZSet 命令正确性。
  6. `cmd_bitmap.rs`：Bitmap 命令正确性。
  7. `transactions.rs`：MULTI/EXEC/DISCARD/WATCH 隔离。
  8. `lua_eval.rs`：EVAL/EVALSHA 脚本与缓存。
  9. `pubsub.rs`：SUBSCRIBE/PUBLISH/PSUBSCRIBE。
  10. `http_api.rs`：/health、/config、/stats、/metrics 管理接口。
  11. `replication_cluster.rs`：REPLICAOF/ROLE、CLUSTER SLOTS/NODES/KEYSLOT。
- 后续补充：`system.rs`（10万键读写门控、内存 bounded、Compaction 稳定性）、`e2e.rs`（RESP/TCP/HTTP/redis-benchmark 兼容性）。

### 4.16 文档同步

**目标文件**：`README.md`、`AGENT.md`

**实现要点**：
- `README.md`：快速开始、架构说明、测试命令、基准测试、生产部署、API 端点、CI/CD 概览。
- `AGENT.md`：复用/更新 `agents.md`，作为项目代理指令源文件。
- 每完成一个里程碑同步更新文档，避免文档与代码脱节。

## 5. 关键决策与假设

1. **单 crate 结构**：优先降低跨 crate 协调成本；未来若集群/复制模块膨胀再拆分为 workspace。
2. **RocksDB 通过 `rust-rocksdb` crate 接入**：不引入 C++ 源码，符合 `agents.md` 质量规则。
3. **命令执行使用线程池而非 async RocksDB**：RocksDB C API 会阻塞，必须隔离到独立线程，避免 Tokio 事件循环阻塞。
4. **热更新采用 `watch` 通道 + 校验器**：不涉及持久化配置到磁盘；运行时 API 更新仅影响内存状态，持久化由调用方决定是否写回配置文件。
5. **Server 与嵌入式共享 `StorageEngine` 与 `ConfigManager`**：Server 在启动时组装这些组件，嵌入式 API 直接返回组装后的句柄。
6. **集群自动故障转移与 Sentinel 不在本次落地**：保留接口，手动迁移与主从复制先可用。
7. **TLS 默认关闭**：仅在配置提供证书时启用，降低默认构建复杂度。
8. **默认压缩**：LZ4 + Zstd，与 `agents.md` 一致；若目标平台不支持则在配置中允许回退到 snappy/none。

## 6. 验证步骤

1. 每次新增模块后执行：
   ```bash
   cargo build --bins
   cargo test --test unit
   cargo fmt --check
   cargo clippy -- -D warnings
   ```
2. 核心 Server 可用后执行：
   ```bash
   cargo run --bin kvdb -- server --config ./kvdb.toml
   redis-cli -p 6379 SET foo bar
   redis-cli -p 6379 GET foo
   cargo test --test smoke
   ```
3. 完整命令集实现后执行：
   ```bash
   cargo test
   cargo run --bin kvdb-benchmark -- --commands SET,GET --clients 64 --requests 100000
   ```
4. 配置热更新验证：
   ```bash
   curl -X PUT http://127.0.0.1:8080/config -H 'Content-Type: application/json' \
     -d '{"storage":{"block_cache_size":268435456}}'
   curl http://127.0.0.1:8080/config
   ```
5. CI 验证：推送至 GitHub，确认 GitHub Actions 在 Ubuntu / macOS / Windows 上全部通过。

## 7. 里程碑建议

| 里程碑 | 内容 | 验收标准 |
| --- | --- | --- |
| M1 | 脚手架 + 配置 + 存储 + RESP | `cargo build` 通过；storage 能读写；RESP 能解析 SET/GET |
| M2 | String/Admin 命令 + Server + Smoke 测试 | `redis-cli SET/GET` 成功；`cargo test --test smoke` 通过 |
| M3 | Hash/List/Set/ZSet/Bitmap + 单元/集成测试 | 八层测试中 unit/integration 通过 |
| M4 | 事务 + Lua + Pub/Sub + HTTP 接口 | E2E 覆盖事务脚本与订阅发布 |
| M5 | 主从复制 + 集群 + 完整测试 | system/e2e/server 全通过；benchmark 可运行 |
| M6 | 文档、CI、性能调优 | README/AGENT.md 更新；CI 绿；满足 100K 读写门控 |
