# Agent Notes

`kvdb.rs` 是一个基于 RocksDB 构建的动态可配置高性能键值数据库，以 Rust 语言实现并兼容 Redis 协议（RESP2/RESP3）。它参考 kvrocks 的架构优化，将 RocksDB 的 LSM-Tree 持久化能力与 Redis 的丰富数据结构及高性能网络模型结合，同时引入运行时热更新配置系统。

目标是以纯 Rust 构建小巧、可读、生产级的代码库，仅在 RocksDB 与 Lua 引擎处通过 C FFI 接入外部依赖。

## 定位与目标

- 面向高吞吐、低延迟、持久化的 KV 存储场景，单机或小型集群部署。
- 以 RocksDB 列族隔离不同 Redis 数据类型（String / Hash / List / Set / ZSet / Stream / Bitmap），共享 WAL 与 Compaction 资源。
- 兼容 Redis 协议：支持 RESP2/RESP3、事务、Lua 脚本、Pub/Sub、主从复制与集群模式。
- **动态可配置**：分层配置模型（Hardcoded Default → 配置文件 → 环境变量 → 运行时 API），支持热更新与校验回滚。
- 多线程异步架构：网络 I/O（Tokio）与命令执行线程池分离，避免阻塞客户端。
- 提供**嵌入式模式**与 **Server 模式**，两者共享同一存储格式与配置系统。

## 当前实现状态

### 已完成

- [x] 脚手架、CI/CD（`.github/workflows/ci.yml`）
- [x] 配置系统（`src/config.rs`）：分层加载、内存校验、热更新
- [x] 存储引擎（`src/storage.rs`）：RocksDB 封装、4 个列族
- [x] RESP2/RESP3 协议（`src/protocol.rs`）
- [x] 命令层（`src/cmd/`）：String、Hash、List、Set、ZSet、Bitmap、Admin、Lua、Pub/Sub、Cluster
- [x] TCP Server（`src/server.rs`）：连接管理、流水线、事务状态机
- [x] 线程池（`src/thread_pool.rs`）
- [x] 事务（MULTI/EXEC/DISCARD/WATCH）
- [x] Lua 脚本（EVAL/EVALSHA、脚本缓存、redis.call/pcall）
- [x] Pub/Sub（SUBSCRIBE/PSUBSCRIBE/PUBLISH/UNSUBSCRIBE）
- [x] 主从复制骨架（REPLICAOF/ROLE、角色与偏移量）
- [x] 集群骨架（16384 槽位、CLUSTER SLOTS/NODES/KEYSLOT）
- [x] HTTP 管理接口（/health、/config、/stats、/metrics）
- [x] 内置 Benchmark（embedded/tcp 模式、QPS/延迟分位点）
- [x] 100K 读写门控系统测试
- [x] 八层测试文件覆盖核心路径
- [x] README.md 与 AGENT.md

### 后续增强

- [ ] 全量/增量复制同步（Checkpoint + WAL tailing）
- [ ] 集群槽位迁移与节点间协议
- [ ] ACL / 配置热更新持久化到磁盘
- [ ] Stream 完整命令集
- [ ] Sentinel 自动故障转移
- [ ] Geo / JSON 数据类型
- [ ] 默认 Web 管理页面（`src/web/`）

## 代码布局

- `storage.rs`: RocksDB 核心封装、Column Family 生命周期管理、WAL 配置。
- `config.rs`: 动态配置系统，分层合并、热更新通道、校验器、回滚。
- `cmd/`: Redis 命令实现目录，按数据类型分文件。
- `protocol.rs`: RESP2/RESP3 协议解析器与序列化器，支持流水线与部分解析。
- `server.rs`: TCP 服务器，Tokio 异步运行时、连接管理、事务状态机。
- `replication.rs`: 主从复制逻辑骨架。
- `cluster.rs`: 集群模式骨架，16384 槽位映射。
- `transaction.rs`: MULTI / EXEC / DISCARD / WATCH 实现。
- `lua.rs`: Lua 脚本引擎封装（mlua），沙箱、脚本缓存、命令路由。
- `pubsub.rs`: 发布订阅实现，基于内存频道。
- `thread_pool.rs`: 固定工作线程池。
- `metrics.rs`: Prometheus 指标收集。
- `http_server.rs`: HTTP 管理接口。
- `benchmark.rs`: 内置性能基准测试框架。
- `tests/`: 八层测试体系。
- `.github/workflows/ci.yml`: GitHub Actions CI。

## 质量规则

- 在涉及 RocksDB 列族创建/删除、WAL 写入边界、MemTable Flush、Compaction 策略选择、SST 元数据读取路径上，必须添加紧凑中文注释，解释一致性边界、内存策略与持久化保证。
- 在涉及配置分层合并、热更新冲突解决、校验失败回滚的代码中，中文注释应解释"优先级与回退策略"，而非仅描述代码行为。
- 在涉及 RESP 协议解析、命令分发路由、事务状态机、Lua 脚本沙箱、主从复制偏移量管理的代码中，中文注释必须说明执行顺序、原子性边界与内存所有权。
- 保持公共 API 窄化：CLI/Server 代码不应感知 RocksDB 内部 SST 文件布局、MemTable 跳表结构或 Compaction 策略实现细节。
- 不在核心读写路径引入永久性的运行时语义分支。
- 不引入 C++；RocksDB 与 Lua 依赖通过 Rust FFI crate（`rust-rocksdb`、`mlua`）解决。
- 修改功能时必须同步更新 README.md 与 AGENT.md。

## 安全与资源约束

- `block_cache_size + write_buffer_size × max_write_buffer_number + 索引/过滤器开销 ≤ 系统物理内存 50%`。
- 在 < 8GB 内存设备上自动下调 Block Cache 至 256MB。
- key/value 大小上限 512MB；协议层对大请求返回 OOM 错误。
- WAL 大小限制默认 64MB。
- 配置热更新时涉及资源上限的变更需通过校验层，拒绝可能导致 OOM 或资源枯竭的参数组合。

## 开发流程

1. 每次新增模块后执行：
   ```bash
   cargo build --bins
   cargo test --test smoke
   cargo fmt --check
   cargo clippy -- -D warnings
   ```
2. 完成功能后执行完整验证：
   ```bash
   cargo test --all-targets
   cargo clippy --all-targets -- -D warnings
   cargo fmt --check
   ```
3. 同步更新 README.md 与 AGENT.md。

## 生产部署 Review

- 使用 `cargo build --release`。
- `db_path` 与 `wal_dir` 建议分离到独立高性能磁盘。
- 确认内存预算 ≤ 物理内存 50%。
- 启用 `level_compaction_dynamic_level_bytes` 与 `enable_pipelined_write`。
- 设置 `maxclients` 与 `timeout` 防止连接泄漏。
- 部署前执行 `cargo test --test smoke`。
