# Changelog

所有显著变更均按里程碑记录。格式参考 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/)。

## [Unreleased]

### Added

- 新增 `CHANGELOG.md`，记录项目里程碑与关键变更。

## [0.1.0] - 2026-07-03

### Added

#### M1：脚手架 + 配置 + 存储 + RESP

- 初始化 `Cargo.toml`，定义 `kvdb-rs` crate 与 `kvdb`、`kvdb-benchmark` 两个 binary。
- 实现分层配置系统（`src/config.rs`）：默认值 → 配置文件（TOML/YAML）→ 环境变量 → 运行时 API，含内存预算校验与热更新回滚。
- 实现 RocksDB 存储引擎封装（`src/storage.rs`）：metadata / subkey / zset_score / pubsub 列族，共享 Block Cache 与 WAL。
- 实现 RESP2/RESP3 协议解析与序列化（`src/protocol.rs`），支持流水线与部分解析。

#### M2：String/Admin 命令 + Server + Smoke 测试

- 实现 String 命令：GET、SET、MGET、MSET、DEL、EXISTS、INCR、DECR、APPEND（`src/cmd/string.rs`）。
- 实现 Admin 命令：INFO、CONFIG、FLUSHDB、FLUSHALL、PING、ECHO、DBSIZE（`src/cmd/admin.rs`）。
- 实现 TCP Server（`src/server.rs`）：Tokio 异步 I/O、每个连接独立任务、流水线批量解析。
- 实现固定工作线程池（`src/thread_pool.rs`），隔离阻塞型 RocksDB 操作。
- 新增 Smoke 测试（`tests/smoke.rs`）。

#### M3：Hash/List/Set/ZSet/Bitmap + 单元/集成测试

- 实现 Hash 命令（`src/cmd/hash.rs`）。
- 实现 List 命令（`src/cmd/list.rs`）。
- 实现 Set 命令（`src/cmd/set.rs`）。
- 实现 ZSet 命令（`src/cmd/zset.rs`）。
- 实现 Bitmap 命令（`src/cmd/bitmap.rs`）。
- 新增对应命令集成测试（`tests/cmd_*.rs`）。

#### M4：事务 + Lua + Pub/Sub + HTTP 接口

- 实现事务（MULTI/EXEC/DISCARD/WATCH），含 WATCH 快照检测（`src/server.rs`）。
- 实现 Lua 脚本引擎（`src/lua.rs`、`src/cmd/lua.rs`）：EVAL/EVALSHA、脚本缓存、`redis.call`/`redis.pcall` 沙箱回调。
- 实现 Pub/Sub（`src/pubsub.rs`、`src/cmd/pubsub.rs`）：SUBSCRIBE/PSUBSCRIBE/PUBLISH/UNSUBSCRIBE。
- 实现 HTTP 管理接口（`src/http_server.rs`）：`/health`、`/config`、`/stats`、`/metrics`。
- 新增事务、Lua、Pub/Sub、HTTP API 测试。

#### M5：主从复制 + 集群骨架 + 完整测试

- 实现主从复制状态管理（`src/replication.rs`）：REPLICAOF/ROLE、角色切换、偏移量跟踪。
- 实现集群骨架（`src/cluster.rs`）：16384 槽位、CRC16-XMODEM、Hash tag、CLUSTER SLOTS/NODES/KEYSLOT。
- 新增复制与集群测试（`tests/replication_cluster.rs`）。

#### M6：文档、CI、性能调优

- 添加 GitHub Actions CI（`.github/workflows/ci.yml`）：fmt、clippy、build、test。
- 实现内置 Benchmark 框架（`src/benchmark.rs`）：embedded/tcp 模式，输出 QPS、p50/p99/p999 延迟。
- 实现 `kvdb-benchmark` CLI（`src/bin/benchmark.rs`）。
- 新增 100K 读写门控系统测试与内存 bounded 测试（`tests/system.rs`）。
- 新增 `README.md` 与 `AGENT.md` 文档。

### Changed

- 将测试文件按命令/特性拆分，便于并行执行与失败定位。
- 同步更新 `todos.md` 与实现计划中的测试文件映射。

### Fixed

- 修复 mlua `ContextHandle` 与 `UserData` 集成问题。
- 修复 `CommandContext` 在测试中的初始化问题。
- 修复 Cluster/Replication 相关编译错误与 Clippy 警告。

## 后续计划

- 全量/增量复制同步（Checkpoint + WAL tailing）。
- 集群槽位迁移与节点间协议。
- ACL / 配置热更新持久化到磁盘。
- Stream 完整命令集。
- Sentinel 自动故障转移。
- Geo / JSON 数据类型。
- 默认 Web 管理页面（`src/web/`）。
