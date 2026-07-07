# Changelog

所有显著变更均按里程碑记录。格式参考 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/)。

## [Unreleased]

### Added

- 新增 Stream 核心命令（`src/cmd/stream.rs`）：XADD（支持 `*` 与 `ms-*` 自动 ID 生成）、XLEN、XRANGE（COUNT 分页）、XREAD（多流 COUNT 轮询），基于 RocksDB 前缀迭代与列族实现。
- 新增 ZSet 反向命令：ZREVRANGE、ZREVRANK、ZREVRANGEBYSCORE、ZINCRBY。
- 新增 ZADD 全选项支持：NX / XX / GT / LT / CH / INCR，含互斥校验（NX+XX、GT+LT、NX+GT/LT）与 INCR 单 score-member 限制。
- 新增 Set 集合运算 SINTER / SDIFF / SUNION，基于 RocksDB 前缀扫描分页实现，避免大集合 OOM。
- 新增 Namespace 多租户隔离：encoding 层支持 namespace 前缀编码，DBSIZE / FLUSHDB 按 namespace 隔离，namespace 长度校验 ≤ 255 字节。
- 新增 List LRANGE 分页扫描：encode_index / decode_index 使用符号位翻转的 8 字节大端编码，确保 RocksDB 字典序与 i64 数值序一致，支持负索引。
- 新增 Hash HGETALL / HSCAN 分页扫描，避免大 Hash OOM。
- 新增 ZSet ZRANGE / ZRANK 分页扫描，避免大 ZSet OOM。
- 新增 `tests/cmd_stream.rs`（7 用例）、`tests/namespace.rs`（3 用例）、`tests/cmd_string.rs`、`tests/storage_perf.rs`（8 用例）。
- 新增 Bloom filter（10 bits/key, block_based）加速点查询，对不存在的 key 可跳过磁盘 IO。
- 新增分片 key 锁池（1024 分片）替代 DashMap，修复 INCR/DECR/APPEND 路径的内存泄漏。

### Changed

- 统一 WRONGTYPE 错误消息：抽取 `wrong_type_error()` 至 `cmd/mod.rs`，所有数据类型共享同一消息格式。
- 统一 Redis 错误码协议：WRONGTYPE / NOSCRIPT 等错误码不再添加 "ERR " 前缀，与 Redis 协议一致。
- Set SINTER / SDIFF / SUNION 改用 RocksDB 前缀扫描分页，替代全量内存加载。
- prefix_scan / prefix_scan_page / count_prefix 显式 `starts_with(prefix)` 过滤，修复无 prefix_extractor 时跨 namespace 数据泄漏。

### Fixed

- 修复 DEL / EXISTS 仅识别 String 类型的问题，现支持所有数据类型（通过 metadata 检查）。
- 修复 Stream read_stream_meta 缺少过期检查的问题。
- 修复 MSET 未使用 WriteBatch 导致非原子的问题。
- 修复 prefix_iterator_cf 无 prefix_extractor 时不自动截断前缀边界导致跨 namespace 数据泄漏的问题。
- 修复 namespace 长度 > 255 时 u8 截断导致键空间损坏的问题。
- 修复 key_locks DashMap 按 key 无限增长导致内存泄漏的问题。
- 修复 prefix_scan_page 到达前缀边界时仍调用 iter.next() 产生无效 IO 的问题。

### Performance

- Bloom filter 开启后，GET / HGET / ZSCORE 等点查询对不存在的 key 可跳过 SST 文件磁盘读取。
- 分片 key 锁池内存占用恒定（1024 × Mutex），不随 key 数量增长。
- prefix_scan_page 反向扫描在前缀边界处提前终止，减少一次无效 IO。

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
