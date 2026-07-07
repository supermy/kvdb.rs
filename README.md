# kvdb.rs

RocksDB 驱动的 Redis 兼容键值数据库，使用 Rust 实现，支持 RESP2/RESP3 协议、动态配置热更新、事务、Lua 脚本、Pub/Sub、主从复制与集群模式。

## 特性

- **Redis 协议兼容**：RESP2/RESP3 解析与序列化，支持流水线。
- **丰富数据结构**：String、Hash、List、Set、ZSet、Bitmap、Stream，全部支持分页扫描避免大集合 OOM。
- **多租户隔离**：Namespace 前缀编码，DBSIZE / FLUSHDB 按 namespace 隔离，namespace 长度校验 ≤ 255 字节。
- **事务**：MULTI / EXEC / DISCARD / WATCH，含 WATCH 快照检测。
- **Lua 脚本**：EVAL / EVALSHA，脚本缓存，`redis.call`/`redis.pcall` 沙箱回调。
- **Pub/Sub**：SUBSCRIBE / PSUBSCRIBE / PUBLISH / UNSUBSCRIBE。
- **主从复制骨架**：REPLICAOF / ROLE、复制角色与偏移量管理。
- **集群骨架**：16384 槽位、CRC16-XMODEM、CLUSTER SLOTS / NODES / KEYSLOT。
- **HTTP 管理接口**：`/health`、`/config`、`/stats`、`/metrics`。
- **嵌入式 API**：`kvdb_rs::open_embedded`，与 Server 模式共享存储格式。
- **内置 Benchmark**：`kvdb-benchmark` 支持 embedded/tcp 模式，输出 QPS 与延迟分位点。
- **性能优化**：Bloom filter 加速点查询、分片 key 锁池避免内存泄漏、前缀扫描边界优化。
- **八层测试体系**：unit / integration / smoke / regression / acceptance / system / e2e / server，104+ 测试用例。

## 快速开始

### 构建

```bash
cargo build --release
```

### 启动 Server

```bash
cargo run --bin kvdb -- server
```

默认监听 `127.0.0.1:6379`（RESP/TCP）与 `127.0.0.1:8080`（HTTP）。

### 使用 redis-cli 测试

```bash
redis-cli -p 6379 SET foo bar
redis-cli -p 6379 GET foo
```

### 使用配置文件

```bash
cargo run --bin kvdb -- server --config ./kvdb.toml
```

示例 `kvdb.toml`：

```toml
[server]
bind = "127.0.0.1:6379"
http_bind = "127.0.0.1:8080"
maxclients = 10000

[storage]
db_path = "./data"
block_cache_size = 268435456
write_buffer_size = 67108864
compression_type = "lz4"
bottommost_compression_type = "zstd"

log_level = "info"
dynamic_config = true
```

## 架构

```text
┌─────────────────────────────────────────────────────────────┐
│  Server (TCP/Unix Socket/TLS)                               │
│  - RESP 协议解析 / 流水线 / 连接管理 / 事务状态机            │
├─────────────────────────────────────────────────────────────┤
│  Command Layer (cmd/)                                       │
│  - String / Hash / List / Set / ZSet / Bitmap / Stream      │
│  - Admin / Transaction / Lua / PubSub / Cluster             │
├─────────────────────────────────────────────────────────────┤
│  Storage Engine (RocksDB)                                   │
│  - metadata / subkey / zset_score / pubsub 列族             │
│  - 共享 Block Cache / WAL / Compaction                      │
├─────────────────────────────────────────────────────────────┤
│  Config Manager                                             │
│  - Default → File → Env → Runtime API，热更新校验与回滚     │
└─────────────────────────────────────────────────────────────┘
```

## 支持的命令

| 类型 | 命令 |
|------|------|
| String | GET, SET, MGET, MSET, DEL, EXISTS, INCR, DECR, APPEND |
| Hash | HGET, HSET, HMGET, HGETALL, HDEL, HLEN, HEXISTS, HSCAN |
| List | LPUSH, RPUSH, LPOP, RPOP, LRANGE, LINDEX, LLEN |
| Set | SADD, SREM, SISMEMBER, SMEMBERS, SCARD, SPOP, SINTER, SDIFF, SUNION |
| ZSet | ZADD (NX/XX/GT/LT/CH/INCR), ZRANGE, ZREVRANGE, ZRANGEBYSCORE, ZREVRANGEBYSCORE, ZREM, ZRANK, ZREVRANK, ZSCORE, ZINCRBY, ZCARD |
| Bitmap | SETBIT, GETBIT, BITCOUNT |
| Stream | XADD, XLEN, XRANGE, XREAD |
| Admin | INFO, CONFIG, FLUSHDB, FLUSHALL, PING, ECHO, DBSIZE |
| Transaction | MULTI, EXEC, DISCARD, WATCH |
| Lua | EVAL, EVALSHA |
| Pub/Sub | SUBSCRIBE, UNSUBSCRIBE, PSUBSCRIBE, PUNSUBSCRIBE, PUBLISH |
| Replication | REPLICAOF, ROLE |
| Cluster | CLUSTER SLOTS, CLUSTER NODES, CLUSTER KEYSLOT |

## HTTP 管理接口

| 端点 | 方法 | 说明 |
|------|------|------|
| `/health` | GET | 健康检查 |
| `/config` | GET | 当前配置 JSON |
| `/config` | PUT | 热更新配置（校验后应用） |
| `/stats` | GET | RocksDB 内部统计 |
| `/metrics` | GET | Prometheus 格式指标 |

示例：

```bash
curl http://127.0.0.1:8080/health
curl http://127.0.0.1:8080/config
curl -X PUT http://127.0.0.1:8080/config \
  -H 'Content-Type: application/json' \
  -d '{"storage":{"block_cache_size":268435456}}'
```

## 基准测试

```bash
# Embedded 模式（默认）
cargo run --bin kvdb-benchmark -- --clients 8 --requests 100000

# TCP 模式
cargo run --bin kvdb-benchmark -- --mode tcp --host 127.0.0.1 --port 6379 \
  -C SET,GET --clients 8 --requests 10000

# 混合命令
cargo run --bin kvdb-benchmark -- -C SET,GET,HSET,HGET,LPUSH,LRANGE,ZADD,ZRANGE
```

示例输出：

```text
Total ops:    1600000
Elapsed:      12.345s
QPS:          129600
p50 latency:  45 us
p99 latency:  1200 us
p999 latency: 8500 us
Errors:       0
```

## 测试

```bash
# 全部测试（104+ 用例）
cargo test --all-targets

# 指定测试层
cargo test --test smoke
cargo test --test system
cargo test --test transactions
cargo test --test storage_perf

# 质量门禁
cargo fmt --check
cargo clippy --all-targets -- -D warnings
```

## 性能优化

| 优化项 | 说明 |
|--------|------|
| Bloom filter | 10 bits/key, block_based；对不存在的 key 可跳过 SST 磁盘读取 |
| 分片 key 锁池 | 1024 分片固定大小，替代 per-key DashMap，避免内存泄漏 |
| 前缀扫描边界 | prefix_scan_page 在前缀边界处提前终止，减少无效 IO |
| 大集合分页 | Hash/List/Set/ZSet 范围查询全部使用 RocksDB 前缀扫描分页，避免 OOM |
| 统一 Block Cache | 索引与过滤器块共享 Block Cache 预算，避免内存无界增长 |

## 生产部署建议

- 使用 `cargo build --release` 构建。
- `db_path` 与 `wal_dir` 建议分离到独立高性能磁盘。
- 确保 `block_cache_size + write_buffer_size × max_write_buffer_number ≤ 物理内存 50%`。
- 启用 `level_compaction_dynamic_level_bytes` 与 `enable_pipelined_write`。
- 配置 `maxclients` 与 `timeout` 防止连接泄漏。
- 部署前执行 `cargo test --test smoke`。

## CI/CD

GitHub Actions 工作流位于 [`.github/workflows/ci.yml`](.github/workflows/ci.yml)，支持 **Ubuntu / macOS / Windows** 三平台，执行：

- `cargo fmt --check`
- `cargo clippy --all-targets -- -D warnings`
- `cargo build --bins`
- `cargo test --all-targets`
- 跨平台构建产物上传

## 更新日志

详见 [CHANGELOG.md](CHANGELOG.md)。

## 许可证

MIT/Apache-2.0
