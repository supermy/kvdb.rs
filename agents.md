# Agent Notes

`kvdb.rs` 是一个基于 RocksDB 构建的动态可配置高性能键值数据库，以 Rust 语言实现并兼容 Redis 协议（RESP2/RESP3）。它参考 kvrocks 的架构优化，将 RocksDB 的 LSM-Tree 持久化能力与 Redis 的丰富数据结构及高性能网络模型结合，同时引入运行时热更新配置系统，使存储引擎参数、网络行为与资源限制可在不重启服务的情况下动态调整。目标是以纯 Rust 构建小巧、可读、生产级的代码库，仅在 RocksDB 与 Lua 引擎处通过 C FFI 接入外部依赖。

## 定位与目标

- 面向高吞吐、低延迟、持久化的 KV 存储场景，单机或小型集群部署。
- 以 RocksDB 列族（Column Families）隔离不同 Redis 数据类型（String / Hash / List / Set / ZSet / Stream / Bitmap），共享 WAL 与 Compaction 资源，避免独立数据库实例的开销。
- 兼容 Redis 协议：支持 RESP2/RESP3 解析、完整命令集、事务（MULTI/EXEC/DISCARD/WATCH）、Lua 脚本、发布订阅（Pub/Sub）及主从复制。
- **动态可配置**：配置系统采用分层模型（Hardcoded Default → 配置文件 → 环境变量 → 运行时 API），支持热更新；RocksDB 参数（Block Cache、Write Buffer、Compaction 策略等）与网络参数（绑定地址、工作线程、最大连接数）均可在线调整。
- 多线程异步架构：网络 I/O（Tokio/async-std）与命令执行线程池分离，避免阻塞客户端。
- 主从复制基于 RocksDB Checkpoint + WAL 增量 tailing；集群模式采用 16384 哈希槽映射，兼容 Redis Cluster 协议。
- 保持 CPU 后端为纯 Rust，RocksDB 与 Lua 通过 FFI 边界隔离；公共 API 不暴露 LSM-Tree 内部细节。
- 提供**嵌入式（embedded）模式**与**Server 模式**：嵌入式直接嵌入业务进程，通过 Rust API 调用；Server 模式通过 Redis 协议对外服务，两者共享同一存储格式与配置系统。

## 质量规则

- 在涉及 RocksDB 列族创建/删除、WAL 写入边界、MemTable Flush 触发点、Compaction 策略选择、SST 元数据读取路径上，必须添加紧凑中文注释，解释一致性边界、内存策略与持久化保证。
- 在涉及配置分层合并、热更新冲突解决、校验失败回滚的代码中，中文注释应解释"优先级与回退策略"，而非仅描述代码行为。
- 在涉及 RESP 协议解析、命令分发路由、事务状态机、Lua 脚本沙箱、主从复制偏移量管理的代码中，中文注释必须说明执行顺序、原子性边界与内存所有权。
- 优先将中文注释写在实现旁，避免独立设计文档。
- 保持公共 API 窄化：CLI/Server 代码不应感知 RocksDB 内部 SST 文件布局、MemTable 跳表结构或 Compaction 策略实现细节。
- 不在核心读写路径引入永久性的运行时语义分支。诊断开关仅用于验证单一发布路径的正确性（如与 Redis 原生命令的行为对比）。
- 不引入 C++；RocksDB 与 Lua 依赖通过 Rust FFI crate（`rust-rocksdb`、`mlua`）解决。

## 安全与资源约束

- [x] 避免单实例打开过多列族（>128）导致内存与文件句柄膨胀；默认列族共享缓存，索引与过滤器块受限于 `block_cache_size` 预算。
- [x] 禁止并发执行多个全量手动 Compaction；实例级后台线程数通过 `max_background_jobs` 控制，防止 I/O 风暴。
- [ ] 优先短查询 smoke 测试做构建验证；大规模数据恢复与全量 Compaction 压力测试仅在显式测试磁盘路径时运行。
- [ ] 单个键值大小限制：key ≤ 512MB，value ≤ 512MB（与 Redis 协议对齐），但默认建议 ≤ 1MB；超过时在协议层返回 `WRONGTYPE` 或 `OOM` 错误，避免写入巨大值导致 LSM-Tree 严重失衡。
- [ ] WAL 大小限制：默认 64MB，超过触发 Flush，防止崩溃恢复时重放日志耗时过长。
- [ ] 内存预算控制：`block_cache_size` + `write_buffer_size` × `max_write_buffer_number` + 索引/过滤器开销 ≤ 系统物理内存 50%；在 < 8GB 内存设备上自动下调 Block Cache 至 256MB。
- [ ] 配置热更新时，涉及资源上限（内存、文件句柄、线程数）的变更需通过校验层，拒绝可能导致 OOM 或资源枯竭的参数组合。

## 代码布局

- `storage.rs`: RocksDB 核心封装、Column Family 生命周期管理、WAL 配置、Compaction 策略（Leveled / Universal / FIFO）动态切换、SST 元数据读取、备份（Checkpoint）与恢复、迭代器封装。
- `config.rs`: 动态配置系统，支持分层合并（Hardcoded Default → 配置文件 TOML/YAML → 环境变量 → 运行时 HTTP API）、热更新通道、配置校验器（Validator）、变更历史回滚点。
- `cmd/`: Redis 命令实现目录，按数据类型分文件：
  - `string.rs`: GET / SET / MGET / MSET / INCR / DECR / APPEND 等。
  - `hash.rs`: HGET / HSET / HMGET / HGETALL / HDEL 等。
  - `list.rs`: LPUSH / RPUSH / LPOP / RPOP / LRANGE / LINDEX 等。
  - `set.rs`: SADD / SREM / SISMEMBER / SMEMBERS / SUNION 等。
  - `zset.rs`: ZADD / ZRANGE / ZRANGEBYSCORE / ZREM / ZRANK 等。
  - `stream.rs`: XADD / XREAD / XRANGE / XGROUP 等（基于 RocksDB 前缀迭代与列族实现）。
  - `bitmap.rs`: SETBIT / GETBIT / BITCOUNT / BITOP 等。
  - `admin.rs`: INFO / CONFIG / FLUSHDB / FLUSHALL / SAVE / BGSAVE / LASTSAVE 等。
- `protocol.rs`: RESP2/RESP3 协议解析器与序列化器，支持流水线（pipelining）与部分解析（streaming parse），零拷贝或最小拷贝策略。
- `server.rs`: TCP/Unix Domain Socket 服务器，多线程异步运行时（Tokio），连接管理、客户端限流（`maxclients`）、空闲超时、TLS 加密（可选）。
- `replication.rs`: 主从复制逻辑，主节点维护复制偏移量与副本连接，从节点通过 RocksDB Checkpoint 做全量同步，随后 WAL tailing 做增量同步。
- `cluster.rs`: 集群模式，16384 槽位映射表，节点间二进制协议通信，槽位迁移与故障转移（手动/自动）。
- `transaction.rs`: MULTI / EXEC / DISCARD / WATCH 实现，基于 RocksDB 悲观事务或乐观事务（`TransactionDB` / `OptimisticTransactionDB`），保证命令队列原子性。
- `lua.rs`: Lua 脚本引擎封装（`mlua`），提供沙箱环境、脚本缓存、命令路由回调，限制执行时间与内存。
- `pubsub.rs`: 发布订阅实现，基于内存频道与客户端连接广播，不持久化至 RocksDB。
- `thread_pool.rs`: 固定工作线程池，命令执行与后台任务（如配置持久化、统计上报）复用，避免无限制线程创建。
- `metrics.rs`: 性能指标收集（QPS、延迟分位点、RocksDB 缓存命中率、Compaction 统计、配置变更次数），暴露 Prometheus 格式。
- `http_server.rs`: HTTP 管理接口，提供 `/metrics`（Prometheus）、`/config`（GET/PUT 动态配置）、`/stats`（RocksDB 内部统计）、`/health`（健康检查）。
- `cli.rs`: 命令行入口、配置文件加载、Server 启动、嵌入式模式直接调用、RocksDB 工具命令（如 `compact`、`repair`）。
- `benchmark.rs`: 内置性能基准测试框架，对比 `redis-benchmark` 兼容的测试集，测量 QPS、延迟、写放大。
- `tests/`: 八层测试体系（unit / integration / smoke / regression / acceptance / system / e2e / server），覆盖全部代码路径。
- `.github/workflows/ci.yml`: GitHub Actions 多平台 CI/CD 配置。
- `src/web/`: Web 管理测试页面（`index.html` + `app.js` + `style.css`），通过 `include_str!` 编译时嵌入到 `http_server.rs`，运行时无文件系统依赖。

### 规划中（尚未实现）

- `acl.rs`: 访问控制列表（ACL），支持用户、密码、命令权限、键空间模式匹配，兼容 Redis ACL 日志。
- `sentinel.rs`: 哨兵模式，监控主从节点健康，自动故障转移与通知客户端。
- `geo.rs`: 地理位置命令（GEOADD / GEORADIUS 等），基于 ZSet 的 Geohash 编码实现。
- `search.rs`: 二级索引与全文检索（基于 Tantivy 或 RocksDB 前缀/后缀索引），支持在 Hash 或 JSON 值上建立索引。
- `json.rs`: RedisJSON 兼容的 JSON 数据类型与命令（JSON.GET / JSON.SET / JSON.ARRAPPEND 等）。

## RocksDB 列族设计与 kvrocks 编码机制

本项目采用 **4 个核心 Column Family** 隔离不同 Redis 数据类型，参考 kvrocks 的编码设计实现：

| 列族名 | 用途 |
|--------|------|
| `metadata` | String 类型数据；Hash/List/Set/ZSet/Bitmap/Stream 的元数据（flags + expire + version + size） |
| `subkey` | Hash field→value、List index→value、Set member→NULL、Bitmap fragment |
| `zset_score` | ZSet 的 score→member 映射（支持按 score 范围查询） |
| `pubsub` | 发布订阅消息传播（非持久化，内存级） |

### 用户 Key 编码格式

所有键统一前缀编码，支持 namespace 隔离与集群槽位：

```
+-------------+-------------+------------------------------+-----------------+------------+-------------+-----------+
|  ns size    |  namespace  |   cluster slot               |  user key size  |  user key  |   version   |  sub key  |
| (1byte: X)  |   (Xbyte)   | (2byte when cluster enabled) |   (4byte: Y)    |   (YByte)  |   (8byte)   |  (ZByte)  |
+-------------+-------------+------------------------------+-----------------+------------+-------------+-----------+
```

- `ns_size` + `namespace`：多租户命名空间隔离
- `cluster_slot`：集群模式下 CRC16 哈希槽（2 字节）
- `user_key_size` + `user_key`：用户原始键
- `version`：8 字节时间戳版本号，用于快速删除（异步回收）
- `sub_key`：子键（Hash 的 field、List 的 index、ZSet 的 member 等）

### flags 字段编码

1 字节，高 4 位为 encoding version，低 4 位为 data type：

```
+----------------------------------------+
|               flags                    |
+----------------------------------------+
|  (1byte: | version -> <- data type |)  |
+----------------------------------------+
```

| data type | enum value |
|-----------|-----------|
| String    | 1         |
| Hash      | 2         |
| List      | 3         |
| Set       | 4         |
| ZSet      | 5         |
| Bitmap    | 6         |
| Stream    | 8         |

- **version 0**：expire 为 4 字节秒级时间戳，size 为 4 字节
- **version 1**：expire 为 8 字节毫秒级时间戳，size 为 8 字节（Ebyte/Sbyte 据此变化）

### String 编码

```
        +----------+------------+--------------------+
key =>  |  flags   |  expire    |       payload      |
        | (1byte)  | (Ebyte)    |       (Nbyte)      |
        +----------+------------+--------------------+
```

- 最简编码：flags + 过期时间 + 原始值
- 零拷贝读取：直接从 RocksDB value 偏移 payload 起始位置

### Hash 编码

**Metadata（存储于 `metadata` 列族）：**

```
legacy encoding:
        +----------+------------+-----------+-----------+
key =>  |  flags   |  expire    |  version  |  size     |
        | (1byte)  | (Ebyte)    |  (8byte)  | (Sbyte)   |
        +----------+------------+-----------+-----------+
```

**Sub keys（存储于 `subkey` 列族）：**

```
legacy encoding:
                     +---------------+
key|version|field => |     value     |
                     +---------------+

field expiration encoding:
                     +----------------+---------------+
key|version|field => | expire (8byte) |     value     |
                     +----------------+---------------+
```

- `version` 来自 metadata，用于快速删除整个 Hash（更新 metadata version 即可，旧 subkey 由后台 Compaction 回收）
- 支持 field 级别过期（扩展编码）

### Set 编码

Metadata 与 Hash 相同。Sub key 的 value 恒为 NULL：

```
                      +---------------+
key|version|member => |     NULL      |
                      +---------------+
```

### List 编码

**Metadata 扩展 head/tail 索引：**

```
        +----------+------------+-----------+-----------+-----------+-----------+
key =>  |  flags   |  expire    |  version  |  size     |  head     |  tail     |
        | (1byte)  | (Ebyte)    |  (8byte)  | (Sbyte)   | (8byte)   | (8byte)   |
        +----------+------------+-----------+-----------+-----------+-----------+
```

**Sub keys：**

```
                     +---------------+
key|version|index => |     value     |
                     +---------------+
```

- `head`/`tail` 为双向队列边界索引
- `LPUSH` 递减 head，`RPUSH` 递增 tail
- 索引计算：`LPOP` 取 head 位置，`RPOP` 取 tail-1 位置

### ZSet 编码

**Metadata 与 Set 相同。**

**双 subkey 设计（支持 member 查询与 score 范围查询）：**

```
                            +---------------+
key|version|member       => |     score     |   (1)  // 存储于 subkey 列族
                            +---------------+

                            +---------------+
key|version|score|member => |     NULL      |   (2)  // 存储于 zset_score 列族
                            +---------------+
```

- (1) 用于 `ZSCORE`、`ZREM`（通过 member 查 score）
- (2) 用于 `ZRANGEBYSCORE`、`ZREVRANGE`（通过 score 范围查 member）
- score 使用 memcomparable 编码，确保字典序即数值序

### Bitmap 编码

**Metadata 与 String 相同。**

**分片存储（1KiB = 8192 bits 每片）：**

```
                     +---------------+
key|version|index => |    fragment   |
                     +---------------+
```

- `index = bit_position / 8192`（分片索引）
- `offset = bit_position % 8192`（片内偏移）
- 使用 LSB（least-significant bit）位序，与 Redis 一致
- 稀疏场景高效：不存在的分片视为全 0
- 片大小可小于 1KiB，padding bits 视为 0

### Stream 编码

**Metadata 扩展流特定字段：**

```
+--------+--------+---------+--------+--------+--------+--------+--------+--------+--------+--------+--------+--------+
| flags  | expire | version | size   | LGE MS | LGE SEQ| RFE MS | RFE SEQ| MDE MS | MDE SEQ| FE MS  | FE SEQ | LE MS  | ...
+--------+--------+---------+--------+--------+--------+--------+--------+--------+--------+--------+--------+--------+
```

- `LGE`：Last Generated Entry ID
- `RFE`：First Entry ID
- `MDE`：Max Deleted Entry ID
- `FE`/`LE`：Current First/Last Entry ID
- `TNE`：Total Number of Entries（8 字节）

**Sub keys：**

```
                              +-----------------------+
key|version|EID MS|EID SEQ => |     encoded value     |
                              +-----------------------+
```

- Entry ID = `MS-SEQ`（毫秒时间戳 + 序列号）
- Value 编码：偶数字符串序列，每串前 4 字节长度前缀

**Consumer Group / Consumer / PEL 元数据：**

- 使用 `UINT64_MAX` 作为分隔符，区分普通 entry 与 group meta
- Group meta key：`key|version|UINT64_MAX|GROUP_META|group_name`
- Consumer meta key：`key|version|UINT64_MAX|CONSUMER_META|group_name|consumer_name`
- PEL entry key：`key|version|UINT64_MAX|PEL_ENTRY|group_name|EID MS|EID SEQ`

### 版本号与快速删除机制

- `version` 为 8 字节，由时间戳 + 随机数组合而成，保证单调递增且全局唯一
- **删除整个 key 时**：仅更新 metadata 中的 version（或删除 metadata），旧 version 的 subkey 不再被访问
- **回收机制**：后台 Compaction 过程中，遇到 version 与当前 metadata 不匹配的 subkey 自动清理
- **优势**：O(1) 删除大 Hash/Set/List，避免遍历数百万 subkey 导致性能抖动

### 编码实现规范

- `src/encoding.rs`：统一编码/解码工具函数，所有数据类型共享
- `src/types.rs`：Redis 数据类型枚举、flags 常量、version 管理
- 中文注释要求：在编码/解码路径上必须说明「为何如此分配字节」，而非仅描述字段含义
- 公共 API 不暴露 version 生成细节、subkey 拼接规则或 flags 位运算

## 存储与查询参数约定

- `db_path`: 数据目录路径，默认 `./data`。
- `wal_dir`: WAL 目录路径，建议与 `db_path` 分离到独立磁盘，默认空（与 `db_path` 同目录）。
- `max_open_files`: RocksDB 最大打开文件数，默认 `-1`（无限制）或 `65536`。
- `write_buffer_size`: 单个 MemTable 大小，默认 `64MB`。
- `max_write_buffer_number`: 最大 MemTable 数量（含不可变），默认 `6`。
- `min_write_buffer_number_to_merge`: 触发 Flush 前最小合并 MemTable 数，默认 `2`。
- `target_file_size_base`: L1 层单个 SST 文件大小，默认 `64MB`。
- `max_bytes_for_level_base`: L1 层总大小阈值，默认 `256MB`。
- `level_compaction_dynamic_level_bytes`: 动态调整层级大小，默认 `true`。
- `compression_type`: 压缩算法，默认 `lz4`（可选 `snappy`、`zstd`、`none`）。
- `bottommost_compression_type`: 最底层 SST 压缩算法，默认 `zstd`。
- `block_cache_size`: 块缓存大小，默认 `512MB`；在 < 8GB 内存设备上自动下调至 `256MB`。
- `cache_index_and_filter_blocks`: 将索引与过滤器块缓存至 Block Cache，默认 `true`。
- `wal_bytes_limit`: WAL 大小限制，默认 `64MB`，超过触发 Flush。
- `max_background_jobs`: 后台 Compaction/Flush 线程总数，默认 `4`。
- `max_subcompactions`: 单个 Compaction 任务子线程数，默认 `2`。
- `enable_pipelined_write`: 流水线写入（组提交），默认 `true`。
- `use_fsync`: 是否使用 `fsync` 替代 `fdatasync`，默认 `false`。
- `bind`: 监听地址，默认 `127.0.0.1:6379`。
- `unix_socket`: Unix Domain Socket 路径，默认空（禁用）。
- `worker_threads`: 异步运行时工作线程数，默认 `CPU 核心数`。
- `maxclients`: 最大客户端连接数，默认 `10000`。
- `tcp_keepalive`: TCP keepalive 间隔（秒），默认 `300`。
- `timeout`: 客户端空闲超时（秒），默认 `0`（不超时）。
- `dynamic_config`: 是否启用运行时配置热更新，默认 `true`。
- `config_file`: 配置文件路径，默认 `./kvdb.toml`。
- `log_level`: 日志级别，默认 `info`。

## 性能与稳定性测试

- 使用 `cargo run --bin kvdb-benchmark` 运行自动化性能基准测试，对比 SET / GET / HSET / HGET / LPUSH / LRANGE / ZADD / ZRANGE 等命令的 QPS 与延迟（p50 / p99 / p999）。
- 使用外部 `redis-benchmark` 进行协议兼容性验证，确保命令行为与 Redis 一致。
- 在 CI 中增加压力测试门控：100K 随机键写入耗时 < 10s，100K 随机读取耗时 < 5s，LRANGE 100 耗时 < 2s。
- 内存稳定性测试：连续写入 1M 键后校验进程 RSS 增长曲线，防止 MemTable 或 Block Cache 泄漏。
- Compaction 压力测试：持续顺序写入触发 L0→L1→L2 Compaction，测量写放大（Write Amplification）与延迟抖动。
- 崩溃恢复测试：随机 kill 进程后重启，校验数据一致性（键总数、校验和），确保 WAL 重放无丢失。
- 配置热更新测试：运行时动态调整 `block_cache_size` 与 `max_background_jobs`，验证性能变化与无崩溃。
- 主从复制测试：全量同步后持续写入，校验从节点延迟 < 1s，故障切换后数据一致。

## 网络接口服务

- `src/server.rs` 实现 TCP 服务器，监听 `0.0.0.0:6379`，兼容 RESP2/RESP3 协议，支持流水线（pipelining）与多命令批量提交。
- Unix 平台支持 Unix Domain Socket，降低本地通信延迟；Windows 回退到纯 TCP。
- 命令解析采用状态机与最小拷贝策略，大参数（如 `value`）直接引用接收缓冲区，避免额外堆分配。
- 支持 TLS 加密连接（可选，通过 `rustls` 实现）。
- HTTP 管理接口（`src/http_server.rs`）提供：
  - `/metrics`：Prometheus 格式性能指标。
  - `/config`：GET 查询当前配置，PUT 热更新配置（需校验）。
  - `/stats`：RocksDB 内部统计（`rocksdb::get_property` 封装）。
  - `/health`：健康检查，返回 200 OK 或 503 Service Unavailable。
- 浏览器端测试页面（`src/web/`）提供实时 QPS/延迟图表、配置热更新界面、键空间浏览器与节点状态监控。

## 集群与复制

- 主从复制基于 RocksDB Checkpoint 做全量同步，随后通过 WAL tailing 或迭代器做增量同步；主节点维护每个从节点的复制偏移量与 ACK 心跳。
- 集群模式采用 16384 哈希槽（hash slots），与 Redis Cluster 协议兼容；键通过 CRC16 映射到槽，槽再映射到节点。
- 支持节点间二进制协议通信，用于槽位迁移、故障检测与配置传播。
- 支持只读副本（replica）与主节点故障转移（手动触发或基于哨兵自动触发）。
- 复制偏移量与 RocksDB Sequence Number 对齐，确保崩溃恢复后复制连续性。

## 默认测试页面（llama-server 风格）

- `src/web/index.html` + `src/web/app.js` + `src/web/style.css` 提供 llama.cpp `llama-server` 风格的默认测试页面。
- 静态资源通过 `include_str!` 编译时嵌入到 `http_server.rs`，运行时无文件系统依赖。
- 功能包括：
  - **命令测试**：单条 Redis 命令交互，快速加载示例（String / Hash / List / Set / ZSet）。
  - **性能测试**：配置命令类型/并发数/数据量，运行 benchmark 并展示 QPS+Latency 图表。
  - **对比分析**：kvdb.rs vs Redis vs kvrocks 性能对比。
  - **数据管理**：键空间统计、配置热更新、RocksDB 内部状态查看。
  - **集群管理**：槽位分布、节点拓扑、复制状态可视化（集群模式启用时）。
- 浏览器端在 `app.js` 中增加 `console.log` 输出（`[kvdb.rs] action` 与 `[kvdb.rs] response`），方便前端调试。

## vs Redis & kvrocks 性能

- `src/benchmark.rs` 内置对比测试框架，测量指标包括：
  - 写入吞吐（SET / HSET / LPUSH / ZADD）
  - 读取吞吐（GET / HGET / LRANGE / ZRANGE）
  - 延迟分位点（p50 / p99 / p999）
  - 不同数据规模（1K / 100K / 1M / 10M 键）下的延迟-吞吐曲线
  - 写放大（Write Amplification）与 Compaction 耗时
  - 配置热更新对性能的影响（无停顿切换）
- 默认测试矩阵覆盖命令类型与并发数 1~256；大于 1M 键的压测在快速基准中自动降低采样率，避免 CI 超时。

## 深度 Review 与 TDD

- 所有模块遵循 TDD：先写测试（位于 `tests/` 八层体系），后实现功能。
- 代码审查 checklist（每次合并前必须满足）：
  - [ ] 配置变更是否通过校验层，避免无效配置导致崩溃或性能劣化
  - [ ] 核心读写路径无永久运行时分支
  - [ ] 公共 API 不暴露 RocksDB SST 布局、MemTable 跳表细节或 Compaction 策略实现
  - [ ] 中文注释解释"为何如此分配/保留"而非仅描述行为
  - [ ] 无 C++ 引入；RocksDB 与 Lua 依赖通过 FFI 解决
  - [ ] 内存预算控制（Block Cache + Write Buffer + 索引/过滤器 ≤ 50% 物理内存）
  - [ ] 动态配置热更新是否回滚安全（校验失败时保持原配置运行）
- 每次代码更新后同步 GitHub，保持远程仓库与本地一致。
- 文档更新与代码变更同步：修改功能时必须同步更新 README.md 与 AGENT.md。

## 生产部署 Review

- 推荐构建模式：`cargo build --release`
- 推荐部署 checklist：
  - [ ] 确认 `db_path` 与 `wal_dir` 挂载在独立高性能磁盘（SSD / NVMe），且 `wal_dir` 建议与数据目录分离
  - [ ] 确认 `block_cache_size` + `write_buffer_size` × `max_write_buffer_number` ≤ 物理内存 50%
  - [ ] 启用 `level_compaction_dynamic_level_bytes` 与 `enable_pipelined_write`
  - [ ] 设置 `max_background_jobs` 与 `max_subcompactions` 匹配 CPU 核心数与磁盘 I/O 能力
  - [ ] 配置 `compression_type` 与 `bottommost_compression_type`（默认 LZ4 + Zstd）
  - [ ] 设置合适的 `maxclients` 与 `timeout`，防止连接泄漏
  - [ ] 启用 `dynamic_config` 并配置 HTTP 管理接口用于运行时调优
  - [ ] 部署前执行 `cargo test --test smoke`
  - [ ] 低峰期调度全量手动 Compaction 或 `rocksdb::compact_range`
- 资源安全：禁止在 < 8GB 内存设备上启用过大 Block Cache；采用 `max_open_files` 限制防止句柄耗尽。
- Server 模式可配合 systemd / launchd 托管；主从复制与集群模式可配合负载均衡器做读写分离。

## CI/CD 配置

- `.github/workflows/ci.yml` 使用 GitHub Actions 实现多平台自动构建、测试与部署。
- 矩阵策略：Ubuntu / macOS / Windows，Rust 1.85+。
- 流水线阶段：
  1. `cargo build --summary all` 构建全部目标（cli / server / benchmark）
  2. 逐层运行八类测试（unit / integration / smoke / regression / acceptance / system / e2e / server）
  3. `cargo fmt --check` 代码格式检查
  4. `clippy` 静态分析
  5. 上传跨平台构建产物
  6. `main` 分支通过后在 CI 中触发 docs 部署

## 测试体系（覆盖率 100%）

项目采用八层测试架构，全部集成到 `Cargo.toml`：

| 测试类型   | 命令                              | 说明                                                                  |
| ---------- | --------------------------------- | --------------------------------------------------------------------- |
| 单元测试   | `cargo test --test unit`        | 模块级函数正确性（storage、config、protocol、cmd、transaction、lua）  |
| 集成测试   | `cargo test --test integration` | 模块间交互（命令组合、事务隔离、主从复制握手、配置热更新）             |
| 冒烟测试   | `cargo test --test smoke`       | 快速构建验证，带 `[SMOKE]` 前缀日志，便于调试                       |
| 回归测试   | `cargo test --test regression`  | 防止已修复 bug 复发（配置回滚、空键、崩溃恢复、负值 INCR、大 value）  |
| 验收测试   | `cargo test --test acceptance`  | 用户可见功能验证（创建-写入-读取、批量操作、Lua 脚本、配置热更新）    |
| 系统测试   | `cargo test --test system`      | 真实负载（10万键读写延迟门控、内存 bounded 校验、Compaction 稳定性）   |
| 端到端测试 | `cargo test --test e2e`         | 模拟真实客户端交互（RESP 协议、TCP/Unix Socket、HTTP API、redis-benchmark） |
| 服务器测试 | `cargo test --test server`      | TCP 服务器连接管理、流水线解析、大请求拒绝、超时断开等                |

- 冒烟测试增加 `tracing::info!("[SMOKE] ...")` 输出，方便构建失败时快速定位。
- 浏览器端 `app.js` 增加 `console.log("[kvdb.rs] action/response", ...)`，方便前端调试。
- 全部测试通过 `cargo test` 一键执行。
- 目标测试覆盖率 100%：每行生产代码至少被一层测试覆盖；未覆盖路径需在代码中以 `// untested:` 中文注释说明原因。

## 文档与同步

- `README.md` 提供快速开始、架构说明、测试命令、基准测试、生产部署、API 端点、CI/CD 概览。
- GitHub 同步：代码已按规范组织，`.github/workflows/ci.yml` 可直接触发多平台构建与测试；后续通过 `git push` 同步到远程仓库。
- AGENT.md 作为项目代理指令源文件，随每次功能迭代同步更新。

## 构建与运行

- 使用 `cargo build` 进行构建验证。
- 使用 `cargo test` 运行全部八层测试（依赖平台工具链与 RocksDB FFI 可用性）。
- 使用 `cargo test --test integration` 运行百万级随机数据的端到端测试，验证 RocksDB 持久化、事务隔离、配置热更新、主从复制一致性。
- 十亿级键压力测试仅在显式测试磁盘/压缩路径时手动触发，不作为 CI 默认任务。
- 使用 `cargo test --test cluster` 运行集群槽位迁移与故障转移测试（规划中）。
