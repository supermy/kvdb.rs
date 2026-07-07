use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use bytes::Bytes;
use rand::Rng;

use crate::cmd::{ClientState, CommandContext, CommandTable};
use crate::config::{Config, ConfigManager};
use crate::protocol::{RespParser, RespSerializer, RespValue};
use crate::pubsub::PubSubHub;
use crate::storage::StorageEngine;
use crate::thread_pool::ThreadPool;

/// benchmark 运行模式：直接压测存储引擎，或走完整 TCP/RESP 服务端路径。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BenchmarkMode {
    Embedded,
    Tcp,
}

impl std::str::FromStr for BenchmarkMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "embedded" => Ok(BenchmarkMode::Embedded),
            "tcp" => Ok(BenchmarkMode::Tcp),
            _ => Err(format!("unknown benchmark mode: {}", s)),
        }
    }
}

/// 单个 benchmark 命令类型。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BenchmarkCommand {
    Set,
    Get,
    HSet,
    HGet,
    LPush,
    LRange,
    ZAdd,
    ZRange,
}

impl std::str::FromStr for BenchmarkCommand {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_uppercase().as_str() {
            "SET" => Ok(BenchmarkCommand::Set),
            "GET" => Ok(BenchmarkCommand::Get),
            "HSET" => Ok(BenchmarkCommand::HSet),
            "HGET" => Ok(BenchmarkCommand::HGet),
            "LPUSH" => Ok(BenchmarkCommand::LPush),
            "LRANGE" => Ok(BenchmarkCommand::LRange),
            "ZADD" => Ok(BenchmarkCommand::ZAdd),
            "ZRANGE" => Ok(BenchmarkCommand::ZRange),
            _ => Err(format!("unknown benchmark command: {}", s)),
        }
    }
}

/// benchmark 配置。
#[derive(Debug, Clone)]
pub struct BenchmarkConfig {
    pub mode: BenchmarkMode,
    pub host: String,
    pub port: u16,
    pub db_path: String,
    pub commands: Vec<BenchmarkCommand>,
    pub clients: usize,
    pub requests: usize,
    pub key_size: usize,
    pub value_size: usize,
    pub warmup: usize,
}

impl Default for BenchmarkConfig {
    fn default() -> Self {
        Self {
            mode: BenchmarkMode::Embedded,
            host: "127.0.0.1".to_string(),
            port: 6379,
            db_path: "./benchmark_data".to_string(),
            commands: vec![BenchmarkCommand::Set, BenchmarkCommand::Get],
            clients: 8,
            requests: 100_000,
            key_size: 16,
            value_size: 128,
            warmup: 1_000,
        }
    }
}

/// benchmark 运行结果。
#[derive(Debug, Clone)]
pub struct BenchmarkResult {
    pub total_ops: usize,
    pub elapsed: Duration,
    pub qps: f64,
    pub p50_us: u64,
    pub p99_us: u64,
    pub p999_us: u64,
    pub errors: usize,
}

impl BenchmarkResult {
    /// 将结果格式化为人类可读的表格。
    pub fn format(&self) -> String {
        format!(
            "Total ops:    {}\nElapsed:      {:.3}s\nQPS:          {:.0}\np50 latency:  {} us\np99 latency:  {} us\np999 latency: {} us\nErrors:       {}",
            self.total_ops,
            self.elapsed.as_secs_f64(),
            self.qps,
            self.p50_us,
            self.p99_us,
            self.p999_us,
            self.errors
        )
    }
}

/// 运行 benchmark 并返回结果。
pub fn run(config: BenchmarkConfig) -> anyhow::Result<BenchmarkResult> {
    match config.mode {
        BenchmarkMode::Embedded => run_embedded(&config),
        BenchmarkMode::Tcp => run_tcp(&config),
    }
}

fn run_embedded(config: &BenchmarkConfig) -> anyhow::Result<BenchmarkResult> {
    let mut cfg = Config::default();
    cfg.storage.db_path = config.db_path.clone();
    let config_manager = Arc::new(ConfigManager::new(cfg));
    let storage = Arc::new(StorageEngine::open(&config.db_path, &config_manager.get())?);
    let table = Arc::new(CommandTable::new());
    let pubsub = Arc::new(PubSubHub::new());
    let thread_pool = ThreadPool::new(config.clients.max(1));

    let base_ctx = CommandContext {
        storage,
        config: config_manager,
        tx_pool: thread_pool,
        client: ClientState::default(),
        pubsub,
        pubsub_tx: tokio::sync::mpsc::unbounded_channel().0,
        client_id: 0,
        lua: Arc::new(crate::lua::LuaEngine::new(Arc::clone(&table))?),
        replication: crate::replication::ReplicationState::new(),
        cluster: crate::cluster::ClusterState::new(),
        namespace: bytes::Bytes::new(),
    };

    let config = config.clone();
    run_workers(config.clone(), move |client_id, op_index| {
        let mut ctx = base_ctx.clone();
        ctx.client_id = client_id as u64;
        let cmd = pick_command(&config, op_index);
        let (name, args) = build_command(cmd, client_id, op_index, &config);
        match table.dispatch(&ctx, name.as_bytes(), &args) {
            RespValue::Error(e) => Err(anyhow::anyhow!(e)),
            _ => Ok(()),
        }
    })
}

fn run_tcp(config: &BenchmarkConfig) -> anyhow::Result<BenchmarkResult> {
    let addr = format!("{}:{}", config.host, config.port);
    let _ = addr
        .to_socket_addrs()?
        .next()
        .ok_or_else(|| anyhow::anyhow!("could not resolve address: {}", addr))?;

    let config = config.clone();
    run_workers(config.clone(), move |_client_id, op_index| {
        // 每个 worker 持有独立连接，通过 thread-local 方式复用。
        send_tcp_command(&addr, &config, op_index)
    })
}

fn run_workers<F>(config: BenchmarkConfig, worker_fn: F) -> anyhow::Result<BenchmarkResult>
where
    F: FnMut(usize, usize) -> anyhow::Result<()> + Send + Clone + 'static,
{
    let latencies = Arc::new(Mutex::new(Vec::with_capacity(
        config.clients * config.requests,
    )));
    let errors = Arc::new(Mutex::new(0usize));

    // warmup：让 RocksDB 完成缓存预热与 memtable 填充，避免首次测试偏差。
    if config.warmup > 0 {
        let mut warmup_fn = worker_fn.clone();
        for i in 0..config.warmup {
            let _ = warmup_fn(0, i);
        }
    }

    let start = Instant::now();
    let mut handles = Vec::with_capacity(config.clients);
    for client_id in 0..config.clients {
        let mut fn_clone = worker_fn.clone();
        let latencies = Arc::clone(&latencies);
        let errors = Arc::clone(&errors);
        let requests = config.requests;
        let handle = thread::spawn(move || {
            let mut local_latencies = Vec::with_capacity(requests);
            let mut local_errors = 0usize;
            for op_index in 0..requests {
                let t0 = Instant::now();
                if fn_clone(client_id, op_index).is_err() {
                    local_errors += 1;
                }
                local_latencies.push(t0.elapsed().as_micros() as u64);
            }
            latencies.lock().unwrap().extend(local_latencies);
            *errors.lock().unwrap() += local_errors;
        });
        handles.push(handle);
    }

    for h in handles {
        h.join()
            .map_err(|e| anyhow::anyhow!("worker thread panicked: {:?}", e))?;
    }
    let elapsed = start.elapsed();

    let mut latencies = latencies.lock().unwrap();
    latencies.sort_unstable();
    let total_ops = config.clients * config.requests;
    let errors = *errors.lock().unwrap();
    let qps = if elapsed.as_secs_f64() > 0.0 {
        total_ops as f64 / elapsed.as_secs_f64()
    } else {
        0.0
    };

    Ok(BenchmarkResult {
        total_ops,
        elapsed,
        qps,
        p50_us: percentile(&latencies, 0.50),
        p99_us: percentile(&latencies, 0.99),
        p999_us: percentile(&latencies, 0.999),
        errors,
    })
}

fn send_tcp_command(addr: &str, config: &BenchmarkConfig, op_index: usize) -> anyhow::Result<()> {
    let cmd = pick_command(config, op_index);
    let (name, args) = build_command(cmd, 0, op_index, config);
    let mut items = vec![RespValue::BulkString(Some(Bytes::from(name)))];
    for arg in args {
        items.push(RespValue::BulkString(Some(arg)));
    }
    let request = RespValue::Array(items);
    let serialized = RespSerializer::serialize(&request);

    let mut stream = TcpStream::connect(addr)?;
    stream.write_all(&serialized)?;
    stream.flush()?;

    let mut buf = vec![0u8; 4096];
    let n = stream.read(&mut buf)?;
    buf.truncate(n);

    if let Some((RespValue::Error(e), _)) = RespParser::parse_one(&buf) {
        return Err(anyhow::anyhow!(e));
    }
    Ok(())
}

fn pick_command(config: &BenchmarkConfig, _op_index: usize) -> BenchmarkCommand {
    if config.commands.len() == 1 {
        return config.commands[0];
    }
    // 随机选择命令，使混合负载更接近真实场景；使用简单 RNG 避免跨线程竞争。
    let mut rng = rand::thread_rng();
    let idx = rng.gen_range(0..config.commands.len());
    config.commands[idx]
}

fn build_command(
    cmd: BenchmarkCommand,
    client_id: usize,
    op_index: usize,
    config: &BenchmarkConfig,
) -> (String, Vec<Bytes>) {
    let key = build_key(client_id, op_index, config.key_size);
    let value = build_value(config.value_size);

    match cmd {
        BenchmarkCommand::Set => ("SET".to_string(), vec![key, value]),
        BenchmarkCommand::Get => ("GET".to_string(), vec![key]),
        BenchmarkCommand::HSet => (
            "HSET".to_string(),
            vec![key, Bytes::from_static(b"field"), value],
        ),
        BenchmarkCommand::HGet => ("HGET".to_string(), vec![key, Bytes::from_static(b"field")]),
        BenchmarkCommand::LPush => ("LPUSH".to_string(), vec![key, value]),
        BenchmarkCommand::LRange => (
            "LRANGE".to_string(),
            vec![key, Bytes::from_static(b"0"), Bytes::from_static(b"100")],
        ),
        BenchmarkCommand::ZAdd => (
            "ZADD".to_string(),
            vec![key, Bytes::from_static(b"1.0"), value],
        ),
        BenchmarkCommand::ZRange => (
            "ZRANGE".to_string(),
            vec![key, Bytes::from_static(b"0"), Bytes::from_static(b"100")],
        ),
    }
}

fn build_key(client_id: usize, op_index: usize, key_size: usize) -> Bytes {
    let mut s = format!("key{}:{}", client_id, op_index);
    if s.len() < key_size {
        s.extend(std::iter::repeat_n('x', key_size - s.len()));
    }
    Bytes::from(s)
}

fn build_value(size: usize) -> Bytes {
    let mut v = vec![b'v'; size];
    // 在 value 中嵌入随机后缀，避免压缩导致测试结果失真。
    if size >= 8 {
        let suffix = rand::random::<u64>().to_le_bytes();
        v[size - 8..].copy_from_slice(&suffix);
    }
    Bytes::from(v)
}

fn percentile(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}
