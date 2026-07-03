use clap::Parser;
use std::str::FromStr;

use kvdb_rs::benchmark::{BenchmarkCommand, BenchmarkConfig, BenchmarkMode, run};

#[derive(Parser)]
#[command(name = "kvdb-benchmark")]
#[command(about = "kvdb.rs benchmark tool")]
struct Cli {
    /// 运行模式：embedded 直接压测存储引擎，tcp 压测 RESP/TCP 服务端
    #[arg(short, long, default_value = "embedded")]
    mode: String,

    /// TCP 模式下的服务端地址
    #[arg(short = 'H', long, default_value = "127.0.0.1")]
    host: String,

    /// TCP 模式下的服务端端口
    #[arg(short, long, default_value_t = 6379)]
    port: u16,

    /// Embedded 模式下的数据库路径
    #[arg(short, long, default_value = "./benchmark_data")]
    db_path: String,

    /// 压测命令列表，逗号分隔，如 SET,GET,HSET,HGET,LPUSH,LRANGE,ZADD,ZRANGE
    #[arg(short = 'C', long, default_value = "SET,GET")]
    commands: String,

    /// 并发客户端（线程）数
    #[arg(short, long, default_value_t = 8)]
    clients: usize,

    /// 每个客户端执行的请求数
    #[arg(short, long, default_value_t = 100_000)]
    requests: usize,

    /// key 大小（字节）
    #[arg(long, default_value_t = 16)]
    key_size: usize,

    /// value 大小（字节）
    #[arg(long, default_value_t = 128)]
    value_size: usize,

    /// 预热请求数
    #[arg(long, default_value_t = 1_000)]
    warmup: usize,
}

fn main() {
    let cli = Cli::parse();

    let mode = BenchmarkMode::from_str(&cli.mode).unwrap_or_else(|e| panic!("invalid mode: {}", e));
    let commands: Vec<BenchmarkCommand> = cli
        .commands
        .split(',')
        .map(|s| BenchmarkCommand::from_str(s.trim()))
        .collect::<Result<Vec<_>, _>>()
        .unwrap_or_else(|e| panic!("invalid command: {}", e));

    let config = BenchmarkConfig {
        mode,
        host: cli.host,
        port: cli.port,
        db_path: cli.db_path,
        commands,
        clients: cli.clients,
        requests: cli.requests,
        key_size: cli.key_size,
        value_size: cli.value_size,
        warmup: cli.warmup,
    };

    match run(config) {
        Ok(result) => println!("{}", result.format()),
        Err(e) => {
            eprintln!("benchmark failed: {}", e);
            std::process::exit(1);
        }
    }
}
