use anyhow::Result;
use clap::{Parser, Subcommand};
use std::path::PathBuf;
use std::sync::Arc;

use crate::config::ConfigManager;
use crate::http_server;
use crate::server::Server;
use crate::storage::StorageEngine;

#[derive(Parser)]
#[command(name = "kvdb")]
#[command(about = "RocksDB based Redis-compatible KV store")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// 启动 RESP/TCP 服务器
    Server {
        /// 配置文件路径
        #[arg(short, long)]
        config: Option<PathBuf>,
    },
    /// 进入嵌入式交互模式（M1 占位）
    Embedded,
}

pub async fn run() -> Result<()> {
    tracing_subscriber::fmt::init();
    let cli = Cli::parse();

    match cli.command {
        Commands::Server { config } => {
            let config = Arc::new(ConfigManager::load(config.as_deref())?);
            let cfg = config.get();
            let storage = Arc::new(StorageEngine::open(&cfg.storage.db_path, &cfg)?);
            let http_bind = cfg.server.http_bind.clone();

            // RESP/TCP 服务器与 HTTP 管理接口并行运行，共享配置与存储。
            let tcp = {
                let config = Arc::clone(&config);
                let storage = Arc::clone(&storage);
                async move {
                    let server = Server::bind(config, storage).await?;
                    server.run().await
                }
            };
            let http = http_server::serve(config, storage, &http_bind);

            tokio::select! {
                r = tcp => r?,
                r = http => r?,
            };
        }
        Commands::Embedded => {
            tracing::info!("embedded mode stub");
        }
    }
    Ok(())
}
