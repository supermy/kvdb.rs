use kvdb_rs::cli;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    cli::run().await
}
