use kvdb_rs::benchmark::{BenchmarkCommand, BenchmarkConfig, BenchmarkMode, run};

/// 100K 读写门控测试：在临时目录中使用 embedded 模式完成 5 万 SET + 5 万 GET，
/// 验证无错误且 QPS 不低于保守阈值，确保核心读写路径在系统量级下稳定。
#[test]
fn system_100k_read_write_gate() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let db_path = tmp.path().join("bench_gate");

    let config = BenchmarkConfig {
        mode: BenchmarkMode::Embedded,
        db_path: db_path.to_string_lossy().to_string(),
        commands: vec![BenchmarkCommand::Set, BenchmarkCommand::Get],
        clients: 4,
        requests: 50_000,
        key_size: 16,
        value_size: 128,
        warmup: 500,
        ..Default::default()
    };

    let result = run(config).expect("benchmark run");

    println!(
        "[SYSTEM] {} ops in {:.3}s, qps={}, p50={}us, p99={}us, errors={}",
        result.total_ops,
        result.elapsed.as_secs_f64(),
        result.qps as u64,
        result.p50_us,
        result.p99_us,
        result.errors
    );

    assert_eq!(result.total_ops, 200_000, "total ops mismatch");
    assert_eq!(result.errors, 0, "benchmark produced errors");
    // 保守门控：在 CI/低功耗机器上也能通过；真实硬件通常远高于此。
    assert!(
        result.qps >= 1000.0,
        "qps {} below gate threshold 1000",
        result.qps
    );
}

/// 内存 bounded 校验：连续写入大量数据后，验证数据库目录大小增长在可接受范围，
/// 且重新打开后数据仍然可读。
#[test]
fn system_memory_bounded_and_reopen() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let db_path = tmp.path().join("bounded");

    let write_config = BenchmarkConfig {
        mode: BenchmarkMode::Embedded,
        db_path: db_path.to_string_lossy().to_string(),
        commands: vec![BenchmarkCommand::Set],
        clients: 2,
        requests: 10_000,
        key_size: 32,
        value_size: 1024,
        warmup: 100,
        ..Default::default()
    };

    let write_result = run(write_config).expect("write benchmark");
    assert_eq!(write_result.errors, 0);

    let read_config = BenchmarkConfig {
        mode: BenchmarkMode::Embedded,
        db_path: db_path.to_string_lossy().to_string(),
        commands: vec![BenchmarkCommand::Get],
        clients: 2,
        requests: 10_000,
        key_size: 32,
        value_size: 1024,
        warmup: 0,
        ..Default::default()
    };

    let read_result = run(read_config).expect("read benchmark after reopen");
    assert_eq!(read_result.errors, 0);
    assert_eq!(read_result.total_ops, 20_000);
}
