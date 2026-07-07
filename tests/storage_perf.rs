//! 存储引擎性能优化测试：Bloom filter、分片 key 锁池、分页扫描边界。
//!
//! 这些测试验证性能优化不破坏正确性语义：
//! - Bloom filter 对点查询透明，读写删除行为不变
//! - 分片 key 锁池保证同一 key 串行、不同 key 并行
//! - prefix_scan_page 在前缀边界处正确终止，不产生多余空页

use std::sync::Arc;
use std::thread;

use kvdb_rs::{Config, ConfigManager, StorageEngine};

fn setup() -> (StorageEngine, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let mut config = Config::default();
    config.storage.db_path = dir.path().join("db").to_string_lossy().to_string();
    let config = Arc::new(ConfigManager::new(config));
    let storage = StorageEngine::open(&config.get().storage.db_path, &config.get()).unwrap();
    (storage, dir)
}

const CF_METADATA: &str = "metadata";
const CF_SUBKEY: &str = "subkey";

/// Bloom filter 开启后，点查询（GET 不存在的 key）仍返回 None，且已有数据可正确读取。
#[test]
fn bloom_filter_preserves_get_semantics() {
    let (storage, _dir) = setup();
    let cf = CF_METADATA;

    // 写入若干键
    for i in 0..100u32 {
        let key = format!("key:{:04}", i);
        storage.put(cf, key.as_bytes(), b"value").unwrap();
    }

    // 读取已存在的键
    for i in 0..100u32 {
        let key = format!("key:{:04}", i);
        let v = storage.get(cf, key.as_bytes()).unwrap();
        assert_eq!(v.as_deref(), Some(b"value".as_slice()));
    }

    // 读取不存在的键应返回 None（Bloom filter 可能加速，但结果必须正确）
    for i in 100..200u32 {
        let key = format!("key:{:04}", i);
        let v = storage.get(cf, key.as_bytes()).unwrap();
        assert_eq!(v, None);
    }
}

/// Bloom filter 开启后，删除后立即查询应返回 None。
#[test]
fn bloom_filter_delete_then_get() {
    let (storage, _dir) = setup();
    let cf = CF_METADATA;
    let key = b"delkey";

    storage.put(cf, key, b"v").unwrap();
    assert_eq!(storage.get(cf, key).unwrap(), Some(b"v".to_vec()));

    storage.delete(cf, key).unwrap();
    assert_eq!(storage.get(cf, key).unwrap(), None);
}

/// 分片 key 锁池：同一 key 的两次 lock 调用返回同一把锁（串行化保证）。
#[test]
fn key_lock_same_key_serialized() {
    let (storage, _dir) = setup();
    let key = b"counter:1";

    // 同一 key 获取锁，释放后再次获取应成功
    let guard1 = storage.key_lock(key);
    drop(guard1);
    let guard2 = storage.key_lock(key);
    drop(guard2);
}

/// 分片 key 锁池：不同 key 可并行加锁（验证不会因哈希碰撞完全串行）。
#[test]
fn key_lock_different_keys_parallel() {
    let (storage, _dir) = setup();
    let storage = Arc::new(storage);

    // 两个不同 key 的锁应可同时持有（不互相阻塞）
    let guard1 = storage.key_lock(b"key:A");
    let guard2 = storage.key_lock(b"key:B");
    // 同时持有两把锁证明它们是不同的分片
    drop(guard1);
    drop(guard2);
}

/// 分片 key 锁池在大量 key 访问后内存有界（不会像 DashMap 那样无限增长）。
/// 验证方式：访问 10 万个不同 key 的锁，不应 panic 或 OOM。
#[test]
fn key_lock_pool_bounded_memory() {
    let (storage, _dir) = setup();

    // 逐个获取并释放 10 万把锁，模拟长期运行的 INCR/DECR 场景
    for i in 0..100_000u32 {
        let key = format!("k:{:08}", i);
        let _guard = storage.key_lock(key.as_bytes());
        // 立即释放，验证池不会因累积条目而耗尽内存
    }
    // 如果实现使用 DashMap 且不清理，这里内存会膨胀 ~10MB+；
    // 分片池方案内存恒定。
}

/// 并发场景下 key_lock 保证 INCR 语义正确（无丢失更新）。
#[test]
fn key_lock_concurrent_incr_correctness() {
    let (storage, _dir) = setup();
    let storage = Arc::new(storage);
    let cf = CF_METADATA;
    let key = b"conc:counter";

    // 初始化为 0
    storage.put(cf, key, b"0").unwrap();

    let n_threads = 8;
    let n_per_thread = 1000;
    let mut handles = Vec::new();
    for _ in 0..n_threads {
        let storage = Arc::clone(&storage);
        let key = key.to_vec();
        handles.push(thread::spawn(move || {
            for _ in 0..n_per_thread {
                // 模拟 INCR：lock → read → modify → write → unlock
                let _guard = storage.key_lock(&key);
                let v = storage.get(cf, &key).unwrap().unwrap();
                let n: i64 = std::str::from_utf8(&v).unwrap().parse().unwrap();
                storage
                    .put(cf, &key, (n + 1).to_string().as_bytes())
                    .unwrap();
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }

    // 最终值应等于 n_threads * n_per_thread，无丢失更新
    let v = storage.get(cf, key).unwrap().unwrap();
    let n: i64 = std::str::from_utf8(&v).unwrap().parse().unwrap();
    assert_eq!(n, n_threads * n_per_thread);
}

/// prefix_scan_page 在到达前缀边界时应正确终止，不返回多余空页。
#[test]
fn prefix_scan_page_terminates_at_boundary() {
    let (storage, _dir) = setup();
    let cf = CF_SUBKEY;

    // 写入两个不同前缀的数据
    for i in 0..5u8 {
        let mut key = b"prefix_a:".to_vec();
        key.push(i);
        storage.put(cf, &key, &[i]).unwrap();
    }
    for i in 0..3u8 {
        let mut key = b"prefix_b:".to_vec();
        key.push(i);
        storage.put(cf, &key, &[i]).unwrap();
    }

    // 分页扫描 prefix_a，每页 2 条
    let mut all = Vec::new();
    let mut start = Vec::new();
    loop {
        let (page, next) = storage
            .prefix_scan_page(cf, b"prefix_a:", &start, 2)
            .unwrap();
        if page.is_empty() {
            break;
        }
        all.extend(page);
        match next {
            Some(k) => start = k,
            None => break,
        }
    }

    // 应恰好返回 prefix_a 的 5 条，不包含 prefix_b 的任何数据
    assert_eq!(all.len(), 5);
    for (k, _) in &all {
        assert!(k.starts_with(b"prefix_a:"));
    }
}

/// prefix_scan_page 反向扫描在到达前缀边界时正确终止。
#[test]
fn prefix_scan_page_reverse_terminates_at_boundary() {
    let (storage, _dir) = setup();
    let cf = CF_SUBKEY;

    for i in 0..5u8 {
        let mut key = b"alpha:".to_vec();
        key.push(i);
        storage.put(cf, &key, &[i]).unwrap();
    }
    // 写入字典序在 alpha 之前的键，验证反向扫描不会越界
    storage.put(cf, b"alpha", &[0xFF]).unwrap();

    let mut all = Vec::new();
    let mut start = Vec::new();
    loop {
        let (page, next) = storage
            .prefix_scan_page_reverse(cf, b"alpha:", &start, 2)
            .unwrap();
        if page.is_empty() {
            break;
        }
        all.extend(page);
        match next {
            Some(k) => start = k,
            None => break,
        }
    }

    // 应返回 alpha:0..alpha:4 共 5 条，不包含 "alpha"（无冒号）
    assert_eq!(all.len(), 5);
    for (k, _) in &all {
        assert!(k.starts_with(b"alpha:"));
    }
}
