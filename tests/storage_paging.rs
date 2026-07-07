use std::sync::Arc;

use kvdb_rs::{Config, ConfigManager, StorageEngine};

fn setup() -> (StorageEngine, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let mut config = Config::default();
    config.storage.db_path = dir.path().join("db").to_string_lossy().to_string();
    let config = Arc::new(ConfigManager::new(config));
    let storage = StorageEngine::open(&config.get().storage.db_path, &config.get()).unwrap();
    (storage, dir)
}

#[test]
fn prefix_scan_page_forward_and_reverse() {
    let (storage, _dir) = setup();
    let cf = kvdb_rs::storage::CF_SUBKEY;
    let prefix = b"pfx:";

    // 写入 5 条有序键
    for i in 0u8..5 {
        let key = [prefix.as_slice(), &[i]].concat();
        storage.put(cf, &key, &[i]).unwrap();
    }

    // 正向第一页（limit=2）
    let (page, next) = storage.prefix_scan_page(cf, prefix, &[], 2).unwrap();
    assert_eq!(page.len(), 2);
    assert!(next.is_some());
    assert_eq!(page[0].0, [prefix.as_slice(), &[0]].concat());
    assert_eq!(page[1].0, [prefix.as_slice(), &[1]].concat());

    // 正向第二页
    let (page2, next2) = storage
        .prefix_scan_page(cf, prefix, next.as_deref().unwrap(), 2)
        .unwrap();
    assert_eq!(page2.len(), 2);
    assert!(next2.is_some());
    assert_eq!(page2[0].0, [prefix.as_slice(), &[2]].concat());
    assert_eq!(page2[1].0, [prefix.as_slice(), &[3]].concat());

    // 正向第三页
    let (page3, next3) = storage
        .prefix_scan_page(cf, prefix, next2.as_deref().unwrap(), 2)
        .unwrap();
    assert_eq!(page3.len(), 1);
    assert!(next3.is_none());
    assert_eq!(page3[0].0, [prefix.as_slice(), &[4]].concat());

    // 反向第一页：从末尾开始
    let (rev_page, rev_next) = storage
        .prefix_scan_page_reverse(cf, prefix, &[], 2)
        .unwrap();
    assert_eq!(rev_page.len(), 2);
    assert!(rev_next.is_some());
    assert_eq!(rev_page[0].0, [prefix.as_slice(), &[4]].concat());
    assert_eq!(rev_page[1].0, [prefix.as_slice(), &[3]].concat());

    // 反向第二页
    let (rev_page2, rev_next2) = storage
        .prefix_scan_page_reverse(cf, prefix, rev_next.as_deref().unwrap(), 2)
        .unwrap();
    assert_eq!(rev_page2.len(), 2);
    assert!(rev_next2.is_some());
    assert_eq!(rev_page2[0].0, [prefix.as_slice(), &[2]].concat());
    assert_eq!(rev_page2[1].0, [prefix.as_slice(), &[1]].concat());

    // 反向第三页
    let (rev_page3, rev_next3) = storage
        .prefix_scan_page_reverse(cf, prefix, rev_next2.as_deref().unwrap(), 2)
        .unwrap();
    assert_eq!(rev_page3.len(), 1);
    assert!(rev_next3.is_none());
    assert_eq!(rev_page3[0].0, [prefix.as_slice(), &[0]].concat());
}
