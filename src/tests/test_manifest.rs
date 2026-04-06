use bytes::Bytes;
use tempfile::tempdir;

use crate::engine::{CompactionConfig, Engine, DEFAULT_MEMTABLE_SIZE_LIMIT};

fn b(s: &str) -> Bytes { Bytes::from(s.to_owned()) }

fn build_test_engine(dir: &tempfile::TempDir) -> Engine {
    Engine::new_with_config(dir.path(), CompactionConfig {
        memtable_size_limit: DEFAULT_MEMTABLE_SIZE_LIMIT,
        max_l0_files:        4,
        max_l0_size:         200 * 1024 * 1024,
        level_multiplier:    10,
        num_levels:          7,
    }).unwrap()
}

fn flush_and_compact(engine: &mut Engine) {
    std::thread::sleep(std::time::Duration::from_millis(200));
    engine.drain_completed_flushes().unwrap();
    std::thread::sleep(std::time::Duration::from_millis(200));
    engine.drain_completed_compactions().unwrap();
}

#[test]
fn test_manifest_recovers_flushed_l0_files_after_restart() {
    let dir = tempdir().unwrap();

    {
        let mut engine = build_test_engine(&dir);

        engine.put(b("a"), b("1")).unwrap();
        engine.freeze_memtable().unwrap();
        engine.put(b("b"), b("2")).unwrap();
        engine.freeze_memtable().unwrap();

        std::thread::sleep(std::time::Duration::from_millis(200));
        engine.drain_completed_flushes().unwrap();

        assert_eq!(engine.l0_count(), 2);
        assert_eq!(engine.get(&b("a")).unwrap(), Some(b("1")));
        assert_eq!(engine.get(&b("b")).unwrap(), Some(b("2")));
    }

    let reopened = Engine::open(dir.path()).unwrap();

    assert_eq!(reopened.l0_count(), 2, "manifest should restore both flushed L0 files");
    assert_eq!(reopened.get(&b("a")).unwrap(), Some(b("1")));
    assert_eq!(reopened.get(&b("b")).unwrap(), Some(b("2")));
}

#[test]
fn test_manifest_recovers_compacted_levels_after_restart() {
    let dir = tempdir().unwrap();

    {
        let mut engine = build_test_engine(&dir);

        for i in 0..4 {
            engine.put(b(&format!("key{:02}", i)), b(&format!("value{}", i))).unwrap();
            engine.freeze_memtable().unwrap();
        }
        flush_and_compact(&mut engine);

        assert_eq!(engine.l0_count(), 0, "L0 should be empty after compaction");
        let total_above_l0: usize = (1..engine.num_levels()).map(|l| engine.level_count(l)).sum();
        assert!(total_above_l0 >= 1, "at least one SST should exist above L0 before restart");
    }

    let reopened = Engine::open(dir.path()).unwrap();

    assert_eq!(reopened.l0_count(), 0, "manifest replay should preserve post-compaction L0 emptiness");
    let total_above_l0: usize = (1..reopened.num_levels()).map(|l| reopened.level_count(l)).sum();
    assert!(total_above_l0 >= 1, "manifest replay should restore SSTs to non-L0 levels");

    for i in 0..4 {
        assert_eq!(
            reopened.get(&b(&format!("key{:02}", i))).unwrap(),
            Some(b(&format!("value{}", i))),
            "key{:02} missing after reopen", i,
        );
    }
}

#[test]
fn test_manifest_and_wal_together_recover_flushed_and_unflushed_data() {
    let dir = tempdir().unwrap();

    {
        let mut engine = build_test_engine(&dir);

        engine.put(b("old"), b("persisted")).unwrap();
        engine.freeze_memtable().unwrap();
        std::thread::sleep(std::time::Duration::from_millis(200));
        engine.drain_completed_flushes().unwrap();

        engine.put(b("new"), b("from-wal")).unwrap();
        // no freeze, no flush — should come back from active WAL replay

        assert_eq!(engine.get(&b("old")).unwrap(), Some(b("persisted")));
        assert_eq!(engine.get(&b("new")).unwrap(), Some(b("from-wal")));
    }

    let reopened = Engine::open(dir.path()).unwrap();

    assert_eq!(reopened.get(&b("old")).unwrap(), Some(b("persisted")), "flushed key should come from manifest + SST");
    assert_eq!(reopened.get(&b("new")).unwrap(), Some(b("from-wal")), "unflushed key should come from WAL replay");
}

#[test]
fn test_manifest_persists_multiple_compaction_rounds() {
    let dir = tempdir().unwrap();

    {
        let mut engine = build_test_engine(&dir);

        for i in 0..4 {
            engine.put(b(&format!("key{:02}", i)), b(&format!("value{}", i))).unwrap();
            engine.freeze_memtable().unwrap();
        }
        flush_and_compact(&mut engine);

        for i in 4..8 {
            engine.put(b(&format!("key{:02}", i)), b(&format!("value{}", i))).unwrap();
            engine.freeze_memtable().unwrap();
        }
        flush_and_compact(&mut engine);
    }

    let reopened = Engine::open(dir.path()).unwrap();

    for i in 0..8 {
        assert_eq!(
            reopened.get(&b(&format!("key{:02}", i))).unwrap(),
            Some(b(&format!("value{}", i))),
            "key{:02} missing after reopen from double-compacted state", i,
        );
    }
}

#[test]
fn test_manifest_preserves_latest_value_across_restart() {
    let dir = tempdir().unwrap();

    {
        let mut engine = build_test_engine(&dir);

        engine.put(b("k"), b("v1")).unwrap();
        engine.freeze_memtable().unwrap();

        engine.put(b("k"), b("v2")).unwrap();
        engine.freeze_memtable().unwrap();

        engine.put(b("k"), b("v3")).unwrap();
        engine.freeze_memtable().unwrap();

        engine.put(b("k"), b("v4")).unwrap();
        engine.freeze_memtable().unwrap();

        flush_and_compact(&mut engine);
        assert_eq!(engine.get(&b("k")).unwrap(), Some(b("v4")));
    }

    let reopened = Engine::open(dir.path()).unwrap();
    assert_eq!(reopened.get(&b("k")).unwrap(), Some(b("v4")), "latest value should survive manifest replay");
}

#[test]
fn test_manifest_preserves_tombstone_across_restart() {
    let dir = tempdir().unwrap();

    {
        let mut engine = build_test_engine(&dir);

        engine.put(b("alive"), b("yes")).unwrap();
        engine.put(b("dead"), b("soon")).unwrap();
        engine.freeze_memtable().unwrap();

        engine.delete(b("dead")).unwrap();
        engine.freeze_memtable().unwrap();

        engine.put(b("later"), b("ok")).unwrap();
        engine.freeze_memtable().unwrap();

        engine.put(b("extra"), b("ok")).unwrap();
        engine.freeze_memtable().unwrap();

        flush_and_compact(&mut engine);

        assert_eq!(engine.get(&b("dead")).unwrap(), None);
        assert_eq!(engine.get(&b("alive")).unwrap(), Some(b("yes")));
    }

    let reopened = Engine::open(dir.path()).unwrap();

    assert_eq!(reopened.get(&b("dead")).unwrap(), None, "deleted key should stay deleted after reopen");
    assert_eq!(reopened.get(&b("alive")).unwrap(), Some(b("yes")));
    assert_eq!(reopened.get(&b("later")).unwrap(), Some(b("ok")));
    assert_eq!(reopened.get(&b("extra")).unwrap(), Some(b("ok")));
}

#[test]
fn test_manifest_bootstraps_legacy_directory_scan_state() {
    let dir = tempdir().unwrap();

    {
        let mut engine = build_test_engine(&dir);
        engine.put(b("legacy"), b("value")).unwrap();
        engine.freeze_memtable().unwrap();
        std::thread::sleep(std::time::Duration::from_millis(200));
        engine.drain_completed_flushes().unwrap();
    }

    let manifest_path = dir.path().join("MANIFEST");
    if manifest_path.exists() {
        std::fs::remove_file(&manifest_path).unwrap();
    }

    let reopened = Engine::open(dir.path()).unwrap();
    assert_eq!(reopened.get(&b("legacy")).unwrap(), Some(b("value")));
    assert!(manifest_path.exists(), "opening a legacy directory should create a manifest");

    let reopened_again = Engine::open(dir.path()).unwrap();
    assert_eq!(reopened_again.get(&b("legacy")).unwrap(), Some(b("value")));
}