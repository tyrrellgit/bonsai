use bytes::Bytes;
use std::ops::Bound;
use tempfile::tempdir;
use crate::engine::{Engine, CompactionConfig, DEFAULT_MEMTABLE_SIZE_LIMIT};

// ── Helpers ───────────────────────────────────────────────────────────────────

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

/// Freeze + flush + wait for all imm memtables to land in L0,
/// then wait for any triggered compaction to complete.
fn flush_and_compact(engine: &mut Engine) {
    std::thread::sleep(std::time::Duration::from_millis(200));
    engine.drain_completed_flushes().unwrap();
    std::thread::sleep(std::time::Duration::from_millis(200));
    engine.drain_completed_compactions().unwrap();
}

// ── Compaction trigger ────────────────────────────────────────────────────────

#[test]
fn test_compaction_triggered_on_l0_overflow() {
    let dir = tempdir().unwrap();
    let mut engine = build_test_engine(&dir);

    for i in 0..4 {
        engine.put(b(&format!("key{}", i)), b(&format!("value{}", i))).unwrap();
        engine.freeze_memtable().unwrap();
    }

    flush_and_compact(&mut engine);

    assert_eq!(engine.l0_count(), 0, "L0 should be empty after compaction");

    let total_above_l0: usize = (1..engine.num_levels())
        .map(|l| engine.level_count(l))
        .sum();
    assert!(total_above_l0 >= 1, "at least one SST should exist above L0");

    // Correctness — data must survive compaction
    for i in 0..4 {
        let key = b(&format!("key{}", i));
        assert!(engine.get(&key).unwrap().is_some(), "key{} missing after compaction", i);
    }
}

// ── Correctness after compaction ──────────────────────────────────────────────

#[test]
fn test_writes_after_compaction_readable() {
    let dir = tempdir().unwrap();
    let mut engine = build_test_engine(&dir);

    // Trigger a compaction with the first batch
    for i in 0..4 {
        engine.put(b(&format!("pre{:02}", i)), b(&format!("value{}", i))).unwrap();
        engine.freeze_memtable().unwrap();
    }
    flush_and_compact(&mut engine);

    // Write new keys after compaction — these go into fresh L0 files
    for i in 0..4 {
        engine.put(b(&format!("post{:02}", i)), b(&format!("value{}", i))).unwrap();
        engine.freeze_memtable().unwrap();
    }
    flush_and_compact(&mut engine);

    // Both pre- and post-compaction keys must be readable
    for i in 0..4 {
        assert!(engine.get(&b(&format!("pre{:02}",  i))).unwrap().is_some(), "pre{:02} missing",  i);
        assert!(engine.get(&b(&format!("post{:02}", i))).unwrap().is_some(), "post{:02} missing", i);
    }
}

#[test]
fn test_l0_overlapping_ranges_resolved_on_compaction() {
    let dir = tempdir().unwrap();
    let mut engine = build_test_engine(&dir);

    // Each flush produces an L0 SST with overlapping key ranges —
    // "key" appears in every one, compaction must keep only the newest value.
    engine.put(b("key"), b("v1")).unwrap();
    engine.put(b("aaa"), b("1")).unwrap();
    engine.freeze_memtable().unwrap();

    engine.put(b("key"), b("v2")).unwrap();
    engine.put(b("bbb"), b("2")).unwrap();
    engine.freeze_memtable().unwrap();

    engine.put(b("key"), b("v3")).unwrap();
    engine.put(b("ccc"), b("3")).unwrap();
    engine.freeze_memtable().unwrap();

    engine.put(b("key"), b("v4")).unwrap();
    engine.put(b("ddd"), b("4")).unwrap();
    engine.freeze_memtable().unwrap();

    flush_and_compact(&mut engine);

    assert_eq!(engine.l0_count(), 0, "L0 should be empty");
    assert_eq!(engine.get(&b("key")).unwrap(), Some(b("v4")), "should have newest value");
    assert_eq!(engine.get(&b("aaa")).unwrap(), Some(b("1")));
    assert_eq!(engine.get(&b("ddd")).unwrap(), Some(b("4")));
}

#[test]
fn test_scan_returns_sorted_deduplicated_after_compaction() {
    let dir = tempdir().unwrap();
    let mut engine = build_test_engine(&dir);

    // Write keys out of order across multiple flushes so they span multiple L0 SSTs
    let batches = vec![
        vec![("key05", "v5"), ("key02", "v2")],
        vec![("key08", "v8"), ("key01", "v1")],
        vec![("key03", "v3"), ("key07", "v7")],
        vec![("key04", "v4"), ("key06", "v6")],
    ];

    for batch in batches {
        for (k, v) in batch {
            engine.put(b(k), b(v)).unwrap();
        }
        engine.freeze_memtable().unwrap();
    }

    flush_and_compact(&mut engine);

    let results: Vec<(Bytes, Bytes)> = engine
        .scan(Bound::Included(b("key01")), Bound::Included(b("key08")))
        .unwrap()
        .collect();

    assert_eq!(results.len(), 8, "should find exactly 8 keys");

    // Verify sorted order
    for w in results.windows(2) {
        assert!(w[0].0 < w[1].0, "results not sorted: {:?} >= {:?}", w[0].0, w[1].0);
    }

    // Verify correct values
    for (i, (key, value)) in results.iter().enumerate() {
        let n = i + 1;
        assert_eq!(*key,   b(&format!("key{:02}", n)));
        assert_eq!(*value, b(&format!("v{}", n)));
    }
}

#[test]
fn test_tombstone_not_visible_in_scan_after_compaction() {
    let dir = tempdir().unwrap();
    let mut engine = build_test_engine(&dir);

    engine.put(b("key1"), b("value1")).unwrap();
    engine.put(b("key2"), b("value2")).unwrap();
    engine.put(b("key3"), b("value3")).unwrap();
    engine.freeze_memtable().unwrap();

    engine.delete(b("key2")).unwrap();
    engine.freeze_memtable().unwrap();

    engine.put(b("key4"), b("value4")).unwrap();
    engine.freeze_memtable().unwrap();

    engine.put(b("key5"), b("value5")).unwrap();
    engine.freeze_memtable().unwrap();

    flush_and_compact(&mut engine);

    // Deleted key must not appear in get or scan
    assert_eq!(engine.get(&b("key2")).unwrap(), None, "deleted key should not be visible");

    let results: Vec<_> = engine
        .scan(Bound::Unbounded, Bound::Unbounded)
        .unwrap()
        .collect();

    let keys: Vec<Bytes> = results.into_iter().map(|(k, _)| k).collect();
    assert!(!keys.contains(&b("key2")), "deleted key2 should not appear in scan");
    assert!(keys.contains(&b("key1")));
    assert!(keys.contains(&b("key3")));
    assert!(keys.contains(&b("key4")));
    assert!(keys.contains(&b("key5")));
}

#[test]
fn test_double_compaction_safe() {
    let dir = tempdir().unwrap();
    let mut engine = build_test_engine(&dir);

    for i in 0..4 {
        engine.put(b(&format!("key{:02}", i)), b(&format!("value{}", i))).unwrap();
        engine.freeze_memtable().unwrap();
    }
    flush_and_compact(&mut engine);

    // Second round — fills L0 again
    for i in 4..8 {
        engine.put(b(&format!("key{:02}", i)), b(&format!("value{}", i))).unwrap();
        engine.freeze_memtable().unwrap();
    }
    flush_and_compact(&mut engine);

    // All keys from both rounds must be present
    for i in 0..8 {
        assert_eq!(
            engine.get(&b(&format!("key{:02}", i))).unwrap(),
            Some(b(&format!("value{}", i))),
            "key{:02} missing after double compaction", i
        );
    }
}

// ── Restart survival ──────────────────────────────────────────────────────────

#[test]
fn test_restart_after_compaction() {
    let dir = tempdir().unwrap();
    let dir_path = dir.path().to_owned();

    {
        let mut engine = Engine::new_with_config(&dir_path, CompactionConfig {
            memtable_size_limit: DEFAULT_MEMTABLE_SIZE_LIMIT,
            max_l0_files:        4,
            max_l0_size:         200 * 1024 * 1024,
            level_multiplier:    10,
            num_levels:          7,
        }).unwrap();

        for i in 0..4 {
            engine.put(b(&format!("key{:02}", i)), b(&format!("value{}", i))).unwrap();
            engine.freeze_memtable().unwrap();
        }

        flush_and_compact(&mut engine);
        // engine drops here — flushes pending WALs
    }

    let engine = Engine::open(&dir_path).unwrap();

    for i in 0..4 {
        assert_eq!(
            engine.get(&b(&format!("key{:02}", i))).unwrap(),
            Some(b(&format!("value{}", i))),
            "key{:02} missing after restart", i
        );
    }
}