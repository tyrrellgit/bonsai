use bytes::Bytes;
use std::ops::Bound;
use tempfile::tempdir;
use crate::engine::{ Engine, CompactionConfig, DEFAULT_MEMTABLE_SIZE_LIMIT };

// helper method
fn b(s: &str) -> Bytes { Bytes::from(s.to_owned()) }

fn build_test_engine(dir: &tempfile::TempDir) -> Engine {

    let compaction_config = CompactionConfig {
            memtable_size_limit: DEFAULT_MEMTABLE_SIZE_LIMIT,
            max_l0_files:        4,
            max_l0_size:         200 * 1024 * 1024,
            level_multiplier:    10,
            num_levels:          7,
        };
    Engine::new_with_config(dir.path(), compaction_config).unwrap()
}

#[test]
fn test_compaction_triggered_on_l0_overflow() {

    let dir = tempdir().unwrap();
    let mut engine = build_test_engine(&dir);

    // Flush up to compaction threshold
    for i in 0..4 {
        engine.put(b(&format!("key{}", i)), b(&format!("value{}", i))).unwrap();
        engine.freeze_memtable().unwrap();
    }

    std::thread::sleep(std::time::Duration::from_millis(100)); // wait for flushes
    engine.drain_completed_flushes().unwrap();                 // install into L0, triggers compaction dispatch

    std::thread::sleep(std::time::Duration::from_millis(100)); // wait for compaction
    engine.drain_completed_compactions().unwrap(); 

    // L0 should be empty after compaction
    assert_eq!(engine.l0_count(), 0, "L0 should be compacted");

    // Data should have been compacted into some level above L0
    let total_above_l0: usize = (1..engine.num_levels())
        .map(|l| engine.level_count(l))
        .sum();
    assert!(total_above_l0 >= 1, "At least one SST should exist above L0 after compaction");

    // Data must still be readable
    for i in 0..4 {
        let key = Bytes::from(format!("key{}", i));
        assert!(engine.get(&key).unwrap().is_some(), "key{} missing after compaction", i);
    }
}

#[test]
fn test_data_survives_compaction() {

    let dir = tempdir().unwrap();
    let mut engine = build_test_engine(&dir);

    // Insert data across multiple flushes
    let test_data = vec![
        ("a", "1"), ("b", "2"), ("c", "3"),
        ("d", "4"), ("e", "5"), ("f", "6"),
    ];

    for (i, (k, v)) in test_data.iter().enumerate() {
        engine.put(b(k), b(v)).unwrap();
        if (i + 1) % 2 == 0 {
            engine.freeze_memtable().unwrap();
            engine.flush_oldest_imm().unwrap();
        }
    }

    // Trigger final flush
    engine.freeze_memtable().unwrap();
    engine.flush_oldest_imm().unwrap();

    // Verify all data is still present
    for (k, v) in test_data {
        assert_eq!(
            engine.get(&b(k)).unwrap(),
            Some(b(v)),
            "Key {} should have value {}", k, v
        );
    }
}

#[test]
fn test_tombstones_cleaned_after_compaction() {
    
    let dir = tempdir().unwrap();
    let mut engine = build_test_engine(&dir);

    // Put some data
    engine.put(b("key1"), b("value1")).unwrap();
    engine.put(b("key2"), b("value2")).unwrap();
    engine.freeze_memtable().unwrap();
    engine.flush_oldest_imm().unwrap();

    // Delete the first key in a new memtable
    engine.delete(b("key1")).unwrap();
    engine.freeze_memtable().unwrap();
    engine.flush_oldest_imm().unwrap();

    // Now add more data to trigger compaction
    engine.put(b("key3"), b("value3")).unwrap();
    engine.freeze_memtable().unwrap();
    engine.flush_oldest_imm().unwrap();

    engine.put(b("key4"), b("value4")).unwrap();
    engine.freeze_memtable().unwrap();
    engine.flush_oldest_imm().unwrap();

    // After compaction, deleted key should still be gone
    assert_eq!(engine.get(&b("key1")).unwrap(), None, "Deleted key should still be deleted after compaction");
    
    // Other keys should still exist
    assert_eq!(engine.get(&b("key2")).unwrap(), Some(b("value2")));
    assert_eq!(engine.get(&b("key3")).unwrap(), Some(b("value3")));
    assert_eq!(engine.get(&b("key4")).unwrap(), Some(b("value4")));
}

#[test]
fn test_duplicate_keys_resolved_during_compaction() {
    
    let dir = tempdir().unwrap();
    let mut engine = build_test_engine(&dir);

    // Put version 1
    engine.put(b("key"), b("v1")).unwrap();
    engine.freeze_memtable().unwrap();
    engine.flush_oldest_imm().unwrap();

    // Put version 2
    engine.put(b("key"), b("v2")).unwrap();
    engine.freeze_memtable().unwrap();
    engine.flush_oldest_imm().unwrap();

    // Put version 3
    engine.put(b("key"), b("v3")).unwrap();
    engine.freeze_memtable().unwrap();
    engine.flush_oldest_imm().unwrap();

    // Add one more to trigger compaction
    engine.put(b("other"), b("data")).unwrap();
    engine.freeze_memtable().unwrap();
    engine.flush_oldest_imm().unwrap();

    // Should get the newest version
    assert_eq!(engine.get(&b("key")).unwrap(), Some(b("v3")));
}

#[test]
fn test_scan_after_compaction() {
    
    let dir = tempdir().unwrap();
    let mut engine = build_test_engine(&dir);

    // Insert data across multiple levels
    for i in 0..10 {
        engine.put(b(&format!("key{:02}", i)), b(&format!("value{}", i))).unwrap();
        if i % 2 == 1 {
            engine.freeze_memtable().unwrap();
            engine.flush_oldest_imm().unwrap();
        }
    }

    // Trigger compaction by adding more
    for i in 10..12 {
        engine.put(b(&format!("key{:02}", i)), b(&format!("value{}", i))).unwrap();
        engine.freeze_memtable().unwrap();
        engine.flush_oldest_imm().unwrap();
    }

    // Scan should work correctly after compaction
    let results: Vec<_> = engine
        .scan(Bound::Included(b("key04")), Bound::Included(b("key07")))
        .unwrap()
        .collect();

    assert!(results.len() >= 4, "Should find at least 4 keys in range");
    assert_eq!(results[0].0, b("key04"));
}

#[test]
fn test_restart_after_compaction() {
    let data = vec![
        ("a", "1"), ("b", "2"), ("c", "3"),
        ("d", "4"), ("e", "5"), ("f", "6"),
    ];

    {
        let dir = tempdir().unwrap();
        let dir_path = dir.path().to_owned();
        let mut engine = build_test_engine(&dir);
        
        // Insert and create multiple levels
        for (i, (k, v)) in data.iter().enumerate() {
            engine.put(b(k), b(v)).unwrap();
            if i % 2 == 1 {
                engine.freeze_memtable().unwrap();
                engine.flush_oldest_imm().unwrap();
            }
        }

        // Trigger compaction
        engine.put(b("g"), b("7")).unwrap();
        engine.freeze_memtable().unwrap();
        engine.flush_oldest_imm().unwrap();

        // Reopen engine
        drop(engine);
        let engine = Engine::open(&dir_path).unwrap();
        
        // All data should still be there
        for (k, v) in &data {
            assert_eq!(
                engine.get(&b(k)).unwrap(),
                Some(b(*v)),
                "Key {} missing after reopening", k
            );
        }
    }
}

#[test]
fn test_multiple_compactions() {

    let dir = tempdir().unwrap();
    let mut engine = build_test_engine(&dir);

    // Perform multiple compaction cycles
    for cycle in 0..3 {
        for i in 0..5 {
            let key_num = cycle * 5 + i;
            engine.put(b(&format!("key{:03}", key_num)), b(&format!("value{}", key_num))).unwrap();
            engine.freeze_memtable().unwrap();
            engine.flush_oldest_imm().unwrap();
        }
    }

    // Verify all data is present
    for i in 0..15 {
        assert_eq!(
            engine.get(&b(&format!("key{:03}", i))).unwrap(),
            Some(b(&format!("value{}", i)))
        );
    }
}
