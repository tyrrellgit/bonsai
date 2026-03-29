use bytes::Bytes;
use std::ops::Bound;
use tempfile::tempdir;
use crate::engine::Engine;

fn b(s: &str) -> Bytes { Bytes::from(s.to_owned()) }

#[test]
fn test_put_and_get() {
    let dir = tempdir().unwrap();
    let mut engine = Engine::new(dir.path()).unwrap();
    engine.put(b("key"), b("value")).unwrap();
    assert_eq!(engine.get(&b("key")).unwrap(), Some(b("value")));
}

#[test]
fn test_delete() {
    let dir = tempdir().unwrap();
    let mut engine = Engine::new(dir.path()).unwrap();
    engine.put(b("key"), b("value")).unwrap();
    engine.delete(b("key")).unwrap();
    assert_eq!(engine.get(&b("key")).unwrap(), None);
}

#[test]
fn test_overwrite() {
    let dir = tempdir().unwrap();
    let mut engine = Engine::new(dir.path()).unwrap();
    engine.put(b("key"), b("v1")).unwrap();
    engine.put(b("key"), b("v2")).unwrap();
    assert_eq!(engine.get(&b("key")).unwrap(), Some(b("v2")));
}

#[test]
fn test_get_missing() {
    let dir = tempdir().unwrap();
    let engine = Engine::new(dir.path()).unwrap();
    assert_eq!(engine.get(&b("nope")).unwrap(), None);
}

#[test]
fn test_flush_and_get_from_sstable() {
    let dir = tempdir().unwrap();
    let mut engine = Engine::new(dir.path()).unwrap();
    engine.put(b("key"), b("value")).unwrap();
    engine.flush_oldest_imm().unwrap();
    assert_eq!(engine.get(&b("key")).unwrap(), Some(b("value")));
}

#[test]
fn test_restart_recovers_from_wal() {
    let dir = tempdir().unwrap();
    {
        let mut engine = Engine::new(dir.path()).unwrap();
        engine.put(b("a"), b("1")).unwrap();
        engine.put(b("b"), b("2")).unwrap();
    }
    let engine = Engine::open(dir.path()).unwrap();
    assert_eq!(engine.get(&b("a")).unwrap(), Some(b("1")));
    assert_eq!(engine.get(&b("b")).unwrap(), Some(b("2")));
}

#[test]
fn test_restart_recovers_sstables() {
    let dir = tempdir().unwrap();
    {
        let mut engine = Engine::new(dir.path()).unwrap();
        engine.put(b("a"), b("1")).unwrap();
        engine.flush_oldest_imm().unwrap();
    }
    let engine = Engine::open(dir.path()).unwrap();
    assert_eq!(engine.get(&b("a")).unwrap(), Some(b("1")));
}

#[test]
fn test_delete_survives_restart() {
    let dir = tempdir().unwrap();
    {
        let mut engine = Engine::new(dir.path()).unwrap();
        engine.put(b("key"), b("value")).unwrap();
        engine.flush_oldest_imm().unwrap();
        engine.delete(b("key")).unwrap();
    }
    let engine = Engine::open(dir.path()).unwrap();
    assert_eq!(engine.get(&b("key")).unwrap(), None);
}

#[test]
fn test_scan_memtable() {
    let dir = tempdir().unwrap();
    let mut engine = Engine::new(dir.path()).unwrap();
    engine.put(b("a"), b("1")).unwrap();
    engine.put(b("b"), b("2")).unwrap();
    engine.put(b("c"), b("3")).unwrap();
    engine.put(b("d"), b("4")).unwrap();

    let results: Vec<_> = engine
        .scan(Bound::Included(b("b")), Bound::Included(b("c")))
        .unwrap()
        .collect();

    assert_eq!(results.len(), 2);
    assert_eq!(results[0], (b("b"), b("2")));
    assert_eq!(results[1], (b("c"), b("3")));
}

#[test]
fn test_scan_across_memtable_and_sstable() {
    let dir = tempdir().unwrap();
    let mut engine = Engine::new(dir.path()).unwrap();
    engine.put(b("a"), b("1")).unwrap();
    engine.put(b("b"), b("2")).unwrap();
    engine.flush_oldest_imm().unwrap();
    engine.put(b("c"), b("3")).unwrap();

    let results: Vec<_> = engine
        .scan(Bound::Unbounded, Bound::Unbounded)
        .unwrap()
        .collect();

    assert_eq!(results.len(), 3);
    assert_eq!(results[2].0, b("c"));
}

#[test]
fn test_scan_respects_tombstone() {
    let dir = tempdir().unwrap();
    let mut engine = Engine::new(dir.path()).unwrap();
    engine.put(b("a"), b("1")).unwrap();
    engine.put(b("b"), b("2")).unwrap();
    engine.delete(b("a")).unwrap();

    let results: Vec<_> = engine
        .scan(Bound::Unbounded, Bound::Unbounded)
        .unwrap()
        .collect();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].0, b("b"));
}