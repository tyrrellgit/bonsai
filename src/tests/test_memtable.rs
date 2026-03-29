use bytes::Bytes;
use std::ops::Bound;
use crate::memtable::MemTable;

fn b(s: &str) -> Bytes { Bytes::from(s.to_owned()) }

#[test]
fn test_put_and_get() {
    let mem = MemTable::new(0);
    mem.put(b("key"), b("value"));
    assert_eq!(mem.get(&b("key")), Some(b("value")));
}

#[test]
fn test_get_missing() {
    let mem = MemTable::new(0);
    assert_eq!(mem.get(&b("missing")), None);
}

#[test]
fn test_delete_writes_tombstone() {
    let mem = MemTable::new(0);
    mem.put(b("key"), b("value"));
    mem.delete(b("key"));
    assert_eq!(mem.get(&b("key")), Some(Bytes::new()));
}

#[test]
fn test_put_overwrites() {
    let mem = MemTable::new(0);
    mem.put(b("key"), b("v1"));
    mem.put(b("key"), b("v2"));
    assert_eq!(mem.get(&b("key")), Some(b("v2")));
}

#[test]
fn test_approximate_size_grows() {
    let mem = MemTable::new(0);
    assert_eq!(mem.approximate_size(), 0);
    mem.put(b("key"), b("value"));
    assert!(mem.approximate_size() > 0);
}

#[test]
fn test_entry_count() {
    let mem = MemTable::new(0);
    mem.put(b("a"), b("1"));
    mem.put(b("b"), b("2"));
    mem.put(b("a"), b("3"));
    assert_eq!(mem.entry_count(), 2);
}

#[test]
fn test_scan_full_range() {
    let mem = MemTable::new(0);
    mem.put(b("a"), b("1"));
    mem.put(b("b"), b("2"));
    mem.put(b("c"), b("3"));

    let results = mem.scan(Bound::Unbounded, Bound::Unbounded);
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].0, b("a"));
    assert_eq!(results[2].0, b("c"));
}

#[test]
fn test_scan_bounded() {
    let mem = MemTable::new(0);
    mem.put(b("a"), b("1"));
    mem.put(b("b"), b("2"));
    mem.put(b("c"), b("3"));
    mem.put(b("d"), b("4"));

    let results = mem.scan(
        Bound::Included(b("b")),
        Bound::Excluded(b("d")),
    );
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].0, b("b"));
    assert_eq!(results[1].0, b("c"));
}

#[test]
fn test_iter_is_sorted() {
    let mem = MemTable::new(0);
    mem.put(b("c"), b("3"));
    mem.put(b("a"), b("1"));
    mem.put(b("b"), b("2"));

    let keys: Vec<_> = mem.iter().map(|(k, _)| k).collect();
    assert_eq!(keys, vec![b("a"), b("b"), b("c")]);
}