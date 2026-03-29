use bytes::Bytes;
use std::ops::Bound;
use tempfile::tempdir;
use crate::memtable::MemTable;
use crate::sstable::SSTable;

fn b(s: &str) -> Bytes { Bytes::from(s.to_owned()) }

fn make_sst(entries: &[(&str, &str)]) -> (SSTable, tempfile::TempDir) {
    let dir  = tempdir().unwrap();
    let path = dir.path().join("test.sst");
    let mem  = MemTable::new(0);
    for (k, v) in entries {
        mem.put(b(k), b(v));
    }
    let sst = SSTable::from_memtable(&mem, &path).unwrap();
    (sst, dir)
}

#[test]
fn test_get_existing_key() {
    let (sst, _dir) = make_sst(&[("a", "1"), ("b", "2"), ("c", "3")]);
    assert_eq!(sst.get(&b("b")).unwrap(), Some(b("2")));
}

#[test]
fn test_get_missing_key() {
    let (sst, _dir) = make_sst(&[("a", "1"), ("c", "3")]);
    assert_eq!(sst.get(&b("b")).unwrap(), None);
}

#[test]
fn test_get_first_key() {
    let (sst, _dir) = make_sst(&[("a", "1"), ("b", "2"), ("c", "3")]);
    assert_eq!(sst.get(&b("a")).unwrap(), Some(b("1")));
}

#[test]
fn test_get_last_key() {
    let (sst, _dir) = make_sst(&[("a", "1"), ("b", "2"), ("c", "3")]);
    assert_eq!(sst.get(&b("c")).unwrap(), Some(b("3")));
}

#[test]
fn test_tombstone_preserved() {
    let dir  = tempdir().unwrap();
    let path = dir.path().join("test.sst");
    let mem  = MemTable::new(0);
    mem.put(b("key"), b("value"));
    mem.delete(b("key"));
    let sst = SSTable::from_memtable(&mem, &path).unwrap();
    assert_eq!(sst.get(&b("key")).unwrap(), Some(Bytes::new()));
}

#[test]
fn test_first_and_last_key() {
    let (sst, _dir) = make_sst(&[("a", "1"), ("b", "2"), ("c", "3")]);
    assert_eq!(sst.first_key, b("a"));
    assert_eq!(sst.last_key,  b("c"));
}

#[test]
fn test_open_restores_from_disk() {
    let dir  = tempdir().unwrap();
    let path = dir.path().join("00000000.sst");
    let mem  = MemTable::new(0);
    mem.put(b("a"), b("1"));
    mem.put(b("b"), b("2"));
    SSTable::from_memtable(&mem, &path).unwrap();

    let sst = SSTable::open(0, path).unwrap();
    assert_eq!(sst.get(&b("a")).unwrap(), Some(b("1")));
    assert_eq!(sst.get(&b("b")).unwrap(), Some(b("2")));
}

#[test]
fn test_scan_bounded() {
    let (sst, _dir) = make_sst(&[("a","1"),("b","2"),("c","3"),("d","4")]);
    let results: Vec<_> = sst
        .scan(Bound::Included(b("b")), Bound::Included(b("c")))
        .unwrap()
        .collect();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].0, b("b"));
    assert_eq!(results[1].0, b("c"));
}

#[test]
fn test_scan_unbounded() {
    let (sst, _dir) = make_sst(&[("a","1"),("b","2"),("c","3")]);
    let results: Vec<_> = sst
        .scan(Bound::Unbounded, Bound::Unbounded)
        .unwrap()
        .collect();
    assert_eq!(results.len(), 3);
}

#[test]
fn test_scan_excluded_bounds() {
    let (sst, _dir) = make_sst(&[("a","1"),("b","2"),("c","3"),("d","4")]);
    let results: Vec<_> = sst
        .scan(Bound::Excluded(b("a")), Bound::Excluded(b("d")))
        .unwrap()
        .collect();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].0, b("b"));
    assert_eq!(results[1].0, b("c"));
}