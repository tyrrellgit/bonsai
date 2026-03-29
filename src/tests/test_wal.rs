use bytes::Bytes;
use tempfile::tempdir;
use crate::wal::{Wal, WalEntry};

fn b(s: &str) -> Bytes { Bytes::from(s.to_owned()) }

#[test]
fn test_put_and_replay() {
    let dir  = tempdir().unwrap();
    let path = dir.path().join("test.wal");
    let mut wal = Wal::new(&path).unwrap();
    wal.put(b"key1", b"val1").unwrap();
    wal.put(b"key2", b"val2").unwrap();

    let entries = Wal::replay(&path).unwrap();
    assert_eq!(entries.len(), 2);
    assert!(matches!(&entries[0],
        WalEntry::Put { key, value } if key == &b("key1") && value == &b("val1")
    ));
}

#[test]
fn test_delete_and_replay() {
    let dir  = tempdir().unwrap();
    let path = dir.path().join("test.wal");
    let mut wal = Wal::new(&path).unwrap();
    wal.put(b"key", b"val").unwrap();
    wal.delete(b"key").unwrap();

    let entries = Wal::replay(&path).unwrap();
    assert_eq!(entries.len(), 2);
    assert!(matches!(&entries[1],
        WalEntry::Delete { key } if key == &b("key")
    ));
}

#[test]
fn test_partial_write_is_discarded() {
    let dir  = tempdir().unwrap();
    let path = dir.path().join("test.wal");
    let mut wal = Wal::new(&path).unwrap();
    wal.put(b"good", b"record").unwrap();

    use std::io::Write;
    let mut file = std::fs::OpenOptions::new().append(true).open(&path).unwrap();
    file.write_all(&[1u8, 0, 4, b't', b'r', b'u']).unwrap(); // truncated record

    let entries = Wal::replay(&path).unwrap();
    assert_eq!(entries.len(), 1);
}

#[test]
fn test_replay_empty_wal() {
    let dir  = tempdir().unwrap();
    let path = dir.path().join("test.wal");
    let _wal = Wal::new(&path).unwrap();
    assert_eq!(Wal::replay(&path).unwrap().len(), 0);
}

#[test]
fn test_append_across_open() {
    let dir  = tempdir().unwrap();
    let path = dir.path().join("test.wal");

    let mut wal = Wal::new(&path).unwrap();
    wal.put(b"a", b"1").unwrap();
    drop(wal);

    let mut wal = Wal::new(&path).unwrap(); // re-open in append mode
    wal.put(b"b", b"2").unwrap();

    let entries = Wal::replay(&path).unwrap();
    assert_eq!(entries.len(), 2);
}