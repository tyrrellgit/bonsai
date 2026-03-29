use bytes::Bytes;
use bonsai::engine::Engine;
use tempfile::tempdir;

fn b(s: &str) -> Bytes { Bytes::from(s.to_owned()) }

#[test]
fn test_crash_mid_write_recovers_completed_records() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("00000000.wal");

    {
        let mut engine = Engine::new(dir.path()).unwrap();
        engine.put(b("user:1"), b("alice")).unwrap();
        engine.put(b("user:2"), b("bob")).unwrap();
        engine.put(b("user:3"), b("charlie")).unwrap();
        // drop without flush — WAL survives, memtable lost
    }

    // Append a truncated record to simulate crash mid-write
    use std::io::Write;
    let mut file = std::fs::OpenOptions::new()
        .append(true)
        .open(&wal_path)
        .unwrap();
    file.write_all(&[1u8, 6, 0, b'u', b's', b'e', b'r']).unwrap();

    let engine = Engine::open(dir.path()).unwrap();

    assert_eq!(engine.get(&b("user:1")).unwrap(), Some(b("alice")));
    assert_eq!(engine.get(&b("user:2")).unwrap(), Some(b("bob")));
    assert_eq!(engine.get(&b("user:3")).unwrap(), Some(b("charlie")));
    assert_eq!(engine.get(&b("user:4")).unwrap(), None);
}

#[test]
fn test_data_survives_clean_restart() {
    let dir = tempdir().unwrap();

    {
        let mut engine = Engine::new(dir.path()).unwrap();
        engine.put(b("key"), b("value")).unwrap();
    }

    let engine = Engine::open(dir.path()).unwrap();
    assert_eq!(engine.get(&b("key")).unwrap(), Some(b("value")));
}

#[test]
fn test_flushed_data_survives_restart_without_wal() {
    let dir = tempdir().unwrap();

    {
        let mut engine = Engine::new(dir.path()).unwrap();
        engine.put(b("key"), b("value")).unwrap();
        engine.flush_oldest_imm().unwrap(); // WAL deleted after flush
    }

    let engine = Engine::open(dir.path()).unwrap();
    assert_eq!(engine.get(&b("key")).unwrap(), Some(b("value")));
}