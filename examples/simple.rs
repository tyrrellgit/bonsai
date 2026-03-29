use bytes::Bytes;
use bonsai::engine::Engine;
use tempfile::tempdir;

fn main() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let mut engine = Engine::new(dir.path())?;

    // Basic put and get
    engine.put(Bytes::from("user:1"), Bytes::from("alice"))?;
    engine.put(Bytes::from("user:2"), Bytes::from("bob"))?;
    engine.put(Bytes::from("user:3"), Bytes::from("charlie"))?;

    let val = engine.get(&Bytes::from("user:1"))?;
    println!("user:1 = {:?}", val.map(|v| String::from_utf8_lossy(&v).to_string()));

    // Overwrite
    engine.put(Bytes::from("user:1"), Bytes::from("alice_updated"))?;
    let val = engine.get(&Bytes::from("user:1"))?;
    println!("user:1 after update = {:?}", val.map(|v| String::from_utf8_lossy(&v).to_string()));

    // Delete
    engine.delete(Bytes::from("user:2"))?;
    let val = engine.get(&Bytes::from("user:2"))?;
    println!("user:2 after delete = {:?}", val); // None

    // Range scan
    println!("\nScan all users:");
    let results = engine.scan(
        std::ops::Bound::Included(Bytes::from("user:1")),
        std::ops::Bound::Included(Bytes::from("user:3")),
    )?;
    for (key, value) in results {
        println!(
            "  {} = {}",
            String::from_utf8_lossy(&key),
            String::from_utf8_lossy(&value),
        );
    }

    // Flush to SSTable and demonstrate persistence
    engine.flush_oldest_imm()?;
    println!("\nFlushed to SSTable");

    // Reopen and verify data is still there
    let engine = Engine::open(dir.path())?;
    let val = engine.get(&Bytes::from("user:3"))?;
    println!("user:3 after reopen = {:?}", val.map(|v| String::from_utf8_lossy(&v).to_string()));

    Ok(())
}