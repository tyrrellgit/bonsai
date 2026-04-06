# bonsai 🌿

**A minimal LSM-tree storage engine written in Rust.**

> **bon·sai** /ˈbɒnsaɪ/ *noun*
> The Japanese art of growing miniature trees in containers through careful
> pruning and shaping — cultivating something complete and living, deliberately
> kept small.


`bonsai` is a minimal key-value store focusing on minimal dependencies, thread safety, and a simple API.

Like the art form it's named after, everything unnecessary has been removed while keeping the essence of its form intact.

## Features

- **MemTable** — lock-free sorted write buffer backed by a `crossbeam` skip list
- **SSTable** — immutable sorted files with bloom filter lookups from `fastbloom`
- **WAL / Manifest** — crash recovery via write-ahead logs and manifest files with CRC32 checksums from `crc32fast`
- **Merge iterator** — sorted range scans across all data sources
- **Compaction** — multi-level SST compaction for improved read and space amplification
- **MVCC** — *(coming soon)*

## Usage

```rust
use bonsai::engine::Engine;
use bytes::Bytes;

let mut engine = Engine::new("./data")?;

engine.put(Bytes::from("user:1"), Bytes::from("alice"))?;
engine.put(Bytes::from("user:2"), Bytes::from("bob"))?;

let val = engine.get(&Bytes::from("user:1"))?;

engine.delete(Bytes::from("user:2"))?;

// Range scan
let results = engine.scan(
    std::ops::Bound::Included(Bytes::from("user:1")),
    std::ops::Bound::Included(Bytes::from("user:9")),
)?;
for (key, value) in results {
    println!("{} = {}", 
        String::from_utf8_lossy(&key),
        String::from_utf8_lossy(&value),
    );
}

// Data survives restarts
drop(engine);
let engine = Engine::open("./data")?;
assert_eq!(engine.get(&Bytes::from("user:1"))?, Some(Bytes::from("alice")));
```

## Run the example
We have included a simple example which can be run via:

```bash
cargo run --example simple
```

# Custom Configuration
`bonsai` aims to keep things *small*.

For example our `memtable` objects in `L0` are only `4MB` (by default) and our default `CompactionConfig` prioritizes a small `L0` to reduce read and space amplification.  This makes small read/write cycles blazingly fast, but we suffer on larger insertions as the tables need to be flushed and compacted more regularly.

Tweaking these is possible, and recommended, if you have larger memory/write requirements; see the `Engine` and `CompactionConfig` for details.

> A quick win for heavy write workloads is usually bumping the `memtable_size_limit`