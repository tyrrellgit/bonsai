use std::hint::black_box;

use bytes::Bytes;
use criterion::{ criterion_group, criterion_main, BenchmarkId, Criterion, Throughput };
use tempfile::tempdir;

use bonsai::engine::Engine;

/*
Benchmarks for core Engine operations, including:

    - Sequential writes (to test memtable performance)
    - Random writes (to test memtable performance under non-sequential keys)
    - Hot reads (keys in active memtable, no disk I/O)
    - Cold reads with hit (keys in SSTable, disk I/O required)
    - Cold reads with miss (keys not present, bloom filter should short-circuit)
    - Full scans (to test SSTable iteration performance)
    - WAL recovery (to test replay performance on startup)
*/

fn sequential_writes(c: &mut Criterion) {
    let mut group = c.benchmark_group("writes");
    
    for size in [100, 1_000, 10_000] {
        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(BenchmarkId::new("sequential", size), &size, |b, &n| {
            b.iter(|| {
                let dir = tempdir().unwrap();
                let mut engine = Engine::new(dir.path()).unwrap();
                for i in 0..n {
                    let key   = Bytes::from(format!("{:016}", i));
                    let value = Bytes::from(format!("value_{}", i));
                    engine.put(black_box(key), black_box(value)).unwrap();
                }
            });
        });
    }
    group.finish();
}

fn random_writes(c: &mut Criterion) {
    let mut group = c.benchmark_group("writes");

    for size in [100, 1_000, 10_000] {
        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(BenchmarkId::new("random", size), &size, |b, &n| {
            b.iter(|| {
                let dir = tempdir().unwrap();
                let mut engine = Engine::new(dir.path()).unwrap();
                for i in 0..n {
                    // xor-shift scrambles key order
                    let scrambled = (i as u64).wrapping_mul(6364136223846793005);
                    let key   = Bytes::from(format!("{:016}", scrambled));
                    let value = Bytes::from(format!("value_{}", i));
                    engine.put(black_box(key), black_box(value)).unwrap();
                }
            });
        });
    }
    group.finish();
}

fn hot_read(c: &mut Criterion) {
    // Key is in the active memtable — no disk I/O
    let dir    = tempdir().unwrap();
    let mut engine = Engine::new(dir.path()).unwrap();
    engine.put(Bytes::from("hot_key"), Bytes::from("hot_value")).unwrap();

    c.bench_function("read/hot", |b| {
        b.iter(|| {
            engine.get(black_box(&Bytes::from("hot_key"))).unwrap()
        });
    });
}

fn cold_read_hit(c: &mut Criterion) {
    // Key is in an SSTable — bloom filter passes, disk scan needed
    let dir    = tempdir().unwrap();
    let mut engine = Engine::new(dir.path()).unwrap();
    engine.put(Bytes::from("cold_key"), Bytes::from("cold_value")).unwrap();
    engine.flush_oldest_imm().unwrap();

    c.bench_function("read/cold_hit", |b| {
        b.iter(|| {
            engine.get(black_box(&Bytes::from("cold_key"))).unwrap()
        });
    });
}

fn cold_read_miss(c: &mut Criterion) {
    // Key does not exist — bloom filter should short-circuit disk I/O
    let dir    = tempdir().unwrap();
    let mut engine = Engine::new(dir.path()).unwrap();
    for i in 0..1000 {
        engine.put(Bytes::from(format!("key_{:04}", i)), Bytes::from("value")).unwrap();
    }
    engine.flush_oldest_imm().unwrap();

    c.bench_function("read/cold_miss", |b| {
        b.iter(|| {
            engine.get(black_box(&Bytes::from("nonexistent_key"))).unwrap()
        });
    });
}

fn scan_full(c: &mut Criterion) {
    let dir    = tempdir().unwrap();
    let mut engine = Engine::new(dir.path()).unwrap();
    for i in 0..1000 {
        engine.put(
            Bytes::from(format!("{:016}", i)),
            Bytes::from("value"),
        ).unwrap();
    }

    c.bench_function("scan/full_1000", |b| {
        b.iter(|| {
            engine.scan(
                black_box(std::ops::Bound::Unbounded),
                black_box(std::ops::Bound::Unbounded),
            ).unwrap().count()
        });
    });
}

fn wal_recovery(c: &mut Criterion) {
    let mut group = c.benchmark_group("recovery");

    for size in [100, 1_000, 10_000] {
        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(BenchmarkId::new("wal_replay", size), &size, |b, &n| {
            // Prepare a WAL with n records
            let dir = tempdir().unwrap();
            {
                let mut engine = Engine::new(dir.path()).unwrap();
                for i in 0..n {
                    engine.put(
                        Bytes::from(format!("{:016}", i)),
                        Bytes::from("value"),
                    ).unwrap();
                }
                // drop without flush — WAL survives
            }

            b.iter(|| {
                Engine::open(black_box(dir.path())).unwrap()
            });
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    sequential_writes,
    random_writes,
    hot_read,
    cold_read_hit,
    cold_read_miss,
    scan_full,
    wal_recovery,
);
criterion_main!(benches);