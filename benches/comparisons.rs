use std::hint::black_box;
use std::ops::Bound;

use bytes::Bytes;
use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput};
use redb::{Database, TableDefinition};
use tempfile::tempdir;

use bonsai::engine::Engine;

const REDB_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("bench");

// ── Helpers ────────────────────────────────────────────────────────────────

fn make_key(i: usize) -> Vec<u8>   { format!("{:016}", i).into_bytes() }
fn make_value(i: usize) -> Vec<u8> { format!("value_{:08}", i).into_bytes() }

fn scramble(i: usize) -> usize {
    (i as u64).wrapping_mul(6364136223846793005) as usize
}

// ── Sequential writes ──────────────────────────────────────────────────────

fn bench_sequential_writes(c: &mut Criterion) {
    let mut group = c.benchmark_group("sequential_writes");

    for n in [1_000usize, 10_000] {
        group.throughput(Throughput::Elements(n as u64));

        // bonsai
        group.bench_with_input(BenchmarkId::new("bonsai", n), &n, |b, &n| {
            b.iter_batched(
                || {
                    let dir = tempdir().unwrap();
                    let engine = Engine::new(dir.path()).unwrap();
                    (dir, engine)
                },
                |(dir, mut engine)| {
                    for i in 0..n {
                        engine.put(Bytes::from(make_key(i)), Bytes::from(make_value(i))).unwrap();
                    }
                    black_box(dir)
                },
                BatchSize::SmallInput,
            );
        });

        // sled
        group.bench_with_input(BenchmarkId::new("sled", n), &n, |b, &n| {
            b.iter_batched(
                || {
                    let dir = tempdir().unwrap();
                    let db = sled::open(dir.path()).unwrap();
                    (dir, db)
                },
                |(dir, db)| {
                    for i in 0..n {
                        db.insert(make_key(i), make_value(i)).unwrap();
                    }
                    black_box(dir)
                },
                BatchSize::SmallInput,
            );
        });

        // redb
        group.bench_with_input(BenchmarkId::new("redb", n), &n, |b, &n| {
            b.iter_batched(
                || {
                    let dir = tempdir().unwrap();
                    let db = Database::create(dir.path().join("bench.redb")).unwrap();
                    let txn = db.begin_write().unwrap();
                    txn.open_table(REDB_TABLE).unwrap();
                    txn.commit().unwrap();
                    (dir, db)
                },
                |(dir, db)| {
                    let txn = db.begin_write().unwrap();
                    {
                        let mut table = txn.open_table(REDB_TABLE).unwrap();
                        for i in 0..n {
                            table.insert(make_key(i).as_slice(), make_value(i).as_slice()).unwrap();
                        }
                    }
                    txn.commit().unwrap();
                    black_box(dir)
                },
                BatchSize::SmallInput,
            );
        });

        // heed (LMDB) — single env, clear between iterations
        group.bench_with_input(BenchmarkId::new("heed (LMDB)", n), &n, |b, &n| {
            let dir = tempdir().unwrap();
            let env = unsafe {
                heed::EnvOpenOptions::new()
                    .map_size(100 * 1024 * 1024)
                    .open(dir.path())
                    .unwrap()
            };
            let mut wtxn = env.write_txn().unwrap();
            let db: heed::Database<heed::types::Bytes, heed::types::Bytes> =
                env.create_database(&mut wtxn, None).unwrap();
            wtxn.commit().unwrap();

            b.iter_batched(
                || {
                    let mut wtxn = env.write_txn().unwrap();
                    db.clear(&mut wtxn).unwrap();
                    wtxn.commit().unwrap();
                },
                |_| {
                    let mut wtxn = env.write_txn().unwrap();
                    for i in 0..n {
                        db.put(&mut wtxn, &make_key(i), &make_value(i)).unwrap();
                    }
                    wtxn.commit().unwrap();
                    black_box(())
                },
                BatchSize::PerIteration,
            );
            let _ = &dir;
        });
    }
    group.finish();
}

// ── Random writes ──────────────────────────────────────────────────────────

fn bench_random_writes(c: &mut Criterion) {
    let mut group = c.benchmark_group("random_writes");

    for n in [1_000usize, 10_000] {
        group.throughput(Throughput::Elements(n as u64));

        // bonsai
        group.bench_with_input(BenchmarkId::new("bonsai", n), &n, |b, &n| {
            b.iter_batched(
                || {
                    let dir = tempdir().unwrap();
                    let engine = Engine::new(dir.path()).unwrap();
                    (dir, engine)
                },
                |(dir, mut engine)| {
                    for i in 0..n {
                        engine.put(
                            Bytes::from(make_key(scramble(i))),
                            Bytes::from(make_value(i)),
                        ).unwrap();
                    }
                    black_box(dir)
                },
                BatchSize::SmallInput,
            );
        });

        // sled
        group.bench_with_input(BenchmarkId::new("sled", n), &n, |b, &n| {
            b.iter_batched(
                || {
                    let dir = tempdir().unwrap();
                    let db = sled::open(dir.path()).unwrap();
                    (dir, db)
                },
                |(dir, db)| {
                    for i in 0..n {
                        db.insert(make_key(scramble(i)), make_value(i)).unwrap();
                    }
                    black_box(dir)
                },
                BatchSize::SmallInput,
            );
        });

        // redb
        group.bench_with_input(BenchmarkId::new("redb", n), &n, |b, &n| {
            b.iter_batched(
                || {
                    let dir = tempdir().unwrap();
                    let db = Database::create(dir.path().join("bench.redb")).unwrap();
                    let txn = db.begin_write().unwrap();
                    txn.open_table(REDB_TABLE).unwrap();
                    txn.commit().unwrap();
                    (dir, db)
                },
                |(dir, db)| {
                    let txn = db.begin_write().unwrap();
                    {
                        let mut table = txn.open_table(REDB_TABLE).unwrap();
                        for i in 0..n {
                            table.insert(
                                make_key(scramble(i)).as_slice(),
                                make_value(i).as_slice(),
                            ).unwrap();
                        }
                    }
                    txn.commit().unwrap();
                    black_box(dir)
                },
                BatchSize::SmallInput,
            );
        });

        // heed (LMDB) — single env, clear between iterations
        group.bench_with_input(BenchmarkId::new("heed (LMDB)", n), &n, |b, &n| {
            let dir = tempdir().unwrap();
            let env = unsafe {
                heed::EnvOpenOptions::new()
                    .map_size(100 * 1024 * 1024)
                    .open(dir.path())
                    .unwrap()
            };
            let mut wtxn = env.write_txn().unwrap();
            let db: heed::Database<heed::types::Bytes, heed::types::Bytes> =
                env.create_database(&mut wtxn, None).unwrap();
            wtxn.commit().unwrap();

            b.iter_batched(
                || {
                    let mut wtxn = env.write_txn().unwrap();
                    db.clear(&mut wtxn).unwrap();
                    wtxn.commit().unwrap();
                },
                |_| {
                    let mut wtxn = env.write_txn().unwrap();
                    for i in 0..n {
                        db.put(&mut wtxn, &make_key(scramble(i)), &make_value(i)).unwrap();
                    }
                    wtxn.commit().unwrap();
                    black_box(())
                },
                BatchSize::PerIteration,
            );
            let _ = &dir;
        });
    }
    group.finish();
}

// ── Point reads ────────────────────────────────────────────────────────────

fn bench_point_reads(c: &mut Criterion) {
    let n = 10_000usize;
    let target_key = make_key(n / 2);

    let mut group = c.benchmark_group("point_reads");
    group.throughput(Throughput::Elements(1));

    // bonsai — hot (memtable)
    {
        let dir = tempdir().unwrap();
        let mut engine = Engine::new(dir.path()).unwrap();
        for i in 0..n {
            engine.put(Bytes::from(make_key(i)), Bytes::from(make_value(i))).unwrap();
        }
        let key = Bytes::from(target_key.clone());
        group.bench_function("bonsai/hot", |b| {
            b.iter(|| engine.get(black_box(&key)).unwrap())
        });
    }

    // bonsai — cold (SSTable)
    {
        let dir = tempdir().unwrap();
        let mut engine = Engine::new(dir.path()).unwrap();
        for i in 0..n {
            engine.put(Bytes::from(make_key(i)), Bytes::from(make_value(i))).unwrap();
        }
        engine.flush_oldest_imm().unwrap();
        let key = Bytes::from(target_key.clone());
        group.bench_function("bonsai/cold", |b| {
            b.iter(|| engine.get(black_box(&key)).unwrap())
        });
    }

    // bonsai — miss (bloom filter short-circuit)
    {
        let dir = tempdir().unwrap();
        let mut engine = Engine::new(dir.path()).unwrap();
        for i in 0..n {
            engine.put(Bytes::from(make_key(i)), Bytes::from(make_value(i))).unwrap();
        }
        engine.flush_oldest_imm().unwrap();
        let missing = Bytes::from(b"zzzzzzzzzzzzzzzzz".to_vec());
        group.bench_function("bonsai/miss", |b| {
            b.iter(|| engine.get(black_box(&missing)).unwrap())
        });
    }

    // sled
    {
        let dir = tempdir().unwrap();
        let db = sled::open(dir.path()).unwrap();
        for i in 0..n {
            db.insert(make_key(i), make_value(i)).unwrap();
        }
        let key = target_key.clone();
        group.bench_function("sled", |b| {
            b.iter(|| db.get(black_box(&key)).unwrap())
        });
    }

    // redb
    {
        let dir = tempdir().unwrap();
        let db  = Database::create(dir.path().join("bench.redb")).unwrap();
        let txn = db.begin_write().unwrap();
        {
            let mut table = txn.open_table(REDB_TABLE).unwrap();
            for i in 0..n {
                table.insert(make_key(i).as_slice(), make_value(i).as_slice()).unwrap();
            }
        }
        txn.commit().unwrap();
        let key = target_key.clone();
        group.bench_function("redb", |b| {
            b.iter(|| {
                let rtxn  = db.begin_read().unwrap();
                let table = rtxn.open_table(REDB_TABLE).unwrap();
                table.get(black_box(key.as_slice())).unwrap()
            })
        });
    }

    // heed (LMDB)
    {
        let dir = tempdir().unwrap();
        let env = unsafe {
            heed::EnvOpenOptions::new()
                .map_size(100 * 1024 * 1024)
                .open(dir.path())
                .unwrap()
        };
        let mut wtxn = env.write_txn().unwrap();
        let db: heed::Database<heed::types::Bytes, heed::types::Bytes> =
            env.create_database(&mut wtxn, None).unwrap();
        for i in 0..n {
            db.put(&mut wtxn, &make_key(i), &make_value(i)).unwrap();
        }
        wtxn.commit().unwrap();
        let key = target_key.clone();
        group.bench_function("heed (LMDB)", |b| {
            b.iter(|| {
                let rtxn  = env.read_txn().unwrap();
                let value = db.get(black_box(&rtxn), black_box(&key)).unwrap();
                let result = value.map(|v| v.to_owned());
                drop(rtxn);
                black_box(result)
            })
        });
        let _ = &dir;
    }

    group.finish();
}

// ── Range scans ────────────────────────────────────────────────────────────

fn bench_range_scans(c: &mut Criterion) {
    let n          = 10_000usize;
    let scan_start = make_key(n / 4);
    let scan_end   = make_key(3 * n / 4);

    let mut group = c.benchmark_group("range_scans");
    group.throughput(Throughput::Elements((n / 2) as u64));

    // bonsai
    {
        let dir = tempdir().unwrap();
        let mut engine = Engine::new(dir.path()).unwrap();
        for i in 0..n {
            engine.put(Bytes::from(make_key(i)), Bytes::from(make_value(i))).unwrap();
        }
        engine.flush_oldest_imm().unwrap();
        let lo = Bytes::from(scan_start.clone());
        let hi = Bytes::from(scan_end.clone());
        group.bench_function("bonsai", |b| {
            b.iter(|| {
                engine.scan(
                    black_box(Bound::Included(lo.clone())),
                    black_box(Bound::Included(hi.clone())),
                ).unwrap().count()
            })
        });
    }

    // sled
    {
        let dir = tempdir().unwrap();
        let db = sled::open(dir.path()).unwrap();
        for i in 0..n {
            db.insert(make_key(i), make_value(i)).unwrap();
        }
        let lo = scan_start.clone();
        let hi = scan_end.clone();
        group.bench_function("sled", |b| {
            b.iter(|| {
                db.range(black_box(lo.clone())..=black_box(hi.clone())).count()
            })
        });
    }

    // redb
    {
        let dir = tempdir().unwrap();
        let db  = Database::create(dir.path().join("bench.redb")).unwrap();
        let txn = db.begin_write().unwrap();
        {
            let mut table = txn.open_table(REDB_TABLE).unwrap();
            for i in 0..n {
                table.insert(make_key(i).as_slice(), make_value(i).as_slice()).unwrap();
            }
        }
        txn.commit().unwrap();
        let lo = scan_start.clone();
        let hi = scan_end.clone();
        group.bench_function("redb", |b| {
            b.iter(|| {
                let rtxn  = db.begin_read().unwrap();
                let table = rtxn.open_table(REDB_TABLE).unwrap();
                table.range(black_box(lo.as_slice())..=black_box(hi.as_slice()))
                    .unwrap()
                    .count()
            })
        });
    }

    // heed (LMDB)
    {
        let dir = tempdir().unwrap();
        let env = unsafe {
            heed::EnvOpenOptions::new()
                .map_size(100 * 1024 * 1024)
                .open(dir.path())
                .unwrap()
        };
        let mut wtxn = env.write_txn().unwrap();
        let db: heed::Database<heed::types::Bytes, heed::types::Bytes> =
            env.create_database(&mut wtxn, None).unwrap();
        for i in 0..n {
            db.put(&mut wtxn, &make_key(i), &make_value(i)).unwrap();
        }
        wtxn.commit().unwrap();
        let lo = scan_start.clone();
        let hi = scan_end.clone();
        group.bench_function("heed (LMDB)", |b| {
            b.iter(|| {
                let rtxn  = env.read_txn().unwrap();
                let range = (
                    Bound::Included(lo.as_slice()),
                    Bound::Included(hi.as_slice()),
                );
                let count = db.range(&rtxn, &range).unwrap().count();
                drop(rtxn);
                black_box(count)
            })
        });
        let _ = &dir;
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_sequential_writes,
    bench_random_writes,
    bench_point_reads,
    bench_range_scans,
);
criterion_main!(benches);