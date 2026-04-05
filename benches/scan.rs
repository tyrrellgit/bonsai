use std::hint::black_box;
use std::ops::Bound;

use bytes::Bytes;
use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use redb::{Database, TableDefinition};
use tempfile::tempdir;

use bonsai::engine::Engine;

const REDB_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("bench");

/*
Benchmark range scan comparisons for bonsai against:
    - sled (B tree)
    - redb (B tree)
    - heed (LMDB binding)
    - rocksDB (LSM tree) — TODO
*/


// ── Helpers ────────────────────────────────────────────────────────────────

fn make_key(i: usize) -> Vec<u8>   { format!("{:016}", i).into_bytes() }
fn make_value(i: usize) -> Vec<u8> { format!("value_{:08}", i).into_bytes() }

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
    bench_range_scans,
);
criterion_main!(benches);