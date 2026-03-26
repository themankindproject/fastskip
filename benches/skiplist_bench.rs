use std::collections::{BTreeMap, HashMap};
use std::hint::black_box;
use std::sync::Arc;
use std::thread;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use rand::seq::SliceRandom;
use rand::thread_rng;

use fastskip::ConcurrentSkipList;

// ---------- DATA PREP ----------

fn gen_data(n: usize) -> (Vec<Vec<u8>>, Vec<Vec<u8>>) {
    let keys = (0..n)
        .map(|i| format!("key_{:08}", i).into_bytes())
        .collect();
    let vals = (0..n)
        .map(|i| format!("val_{:08}", i).into_bytes())
        .collect();
    (keys, vals)
}

// Convert Vec<Vec<u8>> → Vec<&[u8]>
fn borrow(v: &[Vec<u8>]) -> Vec<&[u8]> {
    v.iter().map(|x| x.as_slice()).collect()
}

// ---------- INSERT (SEQUENTIAL) ----------

fn bench_insert_sequential(c: &mut Criterion) {
    let mut group = c.benchmark_group("insert_seq");

    for &size in &[1_000, 10_000] {
        group.throughput(Throughput::Elements(size as u64));

        let (keys_owned, vals_owned) = gen_data(size);
        let keys = borrow(&keys_owned);
        let vals = borrow(&vals_owned);

        // FASTSKIP
        group.bench_with_input(BenchmarkId::new("fastskip", size), &size, |b, &_n| {
            b.iter(|| {
                let sl = ConcurrentSkipList::new();
                for i in 0..size {
                    black_box(sl.insert(keys[i], vals[i]));
                }
            });
        });

        // BTREE
        group.bench_with_input(BenchmarkId::new("btree", size), &size, |b, &_n| {
            b.iter(|| {
                let mut bt: BTreeMap<&[u8], &[u8]> = BTreeMap::new();
                for i in 0..size {
                    black_box(bt.insert(keys[i], vals[i]));
                }
            });
        });

        // HASHMAP
        group.bench_with_input(BenchmarkId::new("hashmap", size), &size, |b, &_n| {
            b.iter(|| {
                let mut hm: HashMap<&[u8], &[u8]> = HashMap::with_capacity(size);
                for i in 0..size {
                    black_box(hm.insert(keys[i], vals[i]));
                }
            });
        });
    }

    group.finish();
}

// ---------- INSERT (RANDOM) ----------

fn bench_insert_random(c: &mut Criterion) {
    let mut group = c.benchmark_group("insert_rand");

    for &size in &[1_000, 10_000] {
        group.throughput(Throughput::Elements(size as u64));

        let (keys_owned, vals_owned) = gen_data(size);
        let mut idx: Vec<usize> = (0..size).collect();
        idx.shuffle(&mut thread_rng());

        let keys = borrow(&keys_owned);
        let vals = borrow(&vals_owned);

        group.bench_with_input(BenchmarkId::new("fastskip", size), &size, |b, &_| {
            b.iter(|| {
                let sl = ConcurrentSkipList::new();
                for &i in &idx {
                    black_box(sl.insert(keys[i], vals[i]));
                }
            });
        });

        group.bench_with_input(BenchmarkId::new("btree", size), &size, |b, &_| {
            b.iter(|| {
                let mut bt: BTreeMap<&[u8], &[u8]> = BTreeMap::new();
                for &i in &idx {
                    black_box(bt.insert(keys[i], vals[i]));
                }
            });
        });

        group.bench_with_input(BenchmarkId::new("hashmap", size), &size, |b, &_| {
            b.iter(|| {
                let mut hm: HashMap<&[u8], &[u8]> = HashMap::with_capacity(size);
                for &i in &idx {
                    black_box(hm.insert(keys[i], vals[i]));
                }
            });
        });
    }

    group.finish();
}

// ---------- GET ----------

fn bench_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("get");

    let size = 10_000;
    let (keys_owned, vals_owned) = gen_data(size);
    let keys = borrow(&keys_owned);
    let vals = borrow(&vals_owned);

    let sl = ConcurrentSkipList::new();
    let mut bt: BTreeMap<&[u8], &[u8]> = BTreeMap::new();
    let mut hm: HashMap<&[u8], &[u8]> = HashMap::with_capacity(size);

    for i in 0..size {
        sl.insert(keys[i], vals[i]);
        bt.insert(keys[i], vals[i]);
        hm.insert(keys[i], vals[i]);
    }

    let hit_key = keys[size / 2];
    let miss_key: &[u8] = b"missing_key_999999";

    group.bench_function("fastskip_hit", |b| {
        b.iter(|| black_box(sl.get(hit_key)));
    });

    group.bench_function("btree_hit", |b| {
        b.iter(|| black_box(bt.get(hit_key)));
    });

    group.bench_function("hashmap_hit", |b| {
        b.iter(|| black_box(hm.get(hit_key)));
    });

    group.bench_function("fastskip_miss", |b| {
        b.iter(|| black_box(sl.get(miss_key)));
    });

    group.bench_function("btree_miss", |b| {
        b.iter(|| black_box(bt.get(miss_key)));
    });

    group.bench_function("hashmap_miss", |b| {
        b.iter(|| black_box(hm.get(miss_key)));
    });

    group.finish();
}

// ---------- ITER ----------

fn bench_iter(c: &mut Criterion) {
    let mut group = c.benchmark_group("iter");

    for &size in &[1_000, 10_000] {
        let (keys_owned, vals_owned) = gen_data(size);
        let keys = borrow(&keys_owned);
        let vals = borrow(&vals_owned);

        let sl = ConcurrentSkipList::new();
        let mut bt: BTreeMap<&[u8], &[u8]> = BTreeMap::new();

        for i in 0..size {
            sl.insert(keys[i], vals[i]);
            bt.insert(keys[i], vals[i]);
        }

        group.bench_with_input(BenchmarkId::new("fastskip", size), &size, |b, _| {
            b.iter(|| {
                let count = sl.iter().count();
                black_box(count);
            });
        });

        group.bench_with_input(BenchmarkId::new("btree", size), &size, |b, _| {
            b.iter(|| {
                let count = bt.len();
                black_box(count);
            });
        });
    }

    group.finish();
}

// ---------- CONCURRENT (BASELINE) ----------

fn bench_concurrent_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent_insert");

    for &threads in &[4, 8] {
        group.bench_with_input(BenchmarkId::new("fastskip", threads), &threads, |b, &t| {
            b.iter(|| {
                let sl = Arc::new(ConcurrentSkipList::with_shards(t * 2));

                let mut handles = vec![];
                for tid in 0..t {
                    let sl = Arc::clone(&sl);
                    handles.push(thread::spawn(move || {
                        for i in 0..10_000 {
                            let key = format!("t{}_{:08}", tid, i);
                            let val = format!("v{}_{:08}", tid, i);
                            sl.insert(key.as_bytes(), val.as_bytes());
                        }
                    }));
                }

                for h in handles {
                    h.join().unwrap();
                }
            });
        });
    }

    group.finish();
}

// ---------- CURSOR (SEEK + SCAN) ----------

fn bench_cursor(c: &mut Criterion) {
    let mut group = c.benchmark_group("cursor");

    for &size in &[1_000, 10_000] {
        let (keys_owned, vals_owned) = gen_data(size);
        let keys = borrow(&keys_owned);
        let vals = borrow(&vals_owned);

        let sl = ConcurrentSkipList::new();
        let mut bt: BTreeMap<&[u8], &[u8]> = BTreeMap::new();

        for i in 0..size {
            sl.insert(keys[i], vals[i]);
            bt.insert(keys[i], vals[i]);
        }

        // Cursor seek to middle
        let seek_key = keys[size / 2];
        group.bench_with_input(BenchmarkId::new("fastskip_seek", size), &size, |b, _| {
            b.iter(|| {
                let cursor = sl.cursor_at(seek_key);
                black_box(cursor.is_some());
            });
        });

        // BTreeMap range (lower_bound equivalent)
        group.bench_with_input(BenchmarkId::new("btree_range", size), &size, |b, _| {
            b.iter(|| {
                let count = bt.range(seek_key..).count();
                black_box(count);
            });
        });

        // Cursor scan from middle
        group.bench_with_input(
            BenchmarkId::new("fastskip_scan_from_mid", size),
            &size,
            |b, _| {
                b.iter(|| {
                    if let Some(cursor) = sl.cursor_at(seek_key) {
                        let count = cursor.count();
                        black_box(count);
                    }
                });
            },
        );
    }

    group.finish();
}

// ---------- SNAPSHOT ----------

fn bench_snapshot(c: &mut Criterion) {
    let mut group = c.benchmark_group("snapshot");

    for &size in &[1_000, 10_000] {
        let (keys_owned, vals_owned) = gen_data(size);
        let keys = borrow(&keys_owned);
        let vals = borrow(&vals_owned);

        let sl = ConcurrentSkipList::new();
        for i in 0..size {
            sl.insert(keys[i], vals[i]);
        }

        group.bench_with_input(
            BenchmarkId::new("fastskip_snapshot_iter", size),
            &size,
            |b, _| {
                b.iter(|| {
                    let snap = sl.snapshot();
                    let count = snap.iter().count();
                    black_box(count);
                });
            },
        );
    }

    group.finish();
}

// ---------- SEAL ----------

fn bench_seal(c: &mut Criterion) {
    let mut group = c.benchmark_group("seal");

    for &size in &[1_000, 10_000] {
        let (keys_owned, vals_owned) = gen_data(size);
        let keys = borrow(&keys_owned);
        let vals = borrow(&vals_owned);

        group.bench_with_input(BenchmarkId::new("seal_and_iter", size), &size, |b, _| {
            b.iter(|| {
                let sl = ConcurrentSkipList::new();
                for i in 0..size {
                    sl.insert(keys[i], vals[i]);
                }
                let (frozen, _fresh) = sl.seal().unwrap();
                let count = frozen.iter().count();
                black_box(count);
            });
        });
    }

    group.finish();
}

// ---------- REGISTER ----------

criterion_group!(
    benches,
    bench_insert_sequential,
    bench_insert_random,
    bench_get,
    bench_iter,
    bench_concurrent_insert,
    bench_cursor,
    bench_snapshot,
    bench_seal,
);

criterion_main!(benches);
