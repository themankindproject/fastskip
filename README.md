# fastskip

Lock-free arena-backed skip list for LSM-tree memtables.

## Features

- **Lock-free writes** — Multiple threads insert/delete concurrently via CAS at level 0
- **Lock-free reads** — Point lookups walk the skip list without locks
- **Snapshot isolation** — Point-in-time snapshots remain consistent under concurrent writes
- **Range scans** — Cursor with lower-bound seek for prefix/range iteration
- **Safe lifecycle** — `seal()` freezes the memtable for flushing and returns a fresh one
- **Arena memory** — Per-thread arena shards, bulk-reclaimed on drop

## Quick start

```rust
use fastskip::ConcurrentSkipList;

let sl = ConcurrentSkipList::new();

sl.insert(b"user:1001", b"alice");
sl.insert(b"user:1002", b"bob");

let (val, tombstone) = sl.get(b"user:1001").unwrap();
assert_eq!(val, b"alice");

sl.delete(b"user:1002");

for entry in sl.iter() {
    println!("{:?} -> {:?}", entry.key, entry.value);
}
```

## LSM memtable lifecycle

```rust
use fastskip::ConcurrentSkipList;

// 1. Create active memtable
let memtable = ConcurrentSkipList::with_shards(4);

// 2. Concurrent writers
memtable.insert(b"key1", b"val1");

// 3. Seal — returns frozen (for flush) + fresh (for writes)
let (frozen, fresh) = memtable.seal().unwrap();

// 4. Flush frozen to SSTable
for entry in frozen.iter() {
    // write to SSTable...
}

// 5. Drop frozen, use fresh
drop(frozen);
fresh.insert(b"key2", b"val2");
```

## Range scans

```rust
use fastskip::ConcurrentSkipList;

let sl = ConcurrentSkipList::new();
sl.insert(b"apple", b"1");
sl.insert(b"banana", b"2");
sl.insert(b"cherry", b"3");

if let Some(cursor) = sl.cursor_at(b"banana") {
    for entry in cursor {
        println!("{:?}", entry.key);
    }
    // prints: b"banana", b"cherry"
}
```

## Configuration

```rust
use fastskip::ConcurrentSkipList;

// Default: CPU count shards, 64KB initial arena per shard
let sl = ConcurrentSkipList::new();

// Custom: 8 shards, 1MB initial arena per shard
let sl = ConcurrentSkipList::with_capacity_and_shards(1024 * 1024, 8);
```

## Design

| Property | Detail |
|----------|--------|
| Algorithm | Lock-free skip list (RocksDB InlineSkipList / CockroachDB arenaskl pattern) |
| Concurrency | Single CAS at level 0, best-effort CAS at upper levels |
| Memory | Per-thread arena shards, no per-node free |
| Ordering | Lexicographic byte ordering |
| Duplicates | Rejected (first value wins) |
| Delete | Tombstone (entry stays, marked deleted) |
| Insert after delete | Rejected in same memtable — use `seal()` + new memtable |

## Testing

```bash
cargo test              # 69 tests (unit, concurrent, stress, correctness)
cargo clippy            # zero warnings
cargo run --example basic
cargo run --example lsm_memtable
```

## License

MIT
