//! LSM-tree memtable lifecycle: write -> seal -> flush -> drop.
//!
//! This is the primary use case for ConcurrentSkipList. An LSM engine
//! writes key-value pairs into the active memtable. When the memtable
//! is full, it is sealed (frozen), a point-in-time snapshot is taken,
//! the snapshot is flushed to an SSTable on disk, and the frozen
//! memtable is dropped.

use fastskip::ConcurrentSkipList;

fn main() {
    // ── 1. Create the active memtable ─────────────────────────────────────
    // Use enough shards for your writer thread count.
    let memtable = ConcurrentSkipList::with_shards(4);

    // ── 2. Write path: insert key-value pairs ─────────────────────────────
    // Multiple threads can call insert() concurrently.
    let entries: Vec<(&[u8], &[u8])> = vec![
        (b"event:001", b"{\"ts\":1000,\"type\":\"click\"}"),
        (b"event:002", b"{\"ts\":1001,\"type\":\"view\"}"),
        (b"event:003", b"{\"ts\":1002,\"type\":\"click\"}"),
        (b"event:004", b"{\"ts\":1003,\"type\":\"purchase\"}"),
        (b"event:005", b"{\"ts\":1004,\"type\":\"click\"}"),
    ];

    for (key, val) in &entries {
        if !memtable.insert(key, val) {
            eprintln!("insert failed (OOM or duplicate): {:?}", key);
        }
    }

    println!(
        "Active memtable: {} entries, {} bytes",
        memtable.len(),
        memtable.memory_usage()
    );

    // Deletes are supported via tombstones
    memtable.delete(b"event:002");

    // ── 3. Seal: freeze the memtable for flushing ─────────────────────────
    // seal() returns a FrozenMemtable (read-only, for flushing) and a
    // fresh ConcurrentSkipList (for new writes). No unsafe code needed.
    let (frozen, fresh_memtable) = memtable.seal().unwrap();

    // Writes to the fresh memtable go into the new one
    fresh_memtable.insert(b"event:006", b"{\"ts\":1005,\"type\":\"view\"}");
    fresh_memtable.insert(b"event:007", b"{\"ts\":1006,\"type\":\"click\"}");

    // ── 4. Flush: iterate the frozen memtable to write SSTable ────────────
    println!("\nFrozen memtable (flush to disk):");
    let mut flushed = 0;
    for entry in frozen.iter() {
        if entry.is_tombstone {
            println!("  DELETE {}", std::str::from_utf8(entry.key).unwrap());
        } else {
            println!(
                "  PUT {} -> {}",
                std::str::from_utf8(entry.key).unwrap(),
                std::str::from_utf8(entry.value).unwrap(),
            );
        }
        flushed += 1;
    }
    println!("Flushed {} entries to SSTable", flushed);

    // ── 5. Drop: reclaim memory after flush ───────────────────────────────
    std::mem::drop(frozen);

    println!(
        "\nFresh memtable after drop: len={}, memory={} bytes",
        fresh_memtable.len(),
        fresh_memtable.memory_usage()
    );
}
