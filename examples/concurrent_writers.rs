//! Multi-threaded concurrent writes with a ConcurrentSkipList.
//!
//! Demonstrates how multiple writer threads share a single skip list
//! using Arc. Each thread gets its own arena shard for allocation,
//! avoiding contention on the arena. The skip list's lock-free CAS
//! at level 0 provides linearizable inserts without mutexes.

use std::sync::Arc;
use std::thread;

use fastskip::ConcurrentSkipList;

fn main() {
    let num_writers = 4;
    let keys_per_writer = 10_000;

    // Shard count MUST be >= number of writer threads.
    // Each thread gets a dedicated arena shard for allocation.
    let sl = Arc::new(ConcurrentSkipList::with_shards(num_writers));

    println!(
        "Starting {} writers, {} keys each...",
        num_writers, keys_per_writer
    );
    let start = std::time::Instant::now();

    let mut handles = vec![];
    for tid in 0..num_writers {
        let sl = Arc::clone(&sl);
        handles.push(thread::spawn(move || {
            for i in 0..keys_per_writer {
                let key = format!("t{:02}_k{:06}", tid, i);
                let val = format!("v{:02}_{:06}", tid, i);
                // insert() returns false on OOM or duplicate.
                // try_insert() distinguishes the two failure cases.
                if !sl.insert(key.as_bytes(), val.as_bytes()) {
                    // In a real engine, you'd handle OOM by flushing
                    // or splitting to a new memtable.
                }
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    let elapsed = start.elapsed();
    let total = num_writers * keys_per_writer;
    println!(
        "Inserted {} entries in {:.2?} ({:.0} ops/sec)",
        total,
        elapsed,
        total as f64 / elapsed.as_secs_f64(),
    );

    // Verify some entries from each writer
    for tid in 0..num_writers {
        let key = format!("t{:02}_k{:06}", tid, 0);
        assert!(
            sl.get(key.as_bytes()).is_some(),
            "missing key from writer {}",
            tid
        );
        let key = format!("t{:02}_k{:06}", tid, keys_per_writer - 1);
        assert!(
            sl.get(key.as_bytes()).is_some(),
            "missing key from writer {}",
            tid
        );
    }

    println!(
        "All entries verified. len={}, memory={} bytes",
        sl.len(),
        sl.memory_usage()
    );
}
