//! Point-in-time snapshot correctness under concurrent writes.
//!
//! Demonstrates that a snapshot taken at time T sees only entries
//! that existed at or before T, even if other threads continue
//! inserting after the snapshot is taken.

use std::sync::Arc;
use std::thread;

use fastskip::ConcurrentSkipList;

fn main() {
    let sl = Arc::new(ConcurrentSkipList::with_shards(4));

    // ── Phase 1: Initial population ───────────────────────────────────────
    for i in 0..100 {
        let key = format!("key_{:04}", i);
        let val = format!("val_{:04}", i);
        sl.insert(key.as_bytes(), val.as_bytes());
    }

    // ── Phase 2: Take snapshot ────────────────────────────────────────────
    let snapshot = sl.snapshot();
    println!("Snapshot taken. Visible entries: counted at snapshot time.");

    // ── Phase 3: Concurrent inserts AFTER snapshot ────────────────────────
    let writer = {
        let sl = Arc::clone(&sl);
        thread::spawn(move || {
            for i in 100..200 {
                let key = format!("key_{:04}", i);
                let val = format!("val_{:04}", i);
                sl.insert(key.as_bytes(), val.as_bytes());
                thread::sleep(std::time::Duration::from_micros(10));
            }
        })
    };

    // Give the writer some time to insert
    thread::sleep(std::time::Duration::from_millis(1));

    // ── Phase 4: Iterate snapshot while writer is active ──────────────────
    // The snapshot MUST only see entries 0..99, never 100+.
    let snap_entries: Vec<_> = snapshot.iter().map(|e| e.key.to_vec()).collect();
    println!("Snapshot saw {} entries (expected 100)", snap_entries.len());

    // Verify: every snapshot key is in range [key_0000, key_0099]
    for key in &snap_entries {
        let s = std::str::from_utf8(key).unwrap();
        let idx: usize = s[5..].parse().unwrap();
        assert!(idx < 100, "snapshot leaked post-snapshot key: {}", s);
    }

    writer.join().unwrap();

    // ── Phase 5: Live iterator sees everything ────────────────────────────
    let live_count = sl.iter().count();
    println!("Live iterator saw {} entries (expected 200)", live_count);
    assert_eq!(live_count, 200);

    println!("Snapshot isolation verified.");
}
