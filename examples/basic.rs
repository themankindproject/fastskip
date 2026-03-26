//! Basic CRUD operations on a ConcurrentSkipList.
//!
//! Demonstrates insert, get, get_live, delete, iteration, cursors, and sealing.

use fastskip::ConcurrentSkipList;

fn main() {
    let sl = ConcurrentSkipList::new();

    // ── Insert ────────────────────────────────────────────────────────────
    sl.insert(b"user:1001", b"alice");
    sl.insert(b"user:1002", b"bob");
    sl.insert(b"user:1003", b"charlie");
    sl.insert(b"user:1004", b"diana");
    sl.insert(b"user:1005", b"eve");

    // Duplicate keys are rejected (first value wins)
    assert!(!sl.insert(b"user:1001", b"ALICE"));

    // ── Point lookup ──────────────────────────────────────────────────────
    let (val, tombstone) = sl.get(b"user:1002").unwrap();
    assert_eq!(val, b"bob");
    assert!(!tombstone);

    assert_eq!(sl.get_live(b"user:1002"), Some(b"bob".as_slice()));
    assert_eq!(sl.get_live(b"user:9999"), None);

    // ── Delete (tombstone) ────────────────────────────────────────────────
    assert!(sl.delete(b"user:1002")); // returns true: found and tombstoned
    assert!(!sl.delete(b"user:1002")); // already tombstoned

    let (_, tomb) = sl.get(b"user:1002").unwrap();
    assert!(tomb);
    assert_eq!(sl.get_live(b"user:1002"), None);

    // ── Iteration ─────────────────────────────────────────────────────────
    println!("All entries (including tombstones):");
    for entry in sl.iter() {
        let status = if entry.is_tombstone {
            " [TOMBSTONE]"
        } else {
            ""
        };
        println!(
            "  {} -> {}{}",
            std::str::from_utf8(entry.key).unwrap(),
            std::str::from_utf8(entry.value).unwrap(),
            status,
        );
    }

    // ── Cursor (range scan) ───────────────────────────────────────────────
    println!("\nCursor scan from user:1003:");
    if let Some(cursor) = sl.cursor_at(b"user:1003") {
        for entry in cursor {
            if !entry.is_tombstone {
                println!(
                    "  {} -> {}",
                    std::str::from_utf8(entry.key).unwrap(),
                    std::str::from_utf8(entry.value).unwrap(),
                );
            }
        }
    }

    // ── Snapshot ──────────────────────────────────────────────────────────
    let snap = sl.snapshot();
    println!(
        "\nSnapshot sees {} entries (including tombstones)",
        snap.iter().count()
    );

    // ── Seal ──────────────────────────────────────────────────────────────
    let (frozen, fresh) = sl.seal().unwrap();

    println!(
        "\nFrozen memtable: len={}, memory={} bytes",
        frozen.len(),
        frozen.memory_usage()
    );
    println!(
        "Fresh memtable: len={}, memory={} bytes",
        fresh.len(),
        fresh.memory_usage()
    );

    // Fresh memtable accepts new writes
    fresh.insert(b"user:2001", b"frank");
    println!("After insert: fresh len={}", fresh.len());

    // Drop frozen to reclaim memory
    std::mem::drop(frozen);
    println!("\nFrozen dropped. Fresh is ready for production use.");
}
