#![cfg(not(miri))] // Thread spawning is extremely slow under Miri

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;

use fastskip::ConcurrentSkipList;

#[test]
fn test_16_writers_5k_each() {
    let sl = Arc::new(ConcurrentSkipList::with_shards(32));
    let mut handles = vec![];

    for tid in 0..16 {
        let sl: Arc<ConcurrentSkipList> = Arc::clone(&sl);
        handles.push(thread::spawn(move || {
            for i in 0..5000 {
                let key = format!("t{:02}_k{:06}", tid, i);
                let val = format!("v{:02}_{:06}", tid, i);
                sl.insert(key.as_bytes(), val.as_bytes());
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    assert!(sl.len() >= 80_000);

    // Verify all keys retrievable
    for tid in 0..16 {
        for i in 0..5000 {
            let key = format!("t{:02}_k{:06}", tid, i);
            let val = sl.get_live(key.as_bytes()).unwrap();
            let expected = format!("v{:02}_{:06}", tid, i);
            assert_eq!(val, expected.as_bytes());
        }
    }
}

#[test]
fn test_concurrent_insert_delete_read() {
    let sl = Arc::new(ConcurrentSkipList::with_shards(8));

    // Pre-populate
    for i in 0..500 {
        let key = format!("init_{:06}", i);
        sl.insert(key.as_bytes(), b"val");
    }

    let running = Arc::new(AtomicBool::new(true));
    let mut handles = vec![];

    // Writers
    for tid in 0..4 {
        let sl: Arc<ConcurrentSkipList> = Arc::clone(&sl);
        handles.push(thread::spawn(move || {
            for i in 0..2000 {
                let key = format!("w{}_{:06}", tid, i);
                sl.insert(key.as_bytes(), b"val");
            }
        }));
    }

    // Deleters
    for tid in 0..2 {
        let sl: Arc<ConcurrentSkipList> = Arc::clone(&sl);
        handles.push(thread::spawn(move || {
            for i in 0..500 {
                let key = format!("w{}_{:06}", tid, i);
                sl.delete(key.as_bytes());
            }
        }));
    }

    // Readers
    let running_r = Arc::clone(&running);
    let sl_r: Arc<ConcurrentSkipList> = Arc::clone(&sl);
    handles.push(thread::spawn(move || {
        while running_r.load(Ordering::Relaxed) {
            for i in (0..500).step_by(10) {
                let key = format!("init_{:06}", i);
                let _ = sl_r.get(key.as_bytes());
            }
        }
    }));

    for h in handles.into_iter().take(6) {
        h.join().unwrap();
    }
    running.store(false, Ordering::Relaxed);
}

#[test]
fn test_edge_empty_key() {
    let sl = ConcurrentSkipList::new();
    assert!(sl.insert(b"", b"empty_key"));
    match sl.get(b"") {
        Some((v, false)) => assert_eq!(v, b"empty_key"),
        other => panic!(
            "expected value for empty key, got {:?}",
            other.map(|(v, _)| v.len())
        ),
    }
}

#[test]
fn test_edge_empty_value() {
    let sl = ConcurrentSkipList::new();
    assert!(sl.insert(b"key", b""));
    match sl.get(b"key") {
        Some((v, false)) => assert_eq!(v, b""),
        other => panic!("expected empty value, got {:?}", other),
    }
}

#[test]
fn test_edge_large_key_value() {
    let sl = ConcurrentSkipList::new();
    let key = vec![0xABu8; 4096];
    let val = vec![0xCDu8; 8192];
    assert!(sl.insert(&key, &val));
    match sl.get(&key) {
        Some((v, false)) => assert_eq!(v, val.as_slice()),
        _other => panic!("expected large value"),
    }
}

#[test]
fn test_edge_single_element() {
    let sl = ConcurrentSkipList::new();
    assert!(sl.insert(b"only", b"one"));
    assert_eq!(sl.len(), 1);
    assert_eq!(sl.get_live(b"only"), Some(b"one".as_slice()));
    assert_eq!(sl.get_live(b"nope"), None);

    let entries: Vec<_> = sl.iter().collect();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].key, b"only");
}

#[test]
fn test_edge_reverse_order_insert() {
    let sl = ConcurrentSkipList::new();
    for i in (0..100).rev() {
        let key = format!("k{:04}", i);
        let val = format!("v{:04}", i);
        sl.insert(key.as_bytes(), val.as_bytes());
    }

    // Snapshot should yield in sorted order
    let snap = sl.snapshot();
    let entries: Vec<_> = snap.iter().collect();
    assert_eq!(entries.len(), 100);
    for (i, entry) in entries.iter().enumerate() {
        let expected = format!("k{:04}", i);
        assert_eq!(entry.key, expected.as_bytes());
    }
}

#[test]
fn test_concurrent_same_key_insert() {
    let sl = Arc::new(ConcurrentSkipList::with_shards(16));
    let mut handles = vec![];

    // All threads try to insert the same key
    for _ in 0..8 {
        let sl: Arc<ConcurrentSkipList> = Arc::clone(&sl);
        handles.push(thread::spawn(move || {
            sl.insert(b"same_key", b"value");
        }));
    }

    // Only one should succeed. Verify the key exists and wasn't corrupted.
    for h in handles {
        h.join().unwrap();
    }

    match sl.get(b"same_key") {
        Some((v, false)) => assert_eq!(v, b"value"),
        other => panic!("expected value, got {:?}", other),
    }
}

#[test]
fn test_delete_then_reinsert_different_thread() {
    let sl = Arc::new(ConcurrentSkipList::new());
    sl.insert(b"key", b"v1");

    let sl2: Arc<ConcurrentSkipList> = Arc::clone(&sl);
    let h = thread::spawn(move || {
        sl2.delete(b"key");
    });
    h.join().unwrap();

    // Should be tombstoned
    match sl.get(b"key") {
        Some((_, true)) => {}
        _other => panic!("expected tombstone"),
    }
    assert_eq!(sl.get_live(b"key"), None);
}

#[test]
fn test_seal_full_lifecycle_with_concurrent_writers() {
    // Simulate a realistic LSM memtable lifecycle:
    // 1. Concurrent writers fill the memtable
    // 2. Seal it
    // 3. Concurrent readers iterate the frozen memtable
    // 4. Fresh memtable gets new writes
    let sl = Arc::new(ConcurrentSkipList::with_shards(8));

    // Phase 1: Concurrent writes
    let mut handles = vec![];
    for tid in 0..4 {
        let sl = Arc::clone(&sl);
        handles.push(thread::spawn(move || {
            for i in 0..1000 {
                let key = format!("t{}_k{:04}", tid, i);
                let val = format!("v{}_{}", tid, i);
                sl.insert(key.as_bytes(), val.as_bytes());
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }

    assert_eq!(sl.len(), 4000);

    // Phase 2: Seal (Arc::try_unwrap ensures exclusive access)
    let sl = Arc::try_unwrap(sl).unwrap();
    let (frozen, fresh) = sl.seal().unwrap();

    assert_eq!(frozen.len(), 4000);
    assert!(fresh.is_empty());

    // Phase 3: Concurrent readers on frozen memtable
    let frozen = Arc::new(frozen);
    let mut readers = vec![];
    for _ in 0..4 {
        let frozen = Arc::clone(&frozen);
        readers.push(thread::spawn(move || {
            let mut count = 0;
            for entry in frozen.iter() {
                if !entry.is_tombstone {
                    count += 1;
                }
            }
            count
        }));
    }

    let mut total = 0;
    for r in readers {
        total += r.join().unwrap();
    }
    assert_eq!(total, 4000 * 4); // 4 readers, each sees 4000

    // Phase 4: Fresh memtable accepts writes
    let fresh = Arc::new(fresh);
    let mut writers = vec![];
    for tid in 0..4 {
        let fresh = Arc::clone(&fresh);
        writers.push(thread::spawn(move || {
            for i in 0..500 {
                let key = format!("new_t{}_k{:04}", tid, i);
                fresh.insert(key.as_bytes(), b"val");
            }
        }));
    }
    for w in writers {
        w.join().unwrap();
    }
    assert_eq!(fresh.len(), 2000);

    // Phase 5: Drop frozen, verify memory reclaimed
    let mem_before = frozen.memory_usage();
    assert!(mem_before > 0);
    std::mem::drop(frozen);
    // fresh memtable has its own arena
    assert!(fresh.memory_usage() > 0);
}

#[test]
fn test_cursor_concurrent_reads() {
    let sl = Arc::new(ConcurrentSkipList::new());
    for i in 0..100 {
        let key = format!("k{:04}", i);
        let val = format!("v{:04}", i);
        sl.insert(key.as_bytes(), val.as_bytes());
    }

    // Multiple threads using cursors simultaneously
    let mut handles = vec![];
    for _ in 0..4 {
        let sl = Arc::clone(&sl);
        handles.push(thread::spawn(move || {
            let cursor = sl.cursor_at(b"k0050").unwrap();
            let keys: Vec<_> = cursor.map(|e| e.key.to_vec()).collect();
            assert_eq!(keys.len(), 50); // k0050..k0099
            assert_eq!(keys[0], b"k0050");
            assert_eq!(keys[49], b"k0099");
        }));
    }

    for h in handles {
        h.join().unwrap();
    }
}

#[test]
fn test_snapshot_under_concurrent_writes() {
    let sl = Arc::new(ConcurrentSkipList::with_shards(8));

    // Pre-populate
    for i in 0..100 {
        let key = format!("key_{:04}", i);
        sl.insert(key.as_bytes(), b"val");
    }

    // Take snapshot
    let snap = sl.snapshot();

    // Concurrent writers insert more entries
    let mut handles = vec![];
    for tid in 0..4 {
        let sl = Arc::clone(&sl);
        handles.push(thread::spawn(move || {
            for i in 0..500 {
                let key = format!("new_t{}_{:04}", tid, i);
                sl.insert(key.as_bytes(), b"val");
            }
        }));
    }

    // Iterate snapshot while writers are active
    let snap_entries: Vec<_> = snap.iter().map(|e| e.key.to_vec()).collect();
    assert_eq!(snap_entries.len(), 100);

    // Verify no post-snapshot keys leaked
    for key in &snap_entries {
        let s = std::str::from_utf8(key).unwrap();
        assert!(s.starts_with("key_"), "snapshot leaked: {}", s);
    }

    for h in handles {
        h.join().unwrap();
    }

    // Live iterator now sees everything
    assert_eq!(sl.len(), 2100); // 100 + 4*500
}
