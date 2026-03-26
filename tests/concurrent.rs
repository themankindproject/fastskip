use std::sync::Arc;
use std::thread;

use fastskip::ConcurrentSkipList;

#[test]
fn test_4_writer_threads_1k_each() {
    let mt = Arc::new(ConcurrentSkipList::with_shards(8));
    let mut handles = vec![];

    for tid in 0..4 {
        let mt: Arc<ConcurrentSkipList> = Arc::clone(&mt);
        handles.push(thread::spawn(move || {
            for i in 0..1000 {
                let key = format!("t{}_k{:04}", tid, i);
                let val = format!("t{}_v{:04}", tid, i);
                mt.insert(key.as_bytes(), val.as_bytes());
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    // Verify all keys are retrievable
    for tid in 0..4 {
        for i in 0..1000 {
            let key = format!("t{}_k{:04}", tid, i);
            match mt.get(key.as_bytes()) {
                Some((v, false)) => {
                    let expected = format!("t{}_v{:04}", tid, i);
                    assert_eq!(v, expected.as_bytes());
                }
                other => panic!("expected value for t{} k{}, got {:?}", tid, i, other),
            }
        }
    }

    assert!(mt.len() >= 4000);
}

#[test]
fn test_8_writer_threads_1k_each() {
    let mt = Arc::new(ConcurrentSkipList::with_shards(32));
    let mut handles = vec![];

    for tid in 0..8 {
        let mt: Arc<ConcurrentSkipList> = Arc::clone(&mt);
        handles.push(thread::spawn(move || {
            for i in 0..1000 {
                let key = format!("t{}_k{:06}", tid, i);
                let val = format!("t{}_v{:06}", tid, i);
                mt.insert(key.as_bytes(), val.as_bytes());
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    // Spot check some keys
    for tid in 0..8 {
        let key = format!("t{}_k000000", tid);
        assert!(mt.get(key.as_bytes()).is_some());
    }

    assert!(mt.len() >= 8000);
}

#[test]
fn test_concurrent_insert_and_read() {
    let mt = Arc::new(ConcurrentSkipList::with_shards(32));

    // Pre-populate
    for i in 0..100 {
        let key = format!("init_{:04}", i);
        let val = format!("val_{:04}", i);
        mt.insert(key.as_bytes(), val.as_bytes());
    }

    // Spawn writers
    let mut handles = vec![];
    for tid in 0..4 {
        let mt: Arc<ConcurrentSkipList> = Arc::clone(&mt);
        handles.push(thread::spawn(move || {
            for i in 0..500 {
                let key = format!("w{}_k{:04}", tid, i);
                let val = format!("w{}_v{:04}", tid, i);
                mt.insert(key.as_bytes(), val.as_bytes());
            }
        }));
    }

    // Spawn readers
    for _ in 0..2 {
        let mt: Arc<ConcurrentSkipList> = Arc::clone(&mt);
        handles.push(thread::spawn(move || {
            for i in 0..100 {
                let key = format!("init_{:04}", i);
                let _ = mt.get(key.as_bytes());
            }
            for _ in 0..100 {
                let _ = mt.get(b"nonexistent_key");
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }
}

#[test]
fn test_no_data_races() {
    let mt = Arc::new(ConcurrentSkipList::with_shards(8));
    let mut handles = vec![];

    // Writers with overlapping key ranges (stress test CAS)
    for tid in 0..4 {
        let mt: Arc<ConcurrentSkipList> = Arc::clone(&mt);
        handles.push(thread::spawn(move || {
            for i in 0..2000 {
                let key = format!("shared_{:06}", i);
                let val = format!("t{}_{:06}", tid, i);
                mt.insert(key.as_bytes(), val.as_bytes());
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    // Each key should have exactly one value (not corrupted)
    for i in 0..2000 {
        let key = format!("shared_{:06}", i);
        match mt.get(key.as_bytes()) {
            Some((v, false)) => {
                let s = std::str::from_utf8(v).unwrap();
                assert!(s.starts_with("t"), "corrupted value: {:?}", s);
            }
            other => panic!("expected value for key {}, got {:?}", i, other),
        }
    }
}

#[test]
fn test_concurrent_snapshot_while_writing() {
    let mt = Arc::new(ConcurrentSkipList::with_shards(4));

    // Pre-populate with known keys
    for i in 0..50 {
        let key = format!("pre_{:04}", i);
        let val = format!("val_{:04}", i);
        mt.insert(key.as_bytes(), val.as_bytes());
    }

    // Take snapshot — should see exactly 50 entries
    let snap = mt.snapshot();
    let snap_entries: Vec<_> = snap.iter().collect();
    assert_eq!(snap_entries.len(), 50);

    // Spawn concurrent writers that add more keys
    let mt2 = Arc::clone(&mt);
    let writer = thread::spawn(move || {
        for i in 0..200 {
            let key = format!("post_{:04}", i);
            let val = format!("val_{:04}", i);
            mt2.insert(key.as_bytes(), val.as_bytes());
        }
    });

    // Iterate snapshot concurrently with writes — must see exactly 50
    let mut count = 0;
    for entry in snap.iter() {
        assert!(
            entry.key.starts_with(b"pre_"),
            "snapshot should not see post_ keys, got {:?}",
            std::str::from_utf8(entry.key).unwrap_or("<non-utf8>")
        );
        count += 1;
    }
    assert_eq!(
        count, 50,
        "snapshot must remain consistent during concurrent writes"
    );

    writer.join().unwrap();

    // After writer finishes, live list has all entries
    let live_count = mt.iter().count();
    assert!(
        live_count >= 250,
        "live list should have >= 250 entries, got {}",
        live_count
    );
}
