use fastskip::ConcurrentSkipList;

#[test]
fn test_insert_10k_sequential() {
    let sl = ConcurrentSkipList::new();
    for i in 0..10_000 {
        let key = format!("key_{:06}", i);
        let val = format!("val_{:06}", i);
        assert!(sl.insert(key.as_bytes(), val.as_bytes()));
    }

    for i in 0..10_000 {
        let key = format!("key_{:06}", i);
        let expected = format!("val_{:06}", i);
        match sl.get(key.as_bytes()) {
            Some((v, false)) => assert_eq!(v, expected.as_bytes(), "mismatch at key {}", i),
            other => panic!("expected value for key {}, got {:?}", i, other),
        }
    }

    assert_eq!(sl.len(), 10_000);
}

#[test]
fn test_insert_10k_random() {
    use std::collections::HashSet;

    let sl = ConcurrentSkipList::new();
    let mut keys = HashSet::new();

    for i in 0u64..10_000 {
        let k = (i.wrapping_mul(2654435761) % 10_000) as u32;
        let key = format!("key_{:06}", k);
        let val = format!("val_{:06}", k);
        if keys.insert(k) {
            assert!(sl.insert(key.as_bytes(), val.as_bytes()));
        } else {
            assert!(!sl.insert(key.as_bytes(), val.as_bytes()));
        }
    }

    for &k in &keys {
        let key = format!("key_{:06}", k);
        let expected = format!("val_{:06}", k);
        match sl.get(key.as_bytes()) {
            Some((v, false)) => assert_eq!(v, expected.as_bytes()),
            other => panic!("expected value for key {}, got {:?}", k, other),
        }
    }
}

#[test]
fn test_delete_half() {
    let sl = ConcurrentSkipList::new();

    for i in 0..1000 {
        let key = format!("key_{:04}", i);
        let val = format!("val_{:04}", i);
        sl.insert(key.as_bytes(), val.as_bytes());
    }

    for i in (0..1000).step_by(2) {
        let key = format!("key_{:04}", i);
        assert!(sl.delete(key.as_bytes()));
    }

    for i in 0..1000 {
        let key = format!("key_{:04}", i);
        if i % 2 == 0 {
            match sl.get(key.as_bytes()) {
                Some((_, true)) => {}
                other => panic!("expected tombstone for key {}, got {:?}", i, other),
            }
            assert_eq!(sl.get_live(key.as_bytes()), None);
        } else {
            let expected = format!("val_{:04}", i);
            match sl.get(key.as_bytes()) {
                Some((v, false)) => assert_eq!(v, expected.as_bytes()),
                other => panic!("expected live value for key {}, got {:?}", i, other),
            }
        }
    }
}

#[test]
fn test_snapshot_consistency() {
    let sl = ConcurrentSkipList::new();
    for i in 0..100 {
        let key = format!("key_{:04}", i);
        let val = format!("val_{:04}", i);
        sl.insert(key.as_bytes(), val.as_bytes());
    }

    let snap = sl.snapshot();
    let entries: Vec<_> = snap.iter().collect();
    assert_eq!(entries.len(), 100);

    for (i, entry) in entries.iter().enumerate() {
        let expected_key = format!("key_{:04}", i);
        assert_eq!(entry.key, expected_key.as_bytes());
    }
}

#[test]
fn test_get_live_returns_none_for_missing() {
    let sl = ConcurrentSkipList::new();
    assert_eq!(sl.get_live(b"nonexistent"), None);
    assert_eq!(sl.get(b"nonexistent"), None);
}

#[test]
fn test_duplicate_insert() {
    let sl = ConcurrentSkipList::new();
    assert!(sl.insert(b"key", b"val1"));
    assert!(!sl.insert(b"key", b"val2"));

    match sl.get(b"key") {
        Some((v, false)) => assert_eq!(v, b"val1"),
        other => panic!("expected val1, got {:?}", other),
    }
}

#[test]
fn test_memory_usage() {
    let sl = ConcurrentSkipList::new();
    let initial = sl.memory_usage();

    for i in 0..100 {
        let key = format!("key_{:04}", i);
        let val = format!("val_{:04}", i);
        sl.insert(key.as_bytes(), val.as_bytes());
    }

    let after = sl.memory_usage();
    assert!(after > initial, "memory should increase after inserts");
}
