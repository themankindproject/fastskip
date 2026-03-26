use fastskip::ConcurrentSkipList;

#[test]
fn test_delete_nonexistent_key() {
    let sl = ConcurrentSkipList::new();
    assert!(!sl.delete(b"nonexistent"));
}

#[test]
fn test_double_delete() {
    let sl = ConcurrentSkipList::new();
    sl.insert(b"key", b"val");
    assert!(sl.delete(b"key"));
    assert!(!sl.delete(b"key")); // already tombstoned
}

#[test]
fn test_get_after_delete_returns_tombstone() {
    let sl = ConcurrentSkipList::new();
    sl.insert(b"key", b"val");
    sl.delete(b"key");

    match sl.get(b"key") {
        Some((v, true)) => {
            assert_eq!(v, b"val");
        }
        other => panic!(
            "expected tombstone, got {:?}",
            other.map(|(v, t)| (std::str::from_utf8(v).unwrap(), t))
        ),
    }
}

#[test]
fn test_get_live_after_delete_returns_none() {
    let sl = ConcurrentSkipList::new();
    sl.insert(b"key", b"val");
    sl.delete(b"key");

    assert_eq!(sl.get_live(b"key"), None);
}

#[test]
fn test_insert_after_delete_rejected() {
    let sl = ConcurrentSkipList::new();
    sl.insert(b"key", b"old");
    sl.delete(b"key");
    // Duplicate key — should be rejected (old tombstoned entry still exists)
    assert!(!sl.insert(b"key", b"new"));
}

#[test]
fn test_tombstone_in_iter() {
    let sl = ConcurrentSkipList::new();
    sl.insert(b"a", b"1");
    sl.insert(b"b", b"2");
    sl.insert(b"c", b"3");
    sl.delete(b"b");

    let entries: Vec<_> = sl.iter().collect();
    assert_eq!(entries.len(), 3);

    let b_entry = entries.iter().find(|e| e.key == b"b").unwrap();
    assert!(b_entry.is_tombstone);

    let a_entry = entries.iter().find(|e| e.key == b"a").unwrap();
    assert!(!a_entry.is_tombstone);

    let c_entry = entries.iter().find(|e| e.key == b"c").unwrap();
    assert!(!c_entry.is_tombstone);
}

#[test]
fn test_delete_all_keys() {
    let sl = ConcurrentSkipList::new();
    for i in 0..100 {
        let key = format!("key_{:04}", i);
        sl.insert(key.as_bytes(), b"val");
    }

    for i in 0..100 {
        let key = format!("key_{:04}", i);
        assert!(sl.delete(key.as_bytes()));
    }

    for i in 0..100 {
        let key = format!("key_{:04}", i);
        assert_eq!(sl.get_live(key.as_bytes()), None);
    }

    let entries: Vec<_> = sl.iter().collect();
    assert_eq!(entries.len(), 100);
    for entry in &entries {
        assert!(entry.is_tombstone);
    }
}
