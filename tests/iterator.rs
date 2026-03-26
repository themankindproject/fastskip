use fastskip::ConcurrentSkipList;

#[test]
fn test_snapshot_sees_consistent_state() {
    let sl = ConcurrentSkipList::new();
    for i in 0..50 {
        let key = format!("key_{:04}", i);
        let val = format!("val_{:04}", i);
        sl.insert(key.as_bytes(), val.as_bytes());
    }

    let snap = sl.snapshot();
    let entries: Vec<_> = snap.iter().collect();
    assert_eq!(entries.len(), 50);

    for entry in &entries {
        assert!(!entry.is_tombstone);
    }
}

#[test]
fn test_empty_list_iter() {
    let sl = ConcurrentSkipList::new();
    let entries: Vec<_> = sl.iter().collect();
    assert!(entries.is_empty());

    let snap = sl.snapshot();
    let snap_entries: Vec<_> = snap.iter().collect();
    assert!(snap_entries.is_empty());
}

#[test]
fn test_iter_order() {
    let sl = ConcurrentSkipList::new();
    sl.insert(b"c", b"3");
    sl.insert(b"a", b"1");
    sl.insert(b"b", b"2");

    let snap = sl.snapshot();
    let entries: Vec<_> = snap.iter().collect();
    assert_eq!(entries.len(), 3);
    assert_eq!(entries[0].key, b"a");
    assert_eq!(entries[1].key, b"b");
    assert_eq!(entries[2].key, b"c");
}

#[test]
fn test_iter_yields_tombstones() {
    let sl = ConcurrentSkipList::new();
    sl.insert(b"key1", b"val1");
    sl.insert(b"key2", b"val2");
    sl.delete(b"key1");

    let entries: Vec<_> = sl.iter().collect();
    assert_eq!(entries.len(), 2);

    let key1_entry = entries.iter().find(|e| e.key == b"key1").unwrap();
    assert!(key1_entry.is_tombstone);

    let key2_entry = entries.iter().find(|e| e.key == b"key2").unwrap();
    assert!(!key2_entry.is_tombstone);
}

#[test]
fn test_live_iter_skips_nothing() {
    let sl = ConcurrentSkipList::new();
    for i in 0..20 {
        let key = format!("k{:02}", i);
        let val = format!("v{:02}", i);
        sl.insert(key.as_bytes(), val.as_bytes());
    }

    let live_entries: Vec<_> = sl.iter().collect();
    assert_eq!(live_entries.len(), 20);
}

#[test]
fn test_snapshot_after_delete() {
    let sl = ConcurrentSkipList::new();
    sl.insert(b"keep", b"yes");
    sl.insert(b"remove", b"no");
    sl.delete(b"remove");

    let snap = sl.snapshot();
    let entries: Vec<_> = snap.iter().collect();

    assert_eq!(entries.len(), 2);

    let keep = entries.iter().find(|e| e.key == b"keep").unwrap();
    assert!(!keep.is_tombstone);
    assert_eq!(keep.value, b"yes");

    let remove = entries.iter().find(|e| e.key == b"remove").unwrap();
    assert!(remove.is_tombstone);
}
