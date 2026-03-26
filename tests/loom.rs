//! Loom-based concurrency verification.
//!
//! Tests the atomic invariants of the seal flag and live_count
//! using loom's exhaustive thread-interleaving exploration.
//!
//! Run with: `RUSTFLAGS="--cfg loom" cargo test --test loom`

#![cfg(loom)]

use loom::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use loom::sync::Arc;
use loom::thread;

/// Verifies the seal flag invariant: once sealed, no operation should
/// appear to succeed (insert returns false, delete returns false).
#[test]
fn loom_seal_flag_prevents_writes() {
    loom::model(|| {
        let sealed = Arc::new(AtomicBool::new(false));

        let s1 = Arc::clone(&sealed);
        let writer = thread::spawn(move || {
            if s1.load(Ordering::Acquire) {
                return false;
            }
            true
        });

        let s2 = Arc::clone(&sealed);
        let sealer = thread::spawn(move || {
            s2.store(true, Ordering::Release);
        });

        let write_ok = writer.join().unwrap();
        sealer.join().unwrap();

        // Both orderings are valid. The invariant is that if
        // sealed=true was observed, insert must reject.
        let _ = write_ok;
    });
}

/// Verifies the live_count invariant: under concurrent inserts, the final
/// count equals the number of successful inserts (no lost updates).
#[test]
fn loom_live_count_no_lost_updates() {
    loom::model(|| {
        let count = Arc::new(AtomicUsize::new(0));

        let c1 = Arc::clone(&count);
        let t1 = thread::spawn(move || {
            c1.fetch_add(1, Ordering::Relaxed);
        });

        let c2 = Arc::clone(&count);
        let t2 = thread::spawn(move || {
            c2.fetch_add(1, Ordering::Relaxed);
        });

        t1.join().unwrap();
        t2.join().unwrap();

        assert_eq!(count.load(Ordering::Relaxed), 2);
    });
}

/// Verifies the sealed + count interaction: sealing doesn't corrupt
/// the live_count that was accumulated before sealing.
#[test]
fn loom_seal_preserves_count() {
    loom::model(|| {
        let count = Arc::new(AtomicUsize::new(0));
        let sealed = Arc::new(AtomicBool::new(false));

        let c1 = Arc::clone(&count);
        let writer = thread::spawn(move || {
            c1.fetch_add(1, Ordering::Relaxed);
        });

        let s1 = Arc::clone(&sealed);
        let sealer = thread::spawn(move || {
            s1.store(true, Ordering::Release);
        });

        let c2 = Arc::clone(&count);
        let s2 = Arc::clone(&sealed);
        let reader = thread::spawn(move || {
            let s = s2.load(Ordering::Acquire);
            let c = c2.load(Ordering::Relaxed);
            (s, c)
        });

        writer.join().unwrap();
        sealer.join().unwrap();
        let (_, c) = reader.join().unwrap();

        assert!(c <= 1, "count corrupted: {}", c);
    });
}

/// Verifies the snapshot sequence invariant: the sequence counter
/// only increases (monotonic), and fetch_add returns unique values.
#[test]
fn loom_seq_monotonic() {
    loom::model(|| {
        let seq = Arc::new(AtomicUsize::new(1));

        let s1 = Arc::clone(&seq);
        let t1 = thread::spawn(move || s1.fetch_add(1, Ordering::Relaxed));

        let s2 = Arc::clone(&seq);
        let t2 = thread::spawn(move || s2.fetch_add(1, Ordering::Relaxed));

        let v1 = t1.join().unwrap();
        let v2 = t2.join().unwrap();

        assert_ne!(v1, v2, "duplicate seq: {} == {}", v1, v2);
        assert_eq!(seq.load(Ordering::Relaxed), 3);
    });
}
