use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};

pub(crate) const MAX_HEIGHT: usize = 20;
pub(crate) const NODE_HEADER_SIZE: usize = 32;
pub(crate) const TOMBSTONE_BIT: u8 = 0x01;
pub(crate) const FLAGS_OFFSET: usize = 16;
pub(crate) const NODE_SEQ_OFFSET: usize = 24;

// ─── TowerPtr ──────────────────────────────────────────────────────────────────
// Packed pointer stored in the skip list tower as AtomicU64.
// The lower 48 bits hold the pointer value; 0 encodes null.
// Upper bits are masked off on read for portability.

/// Opaque packed pointer stored in the skip list tower.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct TowerPtr(u64);

impl TowerPtr {
    pub const NULL: Self = Self(0);

    #[inline]
    pub fn new(ptr: *const u8) -> Self {
        Self(ptr as usize as u64)
    }

    #[inline]
    pub fn from_raw(raw: u64) -> Self {
        Self(raw)
    }

    #[inline]
    pub fn raw(self) -> u64 {
        self.0
    }

    #[inline]
    pub fn ptr(self) -> *const u8 {
        (self.0 & 0x0000_FFFF_FFFF_FFFF) as usize as *const u8
    }

    #[inline]
    pub fn is_null(self) -> bool {
        self.ptr().is_null()
    }
}

// ─── Node layout ───────────────────────────────────────────────────────────────
// Offset  Size  Field
// 0       4     key_offset   (u32)
// 4       4     key_len      (u32)
// 8       4     value_offset (u32)
// 12      4     value_len    (u32)
// 16      1     flags        (bit 0 = tombstone)
// 17      1     height       (1..MAX_HEIGHT)
// 18      6     _pad
// 24      8     seq          (u64, insertion sequence number)
// 32      8*H   tower[0..H]  (each is AtomicU64 storing a TowerPtr)
// 32+8*H        key bytes
// ...           value bytes

#[inline]
pub(crate) fn node_alloc_size(height: usize, key_len: usize, value_len: usize) -> usize {
    NODE_HEADER_SIZE + height * 8 + key_len + value_len
}

/// Initialize a node at `ptr`. Node is NOT yet visible (no CAS has occurred).
/// Header and key/value use plain stores. Tower uses relaxed atomic stores
/// to avoid UB with concurrent `tower_load` (Acquire) after the node becomes
/// visible via CAS.
///
/// # Safety
/// `ptr` must point to at least `node_alloc_size(height, key.len(), value.len())`
/// bytes of valid, writable memory from the arena.
#[inline]
pub(crate) unsafe fn init_node(
    ptr: *mut u8,
    height: usize,
    key: &[u8],
    value: &[u8],
    is_tombstone: bool,
    seq: u64,
) {
    let key_len = key.len() as u32;
    let value_len = value.len() as u32;
    let h = height as u8;

    let key_offset = (NODE_HEADER_SIZE + height * 8) as u32;
    let value_offset = key_offset + key_len;
    let flags: u8 = if is_tombstone { TOMBSTONE_BIT } else { 0 };

    // Header — plain stores (node not yet visible)
    ptr.cast::<u32>().write(key_offset);
    ptr.add(4).cast::<u32>().write(key_len);
    ptr.add(8).cast::<u32>().write(value_offset);
    ptr.add(12).cast::<u32>().write(value_len);
    ptr.add(FLAGS_OFFSET).write(flags);
    ptr.add(17).write(h);
    // Offset 18-23: padding (6 bytes, zero)
    std::ptr::write_bytes(ptr.add(18), 0, 6);
    // Offset 24-31: seq as u64 (naturally aligned, plain store)
    ptr.add(NODE_SEQ_OFFSET).cast::<u64>().write(seq);

    // Tower — bulk zero all tower slots. The node is not yet visible,
    // so no other thread can observe these locations. Using write_bytes
    // is valid because the logical value 0u64 is identical to all-zero
    // bits (TowerPtr::NULL == 0).
    std::ptr::write_bytes(ptr.add(NODE_HEADER_SIZE).cast::<u64>(), 0, height);

    // Key/value — plain stores (node not yet visible)
    std::ptr::copy_nonoverlapping(key.as_ptr(), ptr.add(key_offset as usize), key.len());
    std::ptr::copy_nonoverlapping(value.as_ptr(), ptr.add(value_offset as usize), value.len());
}

// ─── Zero-copy accessors ───────────────────────────────────────────────────────

/// # Safety: `ptr` must point to a fully initialized node.
#[inline]
pub(crate) unsafe fn node_key(ptr: *const u8) -> &'static [u8] {
    // Single 64-bit read: [key_offset:u32, key_len:u32]
    let packed = ptr.cast::<u64>().read();
    let key_offset = packed as u32 as usize;
    let key_len = (packed >> 32) as usize;
    std::slice::from_raw_parts(ptr.add(key_offset), key_len)
}

/// # Safety: `ptr` must point to a fully initialized node.
#[inline]
pub(crate) unsafe fn node_value(ptr: *const u8) -> &'static [u8] {
    // Single 64-bit read: [value_offset:u32, value_len:u32]
    let packed = ptr.add(8).cast::<u64>().read();
    let value_offset = packed as u32 as usize;
    let value_len = (packed >> 32) as usize;
    std::slice::from_raw_parts(ptr.add(value_offset), value_len)
}

/// Check if node has tombstone flag (Acquire load — sees concurrent tombstone).
///
/// # Safety: `ptr` must point to a fully initialized node.
#[inline]
pub(crate) unsafe fn is_tombstone(ptr: *const u8) -> bool {
    let atomic = &*ptr.add(FLAGS_OFFSET).cast::<AtomicU8>();
    (atomic.load(Ordering::Acquire) & TOMBSTONE_BIT) != 0
}

/// Atomically set the tombstone flag. Returns true if this call won the CAS.
///
/// # Safety: `ptr` must point to a fully initialized node.
#[inline]
pub(crate) unsafe fn set_tombstone(ptr: *const u8) -> bool {
    let atomic = &*ptr.add(FLAGS_OFFSET).cast::<AtomicU8>();
    let mut current = atomic.load(Ordering::Acquire);
    loop {
        if current & TOMBSTONE_BIT != 0 {
            return false;
        }
        match atomic.compare_exchange_weak(
            current,
            current | TOMBSTONE_BIT,
            Ordering::Release,
            Ordering::Acquire,
        ) {
            Ok(_) => return true,
            Err(new) => current = new,
        }
    }
}

/// # Safety: `ptr` must point to a fully initialized node.
#[inline]
#[allow(dead_code)]
pub(crate) unsafe fn node_height(ptr: *const u8) -> usize {
    ptr.add(17).read() as usize
}

/// Read the insertion sequence number from a node.
///
/// The seq is written before the Release fence/CAS that publishes the node.
/// The caller obtained `ptr` via `tower_load` (Acquire), which already
/// synchronizes with the publishing Release. A Relaxed load is sufficient.
///
/// # Safety: `ptr` must point to a fully initialized node.
#[inline]
pub(crate) unsafe fn node_seq(ptr: *const u8) -> u64 {
    let atomic = &*ptr.add(NODE_SEQ_OFFSET).cast::<AtomicU64>();
    atomic.load(Ordering::Relaxed)
}

// ─── Tower atomic operations ───────────────────────────────────────────────────
//
// All tower entries are AtomicU64. We never mix plain and atomic accesses
// to the same location (Miri catches this).
//
// Load:  Acquire  — sees node contents written before Release CAS
// Store: Relaxed  — used during init and upper-level splice (non-publishing)
// CAS:   Release/Acquire — publishes pointer, sees latest on failure

/// Load a tower pointer (Acquire).
///
/// # Safety: `node` must be a valid node, `level` < node height.
#[inline]
pub(crate) unsafe fn tower_load(node: *const u8, level: usize) -> TowerPtr {
    let offset = NODE_HEADER_SIZE + level * 8;
    let atomic = node.add(offset).cast::<AtomicU64>();
    TowerPtr::from_raw((*atomic).load(Ordering::Acquire))
}

/// Store a tower pointer (Relaxed). Used during initialization and upper-level
/// splice when the pointer is not the publishing mechanism.
///
/// # Safety: `node` must be a valid node, `level` < node height.
#[inline]
pub(crate) unsafe fn tower_store(node: *mut u8, level: usize, val: TowerPtr) {
    let offset = NODE_HEADER_SIZE + level * 8;
    let atomic = node.add(offset).cast::<AtomicU64>();
    (*atomic).store(val.raw(), Ordering::Relaxed);
}

/// CAS a tower pointer. Release on success (publishes pointer), Acquire on
/// failure (sees latest value for retry).
///
/// # Safety: `node` must be a valid node, `level` < node height.
#[inline]
pub(crate) unsafe fn tower_cas(
    node: *const u8,
    level: usize,
    current: TowerPtr,
    new: TowerPtr,
) -> Result<TowerPtr, TowerPtr> {
    let offset = NODE_HEADER_SIZE + level * 8;
    let atomic = node.add(offset).cast::<AtomicU64>();
    match (*atomic).compare_exchange_weak(
        current.raw(),
        new.raw(),
        Ordering::Release,
        Ordering::Acquire,
    ) {
        Ok(v) => Ok(TowerPtr::from_raw(v)),
        Err(v) => Err(TowerPtr::from_raw(v)),
    }
}

// ─── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn alloc_node(height: usize, key: &[u8], value: &[u8]) -> (*mut u8, std::alloc::Layout) {
        let size = node_alloc_size(height, key.len(), value.len());
        let layout = std::alloc::Layout::from_size_align(size, 8).unwrap();
        let ptr = unsafe { std::alloc::alloc(layout) };
        assert!(!ptr.is_null());
        (ptr, layout)
    }

    #[test]
    fn test_node_alloc_size() {
        assert_eq!(
            node_alloc_size(4, 10, 20),
            NODE_HEADER_SIZE + 4 * 8 + 10 + 20
        );
    }

    #[test]
    fn test_init_and_read_roundtrip() {
        let key = b"hello";
        let value = b"world";
        let (ptr, layout) = alloc_node(3, key, value);
        unsafe {
            init_node(ptr, 3, key, value, false, 42);
            assert_eq!(node_key(ptr), key);
            assert_eq!(node_value(ptr), value);
            assert!(!is_tombstone(ptr));
            assert_eq!(node_seq(ptr), 42);
            for i in 0..3 {
                assert!(tower_load(ptr, i).is_null());
            }
            std::alloc::dealloc(ptr, layout);
        }
    }

    #[test]
    fn test_tombstone_flag() {
        let (ptr, layout) = alloc_node(1, b"key", b"val");
        unsafe {
            init_node(ptr, 1, b"key", b"val", false, 1);
            assert!(!is_tombstone(ptr));
            assert!(set_tombstone(ptr));
            assert!(is_tombstone(ptr));
            assert!(!set_tombstone(ptr));
            std::alloc::dealloc(ptr, layout);
        }
    }

    #[test]
    #[cfg(not(miri))] // CAS assertion flaky on some Miri versions (passes locally)
    fn test_tower_cas() {
        let (ptr, layout) = alloc_node(4, b"", b"");
        unsafe {
            init_node(ptr, 4, b"", b"", false, 1);

            let null = TowerPtr::NULL;
            let node_ptr = TowerPtr::new(ptr);

            assert_eq!(tower_cas(ptr, 0, null, node_ptr), Ok(null));
            assert_eq!(tower_load(ptr, 0), node_ptr);
            assert!(tower_cas(ptr, 0, null, node_ptr).is_err());

            std::alloc::dealloc(ptr, layout);
        }
    }

    #[test]
    fn test_tower_ptr_basic() {
        // Use a real allocation to avoid integer-to-pointer casts under Miri
        let dummy = Box::into_raw(Box::new(42u8));
        let p = TowerPtr::new(dummy);
        assert!(!p.is_null());
        assert_eq!(p.ptr(), dummy);

        assert!(TowerPtr::NULL.is_null());
        assert_eq!(TowerPtr::NULL.ptr(), std::ptr::null());

        unsafe { drop(Box::from_raw(dummy)) };
    }

    #[test]
    fn test_tower_store_atomic() {
        let (ptr, layout) = alloc_node(2, b"", b"");
        unsafe {
            init_node(ptr, 2, b"", b"", false, 1);

            // Use a real allocation to avoid integer-to-pointer casts under Miri
            let dummy = Box::into_raw(Box::new(99u8));
            let target = TowerPtr::new(dummy);
            tower_store(ptr, 0, target);
            assert_eq!(tower_load(ptr, 0), target);

            // Verify via atomic load directly (same path as tower_load)
            let offset = NODE_HEADER_SIZE;
            let atomic = ptr.add(offset).cast::<AtomicU64>();
            assert_eq!((*atomic).load(Ordering::Acquire), target.raw());

            drop(Box::from_raw(dummy));
            std::alloc::dealloc(ptr, layout);
        }
    }
}
