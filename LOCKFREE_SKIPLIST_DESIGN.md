# Lock-Free Arena-Backed Skip List for LSM-Tree Memtable

## Technical Design Document

---

## 1. Concurrent Arena Allocation Strategies

### Problem

fastarena's `alloc_raw_inner` takes `&mut self` — a single-threaded bump pointer. For a concurrent skip list, every insert needs arena allocation for the node + inline key/value. This is the serialization bottleneck.

### Strategy A: Per-Thread Arenas (Recommended)

Each writer thread owns a thread-local arena. Nodes allocated by thread T are pinned to T's arena. The skip list itself is just a set of atomic pointers — it doesn't own memory.

```
Thread 0 → Arena 0 (bump pointer, no sync needed)
Thread 1 → Arena 1 (bump pointer, no sync needed)
Thread 2 → Arena 2 (bump pointer, no sync needed)
          ↓
    SkipList (atomic tower pointers referencing nodes across arenas)
```

**Pros:**
- Zero synchronization on allocation path — pure bump pointer
- Existing `Arena` API works unchanged (`&mut self` is fine)
- Allocation is O(1) with a single bounds check
- Each thread's arena resets independently

**Cons:**
- Memory fragmentation across arenas (mitigated by arena block sizing)
- Can't rewind individual nodes (but memtable is append-only anyway)
- Lifetime management: all arenas must outlive the skip list

**RocksDB precedent:** RocksDB's `ConcurrentArena` uses per-core shards with a spinlock per shard plus small per-core buffers. The shard is selected via `sched_getcpu()` or thread-local. For our case, thread-local arenas are simpler and equally fast.

### Strategy B: Single Arena with Atomic CAS Bump Pointer

Replace the bump pointer with an `AtomicUsize`. To allocate N bytes:

```rust
fn alloc_concurrent(&self, size: usize, align: usize) -> Option<NonNull<u8>> {
    loop {
        let current = self.offset.load(Ordering::Acquire);
        let aligned = align_up(current, align);
        let new_offset = aligned + size;
        if new_offset > self.capacity {
            return None; // need new block
        }
        if self.offset.compare_exchange_weak(
            current, new_offset,
            Ordering::AcqRel, Ordering::Acquire
        ).is_ok() {
            return Some(self.base + aligned);
        }
        // retry
    }
}
```

**Pros:**
- Single arena, simpler memory management
- No cross-arena lifetime issues

**Cons:**
- CAS loop adds contention under high write parallelism
- Block allocation (when current block fills) requires a lock or more complex lock-free protocol
- `InlineVec<Block>` growth is not thread-safe
- ABA problem on the block list

**Verdict:** Strategy A (per-thread arenas) is the correct choice for fastarena. It avoids modifying the existing arena code and provides zero-contention allocation.

### Strategy C: Hybrid — Shared Blocks, Thread-Local Offset Caches

RocksDB's approach: a global arena manages block allocation (with a spinlock). Each thread caches a small buffer (e.g., 1KB) from the current block. When the local cache is exhausted, it grabs another buffer from the global arena.

This is more complex but minimizes memory waste. For an initial implementation, Strategy A is preferred.

---

## 2. Skip List Node Layout

### Constants

```rust
const MAX_HEIGHT: usize = 20;     // supports ~2^20 = 1M nodes
const P: f64 = 0.5;               // each level has 50% of previous
```

With p=0.5 and MAX_HEIGHT=20, the expected memory overhead per node for tower pointers is (1+p+p^2+...+p^19) ≈ 2× the base. At 8 bytes per pointer, that's ~16 pointers × 8 = 128 bytes for the tower.

### Memory Layout

Nodes are allocated as a single contiguous block from the arena:

```
Offset  Size    Field
------  ----    -----
0       8       key_offset  (u32 offset from node base to key data, u32 key_len)
4       4       key_len
8       4       value_offset (u32 offset from node base to value data)
12      4       value_len
16      1       flags       (bit 0: tombstone)
17      1       height      (1..MAX_HEIGHT)
18      2       _pad
20      4       _pad2
24      8*H     tower[0]    (atomic u64 — next pointer at level 0)
24+8    8*H     tower[1]    (atomic u64 — next pointer at level 1)
...             ...
24+8*H          key bytes (variable length)
...             value bytes (variable length)
```

**Total allocation size:**
```
NODE_HEADER_SIZE = 24 bytes
TOWER_SIZE = height * 8 bytes
TOTAL = NODE_HEADER_SIZE + TOWER_SIZE + key_len + value_len
```

### Why inline key/value?

Arena-allocated nodes with inline keys/values give:
- **Zero-copy reads**: `&[u8]` references into arena memory, no indirection
- **Cache locality**: key/value data is adjacent to the node header
- **No secondary allocation**: one arena call per insert

### Tower pointer encoding

Each tower entry is a `u64` that encodes:
- Bits 0-47: node address offset within the arena (48-bit, supports 256TB arena)
- Bits 48-62: aba_tag (15 bits, incremented on logical deletion to avoid ABA)
- Bit 63: marked flag (1 = logically deleted at this level)

```rust
#[repr(transparent)]
struct TowerPtr(u64);

impl TowerPtr {
    fn new(node_offset: u64, tag: u16) -> Self {
        debug_assert!(node_offset < (1u64 << 48));
        Self((node_offset & 0x0000_FFFF_FFFF_FFFF) | ((tag as u64) << 48))
    }

    fn node_offset(self) -> u64 { self.0 & 0x0000_FFFF_FFFF_FFFF }
    fn tag(self) -> u16 { (self.0 >> 48) as u16 }
    fn is_null(self) -> bool { self.node_offset() == 0 }
    fn is_marked(self) -> bool { (self.0 >> 63) == 1 }

    fn with_mark(self) -> Self { Self(self.0 | (1u64 << 63)) }
    fn without_mark(self) -> Self { Self(self.0 & !(1u64 << 63)) }
}
```

**Alternative (simpler):** Use `AtomicU64` where the value is a byte offset from arena base. `0` means null. No ABA tag — see section 8 on why ABA is not a problem in this design.

---

## 3. Lock-Free Insertion Algorithm

Based on the Harris lock-free linked list extended to skip lists (as in RocksDB's `InlineSkipList`, CockroachDB's `arenaskl`, and `al8n/skl`).

### Core Algorithm

The key insight: skip list insertion is a **single CAS at level 0 only**, with non-atomic pointer updates at higher levels. This works because:

1. All searches traverse level 0 eventually
2. Higher-level pointers are "hints" — correctness doesn't depend on them being precise
3. The node becomes visible at level 0 first, then we splice it into upper levels

### Pseudocode

```rust
fn insert(&self, key: &[u8], value: &[u8], arena: &mut Arena) -> bool {
    // 1. Allocate node from arena
    let height = random_height();
    let node_size = NODE_HEADER_SIZE + height * 8 + key.len() + value.len();
    let ptr = arena.alloc_raw(node_size, 8);

    // 2. Initialize node (zero tower, copy key/value)
    let node = init_node(ptr, height, key, value);

    // 3. Find insertion point + CAS at level 0
    loop {
        let (preds, succs) = self.find_less(key);

        // Check if key already exists
        let succ = succs[0];
        if !succ.is_null() && node_key(succ) == key {
            // Key exists — update value (or insert tombstone version for MVCC)
            return update_existing(succ, value);
        }

        // Set new node's tower to point to successors
        for level in 0..height {
            node.set_tower(level, succs[level]);
        }

        // CAS at level 0 — this is the atomic commit point
        if preds[0].tower[0].compare_exchange(
            succs[0].pack(),
            node.pack(),
            Ordering::Release,
            Ordering::Relaxed
        ).is_ok() {
            // Success — now splice into higher levels (non-atomic, best-effort)
            for level in 1..height {
                loop {
                    let pred = preds[level];
                    let succ = succs[level];
                    if pred.tower[level].compare_exchange(
                        succ.pack(),
                        node.pack(),
                        Ordering::Release,
                        Ordering::Relaxed
                    ).is_ok() {
                        break;
                    }
                    // Re-find predecessor at this level
                    let (new_preds, new_succs) = self.find_less_at_level(key, level);
                    // Update node's tower pointer
                    node.set_tower(level, new_succs[level]);
                    // preds/succs may need refresh
                }
            }
            return true;
        }
        // CAS failed — retry from scratch
    }
}
```

### The `find_less` function

This is the search path that records predecessors and successors at each level:

```rust
fn find_less(&self, key: &[u8]) -> ([NodePtr; MAX_HEIGHT], [NodePtr; MAX_HEIGHT]) {
    let mut preds = [NodePtr::NULL; MAX_HEIGHT];
    let mut succs = [NodePtr::NULL; MAX_HEIGHT];

    let mut x = self.head;
    let mut level = self.height.load(Ordering::Acquire) - 1;

    loop {
        let next = x.tower[level].load(Ordering::Acquire);
        let next_node = deref(next);

        if !next.is_null() && node_key(next_node) < key {
            x = next_node;
        } else {
            preds[level] = x;
            succs[level] = if next.is_null() { NodePtr::NULL } else { next };
            if level == 0 {
                break;
            }
            level -= 1;
        }
    }

    (preds, succs)
}
```

### Why single-CAS-at-level-0 works

A reader searching for key K traverses level L from left to right. At each step:
- If `current.key < K`, advance to `current.tower[L]`
- If `current.key >= K`, drop to level L-1

Even if upper-level pointers are stale (pointing past the newly inserted node), the reader will eventually reach level 0 where the CAS-established pointer correctly routes to the new node. The worst case is a reader temporarily skips the new node at an upper level but finds it at a lower level — this is acceptable because the reader was never guaranteed to see a concurrent insert anyway.

---

## 4. Memory Ordering

### Allocation (thread-local arena)

No atomics needed — single-threaded bump pointer.

### Node initialization

```rust
// Writing key/value into node — plain stores are fine
// because the node is not yet visible (no pointer to it exists)
node.key_offset = ...;
node.key_len = key.len();
node.value_offset = ...;
// etc.
```

Use plain stores + compiler fence before the CAS:

```rust
std::sync::atomic::fence(Ordering::Release);
// Now CAS the tower pointer
```

### CAS on tower pointers

```rust
// Writing (insert/update):
pred.tower[level].compare_exchange(
    old_ptr, new_ptr,
    Ordering::Release,    // success: publish node contents
    Ordering::Relaxed     // failure: no ordering needed
);

// Reading (search):
let next = node.tower[level].load(Ordering::Acquire);
// Acquire ensures we see the node's key/value that were written before the Release CAS
```

### Height update (when new tallest node is inserted)

```rust
self.height.compare_exchange(
    old_height, new_height,
    Ordering::Release, Ordering::Relaxed
);

// Readers:
let h = self.height.load(Ordering::Acquire);
```

### Summary table

| Operation | Ordering | Rationale |
|-----------|----------|-----------|
| CAS insert pointer | `Release` | Publishes node contents to other threads |
| Load next pointer | `Acquire` | Reads node contents written before the Release CAS |
| Height store | `Release` | Publishes new height |
| Height load | `Acquire` | Ensures we see all nodes at that level |
| Node initialization | Plain stores | Node not yet reachable |
| Value update CAS | `Release` / `Acquire` | Same as insert |
| Tombstone flag store | `Release` | Publishes deletion |
| Tombstone flag load | `Acquire` | Observes deletion |

**No `SeqCst` is needed.** Skip list operations don't require a total global order — they only require per-location acquire/release semantics.

---

## 5. Key/Value Encoding

### Layout after node header + tower

```
[ node_header (24 bytes) ]
[ tower[0..height-1] (8*height bytes) ]
[ key bytes (key_len bytes) ]
[ value bytes (value_len bytes) ]
```

The key and value are stored contiguously after the tower. The header stores offsets relative to the node base:

```rust
struct NodeHeader {
    key_offset: u32,     // offset from node base to key bytes (= NODE_HEADER_SIZE + height*8)
    key_len: u32,
    value_offset: u32,   // offset from node base to value bytes (= key_offset + key_len)
    value_len: u32,
    flags: u8,           // bit 0 = tombstone
    height: u8,
    _pad: [u8; 6],
}
```

### Accessing key/value

```rust
fn node_key(node: *const u8) -> &[u8] {
    let header = unsafe { &*(node as *const NodeHeader) };
    unsafe {
        std::slice::from_raw_parts(node.add(header.key_offset as usize), header.key_len as usize)
    }
}

fn node_value(node: *const u8) -> &[u8] {
    let header = unsafe { &*(node as *const NodeHeader) };
    unsafe {
        std::slice::from_raw_parts(node.add(header.value_offset as usize), header.value_len as usize)
    }
}
```

### Tombstone encoding

A tombstone is a node where `flags & 0x01 != 0`. The value is typically empty (value_len = 0). During iteration, tombstones are yielded to the caller (the LSM tree's merge iterator handles them).

### Alternative: prefix compression

For keys that share a long prefix with the predecessor, store only the suffix. This reduces memory usage significantly for sorted workloads (e.g., keys like `user:12345:email`, `user:12345:name`).

```rust
struct NodeHeaderCompressed {
    shared_prefix_len: u32,  // bytes shared with predecessor's key
    key_suffix_len: u32,
    // ...
}
```

This requires the predecessor to be stable (it is, since nodes are never moved). Implementation adds complexity to `find_less` and comparison — defer to V2.

---

## 6. Height Generation

### Algorithm

Use `p = 0.5` (coin flip). Each level has a 50% chance of being included.

```rust
fn random_height() -> usize {
    // Thread-local RNG (avoid contention on global RNG)
    let mut rng = thread_local_rng();
    let mut height = 1;
    while height < MAX_HEIGHT && rng.gen::<u32>() < (u32::MAX / 2) {
        height += 1;
    }
    height
}
```

### Fast alternative: count trailing zeros

Generate a single random u64 and count trailing zero bits:

```rust
fn random_height_fast() -> usize {
    let r: u64 = thread_local_rng().gen();
    // Trailing zeros gives geometric distribution with p=0.5
    // Add 1 because height starts at 1
    (r.trailing_zeros() as usize + 1).min(MAX_HEIGHT)
}
```

This is O(1) — no loop. The distribution is geometric(0.5):
- P(height=1) = 1/2
- P(height=2) = 1/4
- P(height=3) = 1/8
- ...

Expected height = 2. With MAX_HEIGHT=20, the probability of hitting the cap is 2^(-20) ≈ 10^(-6).

### Precomputed thresholds (avoids floating point)

```rust
const THRESHOLDS: [u32; MAX_HEIGHT] = {
    let mut t = [0u32; MAX_HEIGHT];
    let mut p = 1.0f64;
    let mut i = 0;
    while i < MAX_HEIGHT {
        t[i] = (p * u32::MAX as f64) as u32;
        p *= 0.5;
        i += 1;
    }
    t
};

fn random_height_table(r: u32) -> usize {
    let mut h = 1;
    while h < MAX_HEIGHT && r < THRESHOLDS[h] {
        h += 1;
    }
    h
}
```

---

## 7. Concurrent Iteration

### The problem

Readers must get a consistent snapshot while writers are actively inserting. In an arena-backed skip list with no node deletion (tombstones only), this is simpler than in a general-purpose skip list.

### Approach: Point-in-time snapshot via arena checkpoint

Since nodes are only appended (never freed), a reader can take a snapshot by:

1. Record the arena's current allocation position (a `Checkpoint`)
2. Record the skip list's current head pointer
3. Iterate from head, **skipping any node whose address is beyond the checkpoint**

Any node allocated after the checkpoint is invisible to this reader. Nodes allocated before the checkpoint are stable (their key/value bytes won't move).

```rust
struct Snapshot {
    arena_checkpoint: Checkpoint,
    head: NodePtr,
    height: usize,
}

impl Snapshot {
    fn iter(&self) -> SnapshotIter {
        SnapshotIter {
            current: self.head,
            checkpoint_limit: self.arena_checkpoint.offset,
        }
    }
}

impl Iterator for SnapshotIter {
    type Item = (/* key */, /* value */, /* tombstone */);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let next = self.current.tower[0].load(Ordering::Acquire);
            if next.is_null() {
                return None;
            }
            let next_node = deref(next);

            // Skip nodes beyond checkpoint
            if node_address(next_node) >= self.checkpoint_limit {
                return None;
            }

            self.current = next_node;
            let key = node_key(next_node);
            let value = node_value(next_node);
            let tombstone = is_tombstone(next_node);

            return Some((key, value, tombstone));
        }
    }
}
```

### Alternative: Epoch-based visibility

Pin a crossbeam-epoch guard during iteration. Record the current epoch. Nodes inserted after this epoch are invisible. This is more general but adds a dependency on crossbeam-epoch.

### Consistency guarantees

- **Linearizable reads**: Each `get()` traverses the live skip list and sees the latest committed value
- **Snapshot reads**: The checkpoint approach gives a consistent point-in-time view
- **No torn reads**: Keys and values are fully written before the Release CAS makes the node visible

---

## 8. Common Pitfalls

### ABA Problem

**In this design, ABA is largely mitigated by the arena allocator.**

In a heap-allocated skip list, a node at address A can be freed and a new node allocated at the same address A, causing a CAS to succeed incorrectly. In an arena-backed skip list:

- **Nodes are never freed** — they live until the arena is reset
- Therefore, address A always refers to the same node
- The ABA problem **cannot occur** on pointer CAS

This is a major advantage of arena allocation for lock-free data structures.

### Logical deletion races

When marking a node as a tombstone, we set the `flags` field. Concurrent readers may or may not see the tombstone depending on timing. This is acceptable — the next read will see it.

```rust
fn delete(&self, key: &[u8]) -> bool {
    loop {
        let node = self.find_exact(key);
        if node.is_null() {
            return false; // key not found
        }

        // Atomic tombstone set via CAS on the flags byte
        let flags_ptr = unsafe { node.add(FLAGS_OFFSET) as *const AtomicU8 };
        let old_flags = unsafe { (*flags_ptr).load(Ordering::Acquire) };
        if old_flags & TOMBSTONE_BIT != 0 {
            return false; // already deleted
        }
        if unsafe { (*flags_ptr).compare_exchange(
            old_flags,
            old_flags | TOMBSTONE_BIT,
            Ordering::Release,
            Ordering::Relaxed
        )}.is_ok() {
            return true;
        }
        // retry
    }
}
```

### Memory reclamation

**Not needed.** Arena lifetime = skip list lifetime. When the memtable is flushed to an SSTable, the entire arena is reset/dropped. No individual node reclamation.

If you wanted to support concurrent memtable flush while writers continue (double-buffering), you'd need:
1. Two arenas: active (writers) and frozen (being flushed)
2. Writers atomically switch to the new arena
3. The frozen arena is immutable — safe to iterate without synchronization

### Stale upper-level pointers

After a CAS at level 0, the node may not yet be visible at levels 1..H. A search that starts at a high level might temporarily miss the node, but will find it when it drops to level 0. This is by design — upper levels are optimization hints.

### Lost updates under high contention

When multiple threads CAS the same predecessor's level-0 pointer, only one succeeds. Losers retry. Under very high contention (>8 writers), consider:
- Batching: accumulate writes in a thread-local buffer, flush periodically
- Sharding: partition the key space across multiple skip lists

### Tombstone accumulation

Deleted keys are never physically removed. Over time, tombstones accumulate and slow down iteration. This is addressed by compaction: when the memtable is flushed to an SSTable, tombstones can be elided for keys that don't exist in older levels.

---

## Pseudocode: Complete Operations

### Data structures

```rust
const MAX_HEIGHT: usize = 20;
const NODE_HEADER_SIZE: usize = 24;
const TOMBSTONE_BIT: u8 = 0x01;

struct SkipList {
    head: *mut u8,                      // sentinel node (key = empty, height = MAX_HEIGHT)
    height: AtomicUsize,                // current max height
    arena: UnsafeCell<Arena>,           // or per-thread arenas via ThreadLocal
}

#[repr(C)]
struct NodeHeader {
    key_offset: u32,
    key_len: u32,
    value_offset: u32,
    value_len: u32,
    flags: u8,
    height: u8,
    _pad: [u8; 6],
}
// followed by tower: [AtomicU64; height]
// followed by key bytes
// followed by value bytes
```

### Get

```rust
fn get(&self, key: &[u8]) -> Option<(&[u8], bool /* tombstone */)> {
    let mut x = self.head;
    let mut level = self.height.load(Ordering::Acquire) - 1;

    loop {
        let next_raw = unsafe { tower_load(x, level) };
        if next_raw == 0 {
            // null — drop level
            if level == 0 { return None; }
            level -= 1;
            continue;
        }
        let next = arena_ptr(next_raw);
        let cmp = unsafe { node_key(next).cmp(key) };

        match cmp {
            Ordering::Less => x = next,
            Ordering::Equal => {
                if level == 0 {
                    let val = unsafe { node_value(next) };
                    let tomb = unsafe { is_tombstone(next) };
                    return Some((val, tomb));
                }
                level -= 1;
            }
            Ordering::Greater => {
                if level == 0 { return None; }
                level -= 1;
            }
        }
    }
}
```

### Insert

```rust
fn insert(&self, key: &[u8], value: &[u8], arena: &mut Arena) -> InsertResult {
    let height = random_height_fast();

    loop {
        // Find predecessors and successors at all levels
        let (preds, succs) = self.find_less(key);

        // Check for existing key
        let succ = succs[0];
        if !ptr_is_null(succ) && unsafe { node_key(succ) == key } {
            // Existing key — can overwrite or insert new version (MVCC)
            return InsertResult::Duplicate;
        }

        // Allocate node
        let node_size = NODE_HEADER_SIZE + height * 8 + key.len() + value.len();
        let node_ptr = match arena.try_alloc_raw(node_size, 8) {
            Some(p) => p.as_ptr(),
            None => return InsertResult::OutOfMemory,
        };

        // Initialize node
        unsafe {
            init_node(node_ptr, height, key, value);
        }

        // Set tower pointers (non-publishing)
        for level in 0..height {
            unsafe { tower_store(node_ptr, level, succs[level]); }
        }

        // CAS at level 0 — the atomic commit
        let node_raw = arena_offset(node_ptr);
        let succ_raw = succs[0];

        if unsafe { tower_cas(preds[0], 0, succ_raw, node_raw) } {
            // Committed at level 0 — node is now visible to searches

            // Splice into upper levels (best-effort, may retry per level)
            for level in 1..height {
                loop {
                    if unsafe { tower_cas(preds[level], level, succs[level], node_raw) } {
                        break;
                    }
                    // Re-find predecessor at this level
                    let (new_pred, new_succ) = self.find_less_at_level(key, level);
                    unsafe { tower_store(node_ptr, level, new_succ); }
                    if unsafe { tower_cas(new_pred, level, new_succ, node_raw) } {
                        break;
                    }
                }
            }

            // Update list height if needed
            let old_h = self.height.load(Ordering::Relaxed);
            if height > old_h {
                let _ = self.height.compare_exchange(
                    old_h, height, Ordering::Release, Ordering::Relaxed
                );
            }

            return InsertResult::Ok;
        }

        // CAS failed — retry (arena allocation is wasted; that's fine, it's an arena)
    }
}
```

### Delete (tombstone)

```rust
fn delete(&self, key: &[u8]) -> bool {
    // Find the node at level 0
    let node = self.find_exact(key);
    if ptr_is_null(node) {
        return false;
    }

    // Set tombstone flag via CAS
    let flags = unsafe { &*(node.add(FLAGS_OFFSET) as *const AtomicU8) };
    loop {
        let old = flags.load(Ordering::Acquire);
        if old & TOMBSTONE_BIT != 0 {
            return false; // already deleted
        }
        if flags.compare_exchange_weak(old, old | TOMBSTONE_BIT, Ordering::Release, Ordering::Relaxed).is_ok() {
            return true;
        }
    }
}
```

### Iterate (live)

```rust
struct Iter {
    current: *const u8,
}

impl Iter {
    fn new(list: &SkipList) -> Self {
        Self { current: list.head }
    }
}

impl Iterator for Iter {
    type Item = IterEntry;

    fn next(&mut self) -> Option<IterEntry> {
        loop {
            let next_raw = unsafe { tower_load(self.current, 0) };
            if next_raw == 0 {
                return None;
            }
            let next = arena_ptr(next_raw);
            self.current = next;

            let key = unsafe { node_key(next) };
            let value = unsafe { node_value(next) };
            let tombstone = unsafe { is_tombstone(next) };

            // Optionally skip tombstones
            // if tombstone { continue; }

            return Some(IterEntry { key, value, tombstone });
        }
    }
}
```

---

## Dependency Structure

```
fastskip (new crate)
  └── fastarena (existing crate)
        └── Arena, Checkpoint
```

The `fastskip` crate depends on `fastarena` for arena allocation. The skip list holds a reference to the arena (or owns it). The public API:

```rust
// fastskip/src/lib.rs

pub struct ConcurrentSkipList {
    inner: SkipList,
    arena: Arena,  // or ThreadLocal<Arena> for concurrent writes
}

impl ConcurrentSkipList {
    pub fn new() -> Self;
    pub fn with_capacity(arena_bytes: usize) -> Self;

    // Lock-free operations
    pub fn insert(&self, key: &[u8], value: &[u8]) -> bool;
    pub fn get(&self, key: &[u8]) -> Option<(&[u8], bool)>;
    pub fn delete(&self, key: &[u8]) -> bool;
    pub fn iter(&self) -> Iter;
    pub fn snapshot(&self) -> Snapshot;

    // Stats
    pub fn len(&self) -> usize;  // approximate
    pub fn memory_usage(&self) -> usize;
}
```

---

## Summary of Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Arena strategy | Per-thread arenas | Zero-contention allocation, no changes to fastarena |
| Max height | 20 | Supports ~1M nodes with p=0.5 |
| Tower pointer size | 8 bytes (u64 offset) | 48-bit address space, room for flags |
| CAS location | Level 0 only | Simplifies algorithm, upper levels are hints |
| Delete strategy | Tombstone flag | Arena nodes can't be freed; tombstones handled at compaction |
| Memory reclamation | None needed | Arena lifetime = skip list lifetime |
| Height generation | `trailing_zeros` on random u64 | O(1), no loop, correct geometric distribution |
| Iteration | Level-0 linked walk | Simple, lock-free, consistent with arena checkpoint |
| Memory ordering | Acquire/Release only | No SeqCst needed for this algorithm |
| Key/value storage | Inline after tower | Zero-copy, single allocation per node |
| ABA protection | Inherent (arena never frees) | Major advantage over heap-allocated designs |
