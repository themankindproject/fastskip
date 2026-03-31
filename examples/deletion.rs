use fastskip::ConcurrentSkipList;

fn main() {
    let memtable = ConcurrentSkipList::new();

    memtable.insert(b"map", b"hello world");
    memtable.insert(b"map2", b"hello world2");
    memtable.insert(b"map3", b"hello world3");

    memtable.delete(b"map");

    // Node is STILL there — just marked tombstone
    match memtable.get(b"map") {
        Some((value, is_tombstone)) => {
            let readable = std::str::from_utf8(value).unwrap();
            println!("Value for 'map': {}", readable);
            println!("Is tombstone: {}", is_tombstone); // true
        }
        None => println!("Key 'map' not found"),
    }

    // get_live filters it out
    println!("get_live: {:?}", memtable.get_live(b"map")); // None

    // Memory is NOT reclaimed — node is still in arena
    println!("Memory usage: {} bytes", memtable.memory_usage());
    println!("Live entries: {}", memtable.len()); // 2

    // Actual reclamation: seal + drop
    let (frozen, fresh) = memtable.seal().unwrap();

    // Flush frozen to "SSTable" — iterate, write tombstones
    for entry in frozen.iter() {
        if entry.is_tombstone {
            println!(
                "SSTable: DELETE {:?}",
                std::str::from_utf8(entry.key).unwrap()
            );
        } else {
            println!(
                "SSTable: PUT {:?} = {:?}",
                std::str::from_utf8(entry.key).unwrap(),
                std::str::from_utf8(entry.value).unwrap()
            );
        }
    }

    // NOW the old arena is freed
    println!(
        "\nFrozen memory before drop: {} bytes",
        frozen.memory_usage()
    );
    drop(frozen);
    println!("Frozen dropped — all memory reclaimed");

    // Fresh memtable is clean
    println!(
        "Fresh memtable: {} entries, {} bytes",
        fresh.len(),
        fresh.memory_usage()
    );
}
