use tempfile::NamedTempFile;

use rand::prelude::SliceRandom;
use rand::Rng;
use std::path::Path;
use std::time::SystemTime;

const ITERATIONS: usize = 3;
const ELEMENTS: usize = 100_000;

/// Returns pairs of key, value
fn gen_data(count: usize, key_size: usize, value_size: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut pairs = vec![];

    for _ in 0..count {
        let key: Vec<u8> = (0..key_size).map(|_| rand::thread_rng().gen()).collect();
        let value: Vec<u8> = (0..value_size).map(|_| rand::thread_rng().gen()).collect();
        pairs.push((key, value));
    }

    pairs
}

fn radb_bench(path: &Path) {
    use radarbase::Database;

    let db = unsafe { Database::open(path).unwrap() };
    let mut table = db.open_table("bench").unwrap();

    let pairs = gen_data(1000, 16, 2000);

    let start = SystemTime::now();
    let mut txn = table.begin_write().unwrap();
    {
        for i in 0..ELEMENTS {
            let (key, value) = &pairs[i % pairs.len()];
            let mut mut_key = key.clone();
            mut_key.extend_from_slice(&i.to_be_bytes());
            txn.insert(&mut_key, value).unwrap();
        }
    }
    txn.commit().unwrap();

    let end = SystemTime::now();
    let duration = end.duration_since(start).unwrap();
    println!(
        "radb: Loaded {} items in {}ms",
        ELEMENTS,
        duration.as_millis()
    );

    let mut key_order: Vec<usize> = (0..ELEMENTS).collect();
    key_order.shuffle(&mut rand::thread_rng());

    let txn = table.read_transaction().unwrap();
    {
        for _ in 0..ITERATIONS {
            let start = SystemTime::now();
            let mut checksum = 0u64;
            let mut expected_checksum = 0u64;
            for i in &key_order {
                let (key, value) = &pairs[*i % pairs.len()];
                let mut mut_key = key.clone();
                mut_key.extend_from_slice(&i.to_be_bytes());
                let result: &[u8] = txn.get(&mut_key).unwrap().unwrap();
                checksum += result[0] as u64;
                expected_checksum += value[0] as u64;
            }
            assert_eq!(checksum, expected_checksum);
            let end = SystemTime::now();
            let duration = end.duration_since(start).unwrap();
            println!(
                "radb: Random read {} items in {}ms",
                ELEMENTS,
                duration.as_millis()
            );
        }
    }
}


fn main() {
    {
        let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
        radb_bench(tmpfile.path());
    }
}
