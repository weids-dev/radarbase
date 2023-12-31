use tempfile::{NamedTempFile, TempDir};

mod common;
use common::*;

use rand::prelude::SliceRandom;
use rand::Rng;
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

fn benchmark<T: BenchTable>(mut db: T) {
    let pairs = gen_data(1000, 16, 1500); // page size?

    let start = SystemTime::now();
    let mut txn = db.write_transaction();
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
        "{}: Bulk loaded {} items in {}ms",
        T::db_type_name(),
        ELEMENTS,
        duration.as_millis()
    );

    let start = SystemTime::now();
    let writes = 100;
    {
        for i in ELEMENTS..(ELEMENTS + writes) {
            let mut txn = db.write_transaction();
            let (key, value) = &pairs[i % pairs.len()];
            let mut mut_key = key.clone();
            mut_key.extend_from_slice(&i.to_be_bytes());
            // Insert one by one
            txn.insert(&mut_key, value).unwrap();
            txn.commit().unwrap();
        }
    }

    let end = SystemTime::now();
    let duration = end.duration_since(start).unwrap();
    println!(
        "{}: Wrote {} individual items in {}ms",
        T::db_type_name(),
        writes,
        duration.as_millis()
    );

    let mut key_order: Vec<usize> = (0..ELEMENTS).collect();
    key_order.shuffle(&mut rand::thread_rng());

    let txn = db.read_transaction();
    {
        for _ in 0..ITERATIONS {
            let start = SystemTime::now();
            let mut checksum = 0u64;
            let mut expected_checksum = 0u64;
            for i in &key_order {
                let (key, value) = &pairs[*i % pairs.len()];
                let mut mut_key = key.clone();
                mut_key.extend_from_slice(&i.to_be_bytes());
                let result = txn.get(&mut_key).unwrap();
                checksum += result.as_ref()[0] as u64;
                expected_checksum += value[0] as u64;
            }
            assert_eq!(checksum, expected_checksum);
            let end = SystemTime::now();
            let duration = end.duration_since(start).unwrap();
            println!(
                "{}: Random read {} items in {}ms",
                T::db_type_name(),
                ELEMENTS,
                duration.as_millis()
            );
        }
    }
}

fn main() {
    {
        let tmpfile: TempDir = tempfile::tempdir().unwrap();
        let env = lmdb::Environment::new().open(tmpfile.path()).unwrap();
        env.set_map_size(4096 * 1024 * 1024).unwrap();
        let table = LmdbRkvBenchTable::new(&env);
        benchmark(table);
    }
    {
        let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
        let db = unsafe { radarbase::Database::open(tmpfile.path()).unwrap() };
        let table = RadbBenchTable::new(&db);
        benchmark(table);
    }
    {
        let tmpfile: TempDir = tempfile::tempdir().unwrap();
        let db = sled::Config::new().path(tmpfile.path()).open().unwrap();
        let table = SledBenchTable::new(&db);
        benchmark(table);
    }
}
