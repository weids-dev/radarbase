use radarbase::btree::BTree; // also radarbase::BTree
use rand::seq::SliceRandom;
use rand::thread_rng;

#[test]
fn test_traverse() {
    let mut tree = BTree::<String, i32>::new();
    let keys = vec![
        "pear".to_string(),
        "apple".to_string(),
        "banana".to_string(),
        "orange".to_string(),
        "kiwi".to_string(),
    ];
    let values = vec![2, 3, 7, 5, 4];

    for (key, value) in keys.iter().zip(values.iter()) {
        tree.insert(key.clone(), *value);
    }

    let kv_pairs = tree.traverse();
    let sorted_keys: Vec<_> = kv_pairs.iter().map(|(k, _)| k.clone()).collect();
    let sorted_values: Vec<_> = kv_pairs.iter().map(|(_, v)| *v).collect();

    let mut expected_keys = keys.clone();
    expected_keys.sort();

    assert_eq!(sorted_keys, expected_keys);

    for (key, value) in sorted_keys.iter().zip(sorted_values.iter()) {
        assert_eq!(tree.search(&key), Some(value));
    }
}

#[test]
fn test_traverse_sorted_keys() {
    let mut tree = BTree::<String, i32>::new();
    let keys: Vec<String> = (1..10000).map(|i| i.to_string()).collect();
    let values: Vec<i32> = (1..10000).collect();

    let mut rng = thread_rng();
    let mut shuffled_keys = keys.clone();
    let mut shuffled_values = values.clone();
    shuffled_keys.shuffle(&mut rng);
    shuffled_values.shuffle(&mut rng);

    for (key, value) in shuffled_keys.iter().zip(shuffled_values.iter()) {
        tree.insert(key.clone(), *value);
    }

    let kv_pairs = tree.traverse();
    let sorted_keys: Vec<String> = kv_pairs.iter().map(|(k, _)| k.clone()).collect();
    let mut expected_keys = keys.clone();
    expected_keys.sort();

    assert_eq!(sorted_keys, expected_keys);
}
