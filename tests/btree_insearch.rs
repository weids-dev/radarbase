use radarbase::btree::BTree; // also radarbase::BTree

fn create_btree() -> BTree<&'static str, i32> {
    let mut btree = BTree::new();

    let keys = ["g", "m", "p", "x", "a", "c", "d", "f", "i", "j", "k", "l", "n", "o", "r", "s", "t", "u", "v", "y", "z"];
    let values = [7, 13, 16, 24, 1, 3, 4, 6, 9, 10, 11, 12, 14, 15, 18, 19, 20, 21, 22, 25, 26];

    for (key, value) in keys.iter().zip(values.iter()) {
        btree.insert(*key, *value);
    }
    btree
}

#[test]
fn test_insert_and_search() {
    let btree = create_btree();

    let keys = ["a", "c", "d", "f", "g", "i", "j", "k", "l", "m", "n", "o", "p", "r", "s", "t", "u", "v", "x", "y", "z"];
    let values = [1, 3, 4, 6, 7, 9, 10, 11, 12, 13, 14, 15, 16, 18, 19, 20, 21, 22, 24, 25, 26];

    for (key, value) in keys.iter().zip(values.iter()) {
        assert_eq!(btree.search(key), Some(value));
    }
}

#[test]
fn test_search_non_existent_key() {
    let btree = create_btree();
    assert_eq!(btree.search(&"b"), None);
    assert_eq!(btree.search(&"h"), None);
    assert_eq!(btree.search(&"q"), None);
    assert_eq!(btree.search(&"w"), None);
}

#[test]
fn test_insert_duplicate_key() {
    let mut btree = create_btree();

    // Insert duplicate key with a different value
    btree.insert("g", 42);
    assert_eq!(btree.search(&"g"), Some(&7));

    // Insert duplicate key with the same value
    btree.insert("g", 7);
    assert_eq!(btree.search(&"g"), Some(&7));
}
