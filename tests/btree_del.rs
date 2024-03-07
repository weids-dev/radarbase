use radarbase::btree::BTree; // also radarbase::BTree
use rand::seq::SliceRandom;
use rand::thread_rng;

fn create_large_btree() -> BTree<&'static str, i32> {
    let mut btree = BTree::new();

    let keys = [
        "g", "m", "p", "x", "a", "c", "d", "f", "i", "j", "k", "l", "n", "o", "r", "s", "t", "u", "v",
        "y", "z", "b", "e", "h", "q", "w", "aa", "ab", "ac", "ad", "ae", "af", "ag", "ah", "ai",
    ];
    let values: Vec<i32> = (1..=keys.len() as i32).collect();

    for (key, value) in keys.iter().zip(values.iter()) {
        btree.insert(*key, *value);
    }
    btree
}

#[test]
fn test_delete_key_large_tree() {
    let mut btree = create_large_btree();

    println!("Large B-Tree before delete");
    btree.print();

    btree.delete(&"d");
    btree.delete(&"g");
    btree.delete(&"b");
    btree.delete(&"a");
    btree.delete(&"ae");
    btree.delete(&"af");
    btree.delete(&"ag");
    btree.delete(&"ah");
    btree.delete(&"ai");

    btree.print();

    assert_eq!(btree.search(&"d"), None);
    assert_eq!(btree.search(&"g"), None);
    assert_eq!(btree.search(&"b"), None);
    assert_eq!(btree.search(&"a"), None);
    assert_eq!(btree.search(&"ae"), None);
    assert_eq!(btree.search(&"af"), None);
    assert_eq!(btree.search(&"ag"), None);
    assert_eq!(btree.search(&"ah"), None);
    assert_eq!(btree.search(&"ai"), None);
}

#[test]
fn test_delete_internal_node_replace_predecessor_successor() {
    let mut btree = BTree::new();
    btree.insert("m", 13);
    btree.insert("g", 7);
    btree.insert("p", 16);
    btree.insert("d", 4);
    btree.insert("j", 10);
    btree.insert("n", 14);
    btree.insert("t", 20);

    // Deleting "m" will require replacing it with the predecessor key ("j").
    btree.delete(&"m");
    assert_eq!(btree.search(&"m"), None);
}

#[test]
fn test_delete_key_requires_merge() {
    let mut btree = BTree::new();
    btree.insert("m", 13);
    btree.insert("g", 7);
    btree.insert("p", 16);
    btree.insert("d", 4);
    btree.insert("j", 10);
    btree.insert("n", 14);
    btree.insert("t", 20);

    // Delete "d" and "g" to force a merge operation
    btree.delete(&"d");
    btree.delete(&"g");

    // Assert keys are deleted
    assert_eq!(btree.search(&"d"), None);
    assert_eq!(btree.search(&"g"), None);
}

#[test]
fn test_delete_key_requires_borrow() {
    let mut btree = BTree::new();
    btree.insert("m", 13);
    btree.insert("g", 7);
    btree.insert("p", 16);
    btree.insert("d", 4);
    btree.insert("j", 10);
    btree.insert("n", 14);
    btree.insert("t", 20);
    btree.insert("a", 1);
    btree.insert("c", 3);
    btree.insert("f", 6);
    btree.insert("i", 9);
    btree.insert("k", 11);
    btree.insert("o", 15);
    btree.insert("r", 18);
    btree.insert("s", 19);
    btree.print();    

    // Delete "m", "p", and "t" to force borrow operations
    btree.delete(&"m");
    btree.print();    
    btree.delete(&"p");
    btree.print();    
    btree.delete(&"t");
    btree.print();    

    // Assert keys are deleted
    assert_eq!(btree.search(&"m"), None);
    assert_eq!(btree.search(&"p"), None);
    assert_eq!(btree.search(&"t"), None);
}

#[test]
fn test_large_insert_delete() {
    let mut tree = BTree::<String, i32>::new();
    let keys: Vec<String> = (1..1000).map(|i| i.to_string()).collect();
    let values: Vec<i32> = (1..1000).collect();

    for (key, value) in keys.iter().zip(values.iter()) {
        tree.insert(key.clone(), *value);
    }

    println!("B-Tree after create");
    tree.print();

    for (key, value) in keys.iter().zip(values.iter()) {
        assert_eq!(tree.search(&key), Some(value));
    }

    println!("B-Tree before delete");
    tree.print();

    for key in keys.iter() {
        tree.print();
        tree.delete(key);
    }

    for key in keys.iter() {
        assert_eq!(tree.search(&key), None);
    }
}

#[test]
fn test_large_random_insert_delete() {
    let mut tree = BTree::<String, i32>::new();
    let mut keys: Vec<String> = (1..100000).map(|i| i.to_string()).collect();
    let mut values: Vec<i32> = (1..100000).collect();

    let mut rng = thread_rng();
    keys.shuffle(&mut rng);
    values.shuffle(&mut rng);

    for (key, value) in keys.iter().zip(values.iter()) {
        tree.insert(key.clone(), *value);
    }

    for (key, value) in keys.iter().zip(values.iter()) {
        assert_eq!(tree.search(&key), Some(value));
    }

    keys.shuffle(&mut rng);

    for key in keys.iter() {
        tree.delete(key);
    }

    for key in keys.iter() {
        assert_eq!(tree.search(&key), None);
    }
}
