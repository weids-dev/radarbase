/*
 * B Tree Implementation
 * In the current implementation, both internal and leaf nodes store key-value pairs
 *
 * The properties of a B-tree are:
 * 1. Every node has at most 2 * B - 1 keys
 * 2. All keys in a node are in the ascending order
 * 3. All keys in the subtree rooted at a child node `i` are greater than the key at index `i - 1`
 *    and less than the key at index `i`
 *
 * ############################################################################################
 *
 * For example, consider this internal node:
 *
 * keys: [K1, K2, K3]
 * values: [V1, V2, V3]
 * children: [C0, C1, C2, C3]
 *
 * Here, all the keys in the subtree rooted at C0 are less than K1, all the keys in the subtree
 * rooted at C1 are between K1 and K2, etc.
 *
 *
 * the B-Tree maintains its properties by carefully inserting and splitting nodes
 * The insert_non_full function ensures that keys are inserted in the correct order.
 * The split_child function takes care of splitting nodes when they become full.
 *
 * ############################################################################################
 *
 * For the split_child function, maybe it is pretty hard to understand, let's draw some figures:
 * Let's say B is 2, here is the current B-Tree:
 *     [5]
 *    /   \
 * {2, 4} {6, 8, 9}
 *
 * Now, let's say we want to insert the kv pair (7, V7). The root node is not full,
 * so we proceed to insert the key-value pair into the appropriate child node. In this case,
 * it is the right child node, which is already full:
 *
 *     [5]
 *    /   \
 * {2, 4} {6, 8, 9, 7}
 *
 * Since the right child node is full, we need to split it, the split_child fn will be called:
 *
 * 1. Identify the middle key and value (8, V8) in this case.
 * 2. Create a new node to store the keys and values to the right of the middle key. (9, V9)
 * 3. Remove the keys and values to the right of the middle key from the original node, as well as
 *    the middle key and value.
 * 4. Insert the middle key and value into the parent node (root) at the appropriate position.
 * 5. Add the newly created node as a child of the parent node (root) to the right of the orginal
 *    child node.
 *
 * After the split, the tree will look like this:
 *    [5, 8]
 *    /   |   \
 * {2, 4} {6, 7} {9}
 *
 *
 */

use std::fmt::Debug;

const B: usize = 3; // minimum degree

#[derive(Clone, Debug)]
pub struct BTree<K: Ord + Clone + Debug, V: Clone + Debug> {
    root: Option<Box<Node<K, V>>>,
}

#[derive(Clone, Debug)]
pub struct Node<K: Ord + Clone + Debug, V: Clone + Debug> {
    keys: Vec<K>,
    values: Vec<V>,
    children: Vec<Box<Node<K, V>>>,
}

impl<K: Ord + Clone + Debug, V: Clone + Debug> BTree<K, V> {
    pub fn new() -> Self {
        BTree { root: None }
    }

    pub fn print(&self) {
        if let Some(root) = &self.root {
            root.print(0);
        }
    }

    pub fn traverse(&self) -> Vec<(K, V)> 
    where
        K: Clone,
        V: Clone,
    {
        let mut kv_pairs = Vec::new();
        if let Some(root) = &self.root {
            Self::dfs(&**root, &mut kv_pairs);
        }
        kv_pairs
    }

    // Add the dfs() method as an associated function
    fn dfs(node: &Node<K, V>, kv_pairs: &mut Vec<(K, V)>) 
    where
        K: Clone,
        V: Clone,
    {
        for i in 0..node.keys.len() {
            if let Some(child) = node.children.get(i) {
                Self::dfs(child, kv_pairs);
            }
            kv_pairs.push((node.keys[i].clone(), node.values[i].clone()));
        }

        if let Some(child) = node.children.last() {
            Self::dfs(child, kv_pairs);
        }
    }

    pub fn insert(&mut self, key: K, value: V) {
        // Insert key-value pair and handle tree updates
        if let Some(root) = &mut self.root { // if root is not None
            // if let patten is checking whether self.root is of type Option<T> and whether it is
            // Some, if it is, then the value inside the Some variant is bound to the var root
            // and the code inside the if let block is executed
            if root.is_full() { // it has 2 * B - 1 keys
                // split it before inserting
                let mut new_root = Box::new(Node::new());
                new_root.children.push(root.clone()); 
                new_root.split_child(0);
                new_root.insert_non_full(key.clone(), value.clone());
                self.root = Some(new_root);
            } else {
                root.insert_non_full(key.clone(), value.clone());
            }
        } else {
            let mut new_root = Box::new(Node::new());
            new_root.insert_non_full(key.clone(), value.clone());
            self.root = Some(new_root)
            // the Some is just a wrapper, it set the Option of new_root to be Some
        }
    }

    pub fn delete(&mut self, key: &K) -> Option<V> {
        println!("Deleting {:?} from root", key);
        if let Some(root) = &mut self.root {
            let deleted_value = root.delete(key);
            if root.keys.is_empty() {
                if root.children.is_empty() {
                    self.root = None;
                } else {
                    self.root = Some(root.children.remove(0));
                }
            }
            deleted_value
        } else {
            None
        }
    }

    pub fn search(&self, key: &K) -> Option<&V> {
        // Search for a key and return the associated value if found
        self.root.as_ref().and_then(|root| root.search(key))
    }


    pub fn print_tree(&self) {
        if let Some(ref root) = self.root {
            root.print_node(0);
        } else {
            println!("Empty tree");
        }
    }
}

impl<K: Ord + Clone + Debug, V: Clone + Debug> Node<K, V> {
    // Helper methods for B-tree operations (insert, delete, search, etc.)
    // Methods like split, merge, and other utility methods will be implemented here
    fn new() -> Self {
        Node {
            keys: Vec::new(),
            values: Vec::new(),
            children: Vec::new(),
        }
    }

    fn print(&self, depth: usize) {
        let indent = "  ".repeat(depth);
        println!("{}{:?} {:?}", indent, self.keys, self.values);
        for child in &self.children {
            child.print(depth + 1);
        }
    }

    fn print_node(&self, depth: usize) {
        println!(
            "{:indent$}{:?}",
            "",
            (self.keys.clone(), self.values.clone()),
            indent = depth * 2
        );

        for child in &self.children {
            child.print_node(depth + 1);
        }
    }

    fn is_full(&self) -> bool {
        self.keys.len() >= 2 * B - 1
    }

    fn split_child(&mut self, index: usize) {
        // index refers to the child node that needs to be split, self refers to the new_root

        // 1. identify the middle key and value
        let split_key = self.children[index].keys[B - 1].clone();
        let split_value = self.children[index].values[B - 1].clone();

        // 2. Create new node to store the keys and values to right of the middle key
        let mut right = Box::new(Node::new());

        // 3. Remove they keys and values to the right of the middle key from the original node
        // (greater part)
        right.keys = self.children[index].keys.split_off(B); // second half of the keys
        right.values = self.children[index].values.split_off(B);

        self.children[index].keys.remove(B-1);
        self.children[index].values.remove(B-1);

        // now the self.children[index] becomes the first half of the keys (left)

        if !self.children[index].children.is_empty() {
            // if the original full root has some other childrens, we split the right part of the
            // child into the right part of the new root's child
            // which also means the root is a internal node, will have at least B children
            right.children = self.children[index].children.split_off(B);
        }

        // 4. insert the middle key and value into the root at the appropriate position
        self.keys.insert(index, split_key);
        self.values.insert(index, split_value);

        // 5. Add the newly created node as a child of the parent node to the right of the original
        // child node
        self.children.insert(index + 1, right);
    }

    fn insert_non_full(&mut self, key: K, value: V) {
        let mut index = match self.keys.binary_search(&key) {
            // the reason we are using binary_seach here is to ensure the keys are sorted
            // which means, find the appropriate position for the new key
            Ok(_) => return, // Key already exists, so we don't need to insert it
            Err(index) => index,
        };
        // the index is the new key's position in the self.keys

        /* In a B-Tree, the internal nodes primarily serve as a way to navigate through the tree
         * structure to reach the leaf nodes, where the actual key-value pairs are stored, by
         * always attempting insert the key into a leaf node, we ensure that the tree remains
         * balanced and that the properties of the B-Tree are maintained, the value of internal
         * node will changed only when the current node is full (children), and we need to split it
         */

        if self.children.is_empty() {
            // Leaf node case
            // termination condition (DFS)
            self.keys.insert(index, key);
            self.values.insert(index, value);
        } else {
            // Internal node case
            if self.children[index].is_full() {
                self.split_child(index); // split the current index

                // After splitting, check if the new key should go to the right child
                if self.keys[index].lt(&key) {
                    index += 1;
                }
            }
            self.children[index].insert_non_full(key, value);
        }
    }

    fn search(&self, key: &K) -> Option<&V> {
        match self.keys.binary_search(key) {
            Ok(index) => Some(&self.values[index]),
            Err(index) => {
                if self.children.is_empty() {
                    None
                } else {
                    println!("Searching value '{:?}' in node: {:?}, next index: {:?}", key, self.values, index);
                    self.children[index].search(key)
                }
            }
        }
    }

    pub fn delete(&mut self, key: &K) -> Option<V> {
        println!("Deleting key '{:?}' from node: {:?}", key, self.keys);
        match self.keys.binary_search(&key) {
            Ok(index) => {
                println!("Found key at index: {:?}", index);
                if self.children.is_empty() {
                    // Case 1: The key is in the current node and it's a leaf node
                    // Then we just simply remove the key and value
                    println!("Case 1: The key '{:?}' is on the leaf node, remove it directly.", key);
                    self.keys.remove(index);
                    return Some(self.values.remove(index));
                } else {
                    // Case 2: The key is in the current node and it's an internal node
                    // To maintain the B-Tree properties, we cannot just remove the key and its
                    // value, instead, we have to find an appropriate replacement key and value.
                    if self.children[index].keys.len() >= B {
                        // Case 2a: If the child node to the left of the key has at least B keys,
                        // (since any node with less than B keys is considered to be deficient)
                        // if it does, we find the predecessor of the key to be deleted (the
                        // largest key in the left subtree), replace the key and its value in the
                        // current node with the successor's key and value, and then recursively
                        // delete the successor key from the left child.
                        
                        /* What do you mean by the left?
                         * As we've mentioned before, the keys in the internal node are used to
                         * navigate through the tree, and the all the childs from 0 to index are
                         * smaller than the key at index (keys). that is called the left part of tree.
                         */
                        let (pred_key, pred_value) = self.children[index].find_predecessor();
                        println!("Case 2a: The key '{:?}' is deleted since it is on the internal node", key);
                        self.keys[index] = pred_key.clone();
                        self.values[index] = pred_value.clone();
                        return self.children[index].delete(&pred_key); // recursive
                    } else if self.children[index + 1].keys.len() >= B {
                        // Case 2b: If the left child doesn't have enough keys, we check if the
                        // right child has at least B keys. If it does, we find the successor of 
                        // the key to be deleted (the smallest key in the right subtree), replace
                        // the key and its value in the current node, and then recursively delete
                        // the successor key from the left child.
                        let (succ_key, succ_value) = self.children[index + 1].find_successor();
                        println!("Case 2b: The key '{:?}' is deleted since it is on the internal node", key);
                        self.keys[index] = succ_key.clone();
                        self.values[index] = succ_value.clone();
                        return self.children[index + 1].delete(&succ_key); // recursive
                    } else {
                        // Case 2c: If both the left and right children have less than B keys
                        // we merge the current node with the left child and then recursively
                        // delete the successor key from the right child.
                        
                        /* Why can both left and right child have less than B keys?
                         * During the delete operation, we might temporaily encounter situations
                         * where both left and right children have less than B keys. This happends
                         * because when a key is deleted from an internal node, we might need to
                         * merge the node with one of its children, which could cause both of them
                         * to temporaily have less than B keys, and they must have exactly B-1 keys.
                         * we can merge the current node with one of its children (left or right) to
                         * ensure that the B-Tree properties are maintained.
                         */

                        /* The usage of merge_with_left:
                         * Since both the left child (self.children[index]) and right child
                         * (self.children[index+1]) do not have enough keys, which means we cannot
                         * directly find a key to replace the key we want to delete, so what we do
                         * is to merge the current node with the left and right child.
                         *
                         * the merge_with_left function move the key and value that we want to
                         * delete and all the key-value in the right child into the left child.
                         * which is B-1 + 1 + B-1 = 2B-1 keys in total.
                         *
                         * Then we perform delete on that left child.
                         */
                        println!("Case 2c: The key '{:?}' is removed on merge_with_left, \
				  and we move our left and right sibling together", key);
                        self.merge_with_left(index+1); 
                        self.children.remove(index+1);
                        return self.children[index].delete(key);
                    }
                }
            }

            Err(index) => {
                // Case 3: The key is not in the current node
                println!("Case 3: The key '{:?}' is not in the current node, the desired index is {:?}", key, index);
                if self.children.is_empty() {
                    // Case 3a: If the current node is a leaf node, then the key is not in the tree
                    println!("Case 3a: If the current node is a leaf node, then the key is not in the tree, NOT FOUND");
                    return None;
                } else {
                    // Case 3b: If the current node is an internal node, we need to ensure that the
                    // child node at the target index has at least B keys before recursively
                    // deleting the key from that child.
                    if self.children[index].keys.len() < B {
                        if index > 0 && self.children[index - 1].keys.len() >= B {
                            // Case 3b1: If the left sibling (at index-1) exists and has at least B
                            // keys, borrow a key from the left sibling
                            println!("Case 3b1: If the left sibling (at index-1) exists and has at least B keys, borrow a key from the left sibling");
                            self.borrow_from_left(index);
                            let borrowed_key = self.keys.remove(index);
                            let borrowed_value = self.values.remove(index);
                            self.children[index].keys.insert(0, borrowed_key);
                            self.children[index].values.insert(0, borrowed_value);
                        } else if index < self.children.len() - 1 && self.children[index + 1].keys.len() >= B {
                            // Case 3b2: If the right sibling (at index+1) exists and has at least
                            // B keys, borrow a key from the right sibling
                            println!("Case 3b2: If the right sibling (at index+1) exists and has at least B keys, borrow a key from the right sibling");
                            self.borrow_from_right(index);
                            let borrowed_key = self.keys.remove(index+1);
                            let borrowed_value = self.values.remove(index+1);
                            self.children[index].keys.push(borrowed_key);
                            self.children[index].values.push(borrowed_value);
                        } else if index > 0 {
                            // Case 3b3: if the left sibling exists but has less than B keys, merge the child
                            // with the left sibling
                            println!("Case 3b3: if the left sibling exists but has less than B keys, merge the child with the left sibling");
                            self.merge_with_left(index);
                            self.children.remove(index);
                            return self.children[index-1].delete(key);
                        } else {
                            // Case 3b4: if the left sibling doesn't exist, merge the child with the right sibling
                            println!("Case 3b4: if the left sibling doesn't exist, merge the child with the right sibling");
                            self.merge_with_right(index);
                            self.children.remove(index+1);
                        }
                    }

                    // Case 3c: After ensuring the child at index, and that child has enough keys,
                    // recursively call the delete method on the child.
                    self.children[index].delete(key)
                }
            }
        }
    }


    // helper functions
    fn borrow_from_left(&mut self, index: usize) {
        // Borrow a key from the left sibling, assuming that the left sibling has more than B-1 keys

        // since it is the right most child, it will not violate the navigational property

        let left_sibling = &mut self.children[index - 1];
        let left_sibling_key = left_sibling.keys.pop().unwrap(); // largest
        let left_sibling_value = left_sibling.values.pop().unwrap();

        self.keys.insert(index - 1, left_sibling_key);
        self.values.insert(index - 1, left_sibling_value);

        // Move the rightmost child of the left sibling to the leftmost child of the current node
        if !left_sibling.children.is_empty() {
            let left_sibling_child = left_sibling.children.pop().unwrap();
            self.children[index].children.insert(0, left_sibling_child);
        }
    }

    fn borrow_from_right(&mut self, index: usize) {
        // Borrow a key from the right sibling, assuming that the right sibling has more than B-1 keys
        
        let right_sibling = &mut self.children[index + 1];
        let right_sibling_key = right_sibling.keys.remove(0);
        let right_sibling_value = right_sibling.values.remove(0);

        println!("right_sibling_key: {:?}", right_sibling_key);
        self.keys.insert(index, right_sibling_key);
        self.values.insert(index, right_sibling_value);
        println!("self.keys: {:?}", self.keys);

        // Move the leftmost child of the right sibling to the rightmost child of the current node
        if !right_sibling.children.is_empty() {
            let right_sibling_child = right_sibling.children.remove(0);
            self.children[index].children.push(right_sibling_child);
        }
    }

    fn merge_with_left(&mut self, index: usize) {
        // Merge the current node with the left sibling, assuming that the left sibling has B-1 keys

        // 1. get the key and value that we want to delete, and delete that from original self.keys
        let parent_key = self.keys.remove(index - 1);
        let parent_value = self.values.remove(index - 1);

        let (left_children, right_children) = self.children.split_at_mut(index);

        let left_sibling = &mut left_children[index - 1]; // merge the current node with the left sibling
        println!("left_sibling.keys: {:?}", left_sibling.keys);

        let current_node = &mut right_children[0];        // the right child of the key you want to delete
        println!("current_node.keys: {:?}", current_node.keys);

        // 2. move the deleted key and value to the left sibling, as well as the right child keys
        // so after that the left sibling will have 2B-1 keys in total
        // still maintain the order of the keys since the keys on left_sibling are sorted, the keys
        // on the right child are sorted, and the deleted key is the largest key on the left sibling
        left_sibling.keys.push(parent_key); 
        left_sibling.values.push(parent_value);
        left_sibling.keys.append(&mut current_node.keys);
        left_sibling.values.append(&mut current_node.values);

        println!("left_sibling.keys: {:?}", left_sibling.keys);

        if !current_node.children.is_empty() {
            left_sibling.children.append(&mut current_node.children);
        }
    }

    fn merge_with_right(&mut self, index: usize) {
        // Merge the current node with the right sibling, assuming that the right sibling has B-1 keys
        let parent_key = self.keys.remove(index);
        let parent_value = self.values.remove(index);


        // Split children at index to create two mutable slices without overlapping
        let (left_children, right_children) = self.children.split_at_mut(index+1);
        
        let current_node = &mut left_children[index];

        let right_sibling = &mut right_children[0];

        current_node.keys.push(parent_key);
        current_node.values.push(parent_value);

        // Move all keys, values, and children from the right sibling to the current node
        current_node.keys.append(&mut right_sibling.keys);
        current_node.values.append(&mut right_sibling.values);

        if !right_sibling.children.is_empty() {
            current_node.children.append(&mut right_sibling.children);
        }

    }

    fn find_predecessor(&self) -> (K, V) {
        // Find the predecessor key and value in the subtree rooted at the current node
        let mut node = self;

        while !node.children.is_empty() {
            // traverse down the tree by always chooseing the rightmost child at each step until we
            // reach the leaf node. This is because in B-Trees, the largest key will always be in
            // the rightmost path of the subtree.
            node = &node.children[node.children.len() - 1];
        }

        // return the largest key and value in the leaf node's clone
        (node.keys[node.keys.len() - 1].clone(), node.values[node.values.len() - 1].clone())
    }

    fn find_successor(&self) -> (K, V) {
        // Find the successor key and value in the subtree rooted at the current node
        let mut node = self;

        while !node.children.is_empty() {
            node = &node.children[0];
        }

        (node.keys[0].clone(), node.values[0].clone())
    }
}
