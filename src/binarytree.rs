use crate::binarytree::Node::{Internal, Leaf};
use std::cell::Cell;
use std::cmp::Ordering;
use std::convert::TryInto;


/// Writes a vector to a byte array.
fn write_vec(value: &[u8], output: &mut [u8], mut index: usize) -> usize {
    output[index..(index + 8)].copy_from_slice(&(value.len() as u64).to_be_bytes());
    index += 8;
    output[index..(index + value.len())].copy_from_slice(value);
    index += value.len();
    index
}

/// Returns the `(offset, len)` of the value for a queried key within a binary tree, if present.
///
/// The binary tree is represented as a byte array. This function is used to find the
/// offset and length of a value associated with a given key.
///
/// Given a key, the function starts from the root of the tree and moves either to the left 
/// or the right child depending on whether the key is less than or more than the current 
/// node's key. This process continues until the key is found or it is determined that the 
/// key does not exist in the tree.
///
/// # Arguments
///
/// * `tree` - The byte array representing the binary tree.
/// * `query` - The key being queried.
/// * `index` - The initial index to start the lookup from within the byte array.
///
/// # Returns
///
/// An `Option` that contains a tuple `(usize, usize)`. The first element is the offset 
/// of the value within the byte array. The second element is the length of the value. 
/// If the key is not found in the tree, it returns `None`.
///
/// # Panics
///
/// This function will panic if it encounters a byte in the tree array that does not
/// correspond to a recognized node type (1 or 2).
pub(in crate) fn lookup_in_raw(
    tree: &[u8],
    query: &[u8],
    mut index: usize,
) -> Option<(usize, usize)> {
    match tree[index] {
        1 => {
            // Leaf node
            index += 1;
            let key_len = u64::from_be_bytes(tree[index..(index + 8)].try_into().unwrap()) as usize;
            index += 8;

            match query.cmp(&tree[index..(index + key_len)]) {
                Ordering::Less => None,
                Ordering::Equal => {
                    index += key_len;
                    let value_len =
                        u64::from_be_bytes(tree[index..(index + 8)].try_into().unwrap()) as usize;
                    index += 8;
                    Some((index, value_len))
                }
                Ordering::Greater => {
                    index += key_len;
                    let value_len =
                        u64::from_be_bytes(tree[index..(index + 8)].try_into().unwrap()) as usize;
                    index += 8 + value_len;
                    let second_key_len =
                        u64::from_be_bytes(tree[index..(index + 8)].try_into().unwrap()) as usize;
                    index += 8;
                    if query == &tree[index..(index + second_key_len)] {
                        index += second_key_len;
                        let value_len =
                            u64::from_be_bytes(tree[index..(index + 8)].try_into().unwrap())
                                as usize;
                        index += 8;
                        Some((index, value_len))
                    } else {
                        None
                    }
                }
            }
        }
        2 => {
            index += 1;
            let key_len = u64::from_be_bytes(tree[index..(index + 8)].try_into().unwrap()) as usize;
            index += 8;
            index += key_len;
            if query <= &tree[(index - key_len)..index] {
                // Skip left-node length
                index += 8;
            } else {
                // Skip left-node
                let left_node_len =
                    u64::from_be_bytes(tree[index..(index + 8)].try_into().unwrap()) as usize;
                index += 8 + left_node_len;
            }
            lookup_in_raw(tree, query, index)
        }
        _ => unreachable!(),
    }
}

#[derive(Eq, PartialEq, Debug)]
pub(in crate) enum Node {
    // To decrease the height of the tree, each leaf node may have two key-value pairs
    Leaf((Vec<u8>, Vec<u8>), Option<(Vec<u8>, Vec<u8>)>),

    Internal(Box<Node>, Vec<u8>, Box<Node>),
}

impl Node {
    /// Returns the size of the node in bytes
    pub(in crate) fn recursive_size(&self) -> usize {
        match self {
            Node::Leaf(left_val, right_val) => {
                let mut size = 1; // 1 byte for node type
                size += 8 + left_val.0.len() + 8 + left_val.1.len();
                if let Some(right) = right_val {
                    size += 8 + right.0.len() + 8 + right.1.len();
                } else {
                    // empty right val stored as a single 0 length key
                    size += 8;
                }
                size
            }
            Node::Internal(left, key, right) => {
                let mut size = 1; // 1 byte for node type
                size += 8 + key.len();
                // Reserve space to prefix the left node with its size, so that we can skip it
                // efficiently
                size += 8 + left.recursive_size();
                size += right.recursive_size();
                size
            } }
    }

    /// Returns the index following the last written
    pub(in crate) fn to_bytes(&self, output: &mut [u8], mut index: usize) -> usize {
        match self {
            Node::Leaf(left_val, right_val) => {
                output[index] = 1;
                index += 1;
                index = write_vec(&left_val.0, output, index);
                index = write_vec(&left_val.1, output, index);
                if let Some(right) = right_val {
                    index = write_vec(&right.0, output, index);
                    index = write_vec(&right.1, output, index);
                } else {
                    // empty right val stored as a single 0 length key
                    index = write_vec(&[], output, index);
                }
            }
            Node::Internal(left, key, right) => {
                output[index] = 2;
                index += 1;
                index = write_vec(key, output, index);
                let left_size = left.recursive_size();
                output[index..(index + 8)].copy_from_slice(&(left_size as u64).to_be_bytes());
                index += 8;
                index = left.to_bytes(output, index);
                index = right.to_bytes(output, index);
            }
        }

        index
    }

    /// Returns the maximum key in the tree
    fn get_max_key(&self) -> Vec<u8> {
        match self {
            Node::Leaf((left_key, _), right_val) => {
                if let Some((right_key, _)) = right_val {
                    right_key.to_vec()
                } else {
                    left_key.to_vec()
                }
            }
            Node::Internal(_left, _key, right) => right.get_max_key(),
        }
    }
}

pub(in crate) struct BinarytreeBuilder {
    pairs: Vec<(Vec<u8>, Vec<u8>)>,
}

impl BinarytreeBuilder {
    pub(in crate) fn new() -> BinarytreeBuilder {
        BinarytreeBuilder { pairs: vec![] }
    }

    pub(in crate) fn add(&mut self, key: &[u8], value: &[u8]) {
        self.pairs.push((key.to_vec(), value.to_vec()));
    }

    /// Builds a balanced binary tree from the provided key-value pairs.
    ///
    /// This function operates by first sorting the pairs by key to ensure balance, then
    /// constructs the tree by creating leaves from pairs of elements and combining them
    /// into internal nodes. If there is an odd number of elements, the last one is handled separately.
    /// 
    /// The tree is built in a bottom-up manner, i.e., leaves are created first and then
    /// internal nodes are created by combining these leaves. This process continues until
    /// we have a single node, which is the root of the tree.
    ///
    /// A critical part of this function is the `maybe_previous_node` variable. 
    /// This variable is used to hold a node from the previous iteration of the loop, 
    /// effectively serving as a 'buffer'. This buffering is essential because, 
    /// for each internal (non-leaf) node, we need two child nodes. However, 
    /// we're processing the nodes one at a time. So after processing one node, 
    /// we store it in `maybe_previous_node` until we process the next node. 
    /// After the second node is processed, we can then create an internal node 
    /// with `maybe_previous_node` and the second node as its children.
    ///
    /// The use of `maybe_previous_node` is similar to a state machine. 
    /// After every two nodes are processed, the state is reset 
    /// (by creating an internal node and clearing maybe_previous_node), 
    /// and the process starts over for the next pair of nodes. 
    /// This continues until we only have one node left, which is the root of the tree.
    ///
    /// # Panics
    /// 
    /// This function will panic if the `pairs` vector is empty, as it's not possible to build
    /// a tree without any nodes.
    ///
    /// It will also panic in case a duplicate key is encountered during tree building, as 
    /// it currently does not support overwriting existing keys.
    ///
    /// # Returns
    /// 
    /// This function returns the root `Node` of the constructed tree.
    pub(in crate) fn build(mut self) -> Node {
        // we want a balanced tree, so we sort the pairs by key
        assert!(!self.pairs.is_empty());
        self.pairs.sort();
        let mut leaves = vec![];

        for group in self.pairs.chunks(2) {
            let leaf = if group.len() == 1 {
                Leaf((group[0].0.to_vec(), group[0].1.to_vec()), None)
            } else {
                assert_eq!(group.len(), 2);
                if group[0].0 == group[1].0 {
                    todo!("support overwriting existing keys");
                }
                Leaf(
                    (group[0].0.to_vec(), group[0].1.to_vec()),
                    Some((group[1].0.to_vec(), group[1].1.to_vec())),
                )
            };
            leaves.push(leaf);
        }

        let mut bottom = leaves;
        let maybe_previous_node: Cell<Option<Node>> = Cell::new(None);

        while bottom.len() > 1 {
            let mut internals = vec![];
            for node in bottom.drain(..) {
                if let Some(previous_node) = maybe_previous_node.take() {
                    let key = previous_node.get_max_key(); // 2
                    let internal = Internal(Box::new(previous_node), key, Box::new(node));
                    internals.push(internal)
                } else {
                    maybe_previous_node.set(Some(node));
                }
            }

            if let Some(previous_node) = maybe_previous_node.take() {
                internals.push(previous_node);
            }

            bottom = internals
        }

        bottom.pop().unwrap()
    }
}

#[cfg(test)]
mod test {
    use crate::binarytree::Node::{Internal, Leaf};
    use crate::binarytree::{BinarytreeBuilder, Node};

    fn gen_tree() -> Node {
        let left = Leaf(
            (b"hello".to_vec(), b"world".to_vec()),
            Some((b"hello2".to_vec(), b"world2".to_vec())),
        );
        let right = Leaf((b"hello3".to_vec(), b"world3".to_vec()), None);
        Internal(Box::new(left), b"hello2".to_vec(), Box::new(right))
    }

    #[test]
    fn serialize() {
        let internal = gen_tree();
        let size = internal.recursive_size();
        let mut buffer = vec![0u8; size];
        internal.to_bytes(&mut buffer, 0);
    }

    #[test]
    fn builder() {
        let expected = gen_tree();
        let mut builder = BinarytreeBuilder::new();
        builder.add(b"hello2", b"world2");
        builder.add(b"hello3", b"world3");
        builder.add(b"hello", b"world");

        assert_eq!(expected, builder.build());
    }
}
