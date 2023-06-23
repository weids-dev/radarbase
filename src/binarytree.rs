use crate::binarytree::Node::{Internal, Leaf};
use std::cell::Cell;
use std::cmp::Ordering;
use std::convert::TryInto;

fn write_vec(value: &[u8], output: &mut [u8], mut index: usize) -> usize {
    output[index..(index + 8)].copy_from_slice(&(value.len() as u64).to_be_bytes());
    index += 8;
    output[index..(index + value.len())].copy_from_slice(value);
    index += value.len();
    index
}

// Returns the (offset, len) of the value for the queried key, if present
pub(in crate) fn lookup_in_raw(
    tree: &[u8],
    query: &[u8],
    mut index: usize,
) -> Option<(usize, usize)> {
    match tree[index] {
        1 => {
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
    Leaf((Vec<u8>, Vec<u8>), Option<(Vec<u8>, Vec<u8>)>),
    Internal(Box<Node>, Vec<u8>, Box<Node>),
}

impl Node {
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
    // Returns the index following the last written
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

    pub(in crate) fn build(mut self) -> Node {
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
                    let key = previous_node.get_max_key();
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