use crate::binarytree::Node::{Internal, Leaf};
use crate::page_manager::{Page, PageManager, PageMut};
use std::cell::Cell;
use std::cmp::Ordering;
use std::convert::TryInto;

const LEAF: u8 = 1;
const INTERNAL: u8 = 2;

/// Writes a vector to a byte array.
fn write_vec(value: &[u8], output: &mut [u8], mut index: usize) -> usize {
    output[index..(index + 8)].copy_from_slice(&(value.len() as u64).to_be_bytes());
    index += 8;
    output[index..(index + value.len())].copy_from_slice(value);
    index += value.len();
    index
}

// Provides a simple zero-copy way to access entries
//
// Entry format is:
// * (8 bytes) key_size
// * (key_size bytes) key_data
// * (8 bytes) value_size
// * (value_size bytes) value_data
struct EntryAccessor<'a> {
    raw: &'a [u8],
}

impl<'a> EntryAccessor<'a> {
    fn new(raw: &'a [u8]) -> Self {
        EntryAccessor { raw }
    }

    fn key_len(&self) -> usize {
        u64::from_be_bytes(self.raw[0..8].try_into().unwrap()) as usize
    }

    fn key(&self) -> &'a [u8] {
        &self.raw[8..(8 + self.key_len())]
    }

    fn value_offset(&self) -> usize {
        8 + self.key_len() + 8
    }

    fn value_len(&self) -> usize {
        let key_len = self.key_len();
        u64::from_be_bytes(
            self.raw[(8 + key_len)..(8 + key_len + 8)]
                .try_into()
                .unwrap(),
        ) as usize
    }

    fn value(&self) -> &'a [u8] {
        &self.raw[self.value_offset()..(self.value_offset() + self.value_len())]
    }

    fn raw_len(&self) -> usize {
        8 + self.key_len() + 8 + self.value_len()
    }
}

// Note the caller is responsible for ensuring that the buffer is large enough
// and rewriting all fields if any dynamically sized fields are written
struct EntryMutator<'a> {
    raw: &'a mut [u8],
}

impl<'a> EntryMutator<'a> {
    fn new(raw: &'a mut [u8]) -> Self {
        EntryMutator { raw }
    }

    fn write_key(&mut self, key: &[u8]) {
        self.raw[0..8].copy_from_slice(&(key.len() as u64).to_be_bytes());
        self.raw[8..(8 + key.len())].copy_from_slice(key);
    }

    fn write_value(&mut self, value: &[u8]) {
        let value_offset = 8 + EntryAccessor::new(self.raw).key_len();
        self.raw[value_offset..(value_offset + 8)]
            .copy_from_slice(&(value.len() as u64).to_be_bytes());
        self.raw[(value_offset + 8)..(value_offset + 8 + value.len())].copy_from_slice(value);
    }
}

// Provides a simple zero-copy way to access a leaf page
//
// Entry format is:
// * (1 byte) type: 1 = LEAF
// * (n bytes) lesser_entry
// * (n bytes) greater_entry: optional
struct LeafAccessor<'a: 'b, 'b> {
    page: &'b Page<'a>,
}

impl<'a: 'b, 'b> LeafAccessor<'a, 'b> {
    fn new(page: &'b Page<'a>) -> Self {
        LeafAccessor { page }
    }

    fn offset_of_lesser(&self) -> usize {
        1
    }

    fn offset_of_greater(&self) -> usize {
        1 + self.lesser().raw_len()
    }

    fn lesser(&self) -> EntryAccessor {
        EntryAccessor::new(&self.page.memory()[self.offset_of_lesser()..])
    }

    fn greater(&self) -> EntryAccessor {
        EntryAccessor::new(&self.page.memory()[self.offset_of_greater()..])
    }
}

// Note the caller is responsible for ensuring that the buffer is large enough
// and rewriting all fields if any dynamically sized fields are written
struct LeafBuilder<'a: 'b, 'b> {
    page: &'b mut PageMut<'a>,
}

impl<'a: 'b, 'b> LeafBuilder<'a, 'b> {
    fn new(page: &'b mut PageMut<'a>) -> Self {
        page.memory_mut()[0] = LEAF;
        LeafBuilder { page }
    }

    fn write_lesser(&mut self, key: &[u8], value: &[u8]) {
        let mut entry = EntryMutator::new(&mut self.page.memory_mut()[1..]);
        entry.write_key(key);
        entry.write_value(value);
    }

    fn write_greater(&mut self, pair: Option<(&[u8], &[u8])>) {
        let offset = 1 + EntryAccessor::new(&self.page.memory()[1..]).raw_len();
        let mut entry = EntryMutator::new(&mut self.page.memory_mut()[offset..]);
        if let Some((key, value)) = pair {
            entry.write_key(key);
            entry.write_value(value);
        } else {
            entry.write_key(&[]);
        }
    }
}

/// Returns a tuple of the form `(Page<'a>, usize, usize)` representing the value for a queried key within a binary tree if present.
///
/// The binary tree is composed of Nodes serialized into `Page`s and maintained by a `PageManager`. This function attempts to locate a key within this tree and if found, returns a tuple where:
/// - The first element is the `Page` in which the value is located
/// - The second element is the offset within that page where the value begins
/// - The third element is the length of the value
///
/// Given a key, the function begins at the root of the tree and traverses to the left or right child depending on whether the key is less or greater than the current node's key. The process is recursive and continues until the key is either found or it is determined that the key does not exist in the tree.
///
/// This function might not be space efficient since it allocates a whole page for each node, leading to a waste of space when nodes don't fully occupy their corresponding pages. Future optimizations could involve storing multiple nodes within a single page, or having variable size pages to better match the size of nodes.
///
/// # Arguments
///
/// * `page` - The `Page` object representing the current node being inspected.
/// * `query` - The key being searched for.
/// * `manager` - The `PageManager` managing the pages.
///
/// # Returns
///
/// An `Option` that contains a tuple `(Page<'a>, usize, usize)`. If the key is found, it returns `Some`, with the `Page` containing the value, the offset of the value within the page, and the length of the value. If the key is not found in the tree, it returns `None`.
///
/// # Panics
///
/// This function will panic if it encounters a byte in the `Page` memory that does not correspond to a recognized node type (1 for leaf node or 2 for internal node).
pub(crate) fn lookup_in_raw<'a>(
    page: Page<'a>,
    query: &[u8],
    manager: &'a PageManager,
) -> Option<(Page<'a>, usize, usize)> {
    let node_mem = page.memory();
    let mut index = 0;
    match node_mem[index] {
        LEAF => {
            // Leaf node
            let accessor = LeafAccessor::new(&page);
            match query.cmp(accessor.lesser().key()) {
                Ordering::Less => None,
                Ordering::Equal => {
                    let offset = accessor.offset_of_lesser() + accessor.lesser().value_offset();
                    let value_len = accessor.lesser().value().len();
                    Some((page, offset, value_len))
                }
                Ordering::Greater => {
                    if query == accessor.greater().key() {
                        let offset =
                            accessor.offset_of_greater() + accessor.greater().value_offset();
                        let value_len = accessor.greater().value().len();
                        Some((page, offset, value_len))
                    } else {
                        None
                    }
                }
            }
        }
        INTERNAL => {
            index += 1;
            let key_len =
                u64::from_be_bytes(node_mem[index..(index + 8)].try_into().unwrap()) as usize;
            index += 8;
            let key = &node_mem[index..(index + key_len)];
            index += key_len;
            let left_page = u64::from_be_bytes(node_mem[index..(index + 8)].try_into().unwrap());
            index += 8;
            let right_page = u64::from_be_bytes(node_mem[index..(index + 8)].try_into().unwrap());
            if query <= key {
                lookup_in_raw(manager.get_page(left_page), query, manager)
            } else {
                lookup_in_raw(manager.get_page(right_page), query, manager)
            }
        }
        _ => unreachable!(),
    }
}

#[derive(Eq, PartialEq, Debug)]
pub(crate) enum Node {
    // To decrease the height of the tree, each leaf node may have two key-value pairs
    Leaf((Vec<u8>, Vec<u8>), Option<(Vec<u8>, Vec<u8>)>),
    Internal(Box<Node>, Vec<u8>, Box<Node>),
}

impl Node {
    // Returns the page number that the node was written to
    pub(crate) fn to_bytes(&self, page_manager: &PageManager) -> u64 {
        match self {
            Node::Leaf(left_val, right_val) => {
                let mut page = page_manager.allocate();
                let mut builder = LeafBuilder::new(&mut page);
                builder.write_lesser(&left_val.0, &left_val.1);
                builder.write_greater(
                    right_val
                        .as_ref()
                        .map(|(key, value)| (key.as_slice(), value.as_slice())),
                );
                page.get_page_number()
            }
            Node::Internal(left, key, right) => {
                let left_page = left.to_bytes(page_manager);
                let right_page = right.to_bytes(page_manager);
                let mut page = page_manager.allocate();
                let mut index = 0;
                let output = page.memory_mut();
                output[index] = INTERNAL;
                index += 1;
                index = write_vec(key, output, index);
                output[index..(index + 8)].copy_from_slice(&left_page.to_be_bytes());
                index += 8;
                output[index..(index + 8)].copy_from_slice(&right_page.to_be_bytes());

                page.get_page_number()
            }
        }
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

pub(crate) struct BinarytreeBuilder {
    pairs: Vec<(Vec<u8>, Vec<u8>)>,
}

impl BinarytreeBuilder {
    pub(crate) fn new() -> BinarytreeBuilder {
        BinarytreeBuilder { pairs: vec![] }
    }

    pub(crate) fn add(&mut self, key: &[u8], value: &[u8]) {
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
    pub(crate) fn build(mut self) -> Node {
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
    fn builder() {
        let expected = gen_tree();
        let mut builder = BinarytreeBuilder::new();
        builder.add(b"hello2", b"world2");
        builder.add(b"hello3", b"world3");
        builder.add(b"hello", b"world");

        assert_eq!(expected, builder.build());
    }
}
