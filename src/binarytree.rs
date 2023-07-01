use crate::binarytree::Node::{Internal, Leaf};
use crate::page_manager::{Page, PageManager, PageMut};
use std::cell::Cell;
use std::cmp::Ordering;
use std::convert::TryInto;

const LEAF: u8 = 1;
const INTERNAL: u8 = 2;

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

    fn greater(&self) -> Option<EntryAccessor> {
        let entry = EntryAccessor::new(&self.page.memory()[self.offset_of_greater()..]);
        if entry.key_len() == 0 {
            None
        } else {
            Some(entry)
        }
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

// Provides a simple zero-copy way to access a leaf page
//
// Entry format is:
// * (1 byte) type: 2 = INTERNAL
// * (8 bytes) key_len
// * (key_len bytes) key_data
// * (8 bytes) lte_page: page number for keys <= key_data
// * (8 bytes) gt_page: page number for keys > key_data
struct InternalAccessor<'a: 'b, 'b> {
    page: &'b Page<'a>,
}

impl<'a: 'b, 'b> InternalAccessor<'a, 'b> {
    fn new(page: &'b Page<'a>) -> Self {
        InternalAccessor { page }
    }

    fn key_len(&self) -> usize {
        u64::from_be_bytes(self.page.memory()[1..9].try_into().unwrap()) as usize
    }

    fn key(&self) -> &[u8] {
        &self.page.memory()[9..(9 + self.key_len())]
    }

    fn lte_page(&self) -> u64 {
        let offset = 9 + self.key_len();
        u64::from_be_bytes(self.page.memory()[offset..(offset + 8)].try_into().unwrap())
    }

    fn gt_page(&self) -> u64 {
        let offset = 9 + self.key_len() + 8;
        u64::from_be_bytes(self.page.memory()[offset..(offset + 8)].try_into().unwrap())
    }
}

// Note the caller is responsible for ensuring that the buffer is large enough
// and rewriting all fields if any dynamically sized fields are written
struct InternalBuilder<'a: 'b, 'b> {
    page: &'b mut PageMut<'a>,
}

impl<'a: 'b, 'b> InternalBuilder<'a, 'b> {
    fn new(page: &'b mut PageMut<'a>) -> Self {
        page.memory_mut()[0] = INTERNAL;
        InternalBuilder { page }
    }

    fn key_len(&self) -> usize {
        u64::from_be_bytes(self.page.memory()[1..9].try_into().unwrap()) as usize
    }

    fn write_key(&mut self, key: &[u8]) {
        self.page.memory_mut()[1..9].copy_from_slice(&(key.len() as u64).to_be_bytes());
        self.page.memory_mut()[9..(9 + key.len())].copy_from_slice(key);
    }

    fn write_lte_page(&mut self, page_number: u64) {
        let offset = 9 + self.key_len();
        self.page.memory_mut()[offset..(offset + 8)].copy_from_slice(&page_number.to_be_bytes());
    }

    fn write_gt_page(&mut self, page_number: u64) {
        let offset = 9 + self.key_len() + 8;
        self.page.memory_mut()[offset..(offset + 8)].copy_from_slice(&page_number.to_be_bytes());
    }
}

// Returns the number of key-value pairs in the tree
pub(crate) fn tree_size<'a>(page: Page<'a>, manager: &'a PageManager) -> usize {
    let node_mem = page.memory();
    match node_mem[0] {
        LEAF => {
            let accessor = LeafAccessor::new(&page);
            if accessor.greater().is_some() {
                2
            } else {
                1
            }
        }
        INTERNAL => {
            let accessor = InternalAccessor::new(&page);
            let left_page = accessor.lte_page();
            let right_page = accessor.gt_page();
            tree_size(manager.get_page(left_page), manager)
                + tree_size(manager.get_page(right_page), manager)
        }
        _ => unreachable!(),
    }
}

// Returns the page number of the sub-tree with this key deleted, or None if the sub-tree is empty.
// If key is not found, guaranteed not to modify the tree
pub(crate) fn tree_delete<'a>(page: Page<'a>, key: &[u8], manager: &'a PageManager) -> Option<u64> {
    let node_mem = page.memory();
    match node_mem[0] {
        LEAF => {
            let accessor = LeafAccessor::new(&page);
            #[allow(clippy::collapsible_else_if)]
            if let Some(greater) = accessor.greater() {
                if key != accessor.lesser().key() && key != greater.key() {
                    // Not found
                    return Some(page.get_page_number());
                }
                // Found, create a new leaf with the other key
                let new_leaf = if key == accessor.lesser().key() {
                    // # TODO: That will make a leaf node with no value in there
                    Leaf((greater.key().to_vec(), greater.value().to_vec()), None)
                } else {
                    Leaf(
                        (
                            accessor.lesser().key().to_vec(),
                            accessor.lesser().value().to_vec(),
                        ),
                        None,
                    )
                };

                // TODO: shouldn't need to drop this, but we can't allocate when there are pages in flight
                drop(page);
                Some(new_leaf.to_bytes(manager))
            } else {
                if key == accessor.lesser().key() {
                    // Deleted the entire left
                    None
                } else {
                    // Not found
                    Some(page.get_page_number())
                }
            }
        }
        INTERNAL => {
            let accessor = InternalAccessor::new(&page);
            let original_left_page = accessor.lte_page();
            let original_right_page = accessor.gt_page();
            let original_page_number = page.get_page_number();
            let mut left_page = accessor.lte_page();
            let mut right_page = accessor.gt_page();
            // TODO: we should recompute our key, since it may now be smaller (if the largest key in the left tree was deleted)
            let our_key = accessor.key().to_vec();
            // TODO: shouldn't need to drop this, but we can't allocate when there are pages in flight
            drop(page);
            #[allow(clippy::collapsible_else_if)]
            if key <= our_key.as_slice() {
                if let Some(page_number) = tree_delete(manager.get_page(left_page), key, manager) {
                    left_page = page_number;
                } else {
                    // The entire left sub-tree was deleted, replace ourself with the right tree
                    return Some(right_page);
                }
            } else {
                if let Some(page_number) = tree_delete(manager.get_page(right_page), key, manager) {
                    right_page = page_number;
                } else {
                    return Some(left_page);
                }
            }

            // The key was not found, since neither sub-tree changed
            if left_page == original_left_page && right_page == original_right_page {
                return Some(original_page_number);
            }

            // MVCC read isolation: (snapshot)
            // If we remove something in the sub-tree, we will allocate spaces
            // for all the affected nodes, actually, which means that the root node
            // will also be a new allocated page, which make us achieve read isolation
            let mut page = manager.allocate();
            let mut builder = InternalBuilder::new(&mut page);
            builder.write_key(&our_key);
            builder.write_lte_page(left_page);
            builder.write_gt_page(right_page);

            Some(page.get_page_number())
        }
        _ => unreachable!(),
    }
}

// Returns the page number of the sub-tree into which the key was inserted
pub(crate) fn tree_insert<'a>(
    page: Page<'a>,
    key: &[u8],
    value: &[u8],
    manager: &'a PageManager,
) -> u64 {
    let node_mem = page.memory();
    match node_mem[0] {
        LEAF => {
            // in a binary search tree (BST), every non-duplicated key-value
            // pair should always be inserted at a leaf node.

            let accessor = LeafAccessor::new(&page);
            // TODO: this is suboptimal, because it may rebuild the leaf page even if it's not necessary:
            // e.g. when we insert a second leaf adjacent without modifying this one
            let mut builder = BinarytreeBuilder::new();
            builder.add(key, value);
            builder.add(accessor.lesser().key(), accessor.lesser().value());
            if let Some(entry) = accessor.greater() {
                builder.add(entry.key(), entry.value());
            }

            // TODO: shouldn't need to drop this, but we can't allocate when there are pages in flight
            drop(page);
            builder.build().to_bytes(manager)
        }
        INTERNAL => {
            let accessor = InternalAccessor::new(&page);
            let mut left_page = accessor.lte_page();
            let mut right_page = accessor.gt_page();
            let our_key = accessor.key().to_vec();
            // TODO: shouldn't need to drop this, but we can't allocate when there are pages in flight
            drop(page);
            if key <= our_key.as_slice() {
                left_page = tree_insert(manager.get_page(left_page), key, value, manager);
            } else {
                right_page = tree_insert(manager.get_page(right_page), key, value, manager);
            }

            // create the new root node
            let mut page = manager.allocate();
            let mut builder = InternalBuilder::new(&mut page);
            builder.write_key(&our_key);
            builder.write_lte_page(left_page);
            builder.write_gt_page(right_page);

            page.get_page_number()
        }
        _ => unreachable!(),
    }
}

/// Returns a tuple of the form `(Page<'a>, usize, usize)` representing the value for
/// a queried key within a binary tree if present.
///
/// The binary tree is composed of Nodes serialized into `Page`s and maintained by a `PageManager`.
/// This function attempts to locate a key within this tree and if found, returns a tuple where:
/// - The first element is the `Page` in which the value is located
/// - The second element is the offset within that page where the value begins
/// - The third element is the length of the value
///
/// Given a key, the function begins at the root of the tree and traverses to the left or right
/// child depending on whether the key is less or greater than the current node's key. The process
/// is recursive and continues until the key is either found or it is determined that the key does not exist in the tree.
///
/// This function might not be space efficient since it allocates a whole page for each node,
/// leading to a waste of space when nodes don't fully occupy their corresponding pages.
/// Future optimizations could involve storing multiple nodes within a single page, or having variable
/// size pages to better match the size of nodes.
///
/// # Arguments
///
/// * `page` - The `Page` object representing the current node being inspected.
/// * `query` - The key being searched for.
/// * `manager` - The `PageManager` managing the pages.
///
/// # Returns
///
/// An `Option` that contains a tuple `(Page<'a>, usize, usize)`. If the key is found, it returns `Some`,
/// with the `Page` containing the value, the offset of the value within the page, and the length of the value.
/// If the key is not found in the tree, it returns `None`.
///
/// # Panics
///
/// This function will panic if it encounters a byte in the `Page` memory that does not correspond to a
/// recognized node type (1 for leaf node or 2 for internal node).
pub(crate) fn lookup_in_raw<'a>(
    page: Page<'a>,
    query: &[u8],
    manager: &'a PageManager,
) -> Option<(Page<'a>, usize, usize)> {
    let node_mem = page.memory();
    match node_mem[0] {
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
                    if let Some(entry) = accessor.greater() {
                        let offset = accessor.offset_of_greater() + entry.value_offset();
                        let value_len = entry.value().len();
                        Some((page, offset, value_len))
                    } else {
                        None
                    }
                }
            }
        }
        INTERNAL => {
            let accessor = InternalAccessor::new(&page);
            let left_page = accessor.lte_page();
            let right_page = accessor.gt_page();
            if query <= accessor.key() {
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
    /// Returns the page number that the node was written to
    /// To support MVCC read isolation, we need to re-allocate a page in
    /// disk for every chanegs.
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
                let mut builder = InternalBuilder::new(&mut page);
                builder.write_key(key);
                builder.write_lte_page(left_page);
                builder.write_gt_page(right_page);

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
