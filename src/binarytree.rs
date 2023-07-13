use crate::binarytree::Node::{Internal, Leaf};
use crate::binarytree::RangeIterState::{
    InitialState, InternalLeft, InternalRight, LeafLeft, LeafRight,
};
use crate::page_manager::{Page, PageManager, PageMut};
use std::cell::Cell;
use std::cmp::Ordering;
use std::convert::TryInto;
use std::ops::{Bound, RangeBounds};

const LEAF: u8 = 1;
const INTERNAL: u8 = 2;

// The references within each variant of the RangeIterState<'a> enum (i.e., the Page
// and parent) must not be dropped before the RangeIterState<'a> itself.

enum RangeIterState<'a> {
    InitialState(Page<'a>, bool),
    LeafLeft {
        page: Page<'a>,
        parent: Option<Box<RangeIterState<'a>>>,
        reversed: bool,
    },
    LeafRight {
        page: Page<'a>,
        parent: Option<Box<RangeIterState<'a>>>,
        reversed: bool,
    },
    InternalLeft {
        page: Page<'a>,
        parent: Option<Box<RangeIterState<'a>>>,
        reversed: bool,
    },
    InternalRight {
        page: Page<'a>,
        parent: Option<Box<RangeIterState<'a>>>,
        reversed: bool,
    },
}

impl<'a> RangeIterState<'a> {
    fn forward_next(self, manager: &'a PageManager) -> Option<RangeIterState> {
        // InternalLeft -> LeaefLeft -> LeafRight -> InternalRight
        match self {
            RangeIterState::InitialState(root_page, ..) => match root_page.memory()[0] {
                LEAF => Some(LeafLeft {
                    page: root_page,
                    parent: None,
                    reversed: false,
                }),
                INTERNAL => Some(InternalLeft {
                    page: root_page,
                    parent: None,
                    reversed: false,
                }),
                _ => unreachable!(),
            },
            RangeIterState::LeafLeft { page, parent, .. } => Some(LeafRight {
                page,
                parent,
                reversed: false,
            }),
            RangeIterState::LeafRight { parent, .. } => parent.map(|x| *x), // back to parent
            RangeIterState::InternalLeft { page, parent, .. } => {
                let child = InternalAccessor::new(&page).lte_page();
                let child_page = manager.get_page(child);
                match child_page.memory()[0] {
                    LEAF => Some(LeafLeft {
                        page: child_page,
                        parent: Some(Box::new(InternalRight {
                            page,
                            parent,
                            reversed: false,
                        })),
                        reversed: false,
                    }),
                    INTERNAL => Some(InternalLeft {
                        page: child_page,
                        parent: Some(Box::new(InternalRight {
                            page,
                            parent,
                            reversed: false,
                        })),
                        reversed: false,
                    }),
                    _ => unreachable!(),
                }
            }
            RangeIterState::InternalRight { page, parent, .. } => {
                let child = InternalAccessor::new(&page).gt_page();
                let child_page = manager.get_page(child);
                match child_page.memory()[0] {
                    LEAF => Some(LeafLeft {
                        page: child_page,
                        parent,
                        reversed: false,
                    }),
                    INTERNAL => Some(InternalLeft {
                        page: child_page,
                        parent,
                        reversed: false,
                    }),
                    _ => unreachable!(),
                }
            }
        }
    }

    fn backward_next(self, manager: &'a PageManager) -> Option<RangeIterState> {
        // InternalRight -> LeafRight -> LeafLeft -> InternalLeft
        match self {
            RangeIterState::InitialState(root_page, ..) => match root_page.memory()[0] {
                LEAF => Some(LeafRight {
                    page: root_page,
                    parent: None,
                    reversed: true,
                }),
                INTERNAL => Some(InternalRight {
                    page: root_page,
                    parent: None,
                    reversed: true,
                }),
                _ => unreachable!(),
            },
            RangeIterState::LeafLeft { parent, .. } => parent.map(|x| *x),
            RangeIterState::LeafRight { page, parent, .. } => Some(LeafLeft {
                page,
                parent,
                reversed: true,
            }),
            RangeIterState::InternalLeft { page, parent, .. } => {
                let child = InternalAccessor::new(&page).lte_page();
                let child_page = manager.get_page(child);
                match child_page.memory()[0] {
                    LEAF => Some(LeafRight {
                        page: child_page,
                        parent,
                        reversed: true,
                    }),
                    INTERNAL => Some(InternalRight {
                        page: child_page,
                        parent,
                        reversed: true,
                    }),
                    _ => unreachable!(),
                }
            }
            RangeIterState::InternalRight { page, parent, .. } => {
                let child = InternalAccessor::new(&page).gt_page();
                let child_page = manager.get_page(child);
                match child_page.memory()[0] {
                    LEAF => Some(LeafRight {
                        page: child_page,
                        parent: Some(Box::new(InternalLeft {
                            page,
                            parent,
                            reversed: true,
                        })),
                        reversed: true,
                    }),
                    INTERNAL => Some(InternalRight {
                        page: child_page,
                        parent: Some(Box::new(InternalLeft {
                            page,
                            parent,
                            reversed: true,
                        })),
                        reversed: true,
                    }),
                    _ => unreachable!(),
                }
            }
        }
    }

    fn next(self, manager: &'a PageManager) -> Option<RangeIterState> {
        match &self {
            InitialState(_, reversed) => {
                if *reversed {
                    self.backward_next(manager)
                } else {
                    self.forward_next(manager)
                }
            }
            RangeIterState::LeafLeft { reversed, .. } => {
                if *reversed {
                    self.backward_next(manager)
                } else {
                    self.forward_next(manager)
                }
            }
            RangeIterState::LeafRight { reversed, .. } => {
                if *reversed {
                    self.backward_next(manager)
                } else {
                    self.forward_next(manager)
                }
            }
            RangeIterState::InternalLeft { reversed, .. } => {
                if *reversed {
                    self.backward_next(manager)
                } else {
                    self.forward_next(manager)
                }
            }
            RangeIterState::InternalRight { reversed, .. } => {
                if *reversed {
                    self.backward_next(manager)
                } else {
                    self.forward_next(manager)
                }
            }
        }
    }

    fn get_entry(&self) -> Option<EntryAccessor> {
        // If it is a leaf, return the entry
        // otherwise, return None
        match self {
            RangeIterState::LeafLeft { page, .. } => Some(LeafAccessor::new(&page).lesser()),
            RangeIterState::LeafRight { page, .. } => LeafAccessor::new(&page).greater(),
            _ => None,
        }
    }
}

pub struct BinarytreeRangeIter<'a, T: RangeBounds<&'a [u8]>> {
    last: Option<RangeIterState<'a>>,
    table_id: u64,
    query_range: T,
    reversed: bool,
    manager: &'a PageManager,
}

impl<'a, T: RangeBounds<&'a [u8]>> BinarytreeRangeIter<'a, T> {
    pub(crate) fn new(
        root_page: Option<Page<'a>>,
        table_id: u64,
        query_range: T,
        manager: &'a PageManager,
    ) -> Self {
        Self {
            last: root_page.map(|p| InitialState(p, false)),
            table_id,
            query_range,
            reversed: false,
            manager,
        }
    }

    pub(crate) fn new_reversed(
        root_page: Option<Page<'a>>,
        table_id: u64,
        query_range: T,
        manager: &'a PageManager,
    ) -> Self {
        Self {
            last: root_page.map(|p| InitialState(p, true)),
            table_id,
            query_range,
            reversed: true,
            manager,
        }
    }

    // TODO: we need generic-associated-types to implement Iterator
    pub fn next(&mut self) -> Option<EntryAccessor> {
        if let Some(mut state) = self.last.take() {
            loop {
                if let Some(new_state) = state.next(self.manager) {
                    if let Some(entry) = new_state.get_entry() {
                        // If the new state is a leaf, check if it's within the query range
                        // TODO: optimize. This is very inefficient to retrieve and then ignore the values
                        if self.table_id == entry.table_id()
                            && self.query_range.contains(&entry.key())
                        {
                            self.last = Some(new_state);
                            return self.last.as_ref().map(|s| s.get_entry().unwrap());
                        } else {
                            #[allow(clippy::collapsible_else_if)]
                            if self.reversed {
                                if let Bound::Included(start) = self.query_range.start_bound() {
                                    if entry.table_and_key() < (self.table_id, *start) {
                                        self.last = None;
                                        return None;
                                    }
                                } else if let Bound::Excluded(start) =
                                    self.query_range.start_bound()
                                {
                                    if entry.table_and_key() <= (self.table_id, *start) {
                                        self.last = None;
                                        return None;
                                    }
                                }
                            } else {
                                if let Bound::Included(end) = self.query_range.end_bound() {
                                    if entry.table_and_key() > (self.table_id, *end) {
                                        self.last = None;
                                        return None;
                                    }
                                } else if let Bound::Excluded(end) = self.query_range.end_bound() {
                                    if entry.table_and_key() >= (self.table_id, *end) {
                                        self.last = None;
                                        return None;
                                    }
                                }
                            };
                            state = new_state;
                        }
                    } else {
                        state = new_state;
                    }
                } else {
                    self.last = None;
                    return None;
                }
            }
        }
        None
    }
}

pub trait BinarytreeEntry<'a: 'b, 'b> {
    fn key(&'b self) -> &'a [u8];
    fn value(&'b self) -> &'a [u8];
}

// Provides a simple zero-copy way to access entries
//
// Entry format is:
// * (8 bytes) key_size
// * (8 bytes) table_id, 64-bit big endian unsigned. Stored between key_size & key_data, so that
// it can be read with key_data as a single key_size + 8 length unique key for the entire db
// * (key_size bytes) key_data
// * (8 bytes) value_size
// * (value_size bytes) value_data
pub struct EntryAccessor<'a> {
    raw: &'a [u8],
}

impl<'a> EntryAccessor<'a> {
    fn new(raw: &'a [u8]) -> Self {
        EntryAccessor { raw }
    }

    fn key_len(&self) -> usize {
        u64::from_be_bytes(self.raw[0..8].try_into().unwrap()) as usize
    }

    pub(crate) fn table_id(&self) -> u64 {
        u64::from_be_bytes(self.raw[8..16].try_into().unwrap())
    }

    fn value_offset(&self) -> usize {
        16 + self.key_len() + 8
    }

    fn value_len(&self) -> usize {
        let key_len = self.key_len();
        u64::from_be_bytes(
            self.raw[(16 + key_len)..(16 + key_len + 8)]
                .try_into()
                .unwrap(),
        ) as usize
    }

    fn raw_len(&self) -> usize {
        16 + self.key_len() + 8 + self.value_len()
    }

    fn table_and_key(&self) -> (u64, &'a [u8]) {
        (self.table_id(), self.key())
    }
}

impl<'a: 'b, 'b> BinarytreeEntry<'a, 'b> for EntryAccessor<'a> {
    fn key(&'b self) -> &'a [u8] {
        &self.raw[16..(16 + self.key_len())]
    }

    fn value(&'b self) -> &'a [u8] {
        &self.raw[self.value_offset()..(self.value_offset() + self.value_len())]
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

    fn write_table_id(&mut self, table_id: u64) {
        self.raw[8..16].copy_from_slice(&table_id.to_be_bytes());
    }

    fn write_key(&mut self, key: &[u8]) {
        self.raw[0..8].copy_from_slice(&(key.len() as u64).to_be_bytes());
        self.raw[16..(16 + key.len())].copy_from_slice(key);
    }

    fn write_value(&mut self, value: &[u8]) {
        let value_offset = EntryAccessor::new(self.raw).value_offset();
        self.raw[(value_offset - 8)..value_offset]
            .copy_from_slice(&(value.len() as u64).to_be_bytes());
        self.raw[value_offset..(value_offset + value.len())].copy_from_slice(value);
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

    fn lesser(&self) -> EntryAccessor<'b> {
        EntryAccessor::new(&self.page.memory()[self.offset_of_lesser()..])
    }

    fn greater(&self) -> Option<EntryAccessor<'b>> {
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

    fn write_lesser(&mut self, table_id: u64, key: &[u8], value: &[u8]) {
        let mut entry = EntryMutator::new(&mut self.page.memory_mut()[1..]);
        entry.write_table_id(table_id);
        entry.write_key(key);
        entry.write_value(value);
    }

    fn write_greater(&mut self, entry: Option<(u64, &[u8], &[u8])>) {
        let offset = 1 + EntryAccessor::new(&self.page.memory()[1..]).raw_len();
        let mut writer = EntryMutator::new(&mut self.page.memory_mut()[offset..]);
        if let Some((table_id, key, value)) = entry {
            writer.write_table_id(table_id);
            writer.write_key(key);
            writer.write_value(value);
        } else {
            writer.write_key(&[]);
        }
    }
}

// Provides a simple zero-copy way to access a leaf page
//
// Entry format is:
// * (1 byte) type: 2 = INTERNAL
// * (8 bytes) key_len
// * (8 bytes) table_id 64-bit big-endian unsigned
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

    fn table_id(&self) -> u64 {
        u64::from_be_bytes(self.page.memory()[9..17].try_into().unwrap())
    }

    fn table_and_key(&self) -> (u64, &[u8]) {
        (self.table_id(), self.key())
    }

    fn key(&self) -> &[u8] {
        &self.page.memory()[17..(17 + self.key_len())]
    }

    fn lte_page(&self) -> u64 {
        let offset = 17 + self.key_len();
        u64::from_be_bytes(self.page.memory()[offset..(offset + 8)].try_into().unwrap())
    }

    fn gt_page(&self) -> u64 {
        let offset = 17 + self.key_len() + 8;
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

    fn write_table_and_key(&mut self, table_id: u64, key: &[u8]) {
        self.page.memory_mut()[1..9].copy_from_slice(&(key.len() as u64).to_be_bytes());
        self.page.memory_mut()[9..17].copy_from_slice(&table_id.to_be_bytes());
        self.page.memory_mut()[17..(17 + key.len())].copy_from_slice(key);
    }

    fn write_lte_page(&mut self, page_number: u64) {
        let offset = 17 + self.key_len();
        self.page.memory_mut()[offset..(offset + 8)].copy_from_slice(&page_number.to_be_bytes());
    }

    fn write_gt_page(&mut self, page_number: u64) {
        let offset = 17 + self.key_len() + 8;
        self.page.memory_mut()[offset..(offset + 8)].copy_from_slice(&page_number.to_be_bytes());
    }
}

// Returns the page number of the sub-tree with this key deleted, or None if the sub-tree is empty.
// If key is not found, guaranteed not to modify the tree
pub(crate) fn tree_delete<'a>(
    page: Page<'a>,
    table: u64,
    key: &[u8],
    manager: &'a PageManager,
) -> Option<u64> {
    let node_mem = page.memory();
    match node_mem[0] {
        LEAF => {
            let accessor = LeafAccessor::new(&page);
            #[allow(clippy::collapsible_else_if)]
            if let Some(greater) = accessor.greater() {
                if (table, key) != accessor.lesser().table_and_key()
                    && (table, key) != greater.table_and_key()
                {
                    // Not found
                    return Some(page.get_page_number());
                }
                // Found, create a new leaf with the other key
                let new_leaf = if (table, key) == accessor.lesser().table_and_key() {
                    Leaf(
                        (
                            greater.table_id(),
                            greater.key().to_vec(),
                            greater.value().to_vec(),
                        ),
                        None,
                    )
                } else {
                    Leaf(
                        (
                            accessor.lesser().table_id(),
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
                if (table, key) == accessor.lesser().table_and_key() {
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
            let our_table = accessor.table_id();
            let our_key = accessor.key().to_vec();
            // TODO: shouldn't need to drop this, but we can't allocate when there are pages in flight
            drop(page);
            #[allow(clippy::collapsible_else_if)]
            if (table, key) <= (our_table, our_key.as_slice()) {
                if let Some(page_number) =
                    tree_delete(manager.get_page(left_page), table, key, manager)
                {
                    left_page = page_number;
                } else {
                    // The entire left sub-tree was deleted, replace ourself with the right tree
                    return Some(right_page);
                }
            } else {
                if let Some(page_number) =
                    tree_delete(manager.get_page(right_page), table, key, manager)
                {
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
            builder.write_table_and_key(our_table, &our_key);
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
    table: u64,
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
            builder.add(table, key, value);
            if (table, key) != accessor.lesser().table_and_key() {
                builder.add(
                    accessor.lesser().table_id(),
                    accessor.lesser().key(),
                    accessor.lesser().value(),
                );
            }
            if let Some(entry) = accessor.greater() {
                if (table, key) != entry.table_and_key() {
                    builder.add(entry.table_id(), entry.key(), entry.value());
                }
            }
            // TODO: shouldn't need to drop this, but we can't allocate when there are pages in flight
            // This guaranteed the MVCC read isolation, since every conflicting page will be dropped.
            drop(page);
            builder.build().to_bytes(manager)
        }
        INTERNAL => {
            let accessor = InternalAccessor::new(&page);
            let mut left_page = accessor.lte_page();
            let mut right_page = accessor.gt_page();
            let our_table = accessor.table_id();
            let our_key = accessor.key().to_vec();
            // TODO: shouldn't need to drop this, but we can't allocate when there are pages in flight
            // This guaranteed the MVCC read isolation, since every conflicting page will be dropped.
            drop(page);
            if (table, key) <= (our_table, our_key.as_slice()) {
                left_page = tree_insert(manager.get_page(left_page), table, key, value, manager);
            } else {
                right_page = tree_insert(manager.get_page(right_page), table, key, value, manager);
            }

            // create the new root node
            let mut page = manager.allocate();
            let mut builder = InternalBuilder::new(&mut page);
            builder.write_table_and_key(our_table, &our_key);
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
    table: u64,
    query: &[u8],
    manager: &'a PageManager,
) -> Option<(Page<'a>, usize, usize)> {
    let node_mem = page.memory();
    match node_mem[0] {
        LEAF => {
            // Leaf node
            let accessor = LeafAccessor::new(&page);
            match (table, query).cmp(&accessor.lesser().table_and_key()) {
                Ordering::Less => None,
                Ordering::Equal => {
                    let offset = accessor.offset_of_lesser() + accessor.lesser().value_offset();
                    let value_len = accessor.lesser().value().len();
                    Some((page, offset, value_len))
                }
                Ordering::Greater => {
                    if let Some(entry) = accessor.greater() {
                        if (table, query) == entry.table_and_key() {
                            let offset = accessor.offset_of_greater() + entry.value_offset();
                            let value_len = entry.value().len();
                            Some((page, offset, value_len))
                        } else {
                            None
                        }
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
            if (table, query) <= accessor.table_and_key() {
                lookup_in_raw(manager.get_page(left_page), table, query, manager)
            } else {
                lookup_in_raw(manager.get_page(right_page), table, query, manager)
            }
        }
        _ => unreachable!(),
    }
}

#[derive(Eq, PartialEq, Debug)]
pub(crate) enum Node {
    Leaf((u64, Vec<u8>, Vec<u8>), Option<(u64, Vec<u8>, Vec<u8>)>),
    Internal(Box<Node>, u64, Vec<u8>, Box<Node>),
}

impl Node {
    // Returns the page number that the node was written to
    pub(crate) fn to_bytes(&self, page_manager: &PageManager) -> u64 {
        match self {
            Node::Leaf(left_val, right_val) => {
                let mut page = page_manager.allocate();
                let mut builder = LeafBuilder::new(&mut page);
                builder.write_lesser(left_val.0, &left_val.1, &left_val.2);
                builder.write_greater(
                    right_val
                        .as_ref()
                        .map(|(table, key, value)| (*table, key.as_slice(), value.as_slice())),
                );

                page.get_page_number()
            }
            Node::Internal(left, table, key, right) => {
                let left_page = left.to_bytes(page_manager);
                let right_page = right.to_bytes(page_manager);
                let mut page = page_manager.allocate();
                let mut builder = InternalBuilder::new(&mut page);
                builder.write_table_and_key(*table, key);
                builder.write_lte_page(left_page);
                builder.write_gt_page(right_page);

                page.get_page_number()
            }
        }
    }

    fn get_max_key(&self) -> (u64, Vec<u8>) {
        match self {
            Node::Leaf((left_table, left_key, _), right_val) => {
                if let Some((right_table, right_key, _)) = right_val {
                    (*right_table, right_key.to_vec())
                } else {
                    (*left_table, left_key.to_vec())
                }
            }
            Node::Internal(_left, _table, _key, right) => right.get_max_key(),
        }
    }
}

pub(crate) struct BinarytreeBuilder {
    pairs: Vec<(u64, Vec<u8>, Vec<u8>)>,
}

impl BinarytreeBuilder {
    pub(crate) fn new() -> BinarytreeBuilder {
        BinarytreeBuilder { pairs: vec![] }
    }

    pub(crate) fn add(&mut self, table: u64, key: &[u8], value: &[u8]) {
        self.pairs.push((table, key.to_vec(), value.to_vec()));
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
                Leaf((group[0].0, group[0].1.to_vec(), group[0].2.to_vec()), None)
            } else {
                assert_eq!(group.len(), 2);
                if (group[0].0, &group[0].1) == (group[1].0, &group[1].1) {
                    // This cannot happend, since we implement the overwriting feature
                    // put the panic here to make sure we don't have bugs in the future
                    panic!("duplicate key: {:?}", group[0].0);
                }
                Leaf(
                    (group[0].0, group[0].1.to_vec(), group[0].2.to_vec()),
                    Some((group[1].0, group[1].1.to_vec(), group[1].2.to_vec())),
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
                    let (table, key) = previous_node.get_max_key();
                    let internal = Internal(Box::new(previous_node), table, key, Box::new(node));
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
            (1, b"hello".to_vec(), b"world".to_vec()),
            Some((1, b"hello2".to_vec(), b"world2".to_vec())),
        );
        let right = Leaf((1, b"hello3".to_vec(), b"world3".to_vec()), None);
        Internal(Box::new(left), 1, b"hello2".to_vec(), Box::new(right))
    }

    #[test]
    fn builder() {
        let expected = gen_tree();
        let mut builder = BinarytreeBuilder::new();
        builder.add(1, b"hello2", b"world2");
        builder.add(1, b"hello3", b"world3");
        builder.add(1, b"hello", b"world");

        assert_eq!(expected, builder.build());
    }
}
