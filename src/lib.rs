mod db;
mod error;
mod table;
mod transactions;
mod storage;

/// This module provides an implementation of a binary tree.
///
/// The binary tree is used to store key-value pairs in a way that allows
/// efficient lookups and updates. It includes a builder for constructing
/// a binary tree from a set of key-value pairs, and a method for looking
/// up values based on their keys.
///
/// This implementation uses a `Node` enum to represent both internal and
/// leaf nodes. Internal nodes contain a key and two child nodes, while leaf
/// nodes can contain up to two key-value pairs. This design helps reduce the
/// height of the tree.
///
/// # Example
///
/// ```
/// use crate::binarytree::BinarytreeBuilder;
///
/// let mut builder = BinarytreeBuilder::new();
/// builder.add(b"key1", b"value1");
/// builder.add(b"key2", b"value2");
///
/// let tree = builder.build();
/// ```
mod binarytree;

pub use db::Database;
pub use error::Error;
pub use table::Table;
pub use storage::AccessGuard;
pub use transactions::{ReadOnlyTransaction, WriteTransaction};
