mod db;
mod error;
mod page_manager;
mod storage;
mod table;
mod transactions;

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
mod binarytree;

pub use db::Database;
pub use error::Error;
pub use storage::AccessGuard;
pub use table::Table;
pub use transactions::{ReadOnlyTransaction, WriteTransaction};
