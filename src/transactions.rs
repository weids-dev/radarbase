use crate::binarytree::BinarytreeRangeIter;
use crate::error::Error;
use crate::storage::{AccessGuard, Storage};
use crate::types::RadbKey;
use std::collections::{HashMap, HashSet};
use std::marker::PhantomData;
use std::ops::RangeBounds;

pub struct WriteTransaction<'mmap, K: RadbKey + ?Sized> {
    storage: &'mmap Storage,
    table_id: u64,
    added: HashMap<Vec<u8>, Vec<u8>>,
    removed: HashSet<Vec<u8>>,
    _key_type: PhantomData<K>,
}

impl<'mmap, K: RadbKey + ?Sized> WriteTransaction<'mmap, K> {
    pub(crate) fn new(table_id: u64, storage: &'mmap Storage) -> WriteTransaction<'mmap, K> {
        WriteTransaction {
            storage,
            table_id,
            added: HashMap::new(),
            removed: HashSet::new(),
            _key_type: Default::default(),
        }
    }

    pub fn insert(&mut self, key: &K, value: &[u8]) -> Result<(), Error> {
        self.removed.remove(key.as_bytes());
        self.added.insert(key.as_bytes().to_vec(), value.to_vec());
        Ok(())
    }

    /// change the in-memory (mmap) data structure
    pub fn commit(self) -> Result<(), Error> {
        self.storage.bulk_insert::<K>(self.table_id, self.added)?;
        for key in self.removed.iter() {
            self.storage.remove::<K>(self.table_id, key)?;
        }
        self.storage.fsync()?;
        Ok(())
    }

    /// Reserve space to insert a key-value pair (without knowing the value yet)
    /// The returned reference will have length equal to value_length
    pub fn insert_reserve(&mut self, key: &K, value_length: usize) -> Result<&mut [u8], Error> {
        self.removed.remove(key.as_bytes());
        self.added
            .insert(key.as_bytes().to_vec(), vec![0; value_length]);
        Ok(self.added.get_mut(key.as_bytes()).unwrap())
    }

    /// Get a value from the transaction. If the value is not in the data,
    /// it will be fetched from the mmap disk storage.
    pub fn get(&self, key: &K) -> Result<Option<AccessGuard>, Error> {
        if let Some(value) = self.added.get(key.as_bytes()) {
            return Ok(Some(AccessGuard::Local(value)));
        }
        self.storage.get::<K>(
            self.table_id,
            key.as_bytes(),
            self.storage.get_root_page_number(),
        )
    }

    pub fn remove(&mut self, key: &K) -> Result<(), Error> {
        self.added.remove(key.as_bytes());
        self.removed.insert(key.as_bytes().to_vec());
        Ok(())
    }

    pub fn abort(self) -> Result<(), Error> {
        Ok(())
    }
}

pub struct ReadOnlyTransaction<'mmap, K: RadbKey + ?Sized> {
    storage: &'mmap Storage,
    root_page: Option<u64>,
    table_id: u64,
    _key_type: PhantomData<K>,
}

impl<'mmap, K: RadbKey + ?Sized> ReadOnlyTransaction<'mmap, K> {
    pub(crate) fn new(table_id: u64, storage: &'mmap Storage) -> ReadOnlyTransaction<'mmap, K> {
        let root_page = storage.get_root_page_number();
        ReadOnlyTransaction {
            storage,
            root_page,
            table_id,
            _key_type: Default::default(),
        }
    }

    pub fn get(&self, key: &K) -> Result<Option<AccessGuard<'mmap>>, Error> {
        self.storage
            .get::<K>(self.table_id, key.as_bytes(), self.root_page)
    }

    pub fn get_range<'a, T: RangeBounds<&'a [u8]>>(
        &'a self,
        range: T,
    ) -> Result<BinarytreeRangeIter<T, K>, Error> {
        self.storage.get_range(self.table_id, range, self.root_page)
    }

    pub fn get_range_reversed<'a, T: RangeBounds<&'a [u8]>>(
        &'a self,
        range: T,
    ) -> Result<BinarytreeRangeIter<T, K>, Error> {
        self.storage
            .get_range_reversed(self.table_id, range, self.root_page)
    }

    pub fn len(&self) -> Result<usize, Error> {
        self.storage.len(self.table_id, self.root_page)
    }

    pub fn is_empty(&self) -> Result<bool, Error> {
        self.storage
            .len(self.table_id, self.root_page)
            .map(|x| x == 0)
    }
}
