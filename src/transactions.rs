use crate::error::Error;
use crate::storage::{AccessGuard, Storage};
use std::collections::HashMap;

pub struct WriteTransaction<'mmap> {
    storage: &'mmap Storage,
    data: HashMap<Vec<u8>, Vec<u8>>,
}

impl<'mmap> WriteTransaction<'mmap> {
    pub(crate) fn new(storage: &'mmap Storage) -> WriteTransaction<'mmap> {
        WriteTransaction {
            storage,
            data: HashMap::new(),
        }
    }

    pub fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        self.data.insert(key.to_vec(), value.to_vec());
        Ok(())
    }

    pub fn commit(self) -> Result<(), Error> {
        for (key, value) in self.data.iter() {
            self.storage.append(key, value)?;
        }
        self.storage.fsync()?;
        Ok(())
    }

    /// Reserve space to insert a key-value pair (without knowing the value yet)
    /// The returned reference will have length equal to value_length
    pub fn insert_reserve(&mut self, key: &[u8], value_length: usize) -> Result<&mut [u8], Error> {
        self.data.insert(key.to_vec(), vec![0; value_length]);
        Ok(self.data.get_mut(key).unwrap())
    }

    /// Get a value from the transaction. If the value is not in the data,
    /// it will be fetched from the mmap disk storage.
    pub fn get(&self, key: &[u8]) -> Result<Option<AccessGuard>, Error> {
        if let Some(value) = self.data.get(key) {
            return Ok(Some(AccessGuard::Local(value)));
        }
        self.storage.get(key)
    }

    pub fn abort(self) -> Result<(), Error> {
        Ok(())
    }
}

pub struct ReadOnlyTransaction<'mmap> {
    storage: &'mmap Storage,
}

impl<'mmap> ReadOnlyTransaction<'mmap> {
    pub(crate) fn new(storage: &'mmap Storage) -> ReadOnlyTransaction<'mmap> {
        ReadOnlyTransaction { storage }
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<AccessGuard<'mmap>>, Error> {
        self.storage.get(key)
    }
}
