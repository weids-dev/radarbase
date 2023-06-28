use crate::storage::Storage;
use crate::table::Table;
use crate::Error;

use memmap2::MmapMut;
use std::fs::OpenOptions;
use std::path::Path;

pub struct Database {
    storage: Storage,
}

impl Database {
    /// Opens the specified file as a radarbase database (radb).
    ///
    /// * if the file does not exist, or is an empty file, a new database will be initialized in it
    /// * if the file is a valid redb database, it will be opened
    /// * otherwise this function will return an error
    pub unsafe fn open(path: &Path) -> Result<Database, Error> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;

        // TODO: make this configurable
        let mut db_size = 16 * 1024 * 1024 * 1024;
        // Ensure that db_size is a multiple of page size, which is required by mmap
        // page_size::get() to retrieve the memory page size of the current system.
        db_size -= db_size % page_size::get();
        file.set_len(db_size as u64)?;

        let mmap = MmapMut::map_mut(&file)?;
        let storage = Storage::new(mmap)?;
        Ok(Database { storage })
    }

    pub fn open_table(&self, _name: &str) -> Result<Table, Error> {
        Table::new(&self.storage)
    }
}
