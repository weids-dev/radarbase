use crate::error::Error;
use crate::storage::Storage;
use crate::transactions::WriteTransaction;
use crate::ReadOnlyTransaction;

pub struct Table<'mmap> {
    storage: &'mmap Storage,
}

impl<'mmap> Table<'mmap> {
    pub(crate) fn new(storage: &'mmap Storage) -> Result<Table<'mmap>, Error> {
        Ok(Table { storage })
    }

    pub fn begin_write(&'_ mut self) -> Result<WriteTransaction<'mmap>, Error> {
        Ok(WriteTransaction::new(self.storage))
    }

    pub fn read_transaction(&'_ self) -> Result<ReadOnlyTransaction<'mmap>, Error> {
        Ok(ReadOnlyTransaction::new(self.storage))
    }
}

#[cfg(test)]
mod test {
    use crate::Database;
    use tempfile::NamedTempFile;

    #[test]
    fn len() {
        let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
        let db = unsafe { Database::open(tmpfile.path()).unwrap() };
        let mut table = db.open_table("").unwrap();
        let mut write_txn = table.begin_write().unwrap();
        write_txn.insert(b"hello", b"world").unwrap();
        write_txn.insert(b"hello2", b"world2").unwrap();
        write_txn.insert(b"hi", b"world").unwrap();
        write_txn.commit().unwrap();
        let read_txn = table.read_transaction().unwrap();
        assert_eq!(read_txn.len().unwrap(), 3);
    }

    #[test]
    fn is_empty() {
        let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
        let db = unsafe { Database::open(tmpfile.path()).unwrap() };
        let mut table = db.open_table("").unwrap();
        let read_txn = table.read_transaction().unwrap();
        assert!(read_txn.is_empty().unwrap());
        let mut write_txn = table.begin_write().unwrap();
        write_txn.insert(b"hello", b"world").unwrap();
        write_txn.commit().unwrap();
        let read_txn = table.read_transaction().unwrap();
        assert!(!read_txn.is_empty().unwrap());
    }

    #[test]
    fn abort() {
        let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
        let db = unsafe { Database::open(tmpfile.path()).unwrap() };
        let mut table = db.open_table("").unwrap();
        let read_txn = table.read_transaction().unwrap();
        assert!(read_txn.is_empty().unwrap());
        let mut write_txn = table.begin_write().unwrap();
        write_txn.insert(b"hello", b"aborted").unwrap();
        assert_eq!(
            b"aborted",
            write_txn.get(b"hello").unwrap().unwrap().as_ref()
        );
        write_txn.abort().unwrap();
        let read_txn = table.read_transaction().unwrap();
        assert!(read_txn.is_empty().unwrap());
        let mut write_txn = table.begin_write().unwrap();
        write_txn.insert(b"hello", b"world").unwrap();
        write_txn.commit().unwrap();
        let read_txn = table.read_transaction().unwrap();
        assert_eq!(b"world", read_txn.get(b"hello").unwrap().unwrap().as_ref());
        assert_eq!(read_txn.len().unwrap(), 1);
    }

    #[test]
    fn insert_reserve() {
        let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
        let db = unsafe { Database::open(tmpfile.path()).unwrap() };
        let mut table = db.open_table("").unwrap();
        let mut write_txn = table.begin_write().unwrap();
        let value = b"world";
        let reserved = write_txn.insert_reserve(b"hello", value.len()).unwrap();
        reserved.copy_from_slice(value);
        write_txn.commit().unwrap();
        let read_txn = table.read_transaction().unwrap();
        assert_eq!(value, read_txn.get(b"hello").unwrap().unwrap().as_ref());
    }

    #[test]
    fn delete() {
        let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
        let db = unsafe { Database::open(tmpfile.path()).unwrap() };
        let mut table = db.open_table("").unwrap();

        let mut write_txn = table.begin_write().unwrap();
        write_txn.insert(b"hello", b"world").unwrap();
        write_txn.insert(b"hello2", b"world").unwrap();
        write_txn.commit().unwrap();
        let read_txn = table.read_transaction().unwrap();
        assert_eq!(b"world", read_txn.get(b"hello").unwrap().unwrap().as_ref());
        assert_eq!(read_txn.len().unwrap(), 2);

        let mut write_txn = table.begin_write().unwrap();
        write_txn.remove(b"hello").unwrap();
        write_txn.commit().unwrap();

        let read_txn2 = table.read_transaction().unwrap();
        assert!(read_txn2.get(b"hello").unwrap().is_none());
    }

    #[test]
    fn no_dirty_reads() {
        // Confirming the isolation property of ACID compliance.
        // In database systems, a "dirty read" happens when a transaction reads data
        // that has been written by another transaction that has not yet committed.
        // This can lead to inconsistencies if the writing transaction fails
        // and rolls back, because the reading transaction will have read
        // (and potentially acted upon) data that was never officially
        // committed to the database.

        let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
        let db = unsafe { Database::open(tmpfile.path()).unwrap() };
        let mut table = db.open_table("").unwrap();

        let mut write_txn = table.begin_write().unwrap();
        write_txn.insert(b"hello", b"world").unwrap();
        let read_txn = table.read_transaction().unwrap();
        assert!(read_txn.get(b"hello").unwrap().is_none());
        assert!(read_txn.is_empty().unwrap());
        write_txn.commit().unwrap();

        let read_txn = table.read_transaction().unwrap();
        assert_eq!(b"world", read_txn.get(b"hello").unwrap().unwrap().as_ref());
    }

    #[test]
    fn read_isolation() {
        // Read isolation in MVCC:
        // Read transactions see a snapshot of the database at the point in
        // the time when they astarted, they are not affected by subsequent
        // write transactions This allows for high concurrency, as read transactions
        // do not need to wait for write transactions to commit, they simply work on
        // the version that was current when they started.
        let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
        let db = unsafe { Database::open(tmpfile.path()).unwrap() };
        let mut table = db.open_table("").unwrap();

        // first write transaction
        let mut write_txn = table.begin_write().unwrap();
        write_txn.insert(b"hello", b"world").unwrap();
        write_txn.commit().unwrap();

        // first read transaction
        let read_txn = table.read_transaction().unwrap();
        assert_eq!(b"world", read_txn.get(b"hello").unwrap().unwrap().as_ref());

        // second write transaction
        let mut write_txn = table.begin_write().unwrap();
        write_txn.remove(b"hello").unwrap();
        write_txn.insert(b"hello2", b"world2").unwrap();
        write_txn.insert(b"hello3", b"world3").unwrap();
        write_txn.commit().unwrap();

        // second read transaction
        let read_txn2 = table.read_transaction().unwrap();
        assert!(read_txn2.get(b"hello").unwrap().is_none());
        assert_eq!(
            b"world2",
            read_txn2.get(b"hello2").unwrap().unwrap().as_ref()
        );
        assert_eq!(
            b"world3",
            read_txn2.get(b"hello3").unwrap().unwrap().as_ref()
        );
        assert_eq!(read_txn2.len().unwrap(), 2);

        // check read isolation: the first read transaction does not see any changes
        assert_eq!(b"world", read_txn.get(b"hello").unwrap().unwrap().as_ref());
        assert!(read_txn.get(b"hello2").unwrap().is_none());
        assert!(read_txn.get(b"hello3").unwrap().is_none());
        assert_eq!(read_txn.len().unwrap(), 1);
    }

    #[test]
    fn read_isolation2() {
        let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
        let db = unsafe { Database::open(tmpfile.path()).unwrap() };
        let mut table = db.open_table("").unwrap();

        // write transaction 1
        let mut write_txn = table.begin_write().unwrap();
        write_txn.insert(b"a", b"world").unwrap(); // "a" will be in a leaf
        write_txn.insert(b"b", b"hello").unwrap(); // "b" will be in a leaf
        write_txn.insert(b"c", b"hi").unwrap(); // "c" will be the root
        write_txn.commit().unwrap();

        // read transaction 1
        let read_txn = table.read_transaction().unwrap();
        assert_eq!(b"world", read_txn.get(b"a").unwrap().unwrap().as_ref());
        assert_eq!(b"hello", read_txn.get(b"b").unwrap().unwrap().as_ref());
        assert_eq!(b"hi", read_txn.get(b"c").unwrap().unwrap().as_ref());

        // write transaction 2
        let mut write_txn = table.begin_write().unwrap();
        write_txn.remove(b"a").unwrap(); // delete from leaf
        write_txn.insert(b"d", b"test").unwrap(); // insert into leaf
        write_txn.commit().unwrap();

        // read transaction 2
        let read_txn2 = table.read_transaction().unwrap();
        assert!(read_txn2.get(b"a").unwrap().is_none());
        assert_eq!(b"hello", read_txn2.get(b"b").unwrap().unwrap().as_ref());
        assert_eq!(b"hi", read_txn2.get(b"c").unwrap().unwrap().as_ref());
        assert_eq!(b"test", read_txn2.get(b"d").unwrap().unwrap().as_ref());

        // check read isolation: read_txn should still see the old state
        assert_eq!(b"world", read_txn.get(b"a").unwrap().unwrap().as_ref());
        assert_eq!(b"hello", read_txn.get(b"b").unwrap().unwrap().as_ref());
        assert_eq!(b"hi", read_txn.get(b"c").unwrap().unwrap().as_ref());
        assert!(read_txn.get(b"d").unwrap().is_none());
    }

    #[test]
    #[ignore]
    fn read_isolation_complex_tree() {
        // Read isolation in MVCC:
        // Read transactions see a snapshot of the database at the point in
        // time when they started, they are not affected by subsequent
        // write transactions. This allows for high concurrency, as read transactions
        // do not need to wait for write transactions to commit, they simply work on
        // the version that was current when they started.
        //
        // TODO: Support read isolation in updating
        let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
        let db = unsafe { Database::open(tmpfile.path()).unwrap() };
        let mut table = db.open_table("").unwrap();

        // first write transaction - create complex tree
        let mut write_txn = table.begin_write().unwrap();
        write_txn.insert(b"hello", b"world").unwrap();
        write_txn.insert(b"foo", b"bar").unwrap();
        write_txn.insert(b"alpha", b"beta").unwrap();
        write_txn.insert(b"rust", b"cool").unwrap();
        write_txn.commit().unwrap();

        // first read transaction
        let read_txn = table.read_transaction().unwrap();
        assert_eq!(b"world", read_txn.get(b"hello").unwrap().unwrap().as_ref());
        assert_eq!(b"bar", read_txn.get(b"foo").unwrap().unwrap().as_ref());
        assert_eq!(b"beta", read_txn.get(b"alpha").unwrap().unwrap().as_ref());
        assert_eq!(b"cool", read_txn.get(b"rust").unwrap().unwrap().as_ref());

        // second write transaction - update existing keys but don't change the root
        let mut write_txn = table.begin_write().unwrap();
        write_txn.insert(b"hello", b"world2").unwrap();
        write_txn.insert(b"foo", b"bar2").unwrap();
        write_txn.insert(b"alpha", b"beta2").unwrap();
        write_txn.insert(b"rust", b"cool2").unwrap();
        write_txn.commit().unwrap();

        // second read transaction
        let read_txn2 = table.read_transaction().unwrap();
        assert_eq!(
            b"world2",
            read_txn2.get(b"hello").unwrap().unwrap().as_ref()
        );
        assert_eq!(b"bar2", read_txn2.get(b"foo").unwrap().unwrap().as_ref());
        assert_eq!(b"beta2", read_txn2.get(b"alpha").unwrap().unwrap().as_ref());
        assert_eq!(b"cool2", read_txn2.get(b"rust").unwrap().unwrap().as_ref());

        // check read isolation: the first read transaction does not see any changes
        assert_eq!(b"world", read_txn.get(b"hello").unwrap().unwrap().as_ref());
        assert_eq!(b"bar", read_txn.get(b"foo").unwrap().unwrap().as_ref());
        assert_eq!(b"beta", read_txn.get(b"alpha").unwrap().unwrap().as_ref());
        assert_eq!(b"cool", read_txn.get(b"rust").unwrap().unwrap().as_ref());
    }

    #[test]
    fn range_query() {
        let tmpfile: NamedTempFile = NamedTempFile::new().unwrap();
        let db = unsafe { Database::open(tmpfile.path()).unwrap() };
        let mut table = db.open_table("").unwrap();

        let mut write_txn = table.begin_write().unwrap();
        for i in 0..10u8 {
            let key = vec![i];
            write_txn.insert(&key, b"value").unwrap();
        }
        write_txn.commit().unwrap();
        let read_txn = table.read_transaction().unwrap();
        let start = vec![3u8];
        let end = vec![7u8];
        let mut iter = read_txn
            .get_range(start.as_slice()..end.as_slice())
            .unwrap();
        for i in 3..7u8 {
            let entry = iter.next().unwrap();
            assert_eq!(&[i], entry.key());
            assert_eq!(b"value", entry.value());
        }
        assert!(iter.next().is_none());
    }
}
