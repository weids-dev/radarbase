pub trait BenchTable {
    type W: BenchWriteTransaction;
    type R: for<'a> BenchReadTransaction<'a>;

    fn db_type_name() -> &'static str;

    fn write_transaction(&mut self) -> Self::W;

    fn read_transaction(&self) -> Self::R;
}

pub trait BenchWriteTransaction {
    fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), ()>;

    fn commit(self) -> Result<(), ()>;
}

pub trait BenchReadTransaction<'a> {
    type Output: AsRef<[u8]> + 'a;

    fn get(&'a self, key: &[u8]) -> Option<Self::Output>;
}

pub struct RadbBenchTable<'a> {
    table: radarbase::Table<'a, [u8]>,
}

impl<'a> RadbBenchTable<'a> {
    pub fn new(db: &'a radarbase::Database) -> Self {
        RadbBenchTable {
            table: db.open_table(b"x").unwrap(),
        }
    }
}

impl<'a> BenchTable for RadbBenchTable<'a> {
    type W = RadbBenchWriteTransaction<'a>;
    type R = RadbBenchReadTransaction<'a>;

    fn db_type_name() -> &'static str {
        "radarbase"
    }

    fn write_transaction(&mut self) -> Self::W {
        RadbBenchWriteTransaction {
            txn: self.table.begin_write().unwrap(),
        }
    }

    fn read_transaction(&self) -> Self::R {
        RadbBenchReadTransaction {
            txn: self.table.read_transaction().unwrap(),
        }
    }
}

pub struct RadbBenchReadTransaction<'a> {
    txn: radarbase::ReadOnlyTransaction<'a, [u8]>,
}

impl<'a, 'b> BenchReadTransaction<'b> for RadbBenchReadTransaction<'a> {
    type Output = radarbase::AccessGuard<'b>;

    fn get(&'b self, key: &[u8]) -> Option<radarbase::AccessGuard<'b>> {
        self.txn.get(key).unwrap()
    }
}

pub struct RadbBenchWriteTransaction<'a> {
    txn: radarbase::WriteTransaction<'a, [u8]>,
}

impl BenchWriteTransaction for RadbBenchWriteTransaction<'_> {
    fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), ()> {
        self.txn.insert(key, value).map_err(|_| ())
    }

    fn commit(self) -> Result<(), ()> {
        self.txn.commit().map_err(|_| ())
    }
}

pub struct SledBenchTable<'a> {
    db: &'a sled::Db,
}

impl<'a> SledBenchTable<'a> {
    pub fn new(db: &'a sled::Db) -> Self {
        SledBenchTable { db }
    }
}

impl<'a> BenchTable for SledBenchTable<'a> {
    type W = SledBenchWriteTransaction<'a>;
    type R = SledBenchReadTransaction<'a>;

    fn db_type_name() -> &'static str {
        "sled"
    }

    fn write_transaction(&mut self) -> Self::W {
        SledBenchWriteTransaction { db: self.db }
    }

    fn read_transaction(&self) -> Self::R {
        SledBenchReadTransaction { db: self.db }
    }
}

pub struct SledBenchReadTransaction<'a> {
    db: &'a sled::Db,
}

impl<'a, 'b> BenchReadTransaction<'b> for SledBenchReadTransaction<'a> {
    type Output = sled::IVec;

    fn get(&'b self, key: &[u8]) -> Option<sled::IVec> {
        self.db.get(key).unwrap()
    }
}

pub struct SledBenchWriteTransaction<'a> {
    db: &'a sled::Db,
}

impl BenchWriteTransaction for SledBenchWriteTransaction<'_> {
    fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), ()> {
        self.db.insert(key, value).map(|_| ()).map_err(|_| ())
    }

    fn commit(self) -> Result<(), ()> {
        self.db.flush().map(|_| ()).map_err(|_| ())
    }
}

pub struct LmdbRkvBenchTable<'a> {
    env: &'a lmdb::Environment,
    db: lmdb::Database,
}

impl<'a> LmdbRkvBenchTable<'a> {
    pub fn new(env: &'a lmdb::Environment) -> Self {
        let db = env.open_db(None).unwrap();
        LmdbRkvBenchTable { env, db }
    }
}

impl<'a> BenchTable for LmdbRkvBenchTable<'a> {
    type W = LmdbRkvBenchWriteTransaction<'a>;
    type R = LmdbRkvBenchReadTransaction<'a>;

    fn db_type_name() -> &'static str {
        "lmdb-rkv"
    }

    fn write_transaction(&mut self) -> Self::W {
        let txn = self.env.begin_rw_txn().unwrap();
        LmdbRkvBenchWriteTransaction { db: self.db, txn }
    }

    fn read_transaction(&self) -> Self::R {
        let txn = self.env.begin_ro_txn().unwrap();
        LmdbRkvBenchReadTransaction { db: self.db, txn }
    }
}

pub struct LmdbRkvBenchWriteTransaction<'a> {
    db: lmdb::Database,
    txn: lmdb::RwTransaction<'a>,
}

impl BenchWriteTransaction for LmdbRkvBenchWriteTransaction<'_> {
    fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), ()> {
        self.txn
            .put(self.db, &key, &value, lmdb::WriteFlags::empty())
            .map_err(|_| ())
    }

    fn commit(self) -> Result<(), ()> {
        use lmdb::Transaction;
        self.txn.commit().map_err(|_| ())
    }
}

pub struct LmdbRkvBenchReadTransaction<'a> {
    db: lmdb::Database,
    txn: lmdb::RoTransaction<'a>,
}

impl<'a, 'b> BenchReadTransaction<'b> for LmdbRkvBenchReadTransaction<'a> {
    type Output = &'b [u8];

    fn get(&'b self, key: &[u8]) -> Option<&'b [u8]> {
        use lmdb::Transaction;
        self.txn.get(self.db, &key).ok()
    }
}
