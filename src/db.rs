use crate::table::Table;
use crate::Error;
use std::path::Path;

pub struct Database {}

impl Database {
    pub unsafe fn open(_path: &Path) -> Result<Database, Error> {
        Ok(Database {})
    }

    pub fn open_table(&self, _name: &str) -> Result<Table, Error> {
        Table::new()
    }
}
