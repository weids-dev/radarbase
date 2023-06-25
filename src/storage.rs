use crate::binarytree::{lookup_in_raw, BinarytreeBuilder};
use crate::Error;
use memmap2::MmapMut;
use std::cell::{Ref, RefCell};
use std::convert::TryInto;

const MAGICNUMBER: [u8; 4] = [b'r', b'e', b'd', b'b'];
const DATA_LEN: usize = MAGICNUMBER.len();
const DATA_OFFSET: usize = DATA_LEN + 8;
const ENTRY_DELETED: u8 = 1;

// Provides a simple zero-copy way to access entries
//
// Entry format is:
// * (1 byte) flags: 1 = DELETED
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

    fn is_deleted(&self) -> bool {
        self.raw[0] & ENTRY_DELETED != 0
    }

    fn key_len(&self) -> usize {
        u64::from_be_bytes(self.raw[1..9].try_into().unwrap()) as usize
    }

    fn key(&self) -> &'a [u8] {
        &self.raw[9..(9 + self.key_len())]
    }

    fn value_len(&self) -> usize {
        let key_len = self.key_len();
        u64::from_be_bytes(
            self.raw[(9 + key_len)..(9 + key_len + 8)]
                .try_into()
                .unwrap(),
        ) as usize
    }

    fn value(&self) -> &'a [u8] {
        let value_offset = 1 + 8 + self.key_len() + 8;
        &self.raw[value_offset..(value_offset + self.value_len())]
    }

    fn raw_len(&self) -> usize {
        1 + 8 + self.key_len() + 8 + self.value_len()
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

    fn raw_len(&self) -> usize {
        EntryAccessor::new(self.raw).raw_len()
    }

    fn write_flags(&mut self, flags: u8) {
        self.raw[0] = flags;
    }

    fn write_key(&mut self, key: &[u8]) {
        self.raw[1..9].copy_from_slice(&(key.len() as u64).to_be_bytes());
        self.raw[9..(9 + key.len())].copy_from_slice(key);
    }

    fn write_value(&mut self, value: &[u8]) {
        let value_offset = 9 + EntryAccessor::new(self.raw).key_len();
        self.raw[value_offset..(value_offset + 8)]
            .copy_from_slice(&(value.len() as u64).to_be_bytes());
        self.raw[(value_offset + 8)..(value_offset + 8 + value.len())].copy_from_slice(value);
    }
}

pub(crate) struct Storage {
    mmap: RefCell<MmapMut>,
}

impl Storage {
    pub(crate) fn new(mmap: MmapMut) -> Storage {
        // Mutate data even there are immutable reference to that data
        Storage {
            mmap: RefCell::new(mmap),
        }
    }

    pub(crate) fn initialize(&self) -> Result<(), Error> {
        let mut mmap = self.mmap.borrow_mut();
        if mmap[0..MAGICNUMBER.len()] == MAGICNUMBER {
            // Already initialized, nothing to do
            return Ok(());
        }
        mmap[DATA_LEN..(DATA_LEN + 8)].copy_from_slice(&0u64.to_be_bytes());
        // indicate that there are no entries
        mmap.flush()?;
        // Write the magic number only after the data structure is initialized and written to disk
        // to ensure that it's crash safe
        mmap[0..MAGICNUMBER.len()].copy_from_slice(&MAGICNUMBER);
        mmap.flush()?;

        Ok(())
    }

    /// Append a new key & value to the end of the file
    pub(crate) fn append(&self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        let mut mmap = self.mmap.borrow_mut();
        let mut data_len =
            u64::from_be_bytes(mmap[DATA_LEN..(DATA_LEN + 8)].try_into().unwrap()) as usize;
        // number of entries (stored in the first 8 bytes of the file after the magic number)

        // do not check whether it is a duplicate key, just append it to the end
        let mut index = DATA_OFFSET + data_len;

        // Append the new key & value
        let mut mutator = EntryMutator::new(&mut mmap[index..]);
        mutator.write_key(key);
        mutator.write_value(value);
        index += mutator.raw_len();
        data_len = index - DATA_OFFSET;

        mmap[DATA_LEN..(DATA_LEN + 8)].copy_from_slice(&data_len.to_be_bytes());
        // update the number of entries

        Ok(())
    }

    /// Get the number of entries
    pub(crate) fn len(&self) -> Result<usize, Error> {
        let mmap = self.mmap.borrow();
        let data_len =
            u64::from_be_bytes(mmap[DATA_LEN..(DATA_LEN + 8)].try_into().unwrap()) as usize;

        let mut index = DATA_OFFSET;

        let mut entries = 0;
        while index < (DATA_OFFSET + data_len) {
            let entry = EntryAccessor::new(&mmap[index..]);
            index += entry.raw_len();
            if !entry.is_deleted() {
                entries += 1;
            }
        }

        Ok(entries)
    }

    /// Flush the data to disk, and rebuild the binary tree
    pub(crate) fn fsync(&self) -> Result<(), Error> {
        let mut builder = BinarytreeBuilder::new();
        let mut mmap = self.mmap.borrow_mut();

        let data_len =
            u64::from_be_bytes(mmap[DATA_LEN..(DATA_LEN + 8)].try_into().unwrap()) as usize;

        let mut index = DATA_OFFSET;
        while index < (DATA_OFFSET + data_len) {
            let entry = EntryAccessor::new(&mmap[index..]);
            if !entry.is_deleted() {
                builder.add(entry.key(), entry.value());
            }
            index += entry.raw_len();
        }

        let node = builder.build(); // rebuild the binary tree
        assert!(DATA_OFFSET + data_len + node.recursive_size() < mmap.len());

        node.to_bytes(&mut mmap[(DATA_OFFSET + data_len)..], 0);
        // write the binary tree to the end of the file

        mmap.flush()?;
        Ok(())
    }

    pub(crate) fn get(&self, key: &[u8]) -> Result<Option<AccessGuard>, Error> {
        let mmap = self.mmap.borrow();

        let data_len =
            u64::from_be_bytes(mmap[DATA_LEN..(DATA_LEN + 8)].try_into().unwrap()) as usize;

        let index = DATA_OFFSET + data_len; // get the offset of the binary tree
        if let Some((offset, len)) = lookup_in_raw(&mmap, key, index) {
            Ok(Some(AccessGuard::Mmap(mmap, offset, len)))
        } else {
            Ok(None)
        }
    }

    // Returns a boolean indicating if an entry was removed
    pub(crate) fn remove(&self, key: &[u8]) -> Result<bool, Error> {
        let mut mmap = self.mmap.borrow_mut();

        let data_len =
            u64::from_be_bytes(mmap[DATA_LEN..(DATA_LEN + 8)].try_into().unwrap()) as usize;

        let index = DATA_OFFSET + data_len;
        if let Some((_, _)) = lookup_in_raw(&mmap, key, index) {
            // Delete the entry from the entry space
            let data_len =
                u64::from_be_bytes(mmap[DATA_LEN..(DATA_LEN + 8)].try_into().unwrap()) as usize;

            let mut index = DATA_OFFSET;
            while index < (DATA_OFFSET + data_len) {
                let entry = EntryAccessor::new(&mmap[index..]);
                if entry.key() == key {
                    drop(entry);
                    let mut entry = EntryMutator::new(&mut mmap[index..]);
                    entry.write_flags(ENTRY_DELETED);
                    break;
                }
                index += entry.raw_len();
            }
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

pub enum AccessGuard<'a> {
    // Either a reference to the mmap or a reference to the local data in memory
    Mmap(Ref<'a, MmapMut>, usize, usize), // offset and length, keep it alive
    Local(&'a [u8]),
}

impl<'mmap> AsRef<[u8]> for AccessGuard<'mmap> {
    fn as_ref(&self) -> &[u8] {
        match self {
            AccessGuard::Mmap(mmap_ref, offset, len) => &mmap_ref[*offset..(*offset + *len)],
            AccessGuard::Local(data_ref) => data_ref,
        }
    }
}
