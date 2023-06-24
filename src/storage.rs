use crate::binarytree::{lookup_in_raw, BinarytreeBuilder};
use crate::Error;
use memmap2::MmapMut;
use std::cell::{Ref, RefCell};
use std::convert::TryInto;

const MAGICNUMBER: [u8; 4] = [b'r', b'e', b'd', b'b'];
const DATA_LEN: usize = MAGICNUMBER.len();
const DATA_OFFSET: usize = DATA_LEN + 8;

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
        mmap[index..(index + 8)].copy_from_slice(&(key.len() as u64).to_be_bytes());
        index += 8;
        mmap[index..(index + key.len())].copy_from_slice(key);
        index += key.len();
        mmap[index..(index + 8)].copy_from_slice(&(value.len() as u64).to_be_bytes());
        index += 8;
        mmap[index..(index + value.len())].copy_from_slice(value);
        index += value.len();
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
            let key_len = u64::from_be_bytes(mmap[index..(index + 8)].try_into().unwrap()) as usize;
            index += 8 + key_len;
            let value_len =
                u64::from_be_bytes(mmap[index..(index + 8)].try_into().unwrap()) as usize;
            index += 8 + value_len;
            entries += 1;
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
            let key_len = u64::from_be_bytes(mmap[index..(index + 8)].try_into().unwrap()) as usize;
            index += 8;
            let key = &mmap[index..(index + key_len)];
            index += key_len;
            let value_len =
                u64::from_be_bytes(mmap[index..(index + 8)].try_into().unwrap()) as usize;
            index += 8;
            let value = &mmap[index..(index + value_len)];
            index += value_len;

            builder.add(key, value);
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
            let guard = AccessGuard {
                mmap_ref: mmap,
                offset,
                len,
            };
            Ok(Some(guard))
        } else {
            Ok(None)
        }
    }
}

pub struct AccessGuard<'mmap> {
    mmap_ref: Ref<'mmap, MmapMut>, // ensure it stays alive
    offset: usize,
    len: usize,
}

impl<'mmap> AsRef<[u8]> for AccessGuard<'mmap> {
    fn as_ref(&self) -> &[u8] {
        &self.mmap_ref[self.offset..(self.offset + self.len)]
    }
}
