use std::{
    cell,
    collections::{hash_map::Entry, HashMap},
    io::{Error as IoError, Write},
    mem,
    ops::Range,
};

use crate::serialize::{Deserialize, SerdeError, Serialize};

/*
* Page Requirements
*
* - A defined layout so that I can use the same layout in memory and on disk
*      - repr(C) might be the thing I need
*      - I will want to do an unsafe cast to and from a [u8] when writing to and reading from disk
*  - All types that will serialize to a page cell or cell pointer also need a defined layout for
*  the same reasons
*  - An availibility list to track free space that is external to the page. This should be
*  derivable from the buffer when reading in a fresh page
*   - methods that modify the contents of the page will take a mutable reference to the availabily
*   list, make the decision on where to write, and update the the list accordingly.
*  - cell pointers must always be sorted by key value (we don't need to move the cells themselves
*  if we just sort the pointers instead)
*    - insert_cell takes a 'location' that is the index of the cell pointer this cell will occupy.
       Everything at this index and to the right should be shifted over by one position before insertion.
       It is up to the higher level stuff to determine this location.
*  - A header that contains:
*    - header_version
*    - page_id
*    - page kind
*    - free space start and end
*    - checksum of everything that is not the checksum
*    - cell count
*    - magic number to validate page is correctly aligned
*    - an optional overflow page_id
*    - flags
*      - is dirty
*      - is compactible
*  - A buffer of raw data (contains pointers, cells, and free space)
*
*
* - Should I use a max payload size, or is that more of a upper level structure, for instance in
* the derived BTree Node? It seems like, at least for data records, I'd want somewhat arbitrary
* page cardinality. (For now, I may want to add the simplifying restriction that all rows will be
* at most BUFFER_SIZE bytes, meaning they can always fit into a page, and I don't need to deal
* with data page overflows.)
*   - This will be an upper_level construct. overflow-related stuff (other than the overflow page
*   id) will not be handled at the page level. Upper level stuff will add the necessary bits to the
*   cell data to indicate that a cell is overflowed and manage accessing and merging the disparate
*   parts when reading
*
*
* Specifically not doing yet:
* - compression
* - right only appends (probably a higher level op anyways)
* - rebalancing (probably a higher level op anyways)
* - vacumming/compression (this is definitely a higher level op since it requires copying data
* from one page to another)
*/

type PageId = u64;
type PageBufferOffset = u16;

// TODO: Convert to 16kb
const PAGE_DATA_SIZE: PageBufferOffset = 4096 * 4; // 16KB
const PAGE_BUFFER_SIZE: PageBufferOffset =
    PAGE_DATA_SIZE - mem::size_of::<PageHeader>() as PageBufferOffset;
const HEADER_VERSION: u8 = 1;
// the byte values spell PAGE
const ALIGNMENT_GUARD_VALUE: u32 = u32::from_be_bytes([50, 41, 47, 45]);
const CELL_POINTER_SIZE: u16 = mem::size_of::<CellPointer>() as u16;

enum PageError {
    IoError(IoError),
    SerdeError(SerdeError),
    NotEnoughSpace,
}
impl From<IoError> for PageError {
    fn from(value: IoError) -> Self {
        Self::IoError(value)
    }
}
impl From<SerdeError> for PageError {
    fn from(value: SerdeError) -> Self {
        Self::SerdeError(value)
    }
}

#[repr(C)]
struct PageFlags {
    flags: u8,
}
impl PageFlags {
    const DIRTY: u8 = 1;
    const COMPACTIBLE: u8 = 1 << 1;

    fn is_dirty(&self) -> bool {
        (self.flags & Self::DIRTY) != 0
    }

    fn is_compactible(&self) -> bool {
        (self.flags & Self::COMPACTIBLE) != 0
    }

    fn set_flag(&mut self, flag: u8, val: bool) {
        let without_flag = self.flags & (u8::MAX ^ flag);
        let desired_val = if val { 1 } else { 0 };
        self.flags = without_flag | desired_val;
    }

    fn set_dirty(&mut self, dirty: bool) {
        self.set_flag(Self::DIRTY, dirty);
    }

    fn set_compactible(&mut self, compactible: bool) {
        self.set_flag(Self::COMPACTIBLE, compactible);
    }
}

#[repr(u8)]
enum PageKind {
    Data,
}

// TODO: Add CRC check in addition to the checksum

/// NOTE: Changing this in any way means that all existing on-disk data will not be read correctly.
///     All changes will requiring incrementing the header_version and maintaining the old & new
///     page layouts while pages with the old layout still exist.
///
/// checksum and header_version must be at the beginning of every version of this struct so that:
/// - The checksum can be validated before continuing
/// - the header_version can be read so that we know what struct version to use
///
/// Our max page size will be 16KB, so free_space_start and free_space_end only need to be u16. If
/// the page size increases, those fields will need to use larger types
#[repr(C)]
struct PageHeader {
    // comments: size, end-of-this-field-with-padding in layout
    checksum: u64,                      // 8, 8
    header_version: u8,                 // 1, 9
    flags: PageFlags,                   // 1, 10
    page_kind: PageKind,                // 1, 12
    alignment_guard: u32,               // 4, 16
    page_id: PageId,                    // 8, 24
    overflow_page_id: Option<PageId>,   // 16, 40
    cell_count: u16,                    // 2, 42
    free_space_start: PageBufferOffset, // 2, 44
    free_space_end: PageBufferOffset,   // 2, 46
    total_free_space: PageBufferOffset, // 2, 48
}

#[repr(C)]
struct PageBuffer {
    data: [u8; PAGE_BUFFER_SIZE as usize],
}
impl PageBuffer {
    fn write_to(&mut self, offset: PageBufferOffset, data: &[u8]) -> Result<(), PageError> {
        let mut writer = self.get_writer(offset, data.len());
        writer.write_all(data)?;
        Ok(())
    }

    fn get_writer(&mut self, offset: PageBufferOffset, data_size: usize) -> impl Write + '_ {
        let offset = offset as usize;
        let size = data_size as usize;
        &mut self.data[offset..offset + size]
    }
}

#[repr(C)]
struct Page {
    header: PageHeader,
    data: PageBuffer,
}
impl Page {
    fn new(id: PageId, kind: PageKind) -> Self {
        let header = PageHeader {
            checksum: todo!(),
            header_version: HEADER_VERSION,
            flags: PageFlags { flags: 0 },
            cell_count: 0,
            page_kind: kind,
            alignment_guard: ALIGNMENT_GUARD_VALUE,
            free_space_start: 0,
            free_space_end: PAGE_BUFFER_SIZE,
            total_free_space: PAGE_BUFFER_SIZE,
            page_id: id,
            overflow_page_id: None,
        };

        let data = PageBuffer {
            data: [0; PAGE_BUFFER_SIZE as usize],
        };

        Page { header, data }
    }

    // TODO: Fix the error type
    fn from_disk(offset: usize) -> Result<Self, ()> {
        unimplemented!();
    }

    fn flush(&mut self) {
        unimplemented!();
    }
}

impl Page {
    fn insert_cell(
        &mut self,
        cell_position: u16, // must be <= cell count
        data: &[u8],
    ) -> Result<u16, PageError> {
        let data_size: PageBufferOffset = data.len().try_into().unwrap();
        // Going to not use an availability_list, and instead track total free space. If there is
        // enough total space, but not enough space in the free area, the page will be defragged

        // TODO: Confirm we have sufficient space before modifying anything.
        // states:
        // - cell pointer
        //   - not enough space in free space for new cell pointer
        //   - enough space for new pointer
        //   - using existing nulled pointer
        // - cell
        //   - using existing available slot
        //   - using free space
        //   - not enough space in free space or available slots
        //
        //   framed differently:
        //   - need new cell pointer
        //      - not enough free space
        //      - enough free space
        //          - using slot for cell
        //          - using free space for cell
        //          - not enough free space
        //  - using existing cell pointer
        //      - using slot for cell
        //      - using free space for cell
        //      - not enough free space
        //
        //  down to four cases: (for have cell pointer, how we get it doesn't matter)
        //  - (have cell pointer, using slot)
        //  - (have cell pointer, using free_space)
        //  - (have cell pointer, not enough free_space)
        //  - not enough for cell pointer
        //
        //  So basically need to, in order:
        //  - determine cell pointer position
        //  - determine cell position
        //  - modify stuff and write data

        // determine cell pointer position:
        // - cases requiring new pointer slot
        //      - cell_postion >= cell count
        //      - all existing cells at and to the right of cell_position are filled
        // - cases not requiring new slot:
        //  - at least one cell at or to the right of cell_position is empty

        // steps to possibly do:
        // - MAYBE: move a range of cell pointers to the right 1
        // - MAYBE: Take space from availability list
        // - MAYBE: "Take" space from free space
        // - write new cell pointer to cell pointer dest
        // - write new cell to cell dest

        // verify we have room for the cell pointer + data
        let total_space_needed = CELL_POINTER_SIZE + data_size;
        if self.header.total_free_space < total_space_needed {
            return Err(PageError::NotEnoughSpace);
        }
        if self.header.free_space_end - self.header.free_space_start < total_space_needed {
            self.defragment()?;
        }
        let cell_end = self.header.free_space_end;

        // write pointer
        self.shift_pointers_write_starting_at(cell_position);
        let cell_pointer = CellPointer {
            end_position: cell_end,
            size: data_size,
        };
        let mut pointer_writer = self
            .data
            .get_writer(self.header.free_space_start, CELL_POINTER_SIZE as usize);
        cell_pointer.write_to_bytes(&mut pointer_writer)?;
        drop(pointer_writer);
        self.header.cell_count += 1;
        self.header.free_space_start += CELL_POINTER_SIZE;

        //write data
        let write_start = cell_end - data_size;
        self.data.write_to(write_start, data)?;
        self.header.free_space_end -= data_size;

        self.header.total_free_space -= total_space_needed;

        // Okay, now we can actual act and change things
        Ok(cell_position)
    }

    fn remove_cell(&mut self, cell_position: u8) {
        unimplemented!();
    }

    fn defragment(&mut self) -> Result<(), PageError> {
        self.header.free_space_end = PAGE_BUFFER_SIZE;
        for i in 0..self.header.cell_count {
            let offset_start = (i * CELL_POINTER_SIZE) as usize;
            let offset_size = CELL_POINTER_SIZE as usize;
            let pointer_slice: [u8; CELL_POINTER_SIZE as usize] = self.data.data
                [offset_start..offset_start + offset_size]
                .try_into()
                .unwrap();
            unsafe {
                let pointer: CellPointer = mem::transmute(pointer_slice);
                let cell_start = pointer.end_position - pointer.size;
                let dest_start = (self.header.free_space_end - pointer.size).into();
                let src_range: Range<usize> = cell_start.into()..pointer.end_position.into();
                self.data.data.copy_within(src_range, dest_start);
                self.header.free_space_end -= pointer.size;
            }
        }
        unimplemented!();
    }

    fn shift_pointers_write_starting_at(&mut self, cell_position: u16) {
        assert!(cell_position <= self.header.cell_count);
        if cell_position == self.header.cell_count {
            // no pointers need to be moved
            return;
        }
        let slice_start = (cell_position * CELL_POINTER_SIZE) as usize;
        let slice_end = (self.header.cell_count * CELL_POINTER_SIZE) as usize;
        let slice = &mut self.data.data[slice_start..slice_end];
        // TODO: Unsure if I should do this with memcopy instead?
        for i in slice_end - 1..slice_start {
            // copy one byte at a time, safely because we're not overwriting any yet-to-be-copied
            // data
            slice[i] = slice[i - 1];
        }
    }
}

struct CellPointer {
    end_position: PageBufferOffset,
    size: PageBufferOffset,
}
impl Serialize for CellPointer {
    fn write_to_bytes(&self, dest: &mut impl std::io::Write) -> Result<(), SerdeError> {
        self.end_position.write_to_bytes(dest)?;
        self.size.write_to_bytes(dest)?;
        Ok(())
    }
}
impl Deserialize for CellPointer {
    type ExtraInfo = ();
    fn from_bytes(
        from: &mut impl std::io::Read,
        _extra: &Self::ExtraInfo,
    ) -> Result<Self, SerdeError> {
        let end_position = PageBufferOffset::from_bytes(from, &())?;
        let size = PageBufferOffset::from_bytes(from, &())?;
        Ok(CellPointer { end_position, size })
    }
}

#[cfg(test)]
mod tests {
    use std::mem;

    use super::*;

    #[test]
    fn struct_sizes() {
        // prove all of the things we expect to be true about our memory layout
        assert_eq!(mem::size_of::<PageKind>(), 1);
        assert_eq!(mem::size_of::<Option<PageId>>(), 16);
        assert_eq!(mem::size_of::<Option<u16>>(), 4);
        assert_eq!(mem::size_of::<PageHeader>(), 48);
        assert_eq!(mem::size_of::<PageBuffer>(), PAGE_BUFFER_SIZE as usize);
        assert_eq!(mem::size_of::<Page>(), PAGE_DATA_SIZE as usize);
        assert_eq!(PAGE_BUFFER_SIZE % 8, 0);
        assert_eq!(mem::size_of::<CellPointer>(), 4);
    }
}
