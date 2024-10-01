use core::slice;
use std::{
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

#[derive(Debug)]
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
            checksum: 0,
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

        let mut page = Page { header, data };
        page.header.checksum = page.calc_checksum();
        page
    }

    fn calc_checksum(&self) -> u64 {
        unsafe {
            let bytes =
                slice::from_raw_parts((self as *const Page) as *const u8, mem::size_of::<Page>());
            checksum(&bytes[8..]).unwrap()
        }
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
    ) -> Result<(), PageError> {
        assert!(cell_position <= self.header.cell_count);
        let data_size: PageBufferOffset = data.len().try_into().unwrap();

        // verify we have room for the cell pointer + data
        let total_space_needed = CELL_POINTER_SIZE + data_size;
        if self.header.total_free_space < total_space_needed {
            return Err(PageError::NotEnoughSpace);
        }
        if self.header.free_space_end - self.header.free_space_start < total_space_needed {
            self.defragment();
        }
        let cell_end = self.header.free_space_end;

        // write pointer
        self.make_room_for_pointer(cell_position);
        let cell_pointer = CellPointer {
            end_position: cell_end,
            size: data_size,
        };
        let mut pointer_writer = self.data.get_writer(
            CELL_POINTER_SIZE * cell_position,
            CELL_POINTER_SIZE as usize,
        );
        cell_pointer.write_to_bytes(&mut pointer_writer)?;
        drop(pointer_writer);
        self.header.cell_count += 1;
        self.header.free_space_start += CELL_POINTER_SIZE;

        //write data
        let write_start = cell_end - data_size;
        self.data.write_to(write_start, data)?;
        self.header.free_space_end -= data_size;

        self.header.total_free_space -= total_space_needed;
        self.header.flags.set_dirty(true);

        Ok(())
    }

    fn remove_cell(&mut self, cell_position: u16) {
        assert!(self.header.cell_count > 0);
        assert!(cell_position < self.header.cell_count);
        let ptr = self.get_cell_pointer(cell_position);
        self.remove_pointer(cell_position);
        self.header.cell_count -= 1;
        self.header.free_space_start -= CELL_POINTER_SIZE;
        self.header.total_free_space += ptr.size + CELL_POINTER_SIZE;
        self.header.flags.set_dirty(true);
        self.header.flags.set_compactible(true);
    }

    fn defragment(&mut self) {
        self.header.free_space_end = PAGE_BUFFER_SIZE;
        for i in 0..self.header.cell_count {
            let pointer = self.get_cell_pointer(i);
            let cell_start = pointer.end_position - pointer.size;
            let dest_start = (self.header.free_space_end - pointer.size).into();
            let src_range: Range<usize> = cell_start.into()..pointer.end_position.into();
            self.data.data.copy_within(src_range, dest_start);
            self.header.free_space_end -= pointer.size;
        }
        self.header.flags.set_compactible(false);
    }

    fn make_room_for_pointer(&mut self, cell_position: u16) {
        assert!(cell_position <= self.header.cell_count);
        if cell_position == self.header.cell_count {
            // no pointers need to be moved
            return;
        }
        let start = (cell_position * CELL_POINTER_SIZE) as usize;
        let end = (self.header.cell_count * CELL_POINTER_SIZE) as usize;
        let dest = start + (CELL_POINTER_SIZE as usize);
        self.data.data.copy_within(start..end, dest);
    }

    fn remove_pointer(&mut self, cell_position: u16) {
        if cell_position == 0 {
            // don't need to move anything
            return;
        }
        let start = ((cell_position + 1) * CELL_POINTER_SIZE) as usize;
        let end = (self.header.cell_count * CELL_POINTER_SIZE) as usize;
        let dest = (cell_position * CELL_POINTER_SIZE) as usize;
        self.data.data.copy_within(start..end, dest);
    }

    fn get_cell_pointer(&self, position: u16) -> CellPointer {
        assert!(position < self.header.cell_count);
        let offset_start = (position * CELL_POINTER_SIZE) as usize;
        let offset_size = CELL_POINTER_SIZE as usize;
        let mut pointer_slice = &self.data.data[offset_start..offset_start + offset_size];
        CellPointer::from_bytes(&mut pointer_slice, &()).unwrap()
    }

    fn get_cell(&self, cell_position: u16) -> Vec<u8> {
        assert!(cell_position < self.header.cell_count);
        let pointer = self.get_cell_pointer(cell_position);
        let start = (pointer.end_position - pointer.size) as usize;
        let end = pointer.end_position as usize;
        let slice = &self.data.data[start..end];

        let mut cell = Vec::with_capacity(pointer.size as usize);
        cell.extend_from_slice(slice);
        cell
    }
}

fn checksum(data: &[u8]) -> Result<u64, SerdeError> {
    assert!(data.len() % 8 == 0);
    let mut reader = data;
    let mut sum = 0;
    for _ in 0..(data.len() / 8) {
        let v = u64::from_bytes(&mut reader, &())?;
        sum += v;
    }
    Ok(sum)
}

#[derive(Debug)]
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

    #[test]
    fn test_checksum() {
        let mut bytes = Vec::new();
        100u64.write_to_bytes(&mut bytes).unwrap();
        200u64.write_to_bytes(&mut bytes).unwrap();
        300u64.write_to_bytes(&mut bytes).unwrap();
        0u32.write_to_bytes(&mut bytes).unwrap();
        100u32.write_to_bytes(&mut bytes).unwrap();
        let res = checksum(&bytes[..]).unwrap();
        assert_eq!(res, 700);
    }

    fn get_all_cells(page: &Page) -> Vec<Vec<u32>> {
        let mut read_cells: Vec<Vec<u32>> = Vec::new();
        for idx in 0..page.header.cell_count {
            let data = page.get_cell(idx);
            let mut reader = &data[..];
            read_cells.push(Vec::from_bytes(&mut reader, &()).unwrap());
        }
        read_cells
    }

    fn print_pointers(page: &Page) {
        for idx in 0..page.header.cell_count {
            println!("{idx}: {:?}", page.get_cell_pointer(idx));
        }
    }

    #[test]
    fn page_basics() {
        // add cells
        let mut page = Page::new(1, PageKind::Data);
        let cells = vec![
            vec![1u32, 2, 3, 4, 5],
            vec![10u32, 20, 30, 40, 50],
            vec![100u32, 200, 300, 400, 500],
        ];
        let mut buffer = Vec::new();
        for (idx, cell) in cells.iter().enumerate() {
            cell.write_to_bytes(&mut buffer).unwrap();
            page.insert_cell(idx as u16, &buffer[..]).unwrap();
            buffer.clear();
        }

        let read_cells = get_all_cells(&page);
        assert_eq!(cells, read_cells);

        //remove
        page.remove_cell(1);
        let read_cells = get_all_cells(&page);
        assert_eq!(
            vec![vec![1u32, 2, 3, 4, 5], vec![100u32, 200, 300, 400, 500]],
            read_cells
        );

        print_pointers(&page);
        println!("----------");

        // add in middle
        let mut buffer = Vec::new();
        vec![10u32, 9, 8, 7].write_to_bytes(&mut buffer).unwrap();
        page.insert_cell(2, &buffer[..]).unwrap();
        buffer.clear();
        print_pointers(&page);
        println!("----------");

        vec![11u32, 12, 13, 14, 15]
            .write_to_bytes(&mut buffer)
            .unwrap();
        page.insert_cell(1, &buffer[..]).unwrap();
        print_pointers(&page);
        println!("----------");

        let read_cells = get_all_cells(&page);
        assert_eq!(
            vec![
                vec![1u32, 2, 3, 4, 5],
                vec![11u32, 12, 13, 14, 15],
                vec![100u32, 200, 300, 400, 500],
                vec![10u32, 9, 8, 7]
            ],
            read_cells
        );
    }

    #[test]
    fn page_defrag() {
        let mut page = Page::new(1, PageKind::Data);
        let cell = vec![10u32, 10, 10, 10, 10];
        let mut bytes = Vec::new();
        cell.write_to_bytes(&mut bytes).unwrap();

        let mut cell_count = 0;
        let mut idx = 0;

        let has_space = |page: &Page| {
            page.header.free_space_end - page.header.free_space_start
                > (bytes.len() as u16 + CELL_POINTER_SIZE)
        };

        // fill up the free space in a fragmented way
        while has_space(&page) {
            page.insert_cell(idx, &bytes[..]).unwrap();
            if !has_space(&page) {
                break;
            }
            page.insert_cell(idx + 1, &bytes[..]).unwrap();
            page.remove_cell(idx);
            idx += 1;
            cell_count += 1;
        }
        assert_eq!(cell_count, page.header.cell_count);
        let read_cells = get_all_cells(&page);
        for c in read_cells {
            assert_eq!(cell, c);
        }
        // add one more to trigger defrag
        page.insert_cell(idx, &bytes[..]).unwrap();
        assert_eq!(cell_count + 1, page.header.cell_count);
        let read_cells = get_all_cells(&page);
        for c in read_cells {
            assert_eq!(cell, c);
        }
        assert_eq!(
            page.header.total_free_space,
            page.header.free_space_end - page.header.free_space_start
        );
    }
}
