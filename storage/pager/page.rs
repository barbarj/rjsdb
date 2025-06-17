use core::slice;
use serde::{Deserialize, Serialize};
use serialize::{from_reader, to_bytes, to_writer, Error as SerdeError};
use std::{
    io::{Error as IoError, Write},
    mem,
    num::NonZeroU64,
    ops::Range,
    os::unix::fs::FileExt,
};

/*
* Page Requirements
*
* - A defined layout so that I can use the same layout in memory and on disk
*      - repr(C) might be the thing I need
*      - I will want to do an unsafe cast to and from a [u8] when writing to and reading from disk
*  - All types that will serialize to a page cell or cell pointer also need a defined layout for
*  the same reasons
  - cell pointers must always be sorted by key value (we don't need to move the cells themselves
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
*/

pub type PageId = u64;
type PageBufferOffset = u16;

pub const PAGE_SIZE: PageBufferOffset = 4096 * 4; // 16KB
pub const PAGE_BUFFER_SIZE: PageBufferOffset =
    PAGE_SIZE - mem::size_of::<PageHeader>() as PageBufferOffset;
const HEADER_VERSION: u8 = 1;
// the byte values spell PAGE
const ALIGNMENT_GUARD_VALUE: u32 = u32::from_be_bytes([50, 41, 47, 45]);
pub const CELL_POINTER_SIZE: u16 = mem::size_of::<CellPointer>() as u16;

#[derive(Debug)]
pub enum PageError {
    IoError(IoError),
    SerdeError(SerdeError),
    NotEnoughSpace,
    Corrupted,
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
#[derive(Debug, PartialEq)]
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
        let desired_val = if val { flag } else { 0 };
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
#[derive(Debug, PartialEq, Copy, Clone)]
pub enum PageKind {
    Unitialized,
    Heap,
    BTreeNode,
    BTreeLeaf,
}

// TODO: Add CRC check in addition to the checksum

/// NOTE: Changing this in any way means that all existing on-disk data will not be read correctly.
///     All changes will requiring incrementing the header_version and maintaining the old & new
///     page layouts while pages with the old layout still exist.
///
/// We must also be careful to lay this out in a way that doesn't have any unitialized memory.
/// This is why we use Option<NonZeroU64> instead of Option<u64> for overflow_page_id. Option<NonZeroU64>
/// can use a compiler optimization to store the None state as 0, so the enum tag doesn\'t need
/// to be stored seperately, meaning we don't need the padding that would require.
///
/// checksum and header_version must be at the beginning of every version of this struct so that:
/// - The checksum can be validated before continuing
/// - the header_version can be read so that we know what struct version to use
///
/// Our max page size will be 16KB, so free_space_start and free_space_end only need to be u16. If
/// the page size increases, those fields will need to use larger types
#[repr(C)]
#[derive(Debug, PartialEq)]
pub struct PageHeader {
    // comments: size, end-of-this-field-with-padding in layout
    checksum: u64,      // 8, 8
    header_version: u8, // 1, 9
    flags: PageFlags,   // 1, 10
    //  (included so that this memory is initialized instead of being padding)
    _padding1: u8,                            // 1, 11
    page_kind: PageKind,                      // 1, 12
    alignment_guard: u32,                     // 4, 16
    page_id: PageId,                          // 8, 24
    pub overflow_page_id: Option<NonZeroU64>, // 8, 32
    cell_count: u16,                          // 2, 34
    free_space_start: PageBufferOffset,       // 2, 36
    free_space_end: PageBufferOffset,         // 2, 38
    total_free_space: PageBufferOffset,       // 2, 40
}

#[repr(C)]
#[derive(Debug, PartialEq)]
struct PageBuffer {
    data: [u8; PAGE_BUFFER_SIZE as usize],
}
impl PageBuffer {
    fn new() -> Self {
        PageBuffer {
            data: [0; PAGE_BUFFER_SIZE as usize],
        }
    }

    fn write_to(&mut self, offset: PageBufferOffset, data: &[u8]) -> Result<(), PageError> {
        let mut writer = self.get_writer(offset, data.len());
        writer.write_all(data)?;
        Ok(())
    }

    fn get_writer(&mut self, offset: PageBufferOffset, data_size: usize) -> impl Write + '_ {
        let offset = offset as usize;
        &mut self.data[offset..offset + data_size]
    }
}

#[repr(C)]
#[derive(Debug, PartialEq)]
pub struct Page {
    pub header: PageHeader,
    data: PageBuffer,
}
impl Page {
    pub fn new(id: PageId, kind: PageKind) -> Self {
        let header = PageHeader {
            checksum: 0,
            header_version: HEADER_VERSION,
            flags: PageFlags { flags: 0 },
            cell_count: 0,
            page_kind: kind,
            _padding1: 0,
            alignment_guard: ALIGNMENT_GUARD_VALUE,
            free_space_start: 0,
            free_space_end: PAGE_BUFFER_SIZE,
            total_free_space: PAGE_BUFFER_SIZE,
            page_id: id,
            overflow_page_id: None,
        };

        let data = PageBuffer::new();

        Page { header, data }
    }

    fn calc_checksum(&self) -> u64 {
        let bytes = self.as_slice();
        checksum(&bytes[8..]).unwrap()
    }

    pub fn id(&self) -> PageId {
        self.header.page_id
    }

    pub fn is_dirty(&self) -> bool {
        self.header.flags.is_dirty()
    }

    pub fn cell_count(&self) -> u16 {
        self.header.cell_count
    }

    pub fn total_free_space(&self) -> u16 {
        self.header.total_free_space
    }

    pub fn can_fit_data(&self, size: u16) -> bool {
        size + CELL_POINTER_SIZE <= self.total_free_space()
    }

    pub fn kind(&self) -> PageKind {
        self.header.page_kind
    }

    pub fn from_disk<F: FileExt>(source: &F, page_id: PageId) -> Result<Self, PageError> {
        let mut new_page = Page::new(0, PageKind::Heap);
        new_page.replace_contents(source, page_id)?;
        Ok(new_page)
    }

    // replaces the contents of this page object with the indicated on-disk page's contents
    pub fn replace_contents<F: FileExt>(
        &mut self,
        source: &F,
        page_id: PageId,
    ) -> Result<(), PageError> {
        let buf = self.as_slice_mut();
        let offset = page_id * PAGE_SIZE as u64;
        // make read all
        Page::read_entire_page(source, buf, offset)?;

        // new page should now have values from disk
        let checksum = self.calc_checksum();
        if checksum != self.header.checksum {
            return Err(PageError::Corrupted);
        }
        Ok(())
    }

    fn as_slice(&self) -> &[u8] {
        unsafe { slice::from_raw_parts(self as *const Self as *const u8, PAGE_SIZE.into()) }
    }

    fn as_slice_mut(&mut self) -> &mut [u8] {
        unsafe { slice::from_raw_parts_mut(self as *mut Self as *mut u8, PAGE_SIZE.into()) }
    }

    fn read_entire_page<F: FileExt>(
        source: &F,
        buf: &mut [u8],
        offset: u64,
    ) -> Result<(), PageError> {
        assert!(buf.len() == PAGE_SIZE as usize);
        source.read_exact_at(buf, offset)?;
        Ok(())
    }

    fn write_entire_page<F: FileExt>(
        dest: &mut F,
        buf: &[u8],
        offset: u64,
    ) -> Result<(), PageError> {
        assert!(buf.len() == PAGE_SIZE as usize);
        dest.write_all_at(buf, offset)?;
        Ok(())
    }

    pub fn write_to_disk<F: FileExt>(&mut self, dest: &mut F) -> Result<(), PageError> {
        self.defragment()?;
        let offset = self.header.page_id * PAGE_SIZE as u64;
        // setting dirty flag before slice cast and write to:
        // 1: Make the effects on other vars easier to reason about.
        // 2: By definition the page on disk should be considered clean
        let dirty_val = self.header.flags.is_dirty();
        self.header.flags.set_dirty(false);
        self.header.checksum = self.calc_checksum();
        let buf = self.as_slice();
        Page::write_entire_page(dest, buf, offset).inspect_err(|_err| {
            self.header.flags.set_dirty(dirty_val);
        })?;
        Ok(())
    }

    // TODO: Remove
    pub fn free_space_end(&self) -> u16 {
        self.header.free_space_end
    }

    pub fn insert_cell(
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
            self.defragment()?;
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
        to_writer(&mut pointer_writer, &cell_pointer)?;
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

    pub fn remove_cell(&mut self, cell_position: u16) {
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

    fn defragment(&mut self) -> Result<(), PageError> {
        if self.header.total_free_space == self.header.free_space_end - self.header.free_space_start
        {
            // nothing to do
            return Ok(());
        }
        self.force_defragment()
    }

    fn force_defragment(&mut self) -> Result<(), PageError> {
        let mut tmp_buffer = PageBuffer::new();
        let mut dest_end = PAGE_BUFFER_SIZE;
        for i in 0..self.header.cell_count {
            let mut pointer = self.get_cell_pointer(i);
            let cell_start = pointer.end_position - pointer.size;
            let src_range: Range<usize> = cell_start.into()..pointer.end_position.into();
            let data = &self.data.data[src_range];

            // write cell
            tmp_buffer.write_to(dest_end - pointer.size, data)?;

            // write updated pointer
            pointer.end_position = dest_end;
            let ptr_data = &to_bytes(&pointer)?;
            // TODO: make it so that the serialization goes directly to the tmp buffer instead of
            // to bytes first
            tmp_buffer.write_to(CELL_POINTER_SIZE * i, ptr_data)?;

            dest_end -= pointer.size;
        }
        self.data.write_to(0, &tmp_buffer.data[..])?;
        self.header.free_space_end = dest_end;
        self.header.flags.set_compactible(false);
        Ok(())
    }

    pub fn reset(&mut self, page_id: PageId, kind: PageKind) {
        self.header.page_id = page_id;
        self.header.page_kind = kind;
        self.header.free_space_start = 0;
        self.header.free_space_end = PAGE_BUFFER_SIZE;
        self.header.total_free_space = PAGE_BUFFER_SIZE;
        self.header.cell_count = 0;
        self.header.overflow_page_id = None;
        self.header.flags = PageFlags { flags: 0 };
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
        if cell_position == self.header.cell_count - 1 {
            // don't need to move anything
            return;
        }
        let start = ((cell_position + 1) * CELL_POINTER_SIZE) as usize;
        let end = (self.header.cell_count * CELL_POINTER_SIZE) as usize;
        let dest = (cell_position * CELL_POINTER_SIZE) as usize;
        self.data.data.copy_within(start..end, dest);
    }

    pub fn get_cell_pointer(&self, position: u16) -> CellPointer {
        assert!(position < self.header.cell_count);
        let offset_start = (position * CELL_POINTER_SIZE) as usize;
        let offset_size = CELL_POINTER_SIZE as usize;
        let mut pointer_slice = &self.data.data[offset_start..offset_start + offset_size];
        from_reader(&mut pointer_slice).unwrap()
    }

    pub fn cell_size(&self, position: u16) -> u16 {
        let ptr = self.get_cell_pointer(position);
        ptr.size
    }

    pub fn get_cell_owned(&self, cell_position: u16) -> Vec<u8> {
        let slice = self.cell_bytes(cell_position);
        let mut cell = Vec::with_capacity(slice.len());
        cell.extend_from_slice(slice);
        cell
    }

    pub fn cell_bytes(&self, cell_position: u16) -> &[u8] {
        assert!(cell_position < self.header.cell_count);
        let pointer = self.get_cell_pointer(cell_position);
        let start = (pointer.end_position - pointer.size) as usize;
        let end = pointer.end_position as usize;
        &self.data.data[start..end]
    }
}

fn checksum(data: &[u8]) -> Result<u64, SerdeError> {
    assert!(data.len() % 8 == 0);
    let mut reader = data;
    let mut sum = 0;
    let chunks = data.len() / 8;
    for _ in 0..chunks {
        let v: u64 = from_reader(&mut reader)?;
        sum += v;
    }
    Ok(sum)
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CellPointer {
    end_position: PageBufferOffset,
    size: PageBufferOffset,
}

#[cfg(test)]
mod tests {
    use std::{
        fs::{self, OpenOptions},
        mem,
    };

    use super::*;

    #[test]
    fn struct_sizes() {
        // prove all of the things we expect to be true about our memory layout
        assert_eq!(mem::size_of::<PageKind>(), 1);
        assert_eq!(mem::size_of::<Option<PageId>>(), 16);
        assert_eq!(mem::size_of::<Option<u16>>(), 4);
        assert_eq!(mem::size_of::<PageHeader>(), 40);
        assert_eq!(mem::size_of::<PageBuffer>(), PAGE_BUFFER_SIZE as usize);
        assert_eq!(mem::size_of::<Page>(), PAGE_SIZE as usize);
        assert_eq!(PAGE_BUFFER_SIZE % 8, 0);
        assert_eq!(mem::size_of::<CellPointer>(), 4);
    }

    #[test]
    fn test_checksum() {
        let mut bytes = Vec::new();
        to_writer(&mut bytes, &100u64).unwrap();
        to_writer(&mut bytes, &200u64).unwrap();
        to_writer(&mut bytes, &300u64).unwrap();
        to_writer(&mut bytes, &0u32).unwrap();
        to_writer(&mut bytes, &100u32).unwrap();

        let res = checksum(&bytes[..]).unwrap();
        assert_eq!(res, 700);
    }

    fn get_all_cells(page: &Page) -> Vec<Vec<u32>> {
        let mut read_cells: Vec<Vec<u32>> = Vec::new();
        for idx in 0..page.header.cell_count {
            let data = page.get_cell_owned(idx);
            let mut reader = &data[..];
            read_cells.push(from_reader(&mut reader).unwrap());
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
        let mut page = Page::new(1, PageKind::Heap);
        let cells = vec![
            vec![1u32, 2, 3, 4, 5],
            vec![10u32, 20, 30, 40, 50],
            vec![100u32, 200, 300, 400, 500],
        ];
        let mut buffer = Vec::new();
        let mut data_sizes = Vec::new();
        for (idx, cell) in cells.iter().enumerate() {
            to_writer(&mut buffer, &cell).unwrap();
            page.insert_cell(idx as u16, &buffer[..]).unwrap();
            data_sizes.push(buffer.len());
            buffer.clear();
        }
        let data_sizes_sum: usize = data_sizes.iter().sum();
        let free_space_end = PAGE_BUFFER_SIZE - data_sizes_sum as u16;
        let free_space_start = CELL_POINTER_SIZE * 3;
        let total_free_space = free_space_end - free_space_start;

        let read_cells = get_all_cells(&page);
        assert_eq!(cells, read_cells);
        assert_eq!(free_space_start, page.header.free_space_start);
        assert_eq!(free_space_end, page.header.free_space_end);
        assert_eq!(total_free_space, page.header.total_free_space);
        assert_eq!(3, page.header.cell_count);

        //remove
        page.remove_cell(1);
        let read_cells = get_all_cells(&page);
        let free_space_start = free_space_start - CELL_POINTER_SIZE;
        #[allow(clippy::redundant_locals)]
        let free_space_end = free_space_end; // no change
        let total_free_space = total_free_space + CELL_POINTER_SIZE + (data_sizes[1] as u16);
        assert_eq!(
            vec![vec![1u32, 2, 3, 4, 5], vec![100u32, 200, 300, 400, 500]],
            read_cells
        );
        assert_eq!(free_space_start, page.header.free_space_start);
        assert_eq!(free_space_end, page.header.free_space_end);
        assert_eq!(total_free_space, page.header.total_free_space);
        assert_eq!(2, page.header.cell_count);

        print_pointers(&page);
        println!("----------");

        // add in middle
        let mut buffer = Vec::new();

        to_writer(&mut buffer, &vec![10u32, 9, 8, 7]).unwrap();
        let middle_cell_size = buffer.len();
        page.insert_cell(2, &buffer[..]).unwrap();
        buffer.clear();
        print_pointers(&page);
        println!("----------");

        to_writer(&mut buffer, &vec![11u32, 12, 13, 14, 15]).unwrap();
        let end_cell_size = buffer.len();
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
        let free_space_start = free_space_start + (CELL_POINTER_SIZE * 2);
        let free_space_end = free_space_end - middle_cell_size as u16 - end_cell_size as u16;
        let total_free_space = total_free_space
            - (CELL_POINTER_SIZE * 2)
            - middle_cell_size as u16
            - end_cell_size as u16;
        assert_eq!(free_space_start, page.header.free_space_start);
        assert_eq!(free_space_end, page.header.free_space_end);
        assert_eq!(total_free_space, page.header.total_free_space);
        assert_eq!(4, page.header.cell_count);
    }

    #[test]
    fn page_defrag() {
        let mut page = Page::new(1, PageKind::Heap);
        let cell10s = vec![10u32, 10, 10, 10, 10];
        let cell20s = vec![20u32, 20, 20, 20, 20];
        let mut bytes10s = Vec::new();
        to_writer(&mut bytes10s, &cell10s).unwrap();
        let mut bytes20s = Vec::new();
        to_writer(&mut bytes20s, &cell20s).unwrap();
        let cell_size = bytes10s.len() as u16;

        let mut cell_count = 0;
        let mut idx = 0;
        let mut used_slots = 0;

        let has_space = |page: &Page| {
            page.header.free_space_end - page.header.free_space_start
                > (2 * (bytes10s.len() as u16 + CELL_POINTER_SIZE))
        };

        // fill up the free space in a fragmented way by:
        // 1) insert 2 cells of 20s and 10s respectively, then remove the one with 20s leaving some
        //    unused space
        // 2) when there's not enough free space left for 2 cells, add 1 (with 10s)
        // 3) Add one more with 10s to trigger defrag
        // 4) Check that all read cells contain 10s and other properties are correct
        while has_space(&page) {
            page.insert_cell(idx, &bytes20s[..]).unwrap();
            page.insert_cell(idx + 1, &bytes10s[..]).unwrap();
            page.remove_cell(idx);
            idx += 1;
            cell_count += 1;
            used_slots += 2;
        }
        let read_cells = get_all_cells(&page);
        for c in read_cells {
            assert_eq!(cell10s, c);
        }
        let free_space_start = cell_count * CELL_POINTER_SIZE;
        let free_space_end = PAGE_BUFFER_SIZE - (used_slots * cell_size);
        let total_free_space = PAGE_BUFFER_SIZE - free_space_start - (cell_count * cell_size);
        assert_eq!(free_space_start, page.header.free_space_start);
        assert_eq!(free_space_end, page.header.free_space_end);
        assert_eq!(total_free_space, page.header.total_free_space);
        assert_eq!(cell_count, page.header.cell_count);
        assert!(page.header.flags.is_dirty());
        assert!(page.header.flags.is_compactible());

        // add two more to trigger defrag
        page.insert_cell(idx, &bytes10s[..]).unwrap();
        page.insert_cell(idx + 1, &bytes20s[..]).unwrap();
        cell_count += 2;
        let read_cells = get_all_cells(&page);
        for c in &read_cells[0..=idx as usize] {
            assert_eq!(cell10s, *c);
        }
        assert_eq!(read_cells[(idx + 1) as usize], cell20s);
        let free_space_start = free_space_start + CELL_POINTER_SIZE + CELL_POINTER_SIZE;
        let free_space_end = PAGE_BUFFER_SIZE - (cell_count * cell_size);
        let total_free_space = free_space_end - free_space_start;
        assert_eq!(free_space_start, page.header.free_space_start);
        assert_eq!(free_space_end, page.header.free_space_end);
        assert_eq!(total_free_space, page.header.total_free_space);
        assert_eq!(cell_count, page.header.cell_count);
        assert!(page.header.flags.is_dirty());
        assert!(!page.header.flags.is_compactible());
    }

    #[test]
    fn defrag_with_non_insertion_order_pointers_no_gaps() {
        let mut page = Page::new(1, PageKind::Heap);
        let cell0 = vec![10u32, 10, 10, 10, 10];
        let cell2 = vec![20u32, 20, 20];
        let cell1 = vec![30u32, 30, 30, 30];

        let mut bytes = Vec::new();
        to_writer(&mut bytes, &cell0).unwrap();
        page.insert_cell(0, &bytes).unwrap();
        bytes.clear();
        print_pointers(&page);
        println!("--------------");

        to_writer(&mut bytes, &cell2).unwrap();
        page.insert_cell(1, &bytes).unwrap();
        bytes.clear();
        print_pointers(&page);
        println!("--------------");

        to_writer(&mut bytes, &cell1).unwrap();
        page.insert_cell(1, &bytes).unwrap();
        bytes.clear();
        print_pointers(&page);
        println!("--------------");

        let undefragged_cells = get_all_cells(&page);
        page.force_defragment().unwrap(); // should have no effect, since there's no space to remove
        print_pointers(&page);
        let defragged_cells = get_all_cells(&page);
        let expected = vec![cell0, cell1, cell2];
        assert_eq!(expected, undefragged_cells);
        assert_eq!(expected, defragged_cells);
    }

    #[test]
    fn defrag_with_non_insertion_order_pointers_with_gaps() {
        let mut page = Page::new(1, PageKind::Heap);
        let cell0 = vec![10u32, 10, 10, 10, 10];
        let cell_deleted = vec![1u32, 2];
        let cell2 = vec![20u32, 20, 20];
        let cell1 = vec![30u32, 30, 30, 30];

        let mut bytes = Vec::new();
        to_writer(&mut bytes, &cell0).unwrap();
        page.insert_cell(0, &bytes).unwrap();
        bytes.clear();
        print_pointers(&page);
        println!("--------------");

        to_writer(&mut bytes, &cell_deleted).unwrap();
        page.insert_cell(1, &bytes).unwrap();
        bytes.clear();
        print_pointers(&page);
        println!("--------------");

        to_writer(&mut bytes, &cell2).unwrap();
        page.insert_cell(2, &bytes).unwrap();
        bytes.clear();
        print_pointers(&page);
        println!("--------------");

        page.remove_cell(1);

        to_writer(&mut bytes, &cell1).unwrap();
        page.insert_cell(1, &bytes).unwrap();
        bytes.clear();
        print_pointers(&page);
        println!("--------------");

        let undefragged_cells = get_all_cells(&page);
        page.force_defragment().unwrap(); // should have no effect, since there's no space to remove
        print_pointers(&page);
        let defragged_cells = get_all_cells(&page);
        let expected = vec![cell0, cell1, cell2];
        assert_eq!(expected, undefragged_cells);
        assert_eq!(expected, defragged_cells);
    }

    #[test]
    fn test_replace_contents() {
        let filename = "replace_contents.test";
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(filename)
            .unwrap();

        // add cells
        let mut page = Page::new(0, PageKind::Heap);
        let cells = vec![
            vec![1u32, 2, 3, 4, 5],
            vec![10u32, 20, 30, 40, 50],
            vec![100u32, 200, 300, 400, 500],
        ];
        let mut buffer = Vec::new();
        let mut data_sizes = Vec::new();
        for (idx, cell) in cells.iter().enumerate() {
            to_writer(&mut buffer, &cell).unwrap();
            page.insert_cell(idx as u16, &buffer[..]).unwrap();
            data_sizes.push(buffer.len());
            buffer.clear();
        }

        page.write_to_disk(&mut file).unwrap();

        let mut replaced_page = Page::new(42, PageKind::Heap);
        replaced_page.replace_contents(&file, 0).unwrap();
        let read_cells = get_all_cells(&replaced_page);
        assert_eq!(cells, read_cells);
        assert_eq!(page, replaced_page);

        drop(file);
        fs::remove_file(filename).unwrap();
    }

    #[test]
    fn to_from_disk_basics() {
        let filename = "to_from_disk_basics.test";
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(filename)
            .unwrap();

        // add cells
        let mut page = Page::new(0, PageKind::Heap);
        let cells = vec![
            vec![1u32, 2, 3, 4, 5],
            vec![10u32, 20, 30, 40, 50],
            vec![100u32, 200, 300, 400, 500],
        ];
        let mut buffer = Vec::new();
        let mut data_sizes = Vec::new();
        for (idx, cell) in cells.iter().enumerate() {
            to_writer(&mut buffer, &cell).unwrap();
            page.insert_cell(idx as u16, &buffer[..]).unwrap();
            data_sizes.push(buffer.len());
            buffer.clear();
        }

        page.write_to_disk(&mut file).unwrap();

        let read_page = Page::from_disk(&file, 0).unwrap();
        let read_cells = get_all_cells(&read_page);
        assert_eq!(cells, read_cells);
        assert_eq!(page, read_page);

        drop(file);
        fs::remove_file(filename).unwrap();
    }

    #[test]
    fn to_from_disk_multiple_pages() {
        let filename = "to_from_disk_multiple_pages.test";
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(filename)
            .unwrap();

        let mut page0 = Page::new(0, PageKind::Heap);
        let cells0 = vec![vec![10, 11, 12, 13]];
        let mut buffer = Vec::new();
        for (idx, cell) in cells0.iter().enumerate() {
            to_writer(&mut buffer, &cell).unwrap();
            page0.insert_cell(idx as u16, &buffer[..]).unwrap();
            buffer.clear();
        }
        page0.write_to_disk(&mut file).unwrap();

        let mut page1 = Page::new(1, PageKind::Heap);
        let cells1 = vec![vec![20, 21, 22, 23]];
        let mut buffer = Vec::new();
        for (idx, cell) in cells1.iter().enumerate() {
            to_writer(&mut buffer, &cell).unwrap();
            page1.insert_cell(idx as u16, &buffer[..]).unwrap();
            buffer.clear();
        }
        page1.write_to_disk(&mut file).unwrap();

        let mut page2 = Page::new(2, PageKind::Heap);
        let cells2 = vec![vec![30, 31, 32, 33]];
        let mut buffer = Vec::new();
        for (idx, cell) in cells2.iter().enumerate() {
            to_writer(&mut buffer, &cell).unwrap();
            page2.insert_cell(idx as u16, &buffer[..]).unwrap();
            buffer.clear();
        }
        page2.write_to_disk(&mut file).unwrap();

        // read out of order now
        let read_page1 = Page::from_disk(&file, 1).unwrap();
        assert_eq!(page1, read_page1);
        assert_eq!(cells1, get_all_cells(&read_page1));

        let read_page2 = Page::from_disk(&file, 2).unwrap();
        assert_eq!(page2, read_page2);
        assert_eq!(cells2, get_all_cells(&read_page2));

        let read_page0 = Page::from_disk(&file, 0).unwrap();
        assert_eq!(page0, read_page0);
        assert_eq!(cells0, get_all_cells(&read_page0));

        drop(file);
        fs::remove_file(filename).unwrap();
    }
}
