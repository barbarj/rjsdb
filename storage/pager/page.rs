use core::slice;
use std::{
    io::{Error as IoError, Write},
    mem,
    num::NonZeroU64,
    ops::Range,
    os::fd::AsFd,
};

use rustix::io::{pread, pwrite, retry_on_intr, Errno as RustixErrno};

use crate::serialize::{Deserialize, SerdeError, Serialize};

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

const PAGE_SIZE: PageBufferOffset = 4096 * 4; // 16KB
const PAGE_BUFFER_SIZE: PageBufferOffset =
    PAGE_SIZE - mem::size_of::<PageHeader>() as PageBufferOffset;
const HEADER_VERSION: u8 = 1;
// the byte values spell PAGE
const ALIGNMENT_GUARD_VALUE: u32 = u32::from_be_bytes([50, 41, 47, 45]);
const CELL_POINTER_SIZE: u16 = mem::size_of::<CellPointer>() as u16;

#[derive(Debug)]
pub enum PageError {
    IoError(IoError),
    SerdeError(SerdeError),
    NotEnoughSpace,
    RustixError(RustixErrno),
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
impl From<RustixErrno> for PageError {
    fn from(value: RustixErrno) -> Self {
        Self::RustixError(value)
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
#[derive(Debug, PartialEq)]
pub enum PageKind {
    Data,
}

// TODO: Add CRC check in addition to the checksum

/// NOTE: Changing this in any way means that all existing on-disk data will not be read correctly.
///     All changes will requiring incrementing the header_version and maintaining the old & new
///     page layouts while pages with the old layout still exist.
///
///     We must also be careful to lay this out in a way that doesn't have any unitialized memory.
///     This is why we use Option<NonZeroU64> instead of Option<u64> for overflow_page_id. Option<NonZeroU64>
///     can use a compiler optimization to store the None state as 0, so the enum tag doesn't need
///     to be stored seperately, meaning we don't need the padding that would require.
///
/// checksum and header_version must be at the beginning of every version of this struct so that:
/// - The checksum can be validated before continuing
/// - the header_version can be read so that we know what struct version to use
///
/// Our max page size will be 16KB, so free_space_start and free_space_end only need to be u16. If
/// the page size increases, those fields will need to use larger types
#[repr(C)]
#[derive(Debug, PartialEq)]
struct PageHeader {
    // comments: size, end-of-this-field-with-padding in layout
    checksum: u64,      // 8, 8
    header_version: u8, // 1, 9
    flags: PageFlags,   // 1, 10
    //  (included so that this memory is initialized instead of being padding)
    _padding1: u8,                        // 1, 11
    page_kind: PageKind,                  // 1, 12
    alignment_guard: u32,                 // 4, 16
    page_id: PageId,                      // 8, 24
    overflow_page_id: Option<NonZeroU64>, // 8, 32
    cell_count: u16,                      // 2, 34
    free_space_start: PageBufferOffset,   // 2, 36
    free_space_end: PageBufferOffset,     // 2, 38
    total_free_space: PageBufferOffset,   // 2, 40
}

#[repr(C)]
#[derive(Debug, PartialEq)]
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
        &mut self.data[offset..offset + data_size]
    }
}

#[repr(C)]
#[derive(Debug, PartialEq)]
pub struct Page {
    header: PageHeader,
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

        let data = PageBuffer {
            data: [0; PAGE_BUFFER_SIZE as usize],
        };

        Page { header, data }
    }

    fn calc_checksum(&self) -> u64 {
        let bytes = self.as_slice();
        checksum(&bytes[8..]).unwrap()
    }

    pub fn from_disk<Fd: AsFd>(fd: Fd, page_id: PageId) -> Result<Self, PageError> {
        let mut new_page = Page::new(0, PageKind::Data);
        let buf = new_page.as_slice_mut();
        let offset = page_id * PAGE_SIZE as u64;
        // make read all
        Page::read_entire_page(fd, buf, offset)?;

        // new page should now have values from disk
        let checksum = new_page.calc_checksum();
        if checksum != new_page.header.checksum {
            return Err(PageError::Corrupted);
        }
        Ok(new_page)
    }

    fn as_slice(&self) -> &[u8] {
        unsafe { slice::from_raw_parts(self as *const Self as *const u8, PAGE_SIZE.into()) }
    }

    fn as_slice_mut(&mut self) -> &mut [u8] {
        unsafe { slice::from_raw_parts_mut(self as *mut Self as *mut u8, PAGE_SIZE.into()) }
    }

    fn read_entire_page<Fd: AsFd>(fd: Fd, buf: &mut [u8], offset: u64) -> Result<(), RustixErrno> {
        assert!(buf.len() == PAGE_SIZE as usize);
        let mut buf = buf;
        let mut offset = offset;
        let mut bytes_left = PAGE_SIZE as usize;
        while bytes_left > 0 {
            let bytes_read = retry_on_intr(|| pread(fd.as_fd(), buf, offset))?;
            bytes_left -= bytes_read;
            buf = &mut buf[bytes_read..];
            offset += bytes_read as u64;
        }
        Ok(())
    }

    fn write_entire_page<Fd: AsFd>(fd: Fd, buf: &[u8], offset: u64) -> Result<(), RustixErrno> {
        assert!(buf.len() == PAGE_SIZE as usize);
        let mut buf = buf;
        let mut offset = offset;
        let mut bytes_left = PAGE_SIZE as usize;
        while bytes_left > 0 {
            let bytes_written = retry_on_intr(|| pwrite(fd.as_fd(), buf, offset))?;
            bytes_left -= bytes_written;
            buf = &buf[bytes_written..];
            offset += bytes_written as u64;
        }
        Ok(())
    }

    pub fn write_to_disk<Fd: AsFd>(&mut self, fd: Fd) -> Result<(), PageError> {
        let offset = self.header.page_id * PAGE_SIZE as u64;
        // setting dirty flag before slice cast and write to:
        // 1: Make the effects on other vars easier to reason about.
        // 2: By definition the page on disk should be considered clean
        let dirty_val = self.header.flags.is_dirty();
        self.header.flags.set_dirty(false);
        self.header.checksum = self.calc_checksum();
        let buf = self.as_slice();
        Page::write_entire_page(fd, buf, offset).inspect_err(|_err| {
            self.header.flags.set_dirty(dirty_val);
        })?;
        Ok(())
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

    pub fn get_cell(&self, cell_position: u16) -> Vec<u8> {
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
    let chunks = data.len() / 8;
    for _ in 0..chunks {
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
    use std::{fs::File, mem, path::Path};

    use rustix::fs::{Mode, OFlags};

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
        let mut data_sizes = Vec::new();
        for (idx, cell) in cells.iter().enumerate() {
            cell.write_to_bytes(&mut buffer).unwrap();
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
        vec![10u32, 9, 8, 7].write_to_bytes(&mut buffer).unwrap();
        let middle_cell_size = buffer.len();
        page.insert_cell(2, &buffer[..]).unwrap();
        buffer.clear();
        print_pointers(&page);
        println!("----------");

        vec![11u32, 12, 13, 14, 15]
            .write_to_bytes(&mut buffer)
            .unwrap();
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
        let mut page = Page::new(1, PageKind::Data);
        let cell = vec![10u32, 10, 10, 10, 10];
        let mut bytes = Vec::new();
        cell.write_to_bytes(&mut bytes).unwrap();
        let cell_size = bytes.len() as u16;

        let mut cell_count = 0;
        let mut idx = 0;
        let mut used_slots = 0;

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
            used_slots += 2;
        }
        let read_cells = get_all_cells(&page);
        for c in read_cells {
            assert_eq!(cell, c);
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

        // add one more to trigger defrag
        page.insert_cell(idx, &bytes[..]).unwrap();
        cell_count += 1;
        let read_cells = get_all_cells(&page);
        for c in read_cells {
            assert_eq!(cell, c);
        }
        let free_space_start = free_space_start + CELL_POINTER_SIZE;
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
    fn to_from_disk_basics() {
        let fd = rustix::fs::open(
            Path::new("to_from_disk_basics.test"),
            OFlags::CREATE | OFlags::TRUNC | OFlags::RDWR,
            Mode::RWXU,
        )
        .unwrap();

        // add cells
        let mut page = Page::new(0, PageKind::Data);
        let cells = vec![
            vec![1u32, 2, 3, 4, 5],
            vec![10u32, 20, 30, 40, 50],
            vec![100u32, 200, 300, 400, 500],
        ];
        let mut buffer = Vec::new();
        let mut data_sizes = Vec::new();
        for (idx, cell) in cells.iter().enumerate() {
            cell.write_to_bytes(&mut buffer).unwrap();
            page.insert_cell(idx as u16, &buffer[..]).unwrap();
            data_sizes.push(buffer.len());
            buffer.clear();
        }

        page.write_to_disk(fd.as_fd()).unwrap();

        let read_page = Page::from_disk(fd.as_fd(), 0).unwrap();
        assert_eq!(page, read_page);
    }
}
