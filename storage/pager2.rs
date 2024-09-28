use std::{cell::RefCell, mem, u8};

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
* TBD:
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

// TODO: Convert to 16kb
const PAGE_DATA_SIZE: usize = 4096; // 4KB
const PAGE_BUFFER_SIZE: usize = PAGE_DATA_SIZE - mem::size_of::<PageHeader>();

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

type PageId = u64;

#[repr(u8)]
enum PageKind {
    Data,
}

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
    checksum: u64,                    // 8, 8
    header_version: u8,               //1, 9
    flags: PageFlags,                 //1, 10
    cell_count: u8,                   //1, 11
    page_kind: PageKind,              // 1, 12
    alignment_guard: u32,             //4, 16
    free_space_start: u16,            // 2, 18,
    free_space_end: u16,              // 2, 24
    page_id: PageId,                  // 8, 32
    overflow_page_id: Option<PageId>, // 16, 48
}

#[repr(C)]
struct PageBuffer {
    data: [u8; PAGE_BUFFER_SIZE],
}

#[repr(C)]
struct Page {
    header: PageHeader,
    data: PageBuffer,
}

#[cfg(test)]
mod tests {
    use std::mem;

    use super::{Page, PageBuffer, PageHeader, PageId, PageKind, PAGE_BUFFER_SIZE, PAGE_DATA_SIZE};

    #[test]
    fn struct_sizes() {
        assert_eq!(mem::size_of::<PageKind>(), 1);
        assert_eq!(mem::size_of::<Option<PageId>>(), 16);
        assert_eq!(mem::size_of::<PageHeader>(), 48);
        assert_eq!(mem::size_of::<PageBuffer>(), PAGE_BUFFER_SIZE);
        assert_eq!(mem::size_of::<Page>(), PAGE_DATA_SIZE);
    }
}
