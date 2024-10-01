use std::{
    cell,
    collections::{hash_map::Entry, HashMap},
    io::{Error as IoError, Write},
    mem,
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
const ALIGNMENT_GUARD_VALUE: u32 = 0; // TODO: Make this the int value of bytes 'P', 'A', 'G', 'E'
                                      //

enum PageError {
    Io(IoError),
    NotEnoughSpace,
}
impl From<IoError> for PageError {
    fn from(value: IoError) -> Self {
        Self::Io(value)
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
    checksum: u64,                         // 8, 8
    header_version: u8,                    // 1, 9
    flags: PageFlags,                      // 1, 10
    page_kind: PageKind,                   // 1, 12
    alignment_guard: u32,                  // 4, 16
    page_id: PageId,                       // 8, 24
    overflow_page_id: Option<PageId>,      // 16, 40
    cell_count: u16,                       // 2, 42
    right_most_free_position: Option<u16>, // 4, 46
    free_space_start: PageBufferOffset,    // 2, 48
    free_space_end: PageBufferOffset,      // 2, 56
}

#[repr(C)]
struct PageBuffer {
    data: [u8; PAGE_BUFFER_SIZE as usize],
}
impl PageBuffer {
    fn write_to(&mut self, offset: PageBufferOffset, data: &[u8]) -> Result<(), PageError> {
        let offset = offset as usize;
        let mut writer = &mut self.data[offset..offset + data.len()];
        writer.write_all(data)?;
        Ok(())
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
            checksum: unimplemented!(),
            header_version: HEADER_VERSION,
            flags: PageFlags { flags: 0 },
            cell_count: 0,
            right_most_free_position: None,
            page_kind: kind,
            alignment_guard: ALIGNMENT_GUARD_VALUE,
            free_space_start: 0,
            free_space_end: PAGE_BUFFER_SIZE,
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

enum CellPointerDecision {
    // use_idx, range_end_inclusive_idx
    UseExistingSlotWithMove(u16, u16),
    // use_idx
    UseExistingSlotNoMove(u16),
    UseNewSlot,
}

struct AvailableSpaceRef<'a> {
    list: &'a mut Vec<AvailableSpace>,
    index: usize,
}
impl<'a> AvailableSpaceRef<'a> {
    fn take(self) -> AvailableSpace {
        self.list.swap_remove(self.index)
    }

    fn space(&self) -> &AvailableSpace {
        self.list.get(self.index).unwrap()
    }
}

enum CellDecision<'a> {
    UseExistingSlot(AvailableSpaceRef<'a>),
    UseNewSlot,
}

impl Page {
    fn insert_cell(
        &mut self,
        cell_position: u16, // must be <= cell count
        data: &[u8],
        availability_list: &mut AvailabilityList,
    ) -> Result<u8, PageError> {
        let data_size: PageBufferOffset = data.len().try_into().unwrap();

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

        // verify we have room for the cell pointer
        let cell_pointer_size: u16 = mem::size_of::<CellPointer>().try_into().unwrap();
        let pointer_decision = match self.header.right_most_free_position {
            Some(rightmost) if rightmost > cell_position => {
                // TODO: Replace use of rightmost free position with the minimum free slot in
                // (cell_position, rightmost]
                CellPointerDecision::UseExistingSlotWithMove(cell_position, rightmost)
            }
            Some(rightmost) if rightmost == cell_position => {
                CellPointerDecision::UseExistingSlotNoMove(cell_position)
            }
            _ => {
                if self.header.free_space_end - self.header.free_space_start < cell_pointer_size {
                    return Err(PageError::NotEnoughSpace);
                }
                CellPointerDecision::UseNewSlot
            }
        };
        let cell_decision = match availability_list.get_best_fit(self.header.page_id, data_size) {
            Some(space_ref) => CellDecision::UseExistingSlot(space_ref),
            None => {
                let mut required = data_size;
                match pointer_decision {
                    CellPointerDecision::UseExistingSlotWithMove(_, _)
                    | CellPointerDecision::UseNewSlot => {
                        required += cell_pointer_size;
                    }
                    _ => {}
                }
                if self.header.free_space_end - self.header.free_space_start < required {
                    return Err(PageError::NotEnoughSpace);
                }
                CellDecision::UseNewSlot
            }
        };

        // update cell pointers
        // - move cells at and to the right of cell_position over 1.
        // - write new cell pointer
        // - update free_space_start
        unimplemented!();
    }

    fn remove_cell(&mut self, cell_position: u8, availability_list: &mut AvailabilityList) {
        unimplemented!();
    }
}

#[derive(Debug, PartialEq)]
struct AvailableSpace {
    end_position: PageBufferOffset,
    size: PageBufferOffset,
}
impl AvailableSpace {
    fn new(end_position: PageBufferOffset, size: PageBufferOffset) -> Self {
        AvailableSpace { end_position, size }
    }
}
struct AvailabilityList {
    // TODO: This is probably better represented as a heap instead of a vec, since we'll be doing a
    // best_fit search of the list often (or at least use a deque or something)
    available_space: HashMap<PageId, Vec<AvailableSpace>>,
}
impl AvailabilityList {
    fn new() -> Self {
        AvailabilityList {
            available_space: HashMap::new(),
        }
    }

    fn add_space(&mut self, page_id: PageId, space: AvailableSpace) {
        let list = match self.available_space.entry(page_id) {
            Entry::Vacant(v) => v.insert(Vec::new()),
            Entry::Occupied(o) => o.into_mut(),
        };
        list.push(space);
    }

    /// # Panics:
    /// - if called for a page not in the availability list, as this likely represents a bug
    /// - if the page is in the list, but there is no entry with the provided end position
    fn remove_space(&mut self, page_id: PageId, space_end_position: PageBufferOffset) {
        let list = match self.available_space.entry(page_id) {
            Entry::Vacant(_) => {
                panic!("Called remove_space for a page id not in this AvailabilityList")
            }
            Entry::Occupied(o) => o.into_mut(),
        };

        let (index_to_remove, _) = list
            .iter()
            .enumerate()
            .find(|(_idx, i)| i.end_position == space_end_position)
            .expect("Tried to remove an entry that does not exist.");
        list.swap_remove(index_to_remove);
    }

    /// Searches the list for this page for the available space with the best fit given the
    /// provided size. If found, removes that entry from the list and returns Some(AvailableSpace),
    /// otherwise returns none
    fn get_best_fit<'a>(
        &'a mut self,
        page_id: PageId,
        size: PageBufferOffset,
    ) -> Option<AvailableSpaceRef<'a>> {
        let list: &'a mut Vec<AvailableSpace> = match self.available_space.entry(page_id) {
            Entry::Vacant(_) => return None,
            Entry::Occupied(o) => o.into_mut(),
        };

        let max = list
            .iter()
            .enumerate()
            .filter(|(_, space)| space.size >= size)
            .min_by_key(|(_, space)| space.size);
        if max.is_none() {
            None
        } else {
            let index = max.unwrap().0;
            Some(AvailableSpaceRef { list, index })
        }
    }

    fn drop_page(&mut self, page_id: PageId) {
        self.available_space.remove(&page_id);
    }

    /// This is meant for adding an entire page efficiently, i.e. all at once. If you need to only
    /// add an entry or two, use add_space
    fn add_page(&mut self, page: &Page) {
        assert!(
            !self.available_space.contains_key(&page.header.page_id),
            "Called add_page for a page already in the availability list"
        );
        // TODO: Implement after I've sorted out the page implementation
        unimplemented!()
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
        assert_eq!(mem::size_of::<PageHeader>(), 56);
        assert_eq!(mem::size_of::<PageBuffer>(), PAGE_BUFFER_SIZE as usize);
        assert_eq!(mem::size_of::<Page>(), PAGE_DATA_SIZE as usize);
        assert_eq!(PAGE_BUFFER_SIZE % 8, 0);
        assert_eq!(mem::size_of::<CellPointer>(), 4);
    }

    #[test]
    fn availability_list_single_page() {
        let mut list = AvailabilityList::new();
        list.add_space(1, AvailableSpace::new(100, 10));
        list.add_space(1, AvailableSpace::new(80, 10));
        list.add_space(1, AvailableSpace::new(60, 20));
        assert_eq!(
            &vec![
                AvailableSpace::new(100, 10),
                AvailableSpace::new(80, 10),
                AvailableSpace::new(60, 20)
            ],
            list.available_space.get(&1).unwrap()
        );

        list.remove_space(1, 80);
        assert_eq!(
            &vec![AvailableSpace::new(100, 10), AvailableSpace::new(60, 20)],
            list.available_space.get(&1).unwrap()
        );
        list.remove_space(1, 100);
        assert_eq!(
            &vec![AvailableSpace::new(60, 20)],
            list.available_space.get(&1).unwrap()
        );
        list.drop_page(1);
        assert!(!list.available_space.contains_key(&1));
    }

    #[test]
    fn availability_list_multiple_pages() {
        let mut list = AvailabilityList::new();
        list.add_space(1, AvailableSpace::new(100, 10));
        list.add_space(1, AvailableSpace::new(180, 10));
        list.add_space(1, AvailableSpace::new(160, 20));
        assert_eq!(
            &vec![
                AvailableSpace::new(100, 10),
                AvailableSpace::new(180, 10),
                AvailableSpace::new(160, 20)
            ],
            list.available_space.get(&1).unwrap()
        );
        list.add_space(2, AvailableSpace::new(200, 10));
        list.add_space(2, AvailableSpace::new(280, 10));
        list.add_space(3, AvailableSpace::new(360, 20));
        assert_eq!(
            &vec![
                AvailableSpace::new(100, 10),
                AvailableSpace::new(180, 10),
                AvailableSpace::new(160, 20)
            ],
            list.available_space.get(&1).unwrap()
        );
        assert_eq!(
            &vec![AvailableSpace::new(200, 10), AvailableSpace::new(280, 10),],
            list.available_space.get(&2).unwrap()
        );
        assert_eq!(
            &vec![AvailableSpace::new(360, 20),],
            list.available_space.get(&3).unwrap()
        );

        list.remove_space(1, 180);
        assert_eq!(
            &vec![AvailableSpace::new(100, 10), AvailableSpace::new(160, 20)],
            list.available_space.get(&1).unwrap()
        );
        assert_eq!(
            &vec![AvailableSpace::new(200, 10), AvailableSpace::new(280, 10),],
            list.available_space.get(&2).unwrap()
        );
        assert_eq!(
            &vec![AvailableSpace::new(360, 20),],
            list.available_space.get(&3).unwrap()
        );

        list.remove_space(2, 200);
        assert_eq!(
            &vec![AvailableSpace::new(100, 10), AvailableSpace::new(160, 20)],
            list.available_space.get(&1).unwrap()
        );
        assert_eq!(
            &vec![AvailableSpace::new(280, 10),],
            list.available_space.get(&2).unwrap()
        );
        assert_eq!(
            &vec![AvailableSpace::new(360, 20),],
            list.available_space.get(&3).unwrap()
        );

        list.drop_page(1);
        assert!(!list.available_space.contains_key(&1));
        assert_eq!(
            &vec![AvailableSpace::new(280, 10),],
            list.available_space.get(&2).unwrap()
        );
        assert_eq!(
            &vec![AvailableSpace::new(360, 20),],
            list.available_space.get(&3).unwrap()
        );
    }

    #[test]
    fn availability_list_take_best_fit() {
        let mut list = AvailabilityList::new();
        list.add_space(1, AvailableSpace::new(200, 50));
        list.add_space(1, AvailableSpace::new(150, 40));
        list.add_space(1, AvailableSpace::new(110, 30));
        assert_eq!(
            &vec![
                AvailableSpace::new(200, 50),
                AvailableSpace::new(150, 40),
                AvailableSpace::new(110, 30)
            ],
            list.available_space.get(&1).unwrap()
        );

        // prove get_best fit does not modify list until taken
        let taken = list.get_best_fit(1, 39);
        assert_eq!(
            Some(&AvailableSpace::new(150, 40)),
            taken.as_ref().map(|x| x.space())
        );
        assert_eq!(
            &vec![
                AvailableSpace::new(200, 50),
                AvailableSpace::new(150, 40),
                AvailableSpace::new(110, 30)
            ],
            list.available_space.get(&1).unwrap()
        );
        let taken = list.get_best_fit(1, 39).unwrap().take();
        assert_eq!(AvailableSpace::new(150, 40), taken);
        assert_eq!(
            &vec![AvailableSpace::new(200, 50), AvailableSpace::new(110, 30)],
            list.available_space.get(&1).unwrap()
        );

        // no fit
        let taken = list.get_best_fit(1, 75);
        assert!(taken.is_none());
        assert_eq!(
            &vec![AvailableSpace::new(200, 50), AvailableSpace::new(110, 30)],
            list.available_space.get(&1).unwrap()
        );

        let taken = list.get_best_fit(1, 30).unwrap().take();
        assert_eq!(AvailableSpace::new(110, 30), taken);
        assert_eq!(
            &vec![AvailableSpace::new(200, 50)],
            list.available_space.get(&1).unwrap()
        );
    }
}
