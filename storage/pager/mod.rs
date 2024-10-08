#![allow(dead_code)]

mod page;

use std::collections::HashMap;

use page::{PageError, PageKind, PAGE_SIZE};
use rustix::{fd::AsFd, fd::AsRawFd, fd::BorrowedFd, io::Errno};

pub type PageId = page::PageId;
pub type Page = page::Page;

/*
 * Pager Requirements:
 * - Stores up to MAX_PAGE_COUNT pages in memory
 * - MAX_PAGER_MEMORY is divisible by PAGE_SIZE
 * - The memory use should be basically constant
 *      - Can't use an array because if the total number of pages in the db is less than
 *      MAX_PAGE_COUNT, what do we do with the rest of the slots?
 *      - Need to basically build a fixed-capicity vec instead
 * - Ability to get a mutable or immutable reference to a page
 * - When page count gets to MAX_PAGE_COUNT and a page not in the cache is requested, another page
 * is evicted (evicted page to be chosen by some yet-to-be-determined cache-eviction algorithm)
 *   - Means we need to wrap borrowed pages in some other struct that limits the operations you can
 *   perform on them
 *   - Pager will expose a "flush cache" operation that flushes all dirty pages
 * - Finding a page should be fast
 *   - Ideally, we can keep a sorted list of page ids in the buffer, and binary search it, giving
 *   us log2(MAX_PAGE_COUNT) page search time.
 * - Pager holds pages from all tables.
 *   - Means we need to associate page ids with file descriptors
 * - Pager should generate page id for new pages:
 *   - probably determine the next id for each fd at startup based on file size, then track that
 *   while running.
 *
 * - To avoid unecessary allocations, we should probably treat this like a page pool, so
 * unused/evicted pages are still in memory, but marked as free. We can add an method to Page that
 * replaces its contents with that of something from disk.
 *
 * - For now, I'm planning on handling the WAL in a seperate buffer
 *
 * Performance Requirements, put differently:
 * - We want to minimize page search time, page eviction time, page load time
 * - Store pages in vec, never move them
 *   - unused locations get stored in some stack
 *      - when a new page is needed, just use the first location on the stack.
 *   - keep track of (fd, page_id) -> index relationships in an array sorted on page id?
 *
 */

const MAX_PAGER_MEMORY: usize = 1024 * 1000 * 20; // 20 MB
const MAX_PAGE_COUNT: usize = MAX_PAGER_MEMORY / PAGE_SIZE as usize;

#[derive(Debug)]
pub enum PagerError {
    IoError(Errno),
    PageError(PageError),
}
impl From<Errno> for PagerError {
    fn from(value: Errno) -> Self {
        Self::IoError(value)
    }
}
impl From<PageError> for PagerError {
    fn from(value: PageError) -> Self {
        Self::PageError(value)
    }
}

type RawFd = i32;
type PageLookupKey = (RawFd, PageId);

struct NextPageId {
    raw_fd: RawFd,
    next_id: PageId,
}
impl NextPageId {
    fn new(raw_fd: RawFd, next_id: PageId) -> Self {
        NextPageId { raw_fd, next_id }
    }

    fn matches_fd<Fd: AsRawFd>(&self, fd: Fd) -> bool {
        self.raw_fd == fd.as_raw_fd()
    }

    fn use_id(&mut self) -> PageId {
        let next_id = self.next_id;
        self.next_id += 1;
        next_id
    }
}

struct ClockCacheHandler {
    hand: usize,
    use_bits: Vec<u8>,
}
impl ClockCacheHandler {
    fn new(page_count: usize) -> Self {
        let size = if page_count % 8 == 0 {
            page_count / 8
        } else {
            (page_count / 8) + 1
        };
        let use_bits = vec![0; size];
        ClockCacheHandler { use_bits, hand: 0 }
    }

    fn get_use_bit(&self, location: usize) -> bool {
        let byte = location / 8;
        let bit = 1 << (7 - (location % 8));
        self.use_bits[byte] & bit > 0
    }

    fn set_use_bit(&mut self, location: usize) {
        let byte = location / 8;
        let bit = 1 << (7 - (location % 8));
        self.use_bits[byte] |= bit;
    }

    fn unset_use_bit(&mut self, location: usize) {
        let byte = location / 8;
        let bit = 1 << (7 - (location % 8));
        let mask = u8::MAX ^ bit;
        self.use_bits[byte] &= mask;
    }

    fn advance_to_next_evictable_location(&mut self) -> usize {
        // advance until we get to a zero bit
        while self.get_use_bit(self.hand) {
            // set 1 bit to 0
            self.unset_use_bit(self.hand);
            // advance hand. Wrap to 0 if necessary
            self.hand += 1;
            if self.hand == self.use_bits.len() * 8 {
                self.hand = 0;
            }
        }
        // now at a zero bit, this is our eviction candidate
        self.hand
    }
}

/*
 * TODO: Put proper documentation of how this works here
 */
// TODO: Try to get a safer way of knowing which file a page id is associated with than using raw
// fds
pub struct Pager {
    pages: Vec<Page>,
    page_locations: HashMap<PageLookupKey, usize>,
    location_fd_mapping: HashMap<usize, RawFd>,
    next_page_ids: Vec<NextPageId>,
    clock_cache: ClockCacheHandler,
}
impl Pager {
    fn new(table_fds: &[BorrowedFd]) -> Self {
        Self::with_page_count(table_fds, MAX_PAGE_COUNT)
    }

    fn with_page_count(table_fds: &[BorrowedFd], page_count: usize) -> Self {
        Pager {
            pages: (0..page_count)
                .map(|_| Page::new(0, PageKind::Data))
                .collect(),
            page_locations: HashMap::with_capacity(page_count),
            location_fd_mapping: HashMap::with_capacity(page_count),
            next_page_ids: table_fds
                .iter()
                .map(|fd| {
                    let next_id = Pager::calc_page_count(fd.as_fd()).unwrap();
                    NextPageId::new(fd.as_raw_fd(), next_id)
                })
                .collect(),
            clock_cache: ClockCacheHandler::new(page_count),
        }
    }

    fn calc_page_count<Fd: AsFd>(fd: Fd) -> Result<u64, PagerError> {
        let size: u64 = rustix::fs::fstat(fd)?.st_size.try_into().unwrap();
        Ok(size / PAGE_SIZE as u64)
    }

    pub fn get_page_mut<Fd: AsRawFd + AsFd>(
        &mut self,
        fd: Fd,
        page_id: PageId,
    ) -> Result<&mut Page, PagerError> {
        match self.page_locations.get(&(fd.as_raw_fd(), page_id)) {
            Some(loc) => {
                self.clock_cache.set_use_bit(*loc);
                Ok(self.pages.get_mut(*loc).unwrap())
            }
            None => {
                let page = self.evict_page_and_replace_with(fd.as_fd(), page_id)?;
                Ok(page)
            }
        }
    }

    // evicts a page and returns the location of that now usable page
    fn evict_page(&mut self) -> Result<usize, PagerError> {
        // NOTE: This code is copied in evict_page_and_replace_with, so changes to this code should
        // be duplicated there
        let location = self.clock_cache.advance_to_next_evictable_location();
        let page = self.pages.get_mut(location).unwrap();

        // handle old page, which may not actually be in use yet
        if self.location_fd_mapping.contains_key(&location) {
            let old_fd = self.location_fd_mapping.get(&location).unwrap();
            let old_fd = unsafe { BorrowedFd::borrow_raw(*old_fd) };
            if page.is_dirty() {
                page.write_to_disk(old_fd)?;
            }
            self.location_fd_mapping.remove(&location);
            self.page_locations.remove(&(old_fd.as_raw_fd(), page.id()));
        }

        Ok(location)
    }

    fn evict_page_and_replace_with<Fd: AsRawFd + AsFd>(
        &mut self,
        new_fd: Fd,
        page_id: PageId,
    ) -> Result<&mut Page, PagerError> {
        let location = self.evict_page()?;
        let page = self.pages.get_mut(location).unwrap();

        // replace with new page
        page.replace_contents(new_fd.as_fd(), page_id)?;
        self.location_fd_mapping
            .insert(location, new_fd.as_raw_fd());
        self.page_locations
            .insert((new_fd.as_raw_fd(), page_id), location);
        self.clock_cache.set_use_bit(location);
        Ok(page)
    }

    pub fn get_page<Fd: AsRawFd + AsFd>(
        &mut self,
        fd: Fd,
        page_id: PageId,
    ) -> Result<&Page, PagerError> {
        let page = self.get_page_mut(fd, page_id)?;
        Ok(&*page)
    }

    pub fn new_page<Fd: AsRawFd>(
        &mut self,
        fd: Fd,
        kind: PageKind,
    ) -> Result<&mut Page, PagerError> {
        let page_id = self
            .next_page_ids
            .iter_mut()
            .find(|npid| npid.matches_fd(fd.as_raw_fd()))
            .unwrap()
            .use_id();

        let location = self.evict_page()?;
        let page = self.pages.get_mut(location).unwrap();
        page.reset(page_id, kind);
        self.page_locations
            .insert((fd.as_raw_fd(), page_id), location);
        self.location_fd_mapping.insert(location, fd.as_raw_fd());
        self.clock_cache.set_use_bit(location);
        Ok(page)
    }
}

#[cfg(test)]
mod tests {
    use rustix::{
        fd::OwnedFd,
        fs::{Mode, OFlags},
    };

    use crate::serialize::{Deserialize, Serialize};

    use super::*;

    #[test]
    fn size_proofs() {
        assert!(MAX_PAGER_MEMORY % PAGE_SIZE as usize == 0);
    }

    fn fill_page(page: &mut Page, starting_at: u64) {
        let id = page.id();
        let fill_val = starting_at + (id * 10);
        let data = vec![fill_val, fill_val, fill_val];
        let mut bytes = Vec::new();
        data.write_to_bytes(&mut bytes).unwrap();
        page.insert_cell(page.cell_count(), &bytes[..]).unwrap();
    }

    fn get_first_cell_from_page(pager: &mut Pager, fd: BorrowedFd, page_id: PageId) -> Vec<u64> {
        let page = pager.get_page(fd, page_id).unwrap();
        assert_eq!(page.id(), page_id);
        let actual_bytes = page.get_cell(0);
        let mut reader = &actual_bytes[..];
        Vec::from_bytes(&mut reader, &()).unwrap()
    }

    fn open_test_file(name: &str) -> OwnedFd {
        rustix::fs::open(
            name,
            OFlags::CREATE | OFlags::TRUNC | OFlags::RDWR,
            Mode::RWXU,
        )
        .unwrap()
    }

    #[test]
    fn basics() {
        let table_fd0 = open_test_file("pager_basics_t0.test");
        let table_fd1 = open_test_file("pager_basics_t1.test");
        let table_fd2 = open_test_file("pager_basics_t2.test");
        let fds = [table_fd0.as_fd(), table_fd1.as_fd(), table_fd2.as_fd()];
        let mut pager = Pager::new(&fds[..]);

        // set up table 0
        let page0 = pager.new_page(table_fd0.as_fd(), PageKind::Data).unwrap();
        fill_page(page0, 0);
        assert_eq!(page0.id(), 0);
        let page1 = pager.new_page(table_fd0.as_fd(), PageKind::Data).unwrap();
        fill_page(page1, 0);
        assert_eq!(page1.id(), 1);
        let page2 = pager.new_page(table_fd0.as_fd(), PageKind::Data).unwrap();
        fill_page(page2, 0);
        assert_eq!(page2.id(), 2);

        // set up table 1
        let page0 = pager.new_page(table_fd1.as_fd(), PageKind::Data).unwrap();
        fill_page(page0, 100);
        assert_eq!(page0.id(), 0);
        let page1 = pager.new_page(table_fd1.as_fd(), PageKind::Data).unwrap();
        fill_page(page1, 100);
        assert_eq!(page1.id(), 1);
        let page2 = pager.new_page(table_fd1.as_fd(), PageKind::Data).unwrap();
        fill_page(page2, 100);
        assert_eq!(page2.id(), 2);

        // set up table 2
        let page0 = pager.new_page(table_fd2.as_fd(), PageKind::Data).unwrap();
        fill_page(page0, 200);
        assert_eq!(page0.id(), 0);
        let page1 = pager.new_page(table_fd2.as_fd(), PageKind::Data).unwrap();
        fill_page(page1, 200);
        assert_eq!(page1.id(), 1);
        let page2 = pager.new_page(table_fd2.as_fd(), PageKind::Data).unwrap();
        fill_page(page2, 200);
        assert_eq!(page2.id(), 2);

        // out of order checks on all pages
        assert_eq!(
            // fd 2, page 0
            vec![200, 200, 200],
            get_first_cell_from_page(&mut pager, table_fd2.as_fd(), 0)
        );
        assert_eq!(
            //fd 0, page 2
            vec![20, 20, 20],
            get_first_cell_from_page(&mut pager, table_fd0.as_fd(), 2)
        );
        assert_eq!(
            //fd 1, page 1
            vec![110, 110, 110],
            get_first_cell_from_page(&mut pager, table_fd1.as_fd(), 1)
        );
        assert_eq!(
            //fd 0, page 0
            vec![0, 0, 0],
            get_first_cell_from_page(&mut pager, table_fd0.as_fd(), 0)
        );
        assert_eq!(
            //fd 1, page 0
            vec![100, 100, 100],
            get_first_cell_from_page(&mut pager, table_fd1.as_fd(), 0)
        );
        assert_eq!(
            //fd 1, page 2
            vec![120, 120, 120],
            get_first_cell_from_page(&mut pager, table_fd1.as_fd(), 2)
        );
        assert_eq!(
            //fd 0, page 1
            vec![10, 10, 10],
            get_first_cell_from_page(&mut pager, table_fd0.as_fd(), 1)
        );
        assert_eq!(
            //fd 1, page 2
            vec![120, 120, 120],
            get_first_cell_from_page(&mut pager, table_fd1.as_fd(), 2)
        );
        assert_eq!(
            //fd 2, page 1
            vec![210, 210, 210],
            get_first_cell_from_page(&mut pager, table_fd2.as_fd(), 1)
        );
        assert_eq!(
            //fd 2, page 2
            vec![220, 220, 220],
            get_first_cell_from_page(&mut pager, table_fd2.as_fd(), 2)
        );
    }
}
