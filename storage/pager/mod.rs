#![allow(dead_code)]

mod page;

use std::{collections::HashMap, os::fd::AsFd};

use page::{PageKind, PAGE_SIZE};
use rustix::{fd::BorrowedFd, io::Errno};

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
}
impl From<Errno> for PagerError {
    fn from(value: Errno) -> Self {
        Self::IoError(value)
    }
}

type PageLookupKey<'db> = (BorrowedFd<'db>, PageId);

pub struct Pager<'db> {
    pages: Vec<Page>,
    page_locations: HashMap<PageLookupKey<'db>, usize>,
    free_locations: Vec<usize>,
    next_page_ids: Vec<(BorrowedFd<'db>, PageId)>,
}
impl<'db> Pager<'db> {
    fn new(table_fds: &'db [BorrowedFd]) -> Self {
        Pager {
            pages: (0..MAX_PAGE_COUNT)
                .map(|_| Page::new(0, PageKind::Data))
                .collect(),
            page_locations: HashMap::with_capacity(MAX_PAGE_COUNT),
            free_locations: (0..MAX_PAGE_COUNT).collect(),
            // TODO: Make this actually calc next page id
            next_page_ids: table_fds
                .iter()
                .map(|fd| (fd.as_fd(), Pager::calc_page_count(fd.as_fd()).unwrap()))
                .collect(),
        }
    }

    fn calc_page_count<Fd: AsFd>(fd: Fd) -> Result<PageId, PagerError> {
        let size: u64 = rustix::fs::fstat(fd)?.st_size.try_into().unwrap();
        Ok(size / PAGE_SIZE as u64)
    }

    pub fn get_page<Fd: AsFd>(&mut self, _fd: Fd, _page_id: PageId) -> &Page {
        unimplemented!();
    }

    pub fn new_page<Fd: AsFd>(&mut self, _fd: Fd, _page_id: PageId) -> &Page {
        unimplemented!();
    }

    fn evict_page(&mut self) {
        unimplemented!();
    }
}

#[cfg(test)]
mod tests {
    use crate::pager::{page::PAGE_SIZE, MAX_PAGER_MEMORY, MAX_PAGE_COUNT};

    #[test]
    fn size_proofs() {
        assert!(MAX_PAGER_MEMORY % PAGE_SIZE as usize == 0);
    }
}
