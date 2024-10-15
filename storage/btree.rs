#![allow(dead_code, unused_imports)]
/*
 * BTreeNode should reference a page and have methods for operating on it.
 * - allow search, which will allocate a new node and use it for the entire search process,
 * replacing the referenced page as necessary.
 */
use std::{
    cmp::{self, Ordering},
    os::fd::RawFd,
    rc::Rc,
};

use crate::{
    pager::{Page, PageId, PageKind, Pager, PagerError},
    serialize::{Deserialize, Serialize},
};

// TODO: Implement overflow handling

/*
 * Logic for overflow pages:
 *
 * There will be some max_payload_size. If payload is larger than that, split it at that size.
 * Go to the overflow page. If there is not one, allocate a new one and go to it. Store the
 * overflow part. Then on the original page, store the non-overflow part.
 * -- need to now include a cell header that indicates if this cell overflows to the overflow page,
 * and if so, the cell position of the overflow bytes in the overflow page.
 *
 */

/*
 * NOTE: Cells point left. The rightmost cell always contains the rightmost value
 */

/*
 * Cell Layout:
 *
 * Key cell:
 * 0                  8                  9             8 + key_size
 * +------------------+------------------+------------------+
 * | [PageId] page_id | [byte] overflows | [Serialized] Key |
 * +------------------+------------------+------------------+
 *
 * Data cell:
 * 0                  1             1 + key_size       1 + key_size + data_size
 * +------------------+------------------+-------------------+
 * | [byte] overflows | [Serialized] Key | [Serialized] Data |
 * +------------------+------------------+-------------------+
 *
 *
 */
const KEY_CELL_KEY_OFFSET: usize = 9;
const DATA_CELL_KEY_OFFSET: usize = 1;
const KEY_CELL_PAGE_ID_OFFSET: usize = 0;

enum SearchResult {
    Found(u16),
    NotFound(u16),
}

pub struct BTreeCursor {
    pager: Pager,
}
impl BTreeCursor {
    fn get_key_from_cell<K: Deserialize<ExtraInfo = ()>>(
        page: &Page,
        position: u16,
    ) -> Result<K, PagerError> {
        assert!(position < page.cell_count());
        let cell_bytes = page.get_cell(position);
        let mut reader = match page.kind() {
            PageKind::Data => &cell_bytes[DATA_CELL_KEY_OFFSET..],
            PageKind::NotData => &cell_bytes[KEY_CELL_KEY_OFFSET..],
        };
        Ok(K::from_bytes(&mut reader, &())?)
    }

    fn get_page_id_from_cell(page: &Page, position: u16) -> Result<PageId, PagerError> {
        assert!(position < page.cell_count());
        assert!(matches!(page.kind(), PageKind::NotData));
        let cell_bytes = page.get_cell(position);
        let mut reader = &cell_bytes[KEY_CELL_PAGE_ID_OFFSET..];
        Ok(PageId::from_bytes(&mut reader, &())?)
    }

    fn get_cell_value<K>(page: &Page, position: u16) -> Result<Vec<u8>, PagerError>
    where
        K: Deserialize<ExtraInfo = ()>,
    {
        assert!(position < page.cell_count());
        assert!(matches!(page.kind(), PageKind::Data));
        let cell_bytes = page.get_cell(position);
        let mut reader = &cell_bytes[DATA_CELL_KEY_OFFSET..];
        // consume key bytes
        _ = K::from_bytes(&mut reader, &())?;
        // return the rest
        Ok(reader.to_vec())
    }

    /// Searches the page. If the page contains the key (i.e., this is a data page and the key is
    /// present, or is not a data page, but happens to have a split key matching the key), return
    /// a SearchResult::Found containing the cell location the key was found in.
    /// If the key was not found, return a SearchResult::NotFound containing the location that:
    /// - if page is a Data page, the key would belong at if inserted into the page.
    /// - if page is not a data page, they location of the cell containing the page id to descend
    /// to in the tree
    fn binary_search_page<K>(page: &Page, key: &K) -> Result<SearchResult, PagerError>
    where
        K: Ord + Deserialize<ExtraInfo = ()>,
    {
        // TODO: Test this!
        if page.cell_count() == 0 {
            return Ok(SearchResult::NotFound(0));
        }
        let mut bottom = 0;
        let mut top = page.cell_count() - 1;
        let mut pos = (top - bottom) / 2;
        while bottom <= top {
            let pos_key = BTreeCursor::get_key_from_cell::<K>(page, pos)?;
            match pos_key.cmp(key) {
                Ordering::Less => {
                    top = pos - 1;
                }
                Ordering::Greater => {
                    bottom = pos + 1;
                }
                Ordering::Equal => return Ok(SearchResult::Found(pos)),
            }
            pos = (top - bottom) / 2;
        }
        Ok(SearchResult::NotFound(pos))
    }

    fn traverse_to_leaf<'db, K>(
        &'db mut self,
        fd: RawFd,
        key: &K,
    ) -> Result<&'db mut Page, PagerError>
    where
        K: Ord + Deserialize<ExtraInfo = ()>,
    {
        // load root page (unmutably)
        let mut page = self.pager.get_page(fd, 0)?;
        // traverse until we hit a leaf page
        while !matches!(page.kind(), PageKind::Data) {
            let position = match BTreeCursor::binary_search_page(page, key)? {
                SearchResult::Found(pos) => pos,
                SearchResult::NotFound(pos) => pos,
            };
            let page_id = BTreeCursor::get_page_id_from_cell(page, position)?;
            page = self.pager.get_page(fd, page_id)?;
        }
        // now re-get that page mutably
        let page_id = page.id();
        let page = self.pager.get_page_mut(fd, page_id)?;
        Ok(page)
    }

    pub fn retrieve<K>(&mut self, fd: RawFd, key: &K) -> Result<Option<Vec<u8>>, PagerError>
    where
        K: Ord + Deserialize<ExtraInfo = ()>,
    {
        let leaf_page = self.traverse_to_leaf(fd, key)?;
        let position = match BTreeCursor::binary_search_page(leaf_page, key)? {
            SearchResult::Found(position) => position,
            SearchResult::NotFound(_) => return Ok(None),
        };
        let data = BTreeCursor::get_cell_value::<K>(leaf_page, position)?;
        Ok(Some(data))
    }

    pub fn contains_key<K>(&mut self, fd: RawFd, key: &K) -> Result<bool, PagerError>
    where
        K: Ord + Deserialize<ExtraInfo = ()>,
    {
        let leaf_page = self.traverse_to_leaf(fd, key)?;
        match BTreeCursor::binary_search_page(leaf_page, key)? {
            SearchResult::Found(_) => Ok(true),
            SearchResult::NotFound(_) => Ok(false),
        }
    }

    pub fn insert<K>(_fd: RawFd, _key: K, _value: &[u8]) -> Result<(), PagerError>
    where
        K: Serialize + Eq + Ord,
    {
        // leaf = traverse_to_leaf
        // binary search over leaf for insertion location
        // insert on page

        // TODO: Handle split case later
        unimplemented!();
    }

    pub fn delete<K>(_fd: RawFd, _key: K) -> Result<(), PagerError>
    where
        K: Serialize + Eq + Ord,
    {
        // leaf = traverse_to_leaf
        // binary search over leaf for key
        // delete cell

        // TODO: Handle merge case later
        unimplemented!();
    }
}
