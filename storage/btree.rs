#![allow(dead_code, unused_imports)]
/*
 * BTreeNode should reference a page and have methods for operating on it.
 * - allow search, which will allocate a new node and use it for the entire search process,
 * replacing the referenced page as necessary.
 */
use std::{
    cmp::{self, Ordering},
    fmt::{Debug, Display},
    os::fd::RawFd,
    rc::Rc,
};

use crate::{
    pager::{Page, PageError, PageId, PageKind, Pager, PagerError},
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
 * NOTE: Cells point left. The rightmost cell always contains the rightmost key
 */

/*
 * BTree Node Cell Layout:
 *
 * 0                  8                  9             8 + key_size
 * +------------------+------------------+------------------+
 * | [u64] PageId     | [byte] overflows | [Serialized] Key |
 * +------------------+------------------+------------------+
 *
 * BTree Leaf Cell Layout:
 *
 * 0                  8                     10                 11               8 + key_size
 * +------------------+---------------------+------------------+------------------+
 * | [u64] PageId     | [u16] cell_position | [byte] overflows | [Serialized] Key |
 * +------------------+---------------------+------------------+------------------+
 * - This points to the HEAP page id
 *
 *
 */
const NODE_CELL_KEY_OFFSET: usize = 9;
const LEAF_CELL_KEY_OFFSET: usize = 11;

#[derive(Debug, PartialEq)]
enum SearchResult {
    Found(u16),
    NotFound(u16),
}

pub struct BTreeCursor {
    pager: Pager,
}
impl BTreeCursor {
    /// Searches the page. If the page contains the key (i.e., this is a data page and the key is
    /// present, or is not a data page, but happens to have a split key matching the key), return
    /// a SearchResult::Found containing the cell location the key was found in.
    /// If the key was not found, return a SearchResult::NotFound containing the location that the
    /// key would belong at if inserted into the page.
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
        while bottom < top {
            let pos_key = BTreeCursor::get_key_from_cell::<K>(page, pos)?;
            match key.cmp(&pos_key) {
                Ordering::Less => {
                    if pos == bottom {
                        break;
                    }
                    top = pos - 1;
                }
                Ordering::Greater => {
                    bottom = pos + 1;
                }
                Ordering::Equal => return Ok(SearchResult::Found(pos)),
            }
            pos = bottom + ((top - bottom) / 2);
        }
        let pos_key = BTreeCursor::get_key_from_cell::<K>(page, pos)?;
        match key.cmp(&pos_key) {
            Ordering::Equal => Ok(SearchResult::Found(pos)),
            Ordering::Greater => Ok(SearchResult::NotFound(pos + 1)),
            Ordering::Less => {
                if bottom == 0 {
                    Ok(SearchResult::NotFound(0))
                } else {
                    Ok(SearchResult::NotFound(pos))
                }
            }
        }
    }

    // TODO: Handle overflow case
    fn get_key_from_cell<K: Deserialize<ExtraInfo = ()>>(
        page: &Page,
        position: u16,
    ) -> Result<K, PagerError> {
        assert!(position < page.cell_count());
        assert!(!matches!(page.kind(), PageKind::Heap));

        let cell_bytes = page.get_cell(position);
        let mut reader = match page.kind() {
            PageKind::BTreeNode | PageKind::BTreeRoot => &cell_bytes[NODE_CELL_KEY_OFFSET..],
            PageKind::BTreeLeaf => &cell_bytes[LEAF_CELL_KEY_OFFSET..],
            PageKind::Heap => unreachable!(),
        };
        Ok(K::from_bytes(&mut reader, &())?)
    }

    fn get_page_id_from_node_cell(page: &Page, position: u16) -> Result<PageId, PagerError> {
        assert!(position < page.cell_count());
        assert!(matches!(
            page.kind(),
            PageKind::BTreeNode | PageKind::BTreeRoot
        ));
        let cell_bytes = page.get_cell(position);
        let mut reader = &cell_bytes[0..];
        Ok(PageId::from_bytes(&mut reader, &())?)
    }

    fn get_heap_insertion_data_from_leaf_cell(
        page: &Page,
        position: u16,
    ) -> Result<HeapInsertionData, PagerError> {
        assert!(position < page.cell_count());
        assert!(matches!(page.kind(), PageKind::BTreeLeaf));
        let cell_bytes = page.get_cell(position);
        let mut reader = &cell_bytes[..];
        let heap_page_id = PageId::from_bytes(&mut reader, &())?;
        let page_position = u16::from_bytes(&mut reader, &())?;
        Ok(HeapInsertionData {
            page_id: heap_page_id,
            cell_position: page_position,
        })
    }

    // TODO: Handle overflow case
    fn get_cell_value(page: &Page, position: u16) -> Result<Vec<u8>, PagerError> {
        assert!(position < page.cell_count());
        assert!(matches!(page.kind(), PageKind::Heap));
        let cell_bytes = page.get_cell(position);
        Ok(cell_bytes)
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
        while !matches!(page.kind(), PageKind::BTreeLeaf) {
            let position = match BTreeCursor::binary_search_page(page, key)? {
                SearchResult::Found(pos) => pos,
                SearchResult::NotFound(pos) => pos,
            };
            let page_id = BTreeCursor::get_page_id_from_node_cell(page, position)?;
            page = self.pager.get_page(fd, page_id)?;
        }
        // now re-get that page mutably
        let page_id = page.id();
        let page = self.pager.get_page_mut(fd, page_id)?;
        Ok(page)
    }

    pub fn retrieve_tuple<K>(
        &mut self,
        btree_fd: RawFd,
        heap_fd: RawFd,
        key: &K,
    ) -> Result<Option<Vec<u8>>, PagerError>
    where
        K: Ord + Deserialize<ExtraInfo = ()>,
    {
        let leaf_page = self.traverse_to_leaf(btree_fd, key)?;
        let position = match BTreeCursor::binary_search_page(leaf_page, key)? {
            SearchResult::Found(position) => position,
            SearchResult::NotFound(_) => return Ok(None),
        };
        let heap_insertion_data =
            BTreeCursor::get_heap_insertion_data_from_leaf_cell(leaf_page, position)?;
        let heap_page = self.pager.get_page(heap_fd, heap_insertion_data.page_id)?;
        let data = BTreeCursor::get_cell_value(heap_page, heap_insertion_data.cell_position)?;
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
}

pub enum InsertionError {
    Error(PagerError),
    KeyAlreadyExists,
}
impl From<PagerError> for InsertionError {
    fn from(value: PagerError) -> Self {
        Self::Error(value)
    }
}
impl From<PageError> for InsertionError {
    fn from(value: PageError) -> Self {
        Self::Error(PagerError::from(value))
    }
}

#[derive(Debug, PartialEq)]
pub struct HeapInsertionData {
    pub page_id: PageId,
    pub cell_position: u16,
}
impl HeapInsertionData {
    pub fn new(page_id: PageId, cell_position: u16) -> Self {
        HeapInsertionData {
            page_id,
            cell_position,
        }
    }
}

impl BTreeCursor {
    fn make_node_cell<K>(key: &K, page_id: PageId) -> Result<Vec<u8>, PagerError>
    where
        K: Serialize,
    {
        let mut bytes = Vec::with_capacity(10); // this is the absolute minimum size we'll need
        page_id.write_to_bytes(&mut bytes)?;
        bytes.push(0); // overflow flag, always false for now
        key.write_to_bytes(&mut bytes)?;
        Ok(bytes)
    }

    fn make_leaf_cell<K>(key: &K, insertion_data: &HeapInsertionData) -> Result<Vec<u8>, PagerError>
    where
        K: Serialize,
    {
        let mut bytes = Vec::with_capacity(12); // absolute minimum size
        insertion_data.page_id.write_to_bytes(&mut bytes)?;
        insertion_data.cell_position.write_to_bytes(&mut bytes)?;
        bytes.push(0); // overflow flag, always false for now
        key.write_to_bytes(&mut bytes)?;
        Ok(bytes)
    }

    pub fn insert<K>(
        &mut self,
        fd: RawFd,
        key: &K,
        insertion_data: &HeapInsertionData,
    ) -> Result<(), InsertionError>
    where
        K: Ord + Serialize + Deserialize<ExtraInfo = ()>,
    {
        let leaf_page = self.traverse_to_leaf(fd, key)?;
        let location = match BTreeCursor::binary_search_page(leaf_page, key)? {
            SearchResult::NotFound(loc) => loc,
            SearchResult::Found(_) => return Err(InsertionError::KeyAlreadyExists),
        };
        // TODO: Handle case where data needs to overflow
        // TODO: Handle non-heap inserts
        let data = BTreeCursor::make_leaf_cell(key, insertion_data)?;
        // TODO: Handle case where a split is necessary. i.e. where cell_count is already at
        // FANOUT_FACTOR
        // TODO: Handle case where parent-node's rightmost value needs to be updated
        leaf_page.insert_cell(location, &data)?;
        Ok(())
    }

    pub fn delete<K>(&mut self, fd: RawFd, key: &K) -> Result<(), PagerError>
    where
        K: Ord + Deserialize<ExtraInfo = ()>,
    {
        let leaf_page = self.traverse_to_leaf(fd, key)?;
        let location = match BTreeCursor::binary_search_page(leaf_page, key)? {
            SearchResult::Found(loc) => loc,
            SearchResult::NotFound(_) => return Ok(()),
        };
        leaf_page.remove_cell(location);
        // TODO: Handle merge case
        // TODO: Handle case where parent-node's rightmost value needs to be updated
        unimplemented!();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_key_from_cell_works() {
        let kv_pairs: Vec<(u16, u64)> = vec![(0, 0), (1, 1), (2, 2), (3, 3), (4, 4)];
        let mut page = Page::new(0, PageKind::BTreeNode);
        for (k, v) in kv_pairs.iter() {
            let cell = BTreeCursor::make_node_cell(k, *v).unwrap();
            page.insert_cell(*k, &cell).unwrap();
        }
        for (k, _) in kv_pairs.iter() {
            let read_key: u16 = BTreeCursor::get_key_from_cell(&page, *k).unwrap();
            assert_eq!(read_key, *k);
        }
    }

    #[test]
    fn binary_search_odd_cell_count() {
        let kv_pairs: Vec<(u16, u64)> = vec![(0, 0), (1, 1), (2, 2), (3, 3), (4, 4)];
        let mut page = Page::new(0, PageKind::BTreeNode);
        for (k, v) in kv_pairs.iter() {
            let cell = BTreeCursor::make_node_cell(k, *v).unwrap();
            page.insert_cell(*k, &cell).unwrap();
        }
        for (k, _) in kv_pairs.iter() {
            let res = BTreeCursor::binary_search_page(&page, k).unwrap();
            assert_eq!(res, SearchResult::Found(*k));
        }
    }

    #[test]
    fn binary_search_even_cell_count() {
        let kv_pairs: Vec<(u16, u64)> = vec![(0, 0), (1, 1), (2, 2), (3, 3), (4, 4), (5, 5)];
        let mut page = Page::new(0, PageKind::BTreeNode);
        for (k, v) in kv_pairs.iter() {
            let cell = BTreeCursor::make_node_cell(k, *v).unwrap();
            page.insert_cell(*k, &cell).unwrap();
        }
        for (k, _) in kv_pairs.iter() {
            let res = BTreeCursor::binary_search_page(&page, k).unwrap();
            assert_eq!(res, SearchResult::Found(*k));
        }
    }

    #[test]
    fn binary_search_not_found_should_insert_right() {
        let kv_pairs: Vec<(u16, u16)> = vec![(0, 0), (2, 1), (4, 2), (6, 3), (8, 4), (10, 5)];
        let mut page = Page::new(0, PageKind::BTreeNode);
        for (k, v) in kv_pairs.iter() {
            let cell = BTreeCursor::make_node_cell(k, *v as PageId).unwrap();
            page.insert_cell(*v, &cell).unwrap();
        }
        for (k, v) in kv_pairs.iter() {
            let search_for = *k + 1;
            let res = BTreeCursor::binary_search_page(&page, &search_for).unwrap();
            assert_eq!(res, SearchResult::NotFound(*v + 1));
        }
    }

    #[test]
    fn binary_search_not_found_should_insert_left() {
        let kv_pairs: Vec<(u16, u16)> = vec![(3, 0), (5, 0), (7, 1), (9, 2), (11, 3), (13, 4)];
        let mut page = Page::new(0, PageKind::BTreeNode);
        for (idx, (k, v)) in kv_pairs.iter().enumerate() {
            let cell = BTreeCursor::make_node_cell(k, *v as PageId).unwrap();
            page.insert_cell(idx as u16, &cell).unwrap();
        }
        for (k, v) in kv_pairs.iter() {
            let search_for = *k - 3;
            let res = BTreeCursor::binary_search_page(&page, &search_for).unwrap();
            assert_eq!(res, SearchResult::NotFound(*v));
        }
    }

    #[test]
    fn node_cell_construction() {
        let mut page = Page::new(0, PageKind::BTreeNode);
        let page_id = 42;
        let key = String::from("foo");
        let cell = BTreeCursor::make_node_cell(&key, page_id).unwrap();
        page.insert_cell(0, &cell).unwrap();

        assert_eq!(
            key,
            BTreeCursor::get_key_from_cell::<String>(&page, 0).unwrap()
        );
        assert_eq!(
            page_id,
            BTreeCursor::get_page_id_from_node_cell(&page, 0).unwrap()
        );
    }

    #[test]
    fn leaf_cell_construction() {
        let mut page = Page::new(0, PageKind::BTreeLeaf);
        let page_id = 42;
        let page_location = 43;
        let key = String::from("foo");
        let heap_data = HeapInsertionData::new(page_id, page_location);
        let cell = BTreeCursor::make_leaf_cell(&key, &heap_data).unwrap();
        page.insert_cell(0, &cell).unwrap();

        assert_eq!(
            key,
            BTreeCursor::get_key_from_cell::<String>(&page, 0).unwrap()
        );
        assert_eq!(
            heap_data,
            BTreeCursor::get_heap_insertion_data_from_leaf_cell(&page, 0).unwrap()
        );
    }
}
