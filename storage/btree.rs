#![allow(dead_code)]
use std::{cell::RefCell, cmp::Ordering, fmt::Debug, os::fd::RawFd, rc::Rc};

use crate::{
    pager::{Page, PageError, PageId, PageKind, Pager, PagerError, PAGE_BUFFER_SIZE},
    serialize::{Deserialize, SerdeError, Serialize},
};

/*
 * GOALS:
 * - Support heap-location indices for Row_id as key
 *   - Not bothering about wraparound
 *   - Leaf nodes store HeapInsertionData
 *   - Support point queries by row id, and table scans
 * - Support row_id indices with user-defined primary key as key
 *   - Leaf nodes store row id
 *   - support same point queries and range scans
 *
 * FUTURE GOALS:
 * - Support secondary indices
 *
 */

/*
 * NOTE: Cells point left. The rightmost cell always contains the rightmost key
 */

/*
* BTree Node Cell Layout:
*
* 0                  8             8 + key_size
* +------------------+------------------+
* | [u64] PageId     | [Serialized] Key |
* +------------------+------------------+
*
 * BTree Leaf Not Heap Cell Layout:
*
* 0                  8             8 + key_size
* +------------------+------------------+
* | [u64] row_id     | [Serialized] Key |
* +------------------+------------------+

*
* BTree Leaf Heap Cell Layout:
*
* 0                  8                     10              10 + key_size
* +------------------+---------------------+------------------+
* | [u64] PageId     | [u16] cell_position | [Serialized] Key |
* +------------------+---------------------+------------------+
* - This points to the HEAP page id
*
*
*/
const NODE_CELL_KEY_OFFSET: usize = 8;
const LEAF_NOTHEAP_CELL_KEY_OFFSET: usize = 8;
const LEAF_HEAP_CELL_KEY_OFFSET: usize = 10;

const MIN_FANOUT_FACTOR: u16 = 4;
const MAX_BTREE_CELL_SIZE: u16 = PAGE_BUFFER_SIZE / MIN_FANOUT_FACTOR;

const METAROOT_ROOT_PTR_LOCATION: u16 = 0;

#[derive(Debug, PartialEq)]
enum SearchResult {
    Found(u16),
    NotFound(u16),
}

#[derive(Debug)]
pub enum BTreeCursorError {
    Serde(SerdeError),
    Pager(PagerError),
    Page(PageError),
    KeyAlreadyExists,
}
impl From<SerdeError> for BTreeCursorError {
    fn from(value: SerdeError) -> Self {
        Self::Serde(value)
    }
}
impl From<PagerError> for BTreeCursorError {
    fn from(value: PagerError) -> Self {
        Self::Pager(value)
    }
}
impl From<PageError> for BTreeCursorError {
    fn from(value: PageError) -> Self {
        Self::Page(value)
    }
}

pub struct BTreeCursor {
    pager: Pager,
    max_cell_size: u16,
}
impl BTreeCursor {
    pub fn new(pager: Pager) -> Self {
        BTreeCursor {
            pager,
            max_cell_size: MAX_BTREE_CELL_SIZE,
        }
    }

    #[cfg(test)]
    pub fn with_max_cell_size(pager: Pager, max_cell_size: u16) -> Self {
        BTreeCursor {
            pager,
            max_cell_size,
        }
    }

    /// Searches the page. If the page contains the key (i.e., this is a data page and the key is
    /// present, or is not a data page, but happens to have a split key matching the key), return
    /// a SearchResult::Found containing the cell location the key was found in.
    /// If the key was not found, return a SearchResult::NotFound containing the location that the
    /// key would belong at if inserted into the page.
    fn binary_search_page<K>(page: &Page, key: &K) -> Result<SearchResult, BTreeCursorError>
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

    fn get_key_from_cell<K: Deserialize<ExtraInfo = ()>>(
        page: &Page,
        position: u16,
    ) -> Result<K, BTreeCursorError> {
        assert!(position < page.cell_count());
        assert!(!matches!(
            page.kind(),
            PageKind::Heap | PageKind::BTreeMetaRoot
        ));

        let cell_bytes = page.get_cell(position);
        let mut reader = match page.kind() {
            PageKind::BTreeNode => &cell_bytes[NODE_CELL_KEY_OFFSET..],
            PageKind::BTreeLeafHeap => &cell_bytes[LEAF_HEAP_CELL_KEY_OFFSET..],
            PageKind::BTreeLeafNotHeap => todo!(),
            PageKind::Heap | PageKind::BTreeMetaRoot => unreachable!(),
        };
        Ok(K::from_bytes(&mut reader, &())?)
    }

    fn get_page_id_from_node_cell(page: &Page, position: u16) -> Result<PageId, BTreeCursorError> {
        assert!(position < page.cell_count());
        assert!(matches!(page.kind(), PageKind::BTreeNode));
        let cell_bytes = page.get_cell(position);
        let mut reader = &cell_bytes[0..];
        Ok(PageId::from_bytes(&mut reader, &())?)
    }

    fn get_heap_insertion_data_from_leaf_cell(
        page: &Page,
        position: u16,
    ) -> Result<HeapInsertionData, BTreeCursorError> {
        assert!(position < page.cell_count());
        assert!(matches!(page.kind(), PageKind::BTreeLeafHeap));
        let cell_bytes = page.get_cell(position);
        let mut reader = &cell_bytes[..];
        let heap_page_id = PageId::from_bytes(&mut reader, &())?;
        let page_position = u16::from_bytes(&mut reader, &())?;
        Ok(HeapInsertionData {
            page_id: heap_page_id,
            cell_position: page_position,
        })
    }

    fn get_cell_value(page: &Page, position: u16) -> Result<Vec<u8>, BTreeCursorError> {
        assert!(position < page.cell_count());
        assert!(matches!(page.kind(), PageKind::Heap));
        let cell_bytes = page.get_cell(position);
        Ok(cell_bytes)
    }

    fn get_root_page_id(&mut self, fd: RawFd) -> Result<PageId, BTreeCursorError> {
        let meta_root_ref = self.pager.get_page(fd, 0)?;
        let meta_root_page = meta_root_ref.borrow();
        assert!(matches!(meta_root_page.kind(), PageKind::BTreeMetaRoot));
        assert!(meta_root_page.cell_count() > METAROOT_ROOT_PTR_LOCATION);
        let ptr_bytes = BTreeCursor::get_cell_value(&meta_root_page, METAROOT_ROOT_PTR_LOCATION)?;
        let mut reader = &ptr_bytes[..];
        Ok(PageId::from_bytes(&mut reader, &())?)
    }

    pub fn contains_key<K>(&mut self, fd: RawFd, key: &K) -> Result<bool, BTreeCursorError>
    where
        K: Ord + Deserialize<ExtraInfo = ()>,
    {
        let traversal_res = self.traverse_to_leaf(fd, key)?;
        let leaf_page = traversal_res.leaf.borrow();
        match BTreeCursor::binary_search_page(&leaf_page, key)? {
            SearchResult::Found(_) => Ok(true),
            SearchResult::NotFound(_) => Ok(false),
        }
    }
}

struct TraversalResult {
    leaf: Rc<RefCell<Page>>,
    breadcrumbs: Vec<Rc<RefCell<Page>>>,
}

impl BTreeCursor {
    fn traverse_to_leaf<K>(
        &mut self,
        fd: RawFd,
        key: &K,
    ) -> Result<TraversalResult, BTreeCursorError>
    where
        K: Ord + Deserialize<ExtraInfo = ()>,
    {
        let root_page_id = self.get_root_page_id(fd)?;
        // load root page (unmutably)
        let mut page_ref = self.pager.get_page(fd, root_page_id)?;
        let mut breadcrumbs = Vec::new();
        // traverse until we hit a leaf page
        while !matches!(page_ref.borrow().kind(), PageKind::BTreeLeafHeap) {
            let page = page_ref.borrow();
            let position = match BTreeCursor::binary_search_page(&page, key)? {
                SearchResult::Found(pos) => pos,
                SearchResult::NotFound(pos) => pos,
            };
            let page_id = BTreeCursor::get_page_id_from_node_cell(&page, position)?;
            drop(page);
            breadcrumbs.push(page_ref);
            page_ref = self.pager.get_page(fd, page_id)?;
        }
        // now re-get that page mutably
        let page_id = page_ref.borrow().id();
        let page_ref = self.pager.get_page(fd, page_id)?;
        Ok(TraversalResult {
            leaf: page_ref,
            breadcrumbs,
        })
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
    fn make_node_cell<K>(key: &K, page_id: PageId) -> Result<Vec<u8>, BTreeCursorError>
    where
        K: Serialize,
    {
        let mut bytes = Vec::with_capacity(10); // this is the absolute minimum size we'll need
        page_id.write_to_bytes(&mut bytes)?;
        key.write_to_bytes(&mut bytes)?;
        Ok(bytes)
    }

    fn make_leaf_cell<K>(
        key: &K,
        insertion_data: &HeapInsertionData,
    ) -> Result<Vec<u8>, BTreeCursorError>
    where
        K: Serialize,
    {
        let mut bytes = Vec::with_capacity(12); // absolute minimum size
        insertion_data.page_id.write_to_bytes(&mut bytes)?;
        insertion_data.cell_position.write_to_bytes(&mut bytes)?;
        key.write_to_bytes(&mut bytes)?;
        Ok(bytes)
    }

    pub fn insert<K>(
        &mut self,
        fd: RawFd,
        key: &K,
        insertion_data: &HeapInsertionData,
    ) -> Result<(), BTreeCursorError>
    where
        K: Ord + Serialize + Deserialize<ExtraInfo = ()>,
    {
        let traversal_res = self.traverse_to_leaf(fd, key)?;
        let mut leaf_page = traversal_res.leaf.borrow_mut();
        let location = match BTreeCursor::binary_search_page(&leaf_page, key)? {
            SearchResult::NotFound(loc) => loc,
            SearchResult::Found(_) => return Err(BTreeCursorError::KeyAlreadyExists),
        };
        // TODO: Handle case where parent-node's rightmost value needs to be updated (will be the
        // case if location == leaf_page.cell_count() )
        let data = BTreeCursor::make_leaf_cell(key, insertion_data)?;
        match leaf_page.insert_cell(location, &data) {
            Err(PageError::NotEnoughSpace) => {
                /* Splitting:
                 * - find cell position to split on such that each resulting page is roughly the
                 * same size (including the size of the to-be-inserted cell)
                 * - make new page
                 * - For all cell locations at or greater than split location, copy those cells to
                 * the new page
                 * - After all cells copied, delete cells in reverse order (to avoid uneccessary
                 * pointer movement)
                 * - insert new cell into the correct page.
                 * - write the node cell containing this page id and it's new rightmost key, and
                 * update the next cell up to point at the new page.
                 *
                 */
                Ok(())
            }
            Err(err) => Err(BTreeCursorError::from(err)),
            Ok(_) => Ok(()),
        }
    }

    pub fn delete<K>(&mut self, fd: RawFd, key: &K) -> Result<(), BTreeCursorError>
    where
        K: Ord + Deserialize<ExtraInfo = ()>,
    {
        let traversal_res = self.traverse_to_leaf(fd, key)?;
        let mut leaf_page = traversal_res.leaf.borrow_mut();
        let location = match BTreeCursor::binary_search_page(&leaf_page, key)? {
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
    fn size_assumptions() {
        assert_eq!(PAGE_BUFFER_SIZE % MIN_FANOUT_FACTOR, 0);
        assert_eq!(PAGE_BUFFER_SIZE % MAX_BTREE_CELL_SIZE, 0);
    }

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
        let mut page = Page::new(0, PageKind::BTreeLeafHeap);
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
