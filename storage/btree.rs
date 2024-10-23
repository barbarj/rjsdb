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
pub enum BTreeCursor {
    Serde(SerdeError),
    Pager(PagerError),
    Page(PageError),
    KeyAlreadyExists,
}
impl From<SerdeError> for BTreeCursor {
    fn from(value: SerdeError) -> Self {
        Self::Serde(value)
    }
}
impl From<PagerError> for BTreeCursor {
    fn from(value: PagerError) -> Self {
        Self::Pager(value)
    }
}
impl From<PageError> for BTreeCursor {
    fn from(value: PageError) -> Self {
        Self::Page(value)
    }
}

pub struct BTree {
    pager: Rc<RefCell<Pager>>,
    max_cell_size: u16,
    fd: RawFd,
}
impl BTree {
    pub fn new(pager: Rc<RefCell<Pager>>, fd: RawFd) -> Self {
        BTree {
            pager,
            max_cell_size: MAX_BTREE_CELL_SIZE,
            fd,
        }
    }

    #[cfg(test)]
    pub fn with_max_cell_size(pager: Rc<RefCell<Pager>>, fd: RawFd, max_cell_size: u16) -> Self {
        BTree {
            pager,
            max_cell_size,
            fd,
        }
    }

    /// Searches the page. If the page contains the key (i.e., this is a data page and the key is
    /// present, or is not a data page, but happens to have a split key matching the key), return
    /// a SearchResult::Found containing the cell location the key was found in.
    /// If the key was not found, return a SearchResult::NotFound containing the location that the
    /// key would belong at if inserted into the page.
    fn binary_search_page<K>(page: &Page, key: &K) -> Result<SearchResult, BTreeCursor>
    where
        K: Ord + Deserialize<ExtraInfo = ()>,
    {
        if page.cell_count() == 0 {
            return Ok(SearchResult::NotFound(0));
        }
        let mut bottom = 0;
        let mut top = page.cell_count() - 1;
        let mut pos = (top - bottom) / 2;
        while bottom < top {
            let pos_key = BTree::get_key_from_cell::<K>(page, pos)?;
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
        let pos_key = BTree::get_key_from_cell::<K>(page, pos)?;
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
    ) -> Result<K, BTreeCursor> {
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

    fn get_page_id_from_node_cell(page: &Page, position: u16) -> Result<PageId, BTreeCursor> {
        assert!(position < page.cell_count());
        assert!(matches!(page.kind(), PageKind::BTreeNode));
        let cell_bytes = page.get_cell(position);
        let mut reader = &cell_bytes[0..];
        Ok(PageId::from_bytes(&mut reader, &())?)
    }

    fn get_heap_insertion_data_from_leaf_cell(
        page: &Page,
        position: u16,
    ) -> Result<HeapInsertionData, BTreeCursor> {
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

    fn get_cell_value(page: &Page, position: u16) -> Result<Vec<u8>, BTreeCursor> {
        assert!(position < page.cell_count());
        assert!(matches!(page.kind(), PageKind::Heap));
        let cell_bytes = page.get_cell(position);
        Ok(cell_bytes)
    }

    fn make_new_root(&mut self, first_cell: &[u8], second_cell: &[u8]) -> Result<(), BTreeCursor> {
        let mut pager = self.pager.borrow_mut();

        let new_root_ref = pager.new_page(self.fd, PageKind::BTreeNode)?;
        let mut new_root = new_root_ref.borrow_mut();
        new_root.insert_cell(0, first_cell)?;
        new_root.insert_cell(1, second_cell)?;
        let mut new_root_page_id_bytes = Vec::new();
        new_root.id().write_to_bytes(&mut new_root_page_id_bytes)?;

        let meta_root_ref = pager.get_page(self.fd, 0)?;
        let mut meta_root_page = meta_root_ref.borrow_mut();
        //remove old id
        meta_root_page.remove_cell(METAROOT_ROOT_PTR_LOCATION);
        //add new id
        meta_root_page.insert_cell(METAROOT_ROOT_PTR_LOCATION, &new_root_page_id_bytes)?;

        Ok(())
    }

    fn get_root_page_id(&mut self) -> Result<PageId, BTreeCursor> {
        let mut pager = self.pager.borrow_mut();
        let meta_root_ref = pager.get_page(self.fd, 0)?;
        let meta_root_page = meta_root_ref.borrow();
        assert!(matches!(meta_root_page.kind(), PageKind::BTreeMetaRoot));
        assert!(meta_root_page.cell_count() > METAROOT_ROOT_PTR_LOCATION);
        let ptr_bytes = BTree::get_cell_value(&meta_root_page, METAROOT_ROOT_PTR_LOCATION)?;
        let mut reader = &ptr_bytes[..];
        Ok(PageId::from_bytes(&mut reader, &())?)
    }

    pub fn contains_key<K>(&mut self, key: &K) -> Result<bool, BTreeCursor>
    where
        K: Ord + Deserialize<ExtraInfo = ()>,
    {
        let traversal_res = self.traverse_to_leaf(key)?;
        let leaf_page = traversal_res.leaf.borrow();
        match BTree::binary_search_page(&leaf_page, key)? {
            SearchResult::Found(_) => Ok(true),
            SearchResult::NotFound(_) => Ok(false),
        }
    }
}

struct TraversalResult {
    leaf: Rc<RefCell<Page>>,
    breadcrumbs: Vec<Rc<RefCell<Page>>>,
}

impl BTree {
    fn traverse_to_leaf<K>(&mut self, key: &K) -> Result<TraversalResult, BTreeCursor>
    where
        K: Ord + Deserialize<ExtraInfo = ()>,
    {
        let root_page_id = self.get_root_page_id()?;
        let mut pager = self.pager.borrow_mut();
        // load root page (unmutably)
        let mut page_ref = pager.get_page(self.fd, root_page_id)?;
        let mut breadcrumbs = Vec::new();
        // traverse until we hit a leaf page
        while !matches!(page_ref.borrow().kind(), PageKind::BTreeLeafHeap) {
            let page = page_ref.borrow();
            let position = match BTree::binary_search_page(&page, key)? {
                SearchResult::Found(pos) => pos,
                SearchResult::NotFound(pos) => pos,
            };
            let page_id = BTree::get_page_id_from_node_cell(&page, position)?;
            drop(page);
            breadcrumbs.push(page_ref);
            page_ref = pager.get_page(self.fd, page_id)?;
        }
        // now re-get that page mutably
        let page_id = page_ref.borrow().id();
        let page_ref = pager.get_page(self.fd, page_id)?;
        Ok(TraversalResult {
            leaf: page_ref,
            breadcrumbs,
        })
    }

    pub fn lookup_value<K>(&mut self, _key: &K) -> Result<Option<HeapInsertionData>, BTreeCursor> {
        unimplemented!();
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

impl BTree {
    fn make_node_cell<K>(key: &K, page_id: PageId) -> Result<Vec<u8>, BTreeCursor>
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
    ) -> Result<Vec<u8>, BTreeCursor>
    where
        K: Serialize,
    {
        let mut bytes = Vec::with_capacity(12); // absolute minimum size
        insertion_data.page_id.write_to_bytes(&mut bytes)?;
        insertion_data.cell_position.write_to_bytes(&mut bytes)?;
        key.write_to_bytes(&mut bytes)?;
        Ok(bytes)
    }

    // TODO: Test this!!!
    fn split_location(page: &Page, insert_location: u16, insert_data_size: u16) -> u16 {
        assert!(page.cell_count() > 0);
        let mut left_sum = 0;
        let mut left_loc = 0;
        let mut right_sum = 0;
        let mut right_loc = page.cell_count() - 1;
        let mut used_insert = false;

        while left_loc <= right_loc {
            // try left side first
            if left_sum <= right_sum {
                if left_loc == insert_location && !used_insert {
                    left_sum += insert_data_size;
                    used_insert = true;
                } else {
                    left_sum += page.cell_size(left_loc);
                    left_loc += 1;
                }
            // otherwise use right
            } else if right_loc == insert_location && !used_insert {
                right_sum += insert_data_size;
                used_insert = true;
            } else {
                right_sum += page.cell_size(right_loc);
                right_loc -= 1;
            }
        }
        right_loc
    }

    fn split_page_after(
        &mut self,
        page: &mut Page,
        split_location: u16,
    ) -> Result<Rc<RefCell<Page>>, BTreeCursor> {
        let mut pager = self.pager.borrow_mut();
        let new_page_ref = pager.new_page(self.fd, page.kind())?;
        let mut new_page = new_page_ref.borrow_mut();
        // copy cells to new page
        for idx in split_location + 1..page.cell_count() {
            let cell = page.get_cell(idx);
            let new_pos = idx - (split_location + 1);
            new_page.insert_cell(new_pos, &cell).unwrap();
        }
        // remove cells from this one
        for idx in page.cell_count() - 1..split_location {
            page.remove_cell(idx);
        }
        drop(new_page);
        Ok(new_page_ref)
    }

    fn split<K>(
        &mut self,
        traversal_res: TraversalResult,
        insert_location: u16,
        key: &K,
        data: &[u8],
    ) -> Result<(), BTreeCursor>
    where
        K: Ord + Serialize + Deserialize<ExtraInfo = ()>,
    {
        let mut orig_page = traversal_res.leaf.borrow_mut();

        let split_location = BTree::split_location(&orig_page, insert_location, data.len() as u16);
        let new_page_ref = self.split_page_after(&mut orig_page, split_location)?;
        let mut new_page = new_page_ref.borrow_mut();

        // insert new data
        if insert_location <= split_location {
            orig_page.insert_cell(insert_location, data)?;
        } else {
            let loc = insert_location - split_location;
            new_page.insert_cell(loc, data)?;
        }

        let orig_page_key = BTree::get_key_from_cell::<K>(&orig_page, orig_page.cell_count() - 1)?;
        let orig_page_id = orig_page.id();
        let orig_page_cell = BTree::make_node_cell(&orig_page_key, orig_page_id)?;

        let new_page_key = BTree::get_key_from_cell::<K>(&new_page, new_page.cell_count() - 1)?;
        let new_page_id = new_page.id();
        let new_page_cell = BTree::make_node_cell(&new_page_key, new_page_id)?;

        // dont need these any more
        drop(orig_page);
        drop(new_page);

        let mut traversal_res = traversal_res;
        if let Some(parent_page_ref) = traversal_res.breadcrumbs.pop() {
            let mut parent_page = parent_page_ref.borrow_mut();
            let search_key_location = match BTree::binary_search_page(&parent_page, key)? {
                SearchResult::Found(pos) => pos,
                SearchResult::NotFound(pos) => pos,
            };
            // remove the old key, replace it with one that contains the correct split key for the
            // orig page
            parent_page.remove_cell(search_key_location);
            parent_page.insert_cell(search_key_location, &orig_page_cell)?;

            // now try inserting the new cell
            match parent_page.insert_cell(search_key_location + 1, &new_page_cell) {
                Err(PageError::NotEnoughSpace) => {
                    drop(parent_page);
                    traversal_res.leaf = parent_page_ref;
                    self.split(
                        traversal_res,
                        search_key_location + 1,
                        &new_page_key,
                        &new_page_cell,
                    )
                }
                Err(err) => Err(BTreeCursor::Page(err)),
                Ok(_) => Ok(()),
            }
        } else {
            self.make_new_root(&orig_page_cell, &new_page_cell)
        }
    }

    pub fn insert<K>(
        &mut self,
        key: &K,
        insertion_data: &HeapInsertionData,
    ) -> Result<(), BTreeCursor>
    where
        K: Ord + Serialize + Deserialize<ExtraInfo = ()>,
    {
        let traversal_res = self.traverse_to_leaf(key)?;
        let mut leaf_page = traversal_res.leaf.borrow_mut();
        let location = match BTree::binary_search_page(&leaf_page, key)? {
            SearchResult::NotFound(loc) => loc,
            SearchResult::Found(_) => return Err(BTreeCursor::KeyAlreadyExists),
        };
        // TODO: Handle case where parent-node's rightmost value needs to be updated (will be the
        // case if location == leaf_page.cell_count() )
        let data = BTree::make_leaf_cell(key, insertion_data)?;
        match leaf_page.insert_cell(location, &data) {
            Err(PageError::NotEnoughSpace) => {
                drop(leaf_page);
                self.split(traversal_res, location, key, &data)?;
                Ok(())
            }
            Err(err) => Err(BTreeCursor::from(err)),
            Ok(_) => Ok(()),
        }
    }

    pub fn delete<K>(&mut self, key: &K) -> Result<(), BTreeCursor>
    where
        K: Ord + Deserialize<ExtraInfo = ()>,
    {
        let traversal_res = self.traverse_to_leaf(key)?;
        let mut leaf_page = traversal_res.leaf.borrow_mut();
        let location = match BTree::binary_search_page(&leaf_page, key)? {
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
            let cell = BTree::make_node_cell(k, *v).unwrap();
            page.insert_cell(*k, &cell).unwrap();
        }
        for (k, _) in kv_pairs.iter() {
            let read_key: u16 = BTree::get_key_from_cell(&page, *k).unwrap();
            assert_eq!(read_key, *k);
        }
    }

    #[test]
    fn binary_search_odd_cell_count() {
        let kv_pairs: Vec<(u16, u64)> = vec![(0, 0), (1, 1), (2, 2), (3, 3), (4, 4)];
        let mut page = Page::new(0, PageKind::BTreeNode);
        for (k, v) in kv_pairs.iter() {
            let cell = BTree::make_node_cell(k, *v).unwrap();
            page.insert_cell(*k, &cell).unwrap();
        }
        for (k, _) in kv_pairs.iter() {
            let res = BTree::binary_search_page(&page, k).unwrap();
            assert_eq!(res, SearchResult::Found(*k));
        }
    }

    #[test]
    fn binary_search_even_cell_count() {
        let kv_pairs: Vec<(u16, u64)> = vec![(0, 0), (1, 1), (2, 2), (3, 3), (4, 4), (5, 5)];
        let mut page = Page::new(0, PageKind::BTreeNode);
        for (k, v) in kv_pairs.iter() {
            let cell = BTree::make_node_cell(k, *v).unwrap();
            page.insert_cell(*k, &cell).unwrap();
        }
        for (k, _) in kv_pairs.iter() {
            let res = BTree::binary_search_page(&page, k).unwrap();
            assert_eq!(res, SearchResult::Found(*k));
        }
    }

    #[test]
    fn binary_search_not_found_should_insert_right() {
        let kv_pairs: Vec<(u16, u16)> = vec![(0, 0), (2, 1), (4, 2), (6, 3), (8, 4), (10, 5)];
        let mut page = Page::new(0, PageKind::BTreeNode);
        for (k, v) in kv_pairs.iter() {
            let cell = BTree::make_node_cell(k, *v as PageId).unwrap();
            page.insert_cell(*v, &cell).unwrap();
        }
        for (k, v) in kv_pairs.iter() {
            let search_for = *k + 1;
            let res = BTree::binary_search_page(&page, &search_for).unwrap();
            assert_eq!(res, SearchResult::NotFound(*v + 1));
        }
    }

    #[test]
    fn binary_search_not_found_should_insert_left() {
        let kv_pairs: Vec<(u16, u16)> = vec![(3, 0), (5, 0), (7, 1), (9, 2), (11, 3), (13, 4)];
        let mut page = Page::new(0, PageKind::BTreeNode);
        for (idx, (k, v)) in kv_pairs.iter().enumerate() {
            let cell = BTree::make_node_cell(k, *v as PageId).unwrap();
            page.insert_cell(idx as u16, &cell).unwrap();
        }
        for (k, v) in kv_pairs.iter() {
            let search_for = *k - 3;
            let res = BTree::binary_search_page(&page, &search_for).unwrap();
            assert_eq!(res, SearchResult::NotFound(*v));
        }
    }

    #[test]
    fn binary_search_empty_page() {
        let page = Page::new(0, PageKind::BTreeNode);
        let res = BTree::binary_search_page(&page, &43).unwrap();
        assert_eq!(res, SearchResult::NotFound(0));
    }

    #[test]
    fn node_cell_construction() {
        let mut page = Page::new(0, PageKind::BTreeNode);
        let page_id = 42;
        let key = String::from("foo");
        let cell = BTree::make_node_cell(&key, page_id).unwrap();
        page.insert_cell(0, &cell).unwrap();

        assert_eq!(key, BTree::get_key_from_cell::<String>(&page, 0).unwrap());
        assert_eq!(
            page_id,
            BTree::get_page_id_from_node_cell(&page, 0).unwrap()
        );
    }

    #[test]
    fn leaf_cell_construction() {
        let mut page = Page::new(0, PageKind::BTreeLeafHeap);
        let page_id = 42;
        let page_location = 43;
        let key = String::from("foo");
        let heap_data = HeapInsertionData::new(page_id, page_location);
        let cell = BTree::make_leaf_cell(&key, &heap_data).unwrap();
        page.insert_cell(0, &cell).unwrap();

        assert_eq!(key, BTree::get_key_from_cell::<String>(&page, 0).unwrap());
        assert_eq!(
            heap_data,
            BTree::get_heap_insertion_data_from_leaf_cell(&page, 0).unwrap()
        );
    }

    fn insert_values(btree: &mut BTree, values: &[u32]) {
        for v in values {
            btree
                .insert(v, &HeapInsertionData::new(*v as u64, 0))
                .unwrap();
        }
    }

    fn lookup_values(btree: &mut BTree, keys: &[u32]) -> Vec<u32> {
        let mut values = Vec::with_capacity(keys.len());
        for key in keys {
            let insertion_info = btree.lookup_value(key).unwrap().unwrap();
            values.push(insertion_info.page_id as u32);
        }
        values
    }

    #[test]
    fn insert_and_split() {
        // test inserts go to right place
        // test lookups work
        // test split root works when full
        // test lookups work
        // test situation where leaf and parent node split
        // test lookups work
    }
}
