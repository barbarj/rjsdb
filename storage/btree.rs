#![allow(dead_code)]
use std::{
    cell::RefCell,
    cmp::Ordering,
    fmt::Debug,
    io::Error as IoError,
    os::{fd::RawFd, unix::fs::MetadataExt},
    rc::Rc,
};

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
pub enum BTreeError {
    Serde(SerdeError),
    Pager(PagerError),
    Page(PageError),
    Io(IoError),
    KeyAlreadyExists,
    KeyTooLarge,
}
impl From<SerdeError> for BTreeError {
    fn from(value: SerdeError) -> Self {
        Self::Serde(value)
    }
}
impl From<PagerError> for BTreeError {
    fn from(value: PagerError) -> Self {
        Self::Pager(value)
    }
}
impl From<PageError> for BTreeError {
    fn from(value: PageError) -> Self {
        Self::Page(value)
    }
}
impl From<IoError> for BTreeError {
    fn from(value: IoError) -> Self {
        Self::Io(value)
    }
}

pub struct BTree {
    pager: Rc<RefCell<Pager>>,
    max_cell_size: u16,
    fd: RawFd,
}
impl BTree {
    pub fn init(pager_ref: Rc<RefCell<Pager>>, fd: RawFd) -> Result<Self, BTreeError> {
        let mut btree = BTree {
            pager: pager_ref,
            max_cell_size: MAX_BTREE_CELL_SIZE,
            fd,
        };

        let pager = btree.pager.borrow();
        let file = pager.file_from_fd(fd).unwrap();
        let file_size = file.metadata()?.size();
        drop(pager);
        if file_size == 0 {
            btree.init_meta_root()?;
        }

        Ok(btree)
    }

    fn init_meta_root(&mut self) -> Result<(), BTreeError> {
        let mut pager = self.pager.borrow_mut();

        // init meta root page
        let meta_root_page_ref = pager.new_page(self.fd, PageKind::BTreeMetaRoot)?;
        let mut meta_root_page = meta_root_page_ref.borrow_mut();
        assert_eq!(meta_root_page.id(), 0);

        let mut id_bytes = Vec::new();
        1u64.write_to_bytes(&mut id_bytes)?;
        meta_root_page.insert_cell(METAROOT_ROOT_PTR_LOCATION, &id_bytes)?;

        // init new root page
        let root_page_ref = pager.new_page(self.fd, PageKind::BTreeLeafHeap)?;
        let roog_page = root_page_ref.borrow();
        assert_eq!(roog_page.id(), 1);

        Ok(())
    }

    #[cfg(test)]
    fn with_max_cell_size(pager: Rc<RefCell<Pager>>, fd: RawFd, max_cell_size: u16) -> Self {
        BTree {
            pager,
            max_cell_size,
            fd,
        }
    }

    fn force_flush(&self) -> Result<(), BTreeError> {
        let mut pager = self.pager.borrow_mut();
        pager.flush_all()?;
        Ok(())
    }

    /// Searches the page. If the page contains the key (i.e., this is a data page and the key is
    /// present, or is not a data page, but happens to have a split key matching the key), return
    /// a SearchResult::Found containing the cell location the key was found in.
    /// If the key was not found, return a SearchResult::NotFound containing the location that the
    /// key would belong at if inserted into the page.
    fn binary_search_page<K>(page: &Page, key: &K) -> Result<SearchResult, BTreeError>
    where
        K: Ord + Deserialize<ExtraInfo = ()> + Debug,
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
    ) -> Result<K, BTreeError> {
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
            PageKind::Heap | PageKind::BTreeMetaRoot | PageKind::Unitialized => unreachable!(),
        };
        Ok(K::from_bytes(&mut reader, &())?)
    }

    fn get_page_id_from_node_cell(page: &Page, position: u16) -> Result<PageId, BTreeError> {
        assert!(position < page.cell_count());
        assert!(matches!(page.kind(), PageKind::BTreeNode));
        let cell_bytes = page.get_cell(position);
        let mut reader = &cell_bytes[0..];
        Ok(PageId::from_bytes(&mut reader, &())?)
    }

    fn get_heap_insertion_data_from_leaf_cell(
        page: &Page,
        position: u16,
    ) -> Result<HeapInsertionData, BTreeError> {
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

    fn make_new_root(&mut self, first_cell: &[u8], second_cell: &[u8]) -> Result<(), BTreeError> {
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

    fn get_root_page_id(&self) -> Result<PageId, BTreeError> {
        let mut pager = self.pager.borrow_mut();
        let meta_root_ref = pager.get_page(self.fd, 0)?;
        let meta_root_page = meta_root_ref.borrow();
        assert!(matches!(meta_root_page.kind(), PageKind::BTreeMetaRoot));
        let ptr_bytes = meta_root_page.get_cell(METAROOT_ROOT_PTR_LOCATION);
        let mut reader = &ptr_bytes[..];
        Ok(PageId::from_bytes(&mut reader, &())?)
    }

    pub fn contains_key<K>(&self, key: &K) -> Result<bool, BTreeError>
    where
        K: Ord + Deserialize<ExtraInfo = ()> + Debug,
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
    fn traverse_to_leaf<K>(&self, key: &K) -> Result<TraversalResult, BTreeError>
    where
        K: Ord + Deserialize<ExtraInfo = ()> + Debug,
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
                SearchResult::NotFound(pos) => {
                    // if the key is greater than our rightmost, go-down the rightmost path anyways
                    if pos == page.cell_count() {
                        pos - 1
                    } else {
                        pos
                    }
                }
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

    pub fn lookup_value<K>(&self, key: &K) -> Result<Option<HeapInsertionData>, BTreeError>
    where
        K: Ord + Deserialize<ExtraInfo = ()> + Debug,
    {
        let traversal_res = self.traverse_to_leaf(key)?;
        let leaf = traversal_res.leaf.borrow();
        let search_result = BTree::binary_search_page(&leaf, key)?;
        match search_result {
            SearchResult::NotFound(_) => Ok(None),
            SearchResult::Found(pos) => Ok(Some(BTree::get_heap_insertion_data_from_leaf_cell(
                &leaf, pos,
            )?)),
        }
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
    fn make_node_cell<K>(key: &K, page_id: PageId) -> Result<Vec<u8>, BTreeError>
    where
        K: Serialize,
    {
        let mut bytes = Vec::with_capacity(10); // this is the absolute minimum size we'll need
        page_id.write_to_bytes(&mut bytes)?;
        key.write_to_bytes(&mut bytes)?;
        Ok(bytes)
    }

    fn make_leaf_cell<K>(key: &K, insertion_data: &HeapInsertionData) -> Result<Vec<u8>, BTreeError>
    where
        K: Serialize,
    {
        let mut bytes = Vec::with_capacity(12); // absolute minimum size
        insertion_data.page_id.write_to_bytes(&mut bytes)?;
        insertion_data.cell_position.write_to_bytes(&mut bytes)?;
        key.write_to_bytes(&mut bytes)?;
        Ok(bytes)
    }

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
    ) -> Result<Rc<RefCell<Page>>, BTreeError> {
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
        for idx in (split_location + 1..page.cell_count()).rev() {
            page.remove_cell(idx);
        }
        drop(new_page);
        Ok(new_page_ref)
    }

    fn split<K>(
        &mut self,
        traversal_res: TraversalResult,
        insert_location: u16,
        data: &[u8],
    ) -> Result<(), BTreeError>
    where
        K: Ord + Serialize + Deserialize<ExtraInfo = ()> + Debug,
    {
        let mut orig_page = traversal_res.leaf.borrow_mut();

        let split_location = BTree::split_location(&orig_page, insert_location, data.len() as u16);
        let new_page_ref = self.split_page_after(&mut orig_page, split_location)?;
        let mut new_page = new_page_ref.borrow_mut();

        // insert new data
        if insert_location <= split_location {
            orig_page.insert_cell(insert_location, data)?;
        } else {
            let loc = insert_location - split_location - 1;
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
            let search_key_location = match BTree::binary_search_page(&parent_page, &orig_page_key)?
            {
                SearchResult::Found(pos) => pos,
                SearchResult::NotFound(pos) => {
                    assert!(pos < parent_page.cell_count(), "The orig_key is always smaller than the prior right-most key, so this should always be true");
                    pos
                }
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
                    self.split::<K>(traversal_res, search_key_location + 1, &new_page_cell)
                }
                Err(err) => Err(BTreeError::Page(err)),
                Ok(_) => Ok(()),
            }
        } else {
            self.make_new_root(&orig_page_cell, &new_page_cell)
        }
    }

    fn fix_right_keys<K>(
        &mut self,
        new_key: &K,
        traversal_res: TraversalResult,
    ) -> Result<(), BTreeError>
    where
        K: Ord + Serialize + Deserialize<ExtraInfo = ()> + Debug,
    {
        let mut traversal_res = traversal_res;
        while let Some(node_page_ref) = traversal_res.breadcrumbs.pop() {
            let mut node_page = node_page_ref.borrow_mut();
            let right_pos = node_page.cell_count() - 1;
            let right_key = BTree::get_key_from_cell::<K>(&node_page, right_pos)?;
            if let Ordering::Less = right_key.cmp(new_key) {
                // update key
                let child_id = BTree::get_page_id_from_node_cell(&node_page, right_pos)?;
                node_page.remove_cell(right_pos);
                let new_cell = BTree::make_node_cell(new_key, child_id)?;
                node_page.insert_cell(right_pos, &new_cell)?;
                drop(node_page);
            } else {
                break;
            }
        }
        Ok(())
    }

    pub fn insert<K>(
        &mut self,
        key: &K,
        insertion_data: &HeapInsertionData,
    ) -> Result<(), BTreeError>
    where
        K: Ord + Serialize + Deserialize<ExtraInfo = ()> + Debug,
    {
        let traversal_res = self.traverse_to_leaf(key)?;
        let mut leaf_page = traversal_res.leaf.borrow_mut();
        let location = match BTree::binary_search_page(&leaf_page, key)? {
            SearchResult::NotFound(loc) => loc,
            SearchResult::Found(_) => return Err(BTreeError::KeyAlreadyExists),
        };
        let data = BTree::make_leaf_cell(key, insertion_data)?;
        if data.len() > self.max_cell_size.into() {
            return Err(BTreeError::KeyTooLarge);
        }
        match leaf_page.insert_cell(location, &data) {
            Err(PageError::NotEnoughSpace) => {
                drop(leaf_page);
                self.split::<K>(traversal_res, location, &data)?;
                Ok(())
            }
            Err(err) => Err(BTreeError::from(err)),
            Ok(_) => {
                if location == leaf_page.cell_count() - 1 {
                    drop(leaf_page);
                    self.fix_right_keys(key, traversal_res)?;
                }
                Ok(())
            }
        }
    }

    pub fn delete<K>(&mut self, key: &K) -> Result<(), BTreeError>
    where
        K: Ord + Deserialize<ExtraInfo = ()> + Debug,
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
    use std::{
        fs::{File, OpenOptions},
        os::fd::AsRawFd,
    };

    use crate::pager::CELL_POINTER_SIZE;

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

    fn insert_values(btree: &mut BTree, values: impl Iterator<Item = u64>) {
        for v in values {
            btree.insert(&v, &HeapInsertionData::new(v, 0)).unwrap();
        }
    }

    fn lookup_values(btree: &BTree, keys: impl Iterator<Item = u64>) -> Vec<u64> {
        let mut values = Vec::new();
        for key in keys {
            let insertion_info = btree.lookup_value(&key).unwrap().unwrap();
            values.push(insertion_info.page_id);
        }
        values
    }

    fn open_test_file(name: &str) -> File {
        OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(name)
            .unwrap()
    }

    #[test]
    fn btree_initialization() {
        let filename = "btree_initialization.test";
        let file = open_test_file(filename);
        let fd = file.as_raw_fd();

        let pager = Rc::new(RefCell::new(Pager::new(vec![file])));

        // new btree test
        let mut btree = BTree::init(pager.clone(), fd).unwrap();
        assert_eq!(btree.get_root_page_id().unwrap(), 1);

        // add cell to btree
        let data = HeapInsertionData::new(42, 43);
        let key = String::from("foo");
        btree.insert(&key, &data).unwrap();

        // flush all pages and drop the btree
        btree.force_flush().unwrap();
        drop(btree);

        // re-init the btree and check that we have the same data in it
        let btree = BTree::init(pager.clone(), fd).unwrap();
        assert_eq!(btree.get_root_page_id().unwrap(), 1);

        let lookup_res = btree.lookup_value(&key).unwrap();
        assert_eq!(lookup_res, Some(data));
    }

    #[test]
    fn insert_and_split() {
        // calc how many cells will fit in a leaf
        let leaf_cell = BTree::make_leaf_cell(&1u64, &HeapInsertionData::new(1, 1)).unwrap();
        let leaf_capacity =
            PAGE_BUFFER_SIZE as u64 / (leaf_cell.len() as u64 + CELL_POINTER_SIZE as u64);

        let filename = "btree_insert_and_split.test";
        let file = open_test_file(filename);
        let fd = file.as_raw_fd();
        let pager = Rc::new(RefCell::new(Pager::new(vec![file])));
        let mut btree = BTree::init(pager, fd).unwrap();

        // test inserts work
        insert_values(&mut btree, 0..leaf_capacity);
        // test lookups work
        let retrieved_vals = lookup_values(&btree, 0..leaf_capacity);
        assert_eq!((0..leaf_capacity).collect::<Vec<u64>>(), retrieved_vals);

        // test split root works when full
        insert_values(&mut btree, leaf_capacity..leaf_capacity + 1);
        let single_split_root_id = btree.get_root_page_id().unwrap();
        assert!(
            single_split_root_id > 1,
            "Prove that root was split, so we have a new root"
        );
        // test lookups work
        let retrieved_vals = lookup_values(&btree, 0..leaf_capacity + 1);
        assert_eq!((0..leaf_capacity + 1).collect::<Vec<u64>>(), retrieved_vals);

        // prove some things about the root page
        let mut pager = btree.pager.borrow_mut();
        let root_page_ref = pager.get_page(fd, single_split_root_id).unwrap();
        let root_page = root_page_ref.borrow();
        assert_eq!(root_page.cell_count(), 2);
        let mid_key = BTree::get_key_from_cell::<u64>(&root_page, 0).unwrap();
        assert_eq!(mid_key, (leaf_capacity / 2) - 1);
        let high_key = BTree::get_key_from_cell::<u64>(&root_page, 1).unwrap();
        assert_eq!(high_key, leaf_capacity);
        let left_page_id = BTree::get_page_id_from_node_cell(&root_page, 0).unwrap();
        let right_page_id = BTree::get_page_id_from_node_cell(&root_page, 1).unwrap();
        drop(root_page);

        // prove some things about the now split pages
        let left_page_ref = pager.get_page(fd, left_page_id).unwrap();
        let left_page = left_page_ref.borrow();
        let right_page_ref = pager.get_page(fd, right_page_id).unwrap();
        let right_page = right_page_ref.borrow();
        // prove we have the expected number of cells in each node, and that the counts are about
        // even
        assert_eq!(
            (right_page.cell_count() as i64 - left_page.cell_count() as i64).abs(),
            1
        );
        assert!((right_page.cell_count() as i64 - (leaf_capacity / 2) as i64).abs() <= 1);
        drop(left_page);
        drop(right_page);
        drop(pager);

        // insert enough values to make the right leaf split
        insert_values(
            &mut btree,
            (0..(leaf_capacity / 2)).map(|x| x + leaf_capacity + 1),
        );
        let vals = 0..(leaf_capacity + 1 + (leaf_capacity / 2));
        let retrieved_vals = lookup_values(&btree, vals.clone());
        assert_eq!(vals.collect::<Vec<u64>>(), retrieved_vals);

        // examine the root
        let mut pager = btree.pager.borrow_mut();
        let root_page_ref = pager.get_page(fd, single_split_root_id).unwrap();
        let root_page = root_page_ref.borrow();
        assert_eq!(
            BTree::get_key_from_cell::<u64>(&root_page, 0).unwrap(),
            (leaf_capacity / 2) - 1
        );
        assert_eq!(
            BTree::get_key_from_cell::<u64>(&root_page, 1).unwrap(),
            leaf_capacity - 1
        );
        let high_page_key = BTree::get_key_from_cell::<u64>(&root_page, 2).unwrap();
        assert_eq!(high_page_key, leaf_capacity + (leaf_capacity / 2));

        // examine the pages
        let left_page_id = BTree::get_page_id_from_node_cell(&root_page, 0).unwrap();
        let left_page_ref = pager.get_page(fd, left_page_id).unwrap();
        let left_page = left_page_ref.borrow();
        let mid_page_id = BTree::get_page_id_from_node_cell(&root_page, 1).unwrap();
        let mid_page_ref = pager.get_page(fd, mid_page_id).unwrap();
        let mid_page = mid_page_ref.borrow();
        let high_page_id = BTree::get_page_id_from_node_cell(&root_page, 2).unwrap();
        let high_page_ref = pager.get_page(fd, high_page_id).unwrap();
        let high_page = high_page_ref.borrow();

        // prove that all of the leaf occupancies are within 1 of each other
        let min = left_page
            .cell_count()
            .min(mid_page.cell_count())
            .min(high_page.cell_count());
        let max = left_page
            .cell_count()
            .max(mid_page.cell_count())
            .max(high_page.cell_count());
        assert!(max - min <= 1);
        drop(root_page);
        drop(high_page);
        drop(mid_page);
        drop(left_page);
        drop(pager);

        // Insert just enough to be 1 entry short of splitting the root again
        let node_cell = BTree::make_node_cell(&42u64, 23).unwrap();
        let node_capacity =
            PAGE_BUFFER_SIZE as u64 / (CELL_POINTER_SIZE as u64 + node_cell.len() as u64);
        let required_inserts_to_split_root = (node_capacity + 1) * (leaf_capacity / 2);
        let insertion_range = high_page_key + 1..required_inserts_to_split_root;
        // show that this is the required amount
        insert_values(&mut btree, insertion_range);
        assert_eq!(btree.get_root_page_id().unwrap(), single_split_root_id);
        let retrieved_vals = lookup_values(&btree, 0..required_inserts_to_split_root);
        assert_eq!(
            (0..required_inserts_to_split_root).collect::<Vec<u64>>(),
            retrieved_vals
        );

        // now add one more and prove root is split
        insert_values(
            &mut btree,
            required_inserts_to_split_root..required_inserts_to_split_root + 1,
        );
        assert_ne!(btree.get_root_page_id().unwrap(), single_split_root_id);
        let retrieved_vals = lookup_values(&btree, 0..required_inserts_to_split_root + 1);
        assert_eq!(
            (0..required_inserts_to_split_root + 1).collect::<Vec<u64>>(),
            retrieved_vals
        );
    }
}
