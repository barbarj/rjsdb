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

const SIBLING_LEFT_PTR_LOCATION: u16 = 0;
const SIBLING_RIGHT_PTR_LOCATION: u16 = 1;
const BTREE_FIRST_CELL_LOCATION: u16 = 2;

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

struct BTreeNode {
    page_ref: Rc<RefCell<Page>>,
}
impl BTreeNode {
    fn build(page_ref: Rc<RefCell<Page>>) -> Self {
        let page = page_ref.borrow();
        assert!(
            page.cell_count() >= 2,
            "This page should already contain sibling pointers"
        );
        drop(page);

        BTreeNode { page_ref }
    }

    fn build_new(page_ref: Rc<RefCell<Page>>) -> Result<Self, BTreeError> {
        let mut page = page_ref.borrow_mut();
        assert_eq!(page.cell_count(), 0, "This should be a fresh page");

        let mut left_ptr_bytes = Vec::with_capacity(8);
        page.id().write_to_bytes(&mut left_ptr_bytes)?;
        let mut right_ptr_bytes = Vec::with_capacity(8);
        page.id().write_to_bytes(&mut right_ptr_bytes)?;

        page.insert_cell(SIBLING_LEFT_PTR_LOCATION, &left_ptr_bytes)?;
        page.insert_cell(SIBLING_RIGHT_PTR_LOCATION, &right_ptr_bytes)?;
        drop(page);

        Ok(BTreeNode { page_ref })
    }

    fn page_id(&self) -> PageId {
        let page = self.page_ref.borrow();
        page.id()
    }

    fn total_free_space(&self) -> u16 {
        let page = self.page_ref.borrow();
        page.total_free_space()
    }

    fn member_count(&self) -> u16 {
        let page = self.page_ref.borrow();
        page.cell_count() - BTREE_FIRST_CELL_LOCATION
    }

    fn left_sibling_id(&self) -> Result<Option<PageId>, BTreeError> {
        let page = self.page_ref.borrow();
        let cell = page.get_cell(SIBLING_LEFT_PTR_LOCATION);
        let mut reader = &cell[..];
        let sibling_id = PageId::from_bytes(&mut reader, &())?;
        if sibling_id != self.page_id() {
            Ok(Some(sibling_id))
        } else {
            Ok(None)
        }
    }

    fn right_sibling_id(&self) -> Result<Option<PageId>, BTreeError> {
        let page = self.page_ref.borrow();
        let cell = page.get_cell(SIBLING_RIGHT_PTR_LOCATION);
        let mut reader = &cell[..];
        let sibling_id = PageId::from_bytes(&mut reader, &())?;
        if sibling_id != self.page_id() {
            Ok(Some(sibling_id))
        } else {
            Ok(None)
        }
    }

    fn kind(&self) -> NodeKind {
        let page = self.page_ref.borrow();
        match page.kind() {
            PageKind::BTreeNode => NodeKind::Node,
            PageKind::BTreeLeafNotHeap => NodeKind::NonHeapLeaf,
            PageKind::BTreeLeafHeap => NodeKind::HeapLeaf,
            PageKind::Heap | PageKind::Unitialized | PageKind::BTreeMetaRoot => unreachable!(),
        }
    }

    fn key_from_cell<K>(&self, position: u16) -> Result<K, BTreeError>
    where
        K: Deserialize<ExtraInfo = ()>,
    {
        let page = self.page_ref.borrow();
        let position = position + BTREE_FIRST_CELL_LOCATION;
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

    fn rightmost_key<K>(&self) -> Result<K, BTreeError>
    where
        K: Deserialize<ExtraInfo = ()>,
    {
        self.key_from_cell::<K>(self.member_count() - 1)
    }

    fn page_id_from_cell(&self, position: u16) -> Result<PageId, BTreeError> {
        assert!(position < self.member_count());
        assert!(matches!(self.kind(), NodeKind::Node));

        let position = position + BTREE_FIRST_CELL_LOCATION;
        let page = self.page_ref.borrow();
        let cell_bytes = page.get_cell(position);
        let mut reader = &cell_bytes[0..];
        Ok(PageId::from_bytes(&mut reader, &())?)
    }

    fn heap_insertion_data_from_leaf_cell(
        &self,
        position: u16,
    ) -> Result<HeapInsertionData, BTreeError> {
        assert!(position < self.member_count());
        assert!(matches!(self.kind(), NodeKind::HeapLeaf));

        let position = position + BTREE_FIRST_CELL_LOCATION;
        let page = self.page_ref.borrow();
        let cell_bytes = page.get_cell(position);
        let mut reader = &cell_bytes[..];
        let heap_page_id = PageId::from_bytes(&mut reader, &())?;
        let page_position = u16::from_bytes(&mut reader, &())?;
        Ok(HeapInsertionData {
            page_id: heap_page_id,
            cell_position: page_position,
        })
    }

    fn insert_cell(&mut self, position: u16, data: &[u8]) -> Result<(), BTreeError> {
        let mut page = self.page_ref.borrow_mut();
        let position = position + BTREE_FIRST_CELL_LOCATION;
        page.insert_cell(position, data)?;
        Ok(())
    }

    fn get_cell(&self, position: u16) -> Vec<u8> {
        assert!(position < self.member_count());
        let position = position + BTREE_FIRST_CELL_LOCATION;
        let page = self.page_ref.borrow();
        page.get_cell(position)
    }

    fn remove_cell(&mut self, position: u16) {
        assert!(position < self.member_count());
        let position = position + BTREE_FIRST_CELL_LOCATION;
        let mut page = self.page_ref.borrow_mut();
        page.remove_cell(position);
    }

    fn cell_size(&self, position: u16) -> u16 {
        assert!(position < self.member_count());
        let position = position + BTREE_FIRST_CELL_LOCATION;
        let page = self.page_ref.borrow();
        page.cell_size(position)
    }

    fn update_sibling_pointer(
        &mut self,
        direction: SiblingPointerDirection,
        new_page_id: PageId,
    ) -> Result<(), BTreeError> {
        let update_loc = match direction {
            SiblingPointerDirection::Left => SIBLING_LEFT_PTR_LOCATION,
            SiblingPointerDirection::Right => SIBLING_RIGHT_PTR_LOCATION,
        };
        let mut page = self.page_ref.borrow_mut();
        page.remove_cell(update_loc);

        let mut cell_bytes = Vec::with_capacity(8);
        new_page_id.write_to_bytes(&mut cell_bytes)?;

        page.insert_cell(update_loc, &cell_bytes)?;
        Ok(())
    }

    /// Searches the page. If the page contains the key (i.e., this is a data page and the key is
    /// present, or is not a data page, but happens to have a split key matching the key), return
    /// a SearchResult::Found containing the cell location the key was found in.
    /// If the key was not found, return a SearchResult::NotFound containing the location that the
    /// key would belong at if inserted into the page.
    fn binary_search_for_key<K>(&self, key: &K) -> Result<SearchResult, BTreeError>
    where
        K: Ord + Deserialize<ExtraInfo = ()>,
    {
        if self.member_count() == 0 {
            return Ok(SearchResult::NotFound(0));
        }
        let mut bottom = 0;
        let mut top = self.member_count() - 1;
        let mut pos = (top - bottom) / 2;
        while bottom < top {
            let pos_key = self.key_from_cell::<K>(pos)?;
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
        let pos_key = self.key_from_cell::<K>(pos)?;
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
}

enum SiblingPointerDirection {
    Left,
    Right,
}

enum NodeKind {
    Node,
    HeapLeaf,
    NonHeapLeaf,
}
impl NodeKind {
    fn into_page_kind(self) -> PageKind {
        match self {
            Self::Node => PageKind::BTreeNode,
            Self::HeapLeaf => PageKind::BTreeLeafHeap,
            Self::NonHeapLeaf => PageKind::BTreeLeafNotHeap,
        }
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

    fn new_node(&mut self, kind: NodeKind) -> Result<BTreeNode, BTreeError> {
        let mut pager = self.pager.borrow_mut();
        let new_page_ref = pager.new_page(self.fd, kind.into_page_kind())?;
        let node = BTreeNode::build_new(new_page_ref)?;
        Ok(node)
    }

    fn init_meta_root(&mut self) -> Result<(), BTreeError> {
        let mut pager = self.pager.borrow_mut();

        // init meta root page
        let meta_root_page_ref = pager.new_page(self.fd, PageKind::BTreeMetaRoot)?;
        let mut meta_root_page = meta_root_page_ref.borrow_mut();
        assert_eq!(meta_root_page.id(), 0);

        // say that page 1 is the root
        let mut id_bytes = Vec::new();
        1u64.write_to_bytes(&mut id_bytes)?;
        meta_root_page.insert_cell(METAROOT_ROOT_PTR_LOCATION, &id_bytes)?;

        // init new root page (with id of 1)
        drop(pager);
        let root_node = self.new_node(NodeKind::HeapLeaf)?;
        assert_eq!(root_node.page_id(), 1);

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

    fn make_new_root(&mut self, first_cell: &[u8], second_cell: &[u8]) -> Result<(), BTreeError> {
        // set up new root
        let mut new_root_node = self.new_node(NodeKind::Node)?;
        new_root_node.insert_cell(0, first_cell)?;
        new_root_node.insert_cell(1, second_cell)?;
        let mut new_root_page_id_bytes = Vec::new();
        new_root_node
            .page_id()
            .write_to_bytes(&mut new_root_page_id_bytes)?;

        // update meta root page
        let mut pager = self.pager.borrow_mut();
        let meta_root_ref = pager.get_page(self.fd, 0)?;
        let mut meta_root_page = meta_root_ref.borrow_mut();
        //remove old id
        meta_root_page.remove_cell(METAROOT_ROOT_PTR_LOCATION);
        //add new id
        meta_root_page.insert_cell(METAROOT_ROOT_PTR_LOCATION, &new_root_page_id_bytes)?;

        Ok(())
    }

    fn get_root_node(&self) -> Result<BTreeNode, BTreeError> {
        let page_id = self.get_root_page_id()?;
        let mut pager = self.pager.borrow_mut();
        let page_ref = pager.get_page(self.fd, page_id)?;
        Ok(BTreeNode::build(page_ref))
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
        match traversal_res.leaf.binary_search_for_key(key)? {
            SearchResult::Found(_) => Ok(true),
            SearchResult::NotFound(_) => Ok(false),
        }
    }
}

struct TraversalResult {
    leaf: BTreeNode,
    breadcrumbs: Vec<BTreeNode>,
}

impl BTree {
    fn traverse_to_leaf<K>(&self, key: &K) -> Result<TraversalResult, BTreeError>
    where
        K: Ord + Deserialize<ExtraInfo = ()>,
    {
        let mut node = self.get_root_node()?;
        let mut pager = self.pager.borrow_mut();
        let mut breadcrumbs = Vec::new();
        // traverse until we hit a leaf page
        while !matches!(node.kind(), NodeKind::HeapLeaf) {
            let position = match node.binary_search_for_key(key)? {
                SearchResult::Found(pos) => pos,
                SearchResult::NotFound(pos) => {
                    // if the key is greater than our rightmost, go-down the rightmost path anyways
                    if pos == node.member_count() {
                        pos - 1
                    } else {
                        pos
                    }
                }
            };
            let page_id = node.page_id_from_cell(position)?;
            breadcrumbs.push(node);
            node = BTreeNode::build(pager.get_page(self.fd, page_id)?);
        }
        Ok(TraversalResult {
            leaf: node,
            breadcrumbs,
        })
    }

    pub fn lookup_value<K>(&self, key: &K) -> Result<Option<HeapInsertionData>, BTreeError>
    where
        K: Ord + Deserialize<ExtraInfo = ()> + Debug,
    {
        let traversal_res = self.traverse_to_leaf(key)?;
        match traversal_res.leaf.binary_search_for_key(key)? {
            SearchResult::NotFound(_) => Ok(None),
            SearchResult::Found(pos) => Ok(Some(
                traversal_res.leaf.heap_insertion_data_from_leaf_cell(pos)?,
            )),
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

    fn split_location(node: &BTreeNode, insert_location: u16, insert_data_size: u16) -> u16 {
        assert!(node.member_count() > 0);
        let mut left_sum = 0;
        let mut left_loc = 0;
        let mut right_sum = 0;
        let mut right_loc = node.member_count() - 1;
        let mut used_insert = false;

        while left_loc <= right_loc {
            // try left side first
            if left_sum <= right_sum {
                if left_loc == insert_location && !used_insert {
                    left_sum += insert_data_size;
                    used_insert = true;
                } else {
                    left_sum += node.cell_size(left_loc);
                    left_loc += 1;
                }
            // otherwise use right
            } else if right_loc == insert_location && !used_insert {
                right_sum += insert_data_size;
                used_insert = true;
            } else {
                right_sum += node.cell_size(right_loc);
                right_loc -= 1;
            }
        }
        right_loc
    }

    fn split_node_after(
        &mut self,
        node: &mut BTreeNode,
        split_location: u16,
    ) -> Result<BTreeNode, BTreeError> {
        let mut new_node = self.new_node(node.kind())?;
        // copy cells to new page
        for idx in split_location + 1..node.member_count() {
            let cell = node.get_cell(idx);
            let new_pos = idx - (split_location + 1);
            new_node.insert_cell(new_pos, &cell).unwrap();
        }
        // remove cells from this one
        for idx in (split_location + 1..node.member_count()).rev() {
            node.remove_cell(idx);
        }
        // update pointers
        node.update_sibling_pointer(SiblingPointerDirection::Right, new_node.page_id())?;
        new_node.update_sibling_pointer(SiblingPointerDirection::Left, node.page_id())?;
        Ok(new_node)
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
        let mut traversal_res = traversal_res;
        let mut orig_node = traversal_res.leaf;

        let split_location = BTree::split_location(&orig_node, insert_location, data.len() as u16);
        let mut new_node = self.split_node_after(&mut orig_node, split_location)?;

        // insert new data
        if insert_location <= split_location {
            orig_node.insert_cell(insert_location, data)?;
        } else {
            let loc = insert_location - split_location - 1;
            new_node.insert_cell(loc, data)?;
        }

        let orig_node_key = orig_node.rightmost_key::<K>()?;
        let orig_node_id = orig_node.page_id();
        let orig_node_cell = BTree::make_node_cell(&orig_node_key, orig_node_id)?;

        let new_node_key = new_node.rightmost_key::<K>()?;
        let new_node_id = new_node.page_id();
        let new_node_cell = BTree::make_node_cell(&new_node_key, new_node_id)?;

        if let Some(parent_node) = traversal_res.breadcrumbs.pop() {
            let mut parent_node = parent_node;
            let search_key_location = match parent_node.binary_search_for_key(&orig_node_key)? {
                SearchResult::Found(pos) => pos,
                SearchResult::NotFound(pos) => {
                    assert!(pos < parent_node.member_count(), "The orig_key is always smaller than the prior right-most key, so this should always be true");
                    pos
                }
            };
            // remove the old key, replace it with one that contains the correct split key for the
            // orig page
            parent_node.remove_cell(search_key_location);
            parent_node.insert_cell(search_key_location, &orig_node_cell)?;

            // now try inserting the new cell
            match parent_node.insert_cell(search_key_location + 1, &new_node_cell) {
                Err(BTreeError::Page(PageError::NotEnoughSpace)) => {
                    traversal_res.leaf = parent_node;
                    self.split::<K>(traversal_res, search_key_location + 1, &new_node_cell)
                }
                Err(err) => Err(err),
                Ok(_) => Ok(()),
            }
        } else {
            self.make_new_root(&orig_node_cell, &new_node_cell)
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
        while let Some(mut node) = traversal_res.breadcrumbs.pop() {
            let right_pos = node.member_count() - 1;
            let right_key = node.rightmost_key::<K>()?;
            if let Ordering::Less = right_key.cmp(new_key) {
                // update key
                let child_id = node.page_id_from_cell(right_pos)?;
                node.remove_cell(right_pos);
                let new_cell = BTree::make_node_cell(new_key, child_id)?;
                node.insert_cell(right_pos, &new_cell)?;
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
        let mut traversal_res = self.traverse_to_leaf(key)?;
        let leaf_node = &mut traversal_res.leaf;
        let location = match leaf_node.binary_search_for_key(key)? {
            SearchResult::NotFound(loc) => loc,
            SearchResult::Found(_) => return Err(BTreeError::KeyAlreadyExists),
        };
        let data = BTree::make_leaf_cell(key, insertion_data)?;
        if data.len() > self.max_cell_size.into() {
            return Err(BTreeError::KeyTooLarge);
        }
        match leaf_node.insert_cell(location, &data) {
            Err(BTreeError::Page(PageError::NotEnoughSpace)) => {
                self.split::<K>(traversal_res, location, &data)?;
                Ok(())
            }
            Err(err) => Err(err),
            Ok(_) => {
                if location == leaf_node.member_count() - 1 {
                    self.fix_right_keys(key, traversal_res)?;
                }
                Ok(())
            }
        }
    }
}

enum MergeLeafPosition {
    Left,
    Right,
}

impl BTree {
    fn get_left_sibling(&mut self, node: &BTreeNode) -> Result<Option<BTreeNode>, BTreeError> {
        let mut pager = self.pager.borrow_mut();
        if let Some(id) = node.left_sibling_id()? {
            let left_sibling = BTreeNode::build(pager.get_page(self.fd, id)?);
            assert_eq!(left_sibling.right_sibling_id()?, Some(node.page_id()));
            Ok(Some(left_sibling))
        } else {
            Ok(None)
        }
    }

    fn get_right_sibling(&mut self, node: &BTreeNode) -> Result<Option<BTreeNode>, BTreeError> {
        let mut pager = self.pager.borrow_mut();
        if let Some(id) = node.right_sibling_id()? {
            let right_sibling = BTreeNode::build(pager.get_page(self.fd, id)?);
            assert_eq!(right_sibling.right_sibling_id()?, Some(node.page_id()));
            Ok(Some(right_sibling))
        } else {
            Ok(None)
        }
    }

    fn merge<K>(
        &mut self,
        traversal_res: TraversalResult,
        leaf_position: MergeLeafPosition,
    ) -> Result<(), BTreeError>
    where
        K: Ord + Serialize + Deserialize<ExtraInfo = ()>,
    {
        let mut traversal_res = traversal_res;
        let leaf = traversal_res.leaf;
        let mut parent = traversal_res.breadcrumbs.pop().unwrap();

        let (mut left_node, right_node) = match leaf_position {
            MergeLeafPosition::Left => {
                let other = self.get_right_sibling(&leaf)?.unwrap();
                (leaf, other)
            }
            MergeLeafPosition::Right => (self.get_left_sibling(&leaf)?.unwrap(), leaf),
        };

        let left_node_pos =
            match parent.binary_search_for_key::<K>(&left_node.rightmost_key::<K>()?)? {
                SearchResult::Found(pos) => pos,
                SearchResult::NotFound(pos) => pos,
            };
        let right_node_pos =
            match parent.binary_search_for_key::<K>(&right_node.rightmost_key::<K>()?)? {
                SearchResult::Found(pos) => pos,
                SearchResult::NotFound(pos) => pos,
            };

        // remove parent ptr cells
        parent.remove_cell(right_node_pos);
        parent.remove_cell(left_node_pos);

        // copy cells from right to left
        let left_offset = left_node.member_count();
        for i in 0..right_node.member_count() {
            let cell = right_node.get_cell(i);
            left_node.insert_cell(left_offset + i, &cell)?;
        }

        // add new pointer to parent
        let ptr_cell =
            BTree::make_node_cell(&left_node.rightmost_key::<K>()?, left_node.page_id())?;
        parent.insert_cell(left_node_pos, &ptr_cell)?;

        // delete right page
        let mut pager = self.pager.borrow_mut();
        pager.delete_page(self.fd, right_node.page_id())?;
        drop(pager);

        // if pages remaining above 'leaf', recurse
        if !traversal_res.breadcrumbs.is_empty() {
            traversal_res.leaf = parent;
            self.merge_if_needed::<K>(traversal_res)
        } else {
            Ok(())
        }
    }

    fn merge_if_needed<K>(&mut self, traversal_res: TraversalResult) -> Result<(), BTreeError>
    where
        K: Ord + Serialize + Deserialize<ExtraInfo = ()>,
    {
        let leaf_node = &traversal_res.leaf;
        if leaf_node.total_free_space() < (PAGE_BUFFER_SIZE / 2) {
            if let Some(left_node) = self.get_left_sibling(leaf_node)? {
                let is_same_parent = {
                    let parent = traversal_res
                        .breadcrumbs
                        .last()
                        .expect("A node with a sibling should always have a parent");
                    let parent_leftmost_key = parent.key_from_cell::<K>(0)?;
                    left_node.rightmost_key::<K>()? >= parent_leftmost_key
                };
                // if same parent and under-filled
                if is_same_parent && left_node.total_free_space() < (PAGE_BUFFER_SIZE / 2) {
                    self.merge::<K>(traversal_res, MergeLeafPosition::Right)?;
                    return Ok(());
                }
            }
            if let Some(right_node) = self.get_right_sibling(leaf_node)? {
                let is_same_parent = {
                    let parent = traversal_res
                        .breadcrumbs
                        .last()
                        .expect("A node with a sibling should always have a parent");
                    right_node.rightmost_key::<K>()? <= parent.rightmost_key::<K>()?
                };
                // if same parent and under-filled
                if is_same_parent && right_node.total_free_space() < (PAGE_BUFFER_SIZE / 2) {
                    self.merge::<K>(traversal_res, MergeLeafPosition::Left)?;
                    return Ok(());
                }
            }
        }
        Ok(())
    }

    pub fn delete<K>(&mut self, key: &K) -> Result<(), BTreeError>
    where
        K: Ord + Serialize + Deserialize<ExtraInfo = ()>,
    {
        let mut traversal_res = self.traverse_to_leaf(key)?;
        let leaf_node = &mut traversal_res.leaf;
        let location = match leaf_node.binary_search_for_key(key)? {
            SearchResult::Found(loc) => loc,
            SearchResult::NotFound(_) => return Ok(()),
        };
        leaf_node.remove_cell(location);
        /*
         * If page free space is greater than half:
         *  - Check left and right sibling pages to see if they are also under-filled.
         *  - Copy data from right page to left page.
         *  - Update parent pointers:
         *      - Remove right pointer
         *      - Update left pointer with new rightmost key
         */
        // TODO: Handle merge case
        self.merge_if_needed::<K>(traversal_res)?;
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

    fn make_empty_tree_node() -> BTreeNode {
        let page_ref = Rc::new(RefCell::new(Page::new(0, PageKind::BTreeNode)));
        BTreeNode::build_new(page_ref).unwrap()
    }

    fn make_empty_leaf_node() -> BTreeNode {
        let page_ref = Rc::new(RefCell::new(Page::new(0, PageKind::BTreeLeafHeap)));
        BTreeNode::build_new(page_ref).unwrap()
    }

    #[test]
    fn btree_node_initialization() {
        let node = make_empty_leaf_node();
        let page = node.page_ref.borrow();
        assert_eq!(page.cell_count(), 2); // 2 sibling pointers present
        drop(page);

        assert_eq!(node.member_count(), 0);
        assert_eq!(node.left_sibling_id().unwrap(), None);
        assert_eq!(node.right_sibling_id().unwrap(), None);
    }

    #[test]
    fn key_from_cell_works() {
        let kv_pairs: Vec<(u16, u64)> = vec![(0, 0), (1, 1), (2, 2), (3, 3), (4, 4)];
        let mut node = make_empty_tree_node();

        for (k, v) in kv_pairs.iter() {
            let cell = BTree::make_node_cell(k, *v).unwrap();
            node.insert_cell(*k, &cell).unwrap();
        }
        for (k, _) in kv_pairs.iter() {
            let read_key: u16 = node.key_from_cell(*k).unwrap();
            assert_eq!(read_key, *k);
        }
    }

    #[test]
    fn binary_search_odd_cell_count() {
        let kv_pairs: Vec<(u16, u64)> = vec![(0, 0), (1, 1), (2, 2), (3, 3), (4, 4)];
        let mut node = make_empty_tree_node();
        for (k, v) in kv_pairs.iter() {
            let cell = BTree::make_node_cell(k, *v).unwrap();
            node.insert_cell(*k, &cell).unwrap();
        }
        for (k, _) in kv_pairs.iter() {
            let res = node.binary_search_for_key(k).unwrap();
            assert_eq!(res, SearchResult::Found(*k));
        }
    }

    #[test]
    fn binary_search_even_cell_count() {
        let kv_pairs: Vec<(u16, u64)> = vec![(0, 0), (1, 1), (2, 2), (3, 3), (4, 4), (5, 5)];
        let mut node = make_empty_tree_node();
        for (k, v) in kv_pairs.iter() {
            let cell = BTree::make_node_cell(k, *v).unwrap();
            node.insert_cell(*k, &cell).unwrap();
        }
        for (k, _) in kv_pairs.iter() {
            let res = node.binary_search_for_key(k).unwrap();
            assert_eq!(res, SearchResult::Found(*k));
        }
    }

    #[test]
    fn binary_search_not_found_should_insert_right() {
        let kv_pairs: Vec<(u16, u16)> = vec![(0, 0), (2, 1), (4, 2), (6, 3), (8, 4), (10, 5)];
        let mut node = make_empty_tree_node();
        for (k, v) in kv_pairs.iter() {
            let cell = BTree::make_node_cell(k, *v as PageId).unwrap();
            node.insert_cell(*v, &cell).unwrap();
        }
        for (k, v) in kv_pairs.iter() {
            let search_for = *k + 1;
            let res = node.binary_search_for_key(&search_for).unwrap();
            assert_eq!(res, SearchResult::NotFound(*v + 1));
        }
    }

    #[test]
    fn binary_search_not_found_should_insert_left() {
        let kv_pairs: Vec<(u16, u16)> = vec![(3, 0), (5, 0), (7, 1), (9, 2), (11, 3), (13, 4)];
        let mut node = make_empty_tree_node();
        for (idx, (k, v)) in kv_pairs.iter().enumerate() {
            let cell = BTree::make_node_cell(k, *v as PageId).unwrap();
            node.insert_cell(idx as u16, &cell).unwrap();
        }
        for (k, v) in kv_pairs.iter() {
            let search_for = *k - 3;
            let res = node.binary_search_for_key(&search_for).unwrap();
            assert_eq!(res, SearchResult::NotFound(*v));
        }
    }

    #[test]
    fn binary_search_empty_page() {
        let node = make_empty_tree_node();
        let res = node.binary_search_for_key(&43).unwrap();
        assert_eq!(res, SearchResult::NotFound(0));
    }

    #[test]
    fn node_cell_construction() {
        let mut node = make_empty_tree_node();
        let page_id = 42;
        let key = String::from("foo");
        let cell = BTree::make_node_cell(&key, page_id).unwrap();
        node.insert_cell(0, &cell).unwrap();

        assert_eq!(key, node.key_from_cell::<String>(0).unwrap(),);
        assert_eq!(page_id, node.page_id_from_cell(0).unwrap(),);
    }

    #[test]
    fn leaf_cell_construction() {
        let mut node = make_empty_leaf_node();
        let page_id = 42;
        let page_location = 43;
        let key = String::from("foo");
        let heap_data = HeapInsertionData::new(page_id, page_location);
        let cell = BTree::make_leaf_cell(&key, &heap_data).unwrap();
        node.insert_cell(0, &cell).unwrap();

        assert_eq!(key, node.key_from_cell::<String>(0).unwrap(),);
        assert_eq!(
            heap_data,
            node.heap_insertion_data_from_leaf_cell(0).unwrap(),
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
        let root_node = btree.get_root_node().unwrap();
        assert_eq!(root_node.page_id(), 1);
        let page = root_node.page_ref.borrow();
        assert_eq!(page.cell_count(), 2);
        drop(page);
        assert_eq!(root_node.member_count(), 0);
        drop(root_node);

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
        let empty_node = make_empty_tree_node();
        let leaf_cell = BTree::make_leaf_cell(&1u64, &HeapInsertionData::new(1, 1)).unwrap();
        let leaf_capacity = empty_node.total_free_space() as u64
            / (leaf_cell.len() as u64 + CELL_POINTER_SIZE as u64);
        println!("capacity: {leaf_capacity}");

        let expected_rightmost_after_splits = |page_count: u64| -> u64 {
            let half_page_size = if leaf_capacity % 2 == 0 {
                leaf_capacity / 2
            } else {
                (leaf_capacity / 2) + 1
            };

            let mut rightmost = leaf_capacity / 2;
            for _ in 2..=page_count {
                rightmost += half_page_size;
            }
            rightmost
        };

        // calc how many cells will fit in a leaf
        // setup btree
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

        // prove we haven't split yet
        let root_node = btree.get_root_node().unwrap();
        assert_eq!(root_node.page_id(), 1);

        // test split root works when full
        insert_values(&mut btree, leaf_capacity..leaf_capacity + 1);
        let single_split_root_id = btree.get_root_page_id().unwrap();
        assert!(
            single_split_root_id > 1,
            "Prove that root was split, so we have a new root"
        );
        // test lookups work
        let retrieved_vals = lookup_values(&btree, 0..leaf_capacity + 1);
        assert_eq!(
            (0..expected_rightmost_after_splits(2) + 1).collect::<Vec<u64>>(),
            retrieved_vals
        );

        // prove some things about the root page
        let root_node = btree.get_root_node().unwrap();
        assert_eq!(root_node.member_count(), 2);
        assert_eq!(
            root_node.key_from_cell::<u64>(0).unwrap(),
            expected_rightmost_after_splits(1)
        );
        assert_eq!(
            root_node.key_from_cell::<u64>(1).unwrap(),
            expected_rightmost_after_splits(2)
        );
        let left_page_id = root_node.page_id_from_cell(0).unwrap();
        let right_page_id = root_node.page_id_from_cell(1).unwrap();

        // prove some things about the now split pages
        let mut pager = btree.pager.borrow_mut();
        let left_page_ref = pager.get_page(fd, left_page_id).unwrap();
        let left_node = BTreeNode::build(left_page_ref);
        let right_page_ref = pager.get_page(fd, right_page_id).unwrap();
        let right_node = BTreeNode::build(right_page_ref);
        // prove we have the expected number of cells in each node, and that the counts are about
        // even
        assert!((right_node.member_count() as i64 - left_node.member_count() as i64).abs() <= 1);
        assert!((right_node.member_count() as i64 - (leaf_capacity / 2) as i64).abs() <= 1);
        drop(pager);

        // insert enough values to make the right leaf split
        insert_values(
            &mut btree,
            expected_rightmost_after_splits(2) + 1..expected_rightmost_after_splits(3) + 1,
        );
        let vals = 0..expected_rightmost_after_splits(3) + 1;
        let retrieved_vals = lookup_values(&btree, vals.clone());
        assert_eq!(vals.collect::<Vec<u64>>(), retrieved_vals);

        // examine the root
        let root_node = btree.get_root_node().unwrap();
        assert_eq!(root_node.member_count(), 3);
        assert_eq!(
            root_node.key_from_cell::<u64>(0).unwrap(),
            expected_rightmost_after_splits(1)
        );
        assert_eq!(
            root_node.key_from_cell::<u64>(1).unwrap(),
            expected_rightmost_after_splits(2),
        );
        assert_eq!(
            root_node.key_from_cell::<u64>(2).unwrap(),
            expected_rightmost_after_splits(3),
        );

        // examine the pages
        let mut pager = btree.pager.borrow_mut();
        let left_page_id = root_node.page_id_from_cell(0).unwrap();
        let left_node = BTreeNode::build(pager.get_page(fd, left_page_id).unwrap());
        let mid_page_id = root_node.page_id_from_cell(1).unwrap();
        let mid_node = BTreeNode::build(pager.get_page(fd, mid_page_id).unwrap());
        let high_page_id = root_node.page_id_from_cell(2).unwrap();
        let high_node = BTreeNode::build(pager.get_page(fd, high_page_id).unwrap());

        // prove that all of the leaf occupancies are within 1 of each other
        let min = left_node
            .member_count()
            .min(mid_node.member_count())
            .min(high_node.member_count());
        let max = left_node
            .member_count()
            .max(mid_node.member_count())
            .max(high_node.member_count());
        assert!(max - min <= 1);
        drop(pager);

        // Insert just enough to be 1 entry short of splitting the root again
        let node_cell = BTree::make_node_cell(&42u64, 23).unwrap();
        let node_capacity = empty_node.total_free_space() as u64
            / (CELL_POINTER_SIZE as u64 + node_cell.len() as u64);
        println!("node_capacity: {node_capacity}");
        let required_inserts_to_split_root = expected_rightmost_after_splits(node_capacity + 1);
        println!("required_inserts_to_split_root: {required_inserts_to_split_root}");
        let insertion_range =
            expected_rightmost_after_splits(3) + 1..required_inserts_to_split_root;

        // to show that required amount is actually what caused the split, not sooner
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
