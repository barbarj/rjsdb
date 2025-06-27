#![allow(dead_code)]

use std::{
    cell::RefCell,
    cmp::Ordering,
    fmt::{Debug, Display},
    marker::PhantomData,
    os::fd::AsRawFd,
    rc::Rc,
};

#[cfg(test)]
use std::iter::Peekable;

use crate::pager::{
    PageBuffer, PageBufferOffset, PageError, PageId, PageKind, PageRef, Pager, PagerError,
    CELL_POINTER_SIZE,
};

#[cfg(test)]
use itertools::Itertools;

use serde::{de::DeserializeOwned, Serialize};
use serialize::{from_reader, serialized_size, to_bytes, Error as SerdeError};

/// # Notes on Page Structure
/// - Leaf node cells are (K, V). Cells at index 0 and 1 are left and right page ids to siblings.
///   page_id 0 is the default, and effectively means None
/// - Internal nodes alternate PageIds and keys, so the cell order looks like:
///    PageId | Key | PageId | Key | PageId... etc.
///    The sequence always starts and end with PageIds. The Keys split the search space that the
///    PageIds represent.

#[derive(Debug)]
pub enum Error {
    Page(PageError),
    Pager(PagerError),
    Serde(SerdeError),
}
impl From<PageError> for Error {
    fn from(value: PageError) -> Self {
        Self::Page(value)
    }
}
impl From<PagerError> for Error {
    fn from(value: PagerError) -> Self {
        Self::Pager(value)
    }
}
impl From<SerdeError> for Error {
    fn from(value: SerdeError) -> Self {
        Self::Serde(value)
    }
}
impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Page(error) => std::fmt::Display::fmt(&error, f),
            Self::Pager(error) => std::fmt::Display::fmt(&error, f),
            Self::Serde(error) => std::fmt::Display::fmt(&error, f),
        }
    }
}
impl std::error::Error for Error {}

type Result<T> = std::result::Result<T, Error>;

pub struct BTree<
    Fd: AsRawFd,
    PB: PageBuffer,
    K: Ord + Serialize + DeserializeOwned + Debug,
    V: Serialize + DeserializeOwned,
> {
    pager_ref: Rc<RefCell<Pager<PB>>>,
    backing_fd: Fd,
    root: Node<PB, K, V>,
    _key: PhantomData<K>,
    _value: PhantomData<V>,
}
impl<
        Fd: AsRawFd + Copy,
        PB: PageBuffer,
        K: Ord + Serialize + DeserializeOwned + Debug,
        V: Serialize + DeserializeOwned,
    > BTree<Fd, PB, K, V>
{
    pub fn init(pager_ref: Rc<RefCell<Pager<PB>>>, backing_fd: Fd) -> Result<Self> {
        let mut pager = pager_ref.borrow_mut();
        let root = if pager.file_has_page(&backing_fd, 0) {
            let node = Node::new(pager.get_page(backing_fd, 0)?);
            drop(pager);
            node
        } else {
            drop(pager);
            Node::init_leaf(&mut PagerInfo::new(pager_ref.clone(), backing_fd))?
        };

        assert_eq!(root.page_id(), 0);
        Ok(BTree {
            pager_ref,
            backing_fd,
            root,
            _key: PhantomData,
            _value: PhantomData,
        })
    }

    fn pager_info(&self) -> PagerInfo<PB, Fd> {
        PagerInfo::new(self.pager_ref.clone(), self.backing_fd)
    }

    pub fn insert(&mut self, key: K, value: V) -> Result<()> {
        let mut pager_info = self.pager_info();
        let insert_res = self.root.insert(key, value, &mut pager_info)?;
        if let InsertResult::Split(split_key, new_page_id_right) = insert_res {
            // get a new page to move data to, representing the left side of the split
            let new_page_left_ref = pager_info.new_page(self.root.page_kind())?;
            let mut new_page_left = new_page_left_ref.borrow_mut();

            // move data currently on the root page to the new page
            let mut root_page = self.root.page_ref.borrow_mut();
            for (i, bytes) in root_page.cell_bytes_iter().enumerate() {
                new_page_left.insert_cell(i as u16, bytes)?;
            }
            root_page.clear_data();

            let new_page_id_left = new_page_left.id();
            drop(new_page_left);

            // update root with new children
            root_page.insert_cell(0, &to_bytes(&new_page_id_left)?)?;
            root_page.insert_cell(1, &to_bytes(&split_key)?)?;
            root_page.insert_cell(2, &to_bytes(&new_page_id_right)?)?;

            root_page.set_kind(PageKind::BTreeNode);
        }
        Ok(())
    }

    pub fn get(&self, key: &K) -> Result<Option<V>> {
        let mut pager_info = self.pager_info();
        self.root.get(key, &mut pager_info)
    }
}

struct PagerInfo<PB: PageBuffer, Fd: AsRawFd + Copy> {
    pager_ref: Rc<RefCell<Pager<PB>>>,
    backing_fd: Fd,
}
impl<PB: PageBuffer, Fd: AsRawFd + Copy> PagerInfo<PB, Fd> {
    fn new(pager_ref: Rc<RefCell<Pager<PB>>>, backing_fd: Fd) -> Self {
        PagerInfo {
            pager_ref,
            backing_fd,
        }
    }

    fn new_page(&mut self, kind: PageKind) -> Result<PageRef<PB>> {
        let mut pager = self.pager_ref.borrow_mut();
        let new_page = pager.new_page(self.backing_fd, kind)?;
        Ok(new_page)
    }

    fn get_page(&mut self, page_id: PageId) -> Result<PageRef<PB>> {
        let mut pager = self.pager_ref.borrow_mut();
        let page = pager.get_page(self.backing_fd, page_id)?;
        Ok(page)
    }

    fn page_node<K, V>(&mut self, page_id: PageId) -> Result<Node<PB, K, V>>
    where
        K: Ord + Debug + Serialize + DeserializeOwned,
        V: Serialize + DeserializeOwned,
    {
        let page = self.get_page(page_id)?;
        Ok(Node::new(page))
    }

    fn new_page_node<K, V>(&mut self, kind: PageKind) -> Result<Node<PB, K, V>>
    where
        K: Ord + Debug + Serialize + DeserializeOwned,
        V: Serialize + DeserializeOwned,
    {
        let page = self.new_page(kind)?;
        Ok(Node::new(page))
    }

    fn buffer_size(&self) -> PageBufferOffset {
        PB::buffer_size()
    }

    fn leaf_effective_buffer_size(&self) -> PageBufferOffset {
        let dummy_id: PageId = 0;
        let siblings_space_used = (serialized_size(&dummy_id) as u16 + CELL_POINTER_SIZE) * 2;
        PB::buffer_size() - siblings_space_used
    }
}

enum InsertResult<K: Ord + Serialize + DeserializeOwned + Debug> {
    Split(K, PageId),
    Done,
}

// TODO: Convert the use of DeserializeOwned to a Deserialization of borrowed data (will need to
// get serialization format to support borrowed data
struct Node<
    PB: PageBuffer,
    K: Ord + Debug + Serialize + DeserializeOwned,
    V: Serialize + DeserializeOwned,
> {
    page_ref: PageRef<PB>,
    _key: PhantomData<K>,
    _value: PhantomData<V>,
}
impl<
        PB: PageBuffer,
        K: Ord + Debug + Serialize + DeserializeOwned,
        V: Serialize + DeserializeOwned,
    > Node<PB, K, V>
{
    fn new(page_ref: PageRef<PB>) -> Self {
        Node {
            page_ref,
            _key: PhantomData,
            _value: PhantomData,
        }
    }

    fn init_leaf<Fd: AsRawFd + Copy>(pager_info: &mut PagerInfo<PB, Fd>) -> Result<Node<PB, K, V>> {
        let page_ref = pager_info.new_page(PageKind::BTreeLeaf)?;
        let mut page = page_ref.borrow_mut();
        let zero_page_id: PageId = 0;
        page.insert_cell(0, &to_bytes(&zero_page_id)?)?;
        page.insert_cell(1, &to_bytes(&zero_page_id)?)?;
        drop(page);
        Ok(Node::new(page_ref))
    }

    fn init_node<Fd: AsRawFd + Copy>(pager_info: &mut PagerInfo<PB, Fd>) -> Result<Node<PB, K, V>> {
        let page_ref = pager_info.new_page(PageKind::BTreeNode)?;
        Ok(Node::new(page_ref))
    }

    fn key_count(&self) -> u16 {
        let page = self.page_ref.borrow();
        if self.is_leaf() {
            // -2 to account for sibling pointers
            page.cell_count() - 2
        } else {
            (page.cell_count() - 1) / 2
        }
    }

    fn can_fit_leaf(&self, key: &K, value: &V) -> bool {
        assert!(self.is_leaf());
        let needed_space: usize = serialized_size(&(key, value)) + CELL_POINTER_SIZE as usize;
        assert!(needed_space <= u16::MAX.into());
        let page = self.page_ref.borrow();
        page.can_fit_data(needed_space as u16)
    }

    fn can_fit_node(&self, key: &K) -> bool {
        assert!(self.is_node());
        let dummy_id: PageId = 42;
        let needed_space =
            serialized_size(&key) + serialized_size(&dummy_id) + (2 * CELL_POINTER_SIZE as usize);
        assert!(needed_space <= u16::MAX.into());
        let page = self.page_ref.borrow();
        page.can_fit_data(needed_space as u16)
    }

    fn is_leaf(&self) -> bool {
        let page = self.page_ref.borrow();
        matches!(page.kind(), PageKind::BTreeLeaf)
    }

    fn is_node(&self) -> bool {
        let page = self.page_ref.borrow();
        matches!(page.kind(), PageKind::BTreeNode)
    }

    fn page_id(&self) -> PageId {
        let page = self.page_ref.borrow();
        page.id()
    }

    fn page_kind(&self) -> PageKind {
        let page = self.page_ref.borrow();
        page.kind()
    }

    fn page_free_space(&self) -> u16 {
        let page = self.page_ref.borrow();
        page.total_free_space()
    }

    fn key_from_leaf(&self, pos: u16) -> Result<K> {
        assert!(self.is_leaf());
        let page = self.page_ref.borrow();
        // +2 to account for sibling pointers
        let (key, _): (K, V) = from_reader(page.cell_bytes(pos + 2))?;
        Ok(key)
    }

    fn value_from_leaf<T: DeserializeOwned>(&self, pos: u16) -> Result<T> {
        assert!(self.is_leaf());
        let page = self.page_ref.borrow();
        // +2 to account for sibling pointers
        let (_, val): (K, T) = from_reader(page.cell_bytes(pos + 2))?;
        Ok(val)
    }

    fn leaf_left_sibling(&self) -> Result<PageId> {
        assert!(self.is_leaf());
        let page = self.page_ref.borrow();
        assert!(page.cell_count() > 2);
        Ok(from_reader(page.cell_bytes(0))?)
    }

    /// Returns the prior sibling
    fn leaf_replace_left_sibling(&mut self, new_left: &PageId) -> Result<PageId> {
        assert!(self.is_leaf());
        let mut page = self.page_ref.borrow_mut();
        assert!(page.cell_count() >= 2);
        let prior_left = from_reader(page.cell_bytes(0))?;
        page.remove_cell(0);
        page.insert_cell(0, &to_bytes(new_left)?)?;
        Ok(prior_left)
    }

    /// Returns the prior sibling
    fn leaf_replace_right_sibling(&mut self, new_right: &PageId) -> Result<PageId> {
        assert!(self.is_leaf());
        let mut page = self.page_ref.borrow_mut();
        assert!(page.cell_count() >= 2);
        let prior_right = from_reader(page.cell_bytes(1))?;
        page.remove_cell(1);
        page.insert_cell(1, &to_bytes(new_right)?)?;
        Ok(prior_right)
    }

    fn leaf_right_sibling(&self) -> Result<PageId> {
        assert!(self.is_leaf());
        let page = self.page_ref.borrow();
        assert!(page.cell_count() > 2);
        Ok(from_reader(page.cell_bytes(1))?)
    }

    fn key_pos_to_cell_pos(key_pos: u16) -> u16 {
        (key_pos * 2) + 1
    }

    fn id_pos_to_cell_pos(id_pos: u16) -> u16 {
        id_pos * 2
    }

    /// Returns None if this cell position will not contain a key
    fn cell_pos_to_key_pos(cell_pos: u16) -> Option<u16> {
        if cell_pos % 2 == 0 {
            None
        } else {
            Some(cell_pos / 2) // integer division makes the division of an odd number (2n + 1) by
                               // 2 result in the same number as if the input were the odd number's
                               // even counterpart (2n)
        }
    }

    fn key_from_inner_node(&self, key_pos: u16) -> Result<K> {
        assert!(self.is_node());
        let pos = Self::key_pos_to_cell_pos(key_pos);
        let page = self.page_ref.borrow();
        let key = from_reader(page.cell_bytes(pos))?;
        Ok(key)
    }

    fn page_id_from_inner_node(&self, id_pos: u16) -> Result<PageId> {
        assert!(self.is_node());
        let pos = Self::id_pos_to_cell_pos(id_pos);
        let page = self.page_ref.borrow();
        let page_id = from_reader(page.cell_bytes(pos))?;
        Ok(page_id)
    }

    fn key_at_pos(&self, pos: u16) -> Result<K> {
        if self.is_node() {
            self.key_from_inner_node(pos)
        } else {
            self.key_from_leaf(pos)
        }
    }

    // TODO: Figure out if I should remove unwraps
    fn binary_search_keys(&self, key: &K) -> std::result::Result<u16, u16> {
        if self.key_count() == 0 {
            return Err(0);
        }
        let mut low = 0;
        let mut high = self.key_count() - 1;
        while low < high {
            let mid = (low + high) / 2;
            let cell_key = self.key_at_pos(mid).unwrap();
            match &cell_key.cmp(key) {
                Ordering::Less => {
                    low = mid + 1;
                }
                Ordering::Equal => return Ok(mid),
                Ordering::Greater => high = mid,
            }
        }
        let cell_key = self.key_at_pos(low).unwrap();
        match &cell_key.cmp(key) {
            Ordering::Greater => Err(low),
            Ordering::Equal => Ok(low),
            Ordering::Less => Err(low + 1),
        }
    }

    fn split_node<Fd: AsRawFd + Copy>(
        &mut self,
        pager_info: &mut PagerInfo<PB, Fd>,
    ) -> Result<(K, Node<PB, K, V>)> {
        let half = pager_info.buffer_size() / 2;
        assert!(self.page_free_space() < half);
        let mut used_space = 0;
        let mut idx = 0;
        let page = self.page_ref.borrow();

        // Find the index of the first "position" at or past the halfway point
        while used_space <= half {
            let id_ptr = page.get_cell_pointer(Self::id_pos_to_cell_pos(idx));
            let key_ptr = page.get_cell_pointer(Self::key_pos_to_cell_pos(idx));
            used_space += id_ptr.size + key_ptr.size + (2 * CELL_POINTER_SIZE);
            idx += 1;
        }
        idx -= 1; // undo the last increment

        // self.key_from_inner_node uses the logical key position amongst other keys, so convert to
        // that before asking for the key
        let split_key = self.key_from_inner_node(idx)?;
        let split_key_pos = Self::key_pos_to_cell_pos(idx);

        // get new page
        let new_node = Self::init_node(pager_info)?;
        let mut new_page = new_node.page_ref.borrow_mut();

        // copy cells to new page, starting with the cell after the split key
        let cells_skipped = (Self::key_pos_to_cell_pos(idx) + 1).into();
        for (i, bytes) in page.cell_bytes_iter().skip(cells_skipped).enumerate() {
            new_page.insert_cell(i as u16, bytes)?;
        }

        drop(page);
        // remove moved cells, plus the now hanging right key from this node
        let mut page = self.page_ref.borrow_mut();
        for i in (split_key_pos..page.cell_count()).rev() {
            page.remove_cell(i);
        }
        drop(new_page);
        drop(page);
        Ok((split_key, new_node))
    }

    fn split_leaf<Fd: AsRawFd + Copy>(
        &mut self,
        pager_info: &mut PagerInfo<PB, Fd>,
    ) -> Result<(K, Node<PB, K, V>)> {
        let half = pager_info.leaf_effective_buffer_size() / 2;
        let mut used_space = 0;
        let mut idx = 2; // start at 2 to skip the sibling pointers
        let page = self.page_ref.borrow();

        // Find the index of the first cell that begins past the halfway point
        while used_space < half {
            let ptr = page.get_cell_pointer(idx);
            used_space += ptr.size + CELL_POINTER_SIZE;
            idx += 1;
        }
        // keys point left, and cell number idx is going to be the first cell in the new page,
        // so the split key should be one to the left. (minus another 2 to account for our sibling
        // pointer offset
        assert!(idx > 2);
        let split_key = self.key_from_leaf(idx - 3)?;
        println!("split key: {split_key:?}");

        drop(page);

        // get new page
        let mut new_node = Self::init_leaf(pager_info)?;

        // update sibling pointers
        let old_right = self.leaf_replace_right_sibling(&new_node.page_id())?;
        new_node.leaf_replace_left_sibling(&self.page_id())?;
        new_node.leaf_replace_right_sibling(&old_right)?;

        // copy cells to new page and remove cells from old page
        let mut new_page = new_node.page_ref.borrow_mut();
        let mut page = self.page_ref.borrow_mut();
        for (i, _) in (idx..page.cell_count()).enumerate() {
            let insert_at = (i as u16) + 2; // skip sibling pointers
            new_page.insert_cell(insert_at, page.cell_bytes(idx))?;
            page.remove_cell(idx);
        }
        drop(new_page);

        Ok((split_key, new_node))
    }

    fn insert_as_leaf<Fd: AsRawFd + Copy>(
        &mut self,
        key: K,
        value: V,
        pager_info: &mut PagerInfo<PB, Fd>,
    ) -> Result<InsertResult<K>> {
        assert!(self.is_leaf());
        if let Ok(pos) = self.binary_search_keys(&key) {
            // this leaf already has this key, so we can just remove it and insert the new one. No
            // need to check space requirements because we're using existing space
            let pos = pos + 2; // to account for sibling pointers
            let mut page = self.page_ref.borrow_mut();
            page.remove_cell(pos);
            page.insert_cell(pos, &to_bytes(&(key, value))?)?;
            Ok(InsertResult::Done)
        } else if !self.can_fit_leaf(&key, &value) {
            let (split_key, mut new_node) = self.split_leaf(pager_info)?;
            assert!(new_node.is_leaf());
            if key > split_key {
                new_node.insert_as_leaf(key, value, pager_info)?;
            } else {
                self.insert_as_leaf(key, value, pager_info)?;
            }
            Ok(InsertResult::Split(split_key, new_node.page_id()))
        } else {
            match self.binary_search_keys(&key) {
                Ok(_) => {
                    unreachable!();
                }
                Err(pos) => {
                    let pos = pos + 2; // to account for sibling pointers
                    let mut page = self.page_ref.borrow_mut();
                    page.insert_cell(pos, &to_bytes(&(key, value))?)?;
                }
            }
            Ok(InsertResult::Done)
        }
    }

    /// For node searches, we only care about which child to descend to,
    /// so an exact match doesn't provide any additional information
    fn search_keys_as_node(&self, key: &K) -> u16 {
        match self.binary_search_keys(key) {
            Ok(pos) => pos,
            Err(pos) => pos,
        }
    }

    fn descendent_node_at_pos<Fd: AsRawFd + Copy>(
        &self,
        pos: u16,
        pager_info: &mut PagerInfo<PB, Fd>,
    ) -> Result<Node<PB, K, V>> {
        assert!(self.is_node());
        pager_info.page_node(self.page_id_from_inner_node(pos)?)
    }

    fn get_descendent_by_key<Fd: AsRawFd + Copy>(
        &self,
        key: &K,
        pager_info: &mut PagerInfo<PB, Fd>,
    ) -> Result<(u16, Node<PB, K, V>)> {
        assert!(self.is_node());
        let pos = self.search_keys_as_node(key);
        let descendent = self.descendent_node_at_pos(pos, pager_info)?;
        Ok((pos, descendent))
    }

    fn insert_split_results_into_node(
        &mut self,
        pos: u16,
        split_key: &K,
        new_page_id: PageId,
    ) -> Result<()> {
        let prior_key = self.replace_inner_node_key(pos, split_key)?;
        let id_cell_pos = Self::id_pos_to_cell_pos(pos + 1);
        let mut page = self.page_ref.borrow_mut();
        page.insert_cell(id_cell_pos, &to_bytes(&new_page_id)?)?;
        if let Some(k) = prior_key {
            page.insert_cell(id_cell_pos + 1, &to_bytes(&k)?)?;
        }
        Ok(())
    }

    /// replaces the key at key position pos with the new key, and returns the old key if there was
    /// one at that position
    fn replace_inner_node_key(&mut self, key_pos: u16, new_key: &K) -> Result<Option<K>> {
        assert!(key_pos <= self.key_count());
        let old_key = if key_pos < self.key_count() {
            Some(self.key_from_inner_node(key_pos)?)
        } else {
            None
        };
        let cell_idx = Self::key_pos_to_cell_pos(key_pos);
        let mut page = self.page_ref.borrow_mut();
        if old_key.is_some() {
            page.remove_cell(cell_idx);
        }
        page.insert_cell(cell_idx, &to_bytes(new_key)?)?;
        Ok(old_key)
    }

    fn insert_as_node<Fd: AsRawFd + Copy>(
        &mut self,
        key: K,
        value: V,
        pager_info: &mut PagerInfo<PB, Fd>,
    ) -> Result<InsertResult<K>> {
        assert!(self.is_node());
        let (pos, mut child_node) = self.get_descendent_by_key(&key, pager_info)?;
        if let InsertResult::Split(split_key, new_page_id) =
            child_node.insert(key, value, pager_info)?
        {
            if !self.can_fit_node(&split_key) {
                let (parent_split_key, mut parent_new_node) = self.split_node(pager_info)?;
                assert!(parent_new_node.is_node());

                if pos < self.key_count() {
                    self.insert_split_results_into_node(pos, &split_key, new_page_id)?
                } else {
                    // after the split, there's one less key between the two nodes, so account for
                    // that
                    let pos = pos - self.key_count() - 1;
                    parent_new_node.insert_split_results_into_node(pos, &split_key, new_page_id)?;
                }
                Ok(InsertResult::Split(
                    parent_split_key,
                    parent_new_node.page_id(),
                ))
            } else {
                self.insert_split_results_into_node(pos, &split_key, new_page_id)?;
                Ok(InsertResult::Done)
            }
        } else {
            Ok(InsertResult::Done)
        }
    }

    fn insert<Fd: AsRawFd + Copy>(
        &mut self,
        key: K,
        value: V,
        pager_info: &mut PagerInfo<PB, Fd>,
    ) -> Result<InsertResult<K>> {
        if self.is_leaf() {
            self.insert_as_leaf(key, value, pager_info)
        } else {
            self.insert_as_node(key, value, pager_info)
        }
    }

    fn get<Fd: AsRawFd + Copy>(
        &self,
        key: &K,
        pager_info: &mut PagerInfo<PB, Fd>,
    ) -> Result<Option<V>> {
        if self.is_leaf() {
            match self.binary_search_keys(key) {
                Ok(pos) => Ok(Some(self.value_from_leaf(pos)?)),
                Err(_) => Ok(None),
            }
        } else {
            assert!(self.is_node());
            let (_, child_node) = self.get_descendent_by_key(key, pager_info)?;
            child_node.get(key, pager_info)
        }
    }
}

#[cfg(test)]
/// This size allows for nodes with 5 keys and leaves with 7
const TEST_BUFFER_SIZE: u16 = 112;
#[cfg(test)]
struct TestPageBuffer {
    data: [u8; TEST_BUFFER_SIZE as usize],
}
#[cfg(test)]
impl PageBuffer for TestPageBuffer {
    fn new() -> Self
    where
        Self: Sized,
    {
        Self {
            data: [0; TEST_BUFFER_SIZE as usize],
        }
    }

    fn buffer_size() -> u16 {
        TEST_BUFFER_SIZE
    }

    fn data(&self) -> &[u8] {
        &self.data
    }

    fn data_mut(&mut self) -> &mut [u8] {
        &mut self.data
    }
}

#[cfg(test)]
impl BTree<i32, TestPageBuffer, u32, u32> {
    /*
     * An example description looks something like this:
    0: [12, 23] (3)
    0->0: [3, 6, 9] (4)
    0->1: [15, 17, 20] (4)
    0->2: [28] (2)
    0->0->0: L[1, 2, 3] (0)
    0->0->1: L[4, 5, 6] (0)
    0->0->2: L[7, 8, 9] (0)
    0->0->3: L[10, 11, 12] (0)
    0->1->0: L[13, 14, 15] (0)
    0->1->1: L[16, 17] (0)
    0->1->2: L[18, 19, 20] (0)
    0->1->3: L[21, 22, 23] (0)
    0->2->0: L[24, 25, 26, 27] (0)
    0->2->1: L[29, 30, 31] (0)
        */
    pub fn from_description(
        description: &str,
        pager_ref: Rc<RefCell<Pager<TestPageBuffer>>>,
        backing_fd: i32,
    ) -> BTree<i32, TestPageBuffer, u32, u32> {
        let mut lines = description
            .trim()
            .split('\n')
            .map(|x| x.trim())
            .map(DescriptionLine::from_str)
            .peekable();

        assert!(lines.peek().is_some());

        // initalize pages
        let mut pager_info = PagerInfo::new(pager_ref.clone(), backing_fd);

        // init root page
        let first_line = lines.next().unwrap();
        let root: Node<TestPageBuffer, u32, u32> = match first_line.is_leaf {
            true => Node::init_leaf(&mut pager_info).unwrap(),
            false => Node::init_node(&mut pager_info).unwrap(),
        };
        let first_page_id = root.page_id();
        assert_eq!(first_page_id, 0);
        drop(root);

        let _root = Node::from_description_lines(
            &mut pager_info,
            first_line,
            &mut lines,
            first_page_id,
            (0, 0),
        );

        let tree = BTree::init(pager_ref, backing_fd).unwrap();
        assert_subtree_valid(&tree.root, &mut pager_info);
        tree
    }

    fn to_description(&self) -> String {
        let mut pager_info = self.pager_info();
        Self::node_to_description(&mut pager_info, self.root.page_id())
    }

    fn node_to_description(
        pager_info: &mut PagerInfo<TestPageBuffer, i32>,
        page_id: PageId,
    ) -> String {
        use std::collections::VecDeque;

        let mut description = String::new();
        let mut queue = VecDeque::new();
        queue.push_back((vec![0], page_id));
        while let Some((ancestry, page_id)) = queue.pop_front() {
            let node: Node<TestPageBuffer, u32, u32> = pager_info.page_node(page_id).unwrap();
            let path_parts: Vec<_> = ancestry.iter().map(|x| x.to_string()).collect();
            let path = path_parts.join("->");
            if node.is_leaf() {
                let s = format!("{path}: L{:?} ({})\n", node.keys(), node.descendent_count());
                description.push_str(&s);
            } else {
                let s = format!("{path}: {:?} ({})\n", node.keys(), node.descendent_count());
                description.push_str(&s);
            }
            queue.extend(node.descendent_page_ids().into_iter().enumerate().map(
                |(idx, page_id)| {
                    let mut child_ancestry = ancestry.clone();
                    child_ancestry.push(idx);
                    (child_ancestry, page_id)
                },
            ));
        }
        description
    }

    fn display_subtree(pager_info: &mut PagerInfo<TestPageBuffer, i32>, root_page_id: PageId) {
        let description = Self::node_to_description(pager_info, root_page_id);
        print!("{description}");
    }
}

impl<
        PB: PageBuffer,
        K: Ord + Debug + Serialize + DeserializeOwned,
        V: Serialize + DeserializeOwned,
    > Node<PB, K, V>
{
    fn keys(&self) -> Vec<K> {
        if self.is_leaf() {
            (0..self.key_count())
                .map(|i| self.key_from_leaf(i).unwrap())
                .collect()
        } else {
            (0..self.key_count())
                .map(|i| self.key_from_inner_node(i).unwrap())
                .collect()
        }
    }

    #[allow(clippy::reversed_empty_ranges)]
    fn descendent_iter<'a, Fd: AsRawFd + Copy>(
        &'a self,
        pager_info: &'a mut PagerInfo<PB, Fd>,
    ) -> impl Iterator<Item = Self> + 'a {
        let range = if self.is_node() {
            0..=self.key_count()
        } else {
            // intentionally create an empty range here because leaves have no descendents
            1..=0
        };
        range.map(|i| self.descendent_node_at_pos(i, pager_info).unwrap())
    }

    fn descendent_page_ids(&self) -> Vec<PageId> {
        if self.is_leaf() {
            Vec::new()
        } else {
            (0..=self.key_count())
                .map(|i| self.page_id_from_inner_node(i).unwrap())
                .collect()
        }
    }

    fn descendent_count(&self) -> u16 {
        if self.is_leaf() {
            0
        } else {
            let page = self.page_ref.borrow();
            page.cell_count() - self.key_count()
        }
    }
}

#[cfg(test)]
impl Node<TestPageBuffer, u32, u32> {
    fn from_description_lines<Fd: AsRawFd + Copy, I: Iterator<Item = DescriptionLine>>(
        pager_info: &mut PagerInfo<TestPageBuffer, Fd>,
        this_node_line: DescriptionLine,
        lines: &mut Peekable<I>,
        this_page_id: PageId,
        sibling_page_ids: (PageId, PageId),
    ) -> Self {
        let mut new_node = pager_info.page_node(this_page_id).unwrap();
        let mut page = new_node.page_ref.borrow_mut();

        if this_node_line.is_leaf {
            // update sibling pointers
            for (i, key) in this_node_line.keys.iter().enumerate() {
                let bytes = to_bytes(&(key, key)).unwrap();
                // +2 to account for sibling pointers
                let insertion_pos = (i as u16) + 2;
                page.insert_cell(insertion_pos, &bytes).unwrap();
            }
            drop(page);
            let (left_sibling, right_sibling) = sibling_page_ids;
            new_node.leaf_replace_left_sibling(&left_sibling).unwrap();
            new_node.leaf_replace_right_sibling(&right_sibling).unwrap();
        } else {
            let child_lines: Vec<_> = lines
                .peeking_take_while(|l| this_node_line.is_child_line(l))
                .collect();
            assert_eq!(child_lines.len(), this_node_line.child_count);
            assert_eq!(this_node_line.keys.len() + 1, this_node_line.child_count);
            let mut children = Vec::new();

            //this  fills up with page ids, bookended with 0s, so that to know the siblings of
            //position i in children, you just need position i and i + 2 from siblings
            let mut siblings = vec![0];

            for (idx, child_line) in child_lines.into_iter().enumerate() {
                // init child page so we can get the page id
                let child_node: Self = match child_line.is_leaf {
                    true => Self::init_leaf(pager_info).unwrap(),
                    false => Self::init_node(pager_info).unwrap(),
                };
                let page_id = child_node.page_id();
                children.push((child_line, page_id)); // store for later
                siblings.push(page_id);
                drop(child_node);

                let page_id_bytes = to_bytes(&page_id).unwrap();
                page.insert_cell(Self::id_pos_to_cell_pos(idx as u16), &page_id_bytes)
                    .unwrap();

                // Because we know that there is always 1 more child line that there is keys,
                // this will only be None on the last child line
                if idx < this_node_line.keys.len() {
                    let key = &this_node_line.keys[idx];
                    let key_bytes = to_bytes(key).unwrap();
                    page.insert_cell(Self::key_pos_to_cell_pos(idx as u16), &key_bytes)
                        .unwrap();
                }
            }
            siblings.push(0);
            drop(page);

            // Process the children we set aside earlier
            for (i, (child_line, page_id)) in children.into_iter().enumerate() {
                Self::from_description_lines(
                    pager_info,
                    child_line,
                    lines,
                    page_id,
                    (siblings[i], siblings[i + 2]),
                );
            }
        }

        new_node
    }
}

#[cfg(test)]
#[derive(Debug, Clone)]
struct DescriptionLine {
    traversal_path: Vec<usize>,
    is_leaf: bool,
    keys: Vec<u32>,
    child_count: usize,
}
#[cfg(test)]
impl DescriptionLine {
    fn from_str(s: &str) -> Self {
        let mut parts = s.split(": ");
        let traversal_path = parts
            .next()
            .unwrap()
            .split("->")
            .map(|x| x.parse::<usize>().unwrap())
            .collect();

        let second_half = parts.next().unwrap();
        assert!(second_half.starts_with("L[") || second_half.starts_with("["));
        let is_leaf = second_half.starts_with("L");
        let skip_num = if is_leaf { 2 } else { 1 };

        let closing_bracket_pos = second_half.chars().position(|c| c == ']').unwrap();
        let num_strs = second_half[skip_num..closing_bracket_pos].split(", ");
        let keys: Vec<u32> = num_strs.map(|x| x.parse::<u32>().unwrap()).collect();

        let child_count = second_half[closing_bracket_pos + 3..]
            .split(")")
            .next()
            .unwrap()
            .parse::<usize>()
            .unwrap();

        if is_leaf {
            assert_eq!(child_count, 0);
        } else {
            assert_eq!(keys.len() + 1, child_count);
        }

        DescriptionLine {
            traversal_path,
            is_leaf,
            keys,
            child_count,
        }
    }

    fn is_child_line(&self, candidate: &DescriptionLine) -> bool {
        let tvlen = self.traversal_path.len();
        candidate.traversal_path.len() == tvlen + 1
            && candidate.traversal_path[0..tvlen] == self.traversal_path
    }
}

#[cfg(test)]
fn tree_keys_fully_ordered(root: &Node<TestPageBuffer, u32, u32>) -> bool {
    let keys = root.keys();
    let mut sorted_keys = keys.clone();
    sorted_keys.sort();
    keys == sorted_keys
}

#[cfg(test)]
fn all_node_keys_ordered_and_deduped(
    node: &Node<TestPageBuffer, u32, u32>,
    pager_info: &mut PagerInfo<TestPageBuffer, i32>,
) -> bool {
    let mut sorted_keys = node.keys();
    sorted_keys.sort();
    sorted_keys.dedup();
    let nodes: Vec<_> = node.descendent_iter(pager_info).collect();
    sorted_keys == node.keys()
        && nodes
            .into_iter()
            .all(|node| all_node_keys_ordered_and_deduped(&node, pager_info))
}

#[cfg(test)]
fn all_keys_in_range(node: &Node<TestPageBuffer, u32, u32>, min: u32, max: u32) -> bool {
    node.keys().iter().all(|k| (min..=max).contains(k))
}

#[cfg(test)]
fn all_subnode_keys_ordered_relative_to_node_keys(
    node: &Node<TestPageBuffer, u32, u32>,
    pager_info: &mut PagerInfo<TestPageBuffer, i32>,
) -> bool {
    if node.is_leaf() {
        return true;
    }
    let mut min_key = u32::MIN;
    for (idx, k) in node.keys().iter().enumerate() {
        let max_key = *k;
        if !all_keys_in_range(
            &node.descendent_node_at_pos(idx as u16, pager_info).unwrap(),
            min_key,
            max_key,
        ) {
            return false;
        }
        min_key = k + 1;
    }
    all_keys_in_range(
        &node
            .descendent_node_at_pos(node.key_count(), pager_info)
            .unwrap(),
        min_key,
        u32::MAX,
    )
}

#[cfg(test)]
fn all_nodes_sized_correctly(
    root: &Node<TestPageBuffer, u32, u32>,
    pager_info: &mut PagerInfo<TestPageBuffer, i32>,
) -> bool {
    fn all_nodes_sized_correctly_not_root(
        node: &Node<TestPageBuffer, u32, u32>,
        pager_info: &mut PagerInfo<TestPageBuffer, i32>,
    ) -> bool {
        let third_size = TestPageBuffer::buffer_size() / 3;
        let meets_minimum_size = node.page_free_space() >= third_size;

        let children_pass = if node.is_leaf() {
            true
        } else {
            let children: Vec<_> = node.descendent_iter(pager_info).collect();
            children
                .iter()
                .all(|node| all_nodes_sized_correctly_not_root(node, pager_info))
        };

        meets_minimum_size && children_pass
    }

    let children: Vec<_> = root.descendent_iter(pager_info).collect();
    children
        .iter()
        .all(|node| all_nodes_sized_correctly(node, pager_info))
}

#[cfg(test)]
fn all_leaves_same_level(
    root: &Node<TestPageBuffer, u32, u32>,
    pager_info: &mut PagerInfo<TestPageBuffer, i32>,
) -> bool {
    fn leaf_levels(
        node: &Node<TestPageBuffer, u32, u32>,
        level: usize,
        pager_info: &mut PagerInfo<TestPageBuffer, i32>,
    ) -> Vec<usize> {
        if node.is_leaf() {
            return vec![level];
        }
        let children: Vec<_> = node.descendent_iter(pager_info).collect();
        children
            .iter()
            .flat_map(|c| leaf_levels(c, level + 1, pager_info))
            .collect()
    }

    let mut levels = leaf_levels(root, 0, pager_info).into_iter();
    let first = levels.next().unwrap();
    levels.all(|x| x == first)
}

#[cfg(test)]
fn assert_subtree_valid(
    node: &Node<TestPageBuffer, u32, u32>,
    pager_info: &mut PagerInfo<TestPageBuffer, i32>,
) {
    assert!(tree_keys_fully_ordered(node));
    assert!(all_node_keys_ordered_and_deduped(node, pager_info));
    assert!(all_subnode_keys_ordered_relative_to_node_keys(
        node, pager_info
    ));
    assert!(all_nodes_sized_correctly(node, pager_info));
    assert!(all_leaves_same_level(node, pager_info));
}

#[cfg(test)]
mod tests {
    use std::{
        cell::RefCell,
        fs::{self, File, OpenOptions},
        os::fd::AsRawFd,
        rc::Rc,
    };

    use itertools::Itertools;
    use serialize::serialized_size;

    use crate::{
        btree_disk::{assert_subtree_valid, TEST_BUFFER_SIZE},
        pager::{PageId, Pager, CELL_POINTER_SIZE},
    };

    use super::{BTree, TestPageBuffer};

    fn trim_lines(s: &str) -> String {
        s.trim().lines().map(|l| l.trim()).join("\n") + "\n"
    }

    fn open_file(filename: &str) -> File {
        OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(filename)
            .unwrap()
    }

    fn init_tree_from_description_in_file(
        filename: &str,
        description: &str,
    ) -> BTree<i32, TestPageBuffer, u32, u32> {
        let file = open_file(filename);
        let backing_fd = file.as_raw_fd();
        let pager_ref = Rc::new(RefCell::new(Pager::new(vec![file])));

        BTree::from_description(description, pager_ref, backing_fd)
    }

    fn init_tree_in_file(filename: &str) -> BTree<i32, TestPageBuffer, u32, u32> {
        let file = open_file(filename);
        let backing_fd = file.as_raw_fd();
        let pager_ref = Rc::new(RefCell::new(Pager::new(vec![file])));

        BTree::init(pager_ref, backing_fd).unwrap()
    }

    #[test]
    fn sizing_proofs() {
        // These constants may change in the future. They're just tested here to prove that my
        // assumptions about tree construction during the tests are correct.

        let leaf_key_size = serialized_size(&(42u32, 52u32));
        assert_eq!(leaf_key_size, 8);
        let node_key_size = serialized_size(&42u32);
        assert_eq!(node_key_size, 4);
        let page_id: PageId = 42;
        let node_page_id_size = serialized_size(&page_id);
        assert_eq!(node_page_id_size, 8);

        let sibling_pointers_size = (serialized_size(&page_id) as u16 + CELL_POINTER_SIZE) * 2;
        assert_eq!(sibling_pointers_size, 24);

        let leaf_entry_size = CELL_POINTER_SIZE as usize + leaf_key_size;
        let buffer_minus_siblings = TEST_BUFFER_SIZE - sibling_pointers_size;
        assert_eq!(leaf_entry_size, 12);
        assert_eq!(buffer_minus_siblings as usize / leaf_entry_size, 7);

        let node_key_entry_size = CELL_POINTER_SIZE as usize + node_key_size;
        let node_page_id_entry_size = CELL_POINTER_SIZE as usize + node_page_id_size;
        assert_eq!(node_key_entry_size, 8);
        assert_eq!(node_page_id_entry_size, 12);

        let enough_space_for_5_node_keys =
            (node_key_entry_size * 5) + (node_page_id_entry_size * 6);
        assert_eq!(TEST_BUFFER_SIZE as usize, enough_space_for_5_node_keys);
    }

    #[test]
    fn end_to_end_description() {
        let input_description = "
            0: [12, 23] (3)
            0->0: [3, 6, 9] (4)
            0->1: [15, 17, 20] (4)
            0->2: [28] (2)
            0->0->0: L[1, 2, 3] (0)
            0->0->1: L[4, 5, 6] (0)
            0->0->2: L[7, 8, 9] (0)
            0->0->3: L[10, 11, 12] (0)
            0->1->0: L[13, 14, 15] (0)
            0->1->1: L[16, 17] (0)
            0->1->2: L[18, 19, 20] (0)
            0->1->3: L[21, 22, 23] (0)
            0->2->0: L[24, 25, 26, 27] (0)
            0->2->1: L[29, 30, 31] (0)";
        let input_description = trim_lines(input_description);

        let filename = "end_to_end_description.test";
        let tree = init_tree_from_description_in_file(filename, &input_description);

        assert_eq!(tree.root.page_id(), 0);
        assert_eq!(&tree.to_description(), &input_description);

        drop(tree);
        fs::remove_file(filename).unwrap();
    }

    #[test]
    fn binary_search_keys_empty() {
        let filename = "binary_search_keys_empty.test";

        let tree = init_tree_in_file(filename);

        assert!(matches!(tree.root.binary_search_keys(&42), Err(0)));

        drop(tree);
        fs::remove_file(filename).unwrap();
    }

    #[test]
    fn binary_search_keys_single() {
        let filename = "binary_search_keys_single.test";
        let description = "0: L[2] (0)";

        let tree = init_tree_from_description_in_file(filename, description);

        // less
        assert_eq!(tree.root.binary_search_keys(&1), Err(0));
        // equal
        assert_eq!(tree.root.binary_search_keys(&2), Ok(0));
        // greater
        assert_eq!(tree.root.binary_search_keys(&3), Err(1));

        drop(tree);
        fs::remove_file(filename).unwrap();
    }

    #[test]
    fn binary_search_keys_multiple() {
        // smaller
        let filename = "binary_search_keys_multiple.test";
        let description = "0: L[2, 4, 6] (0)";

        let tree = init_tree_from_description_in_file(filename, description);

        assert_eq!(tree.root.binary_search_keys(&1), Err(0));
        assert_eq!(tree.root.binary_search_keys(&2), Ok(0));
        assert_eq!(tree.root.binary_search_keys(&3), Err(1));
        assert_eq!(tree.root.binary_search_keys(&4), Ok(1));
        assert_eq!(tree.root.binary_search_keys(&5), Err(2));
        assert_eq!(tree.root.binary_search_keys(&6), Ok(2));
        assert_eq!(tree.root.binary_search_keys(&7), Err(3));

        drop(tree);
        fs::remove_file(filename).unwrap();

        // bigger
        let filename = "binary_search_keys_multiple.test";
        let description = "0: L[2, 4, 6, 8, 10] (0)";

        let tree = init_tree_from_description_in_file(filename, description);

        assert_eq!(tree.root.binary_search_keys(&1), Err(0));
        assert_eq!(tree.root.binary_search_keys(&2), Ok(0));
        assert_eq!(tree.root.binary_search_keys(&3), Err(1));
        assert_eq!(tree.root.binary_search_keys(&4), Ok(1));
        assert_eq!(tree.root.binary_search_keys(&5), Err(2));
        assert_eq!(tree.root.binary_search_keys(&6), Ok(2));
        assert_eq!(tree.root.binary_search_keys(&7), Err(3));
        assert_eq!(tree.root.binary_search_keys(&8), Ok(3));
        assert_eq!(tree.root.binary_search_keys(&9), Err(4));
        assert_eq!(tree.root.binary_search_keys(&10), Ok(4));
        assert_eq!(tree.root.binary_search_keys(&11), Err(5));

        drop(tree);
        fs::remove_file(filename).unwrap();
    }

    #[test]
    fn replace_value_in_full_leaf() {
        let filename = "replace_value_in_full_leaf.test";
        let input_tree = trim_lines("0: L[1, 2, 3, 4, 5, 6, 7] (0)");
        let expected_tree = trim_lines("0: L[1, 2, 3, 4, 5, 6, 7] (0)");

        let mut t = init_tree_from_description_in_file(filename, &input_tree);
        t.insert(5, 42).unwrap();

        assert_eq!(t.to_description(), expected_tree);
        assert_eq!(t.get(&5).unwrap(), Some(42));
        assert_subtree_valid(&t.root, &mut t.pager_info());
    }

    #[test]
    fn single_insertion() {
        let filename = "single_insertion.test";
        let expected_tree = "0: L[1] (0)";
        let expected_tree = trim_lines(expected_tree);

        let mut tree = init_tree_in_file(filename);
        tree.insert(1, 1).unwrap();

        assert_eq!(&tree.to_description(), &expected_tree);
        assert_subtree_valid(&tree.root, &mut tree.pager_info());

        drop(tree);
        fs::remove_file(filename).unwrap();
    }

    #[test]
    fn leaf_root_insertion() {
        let filename = "leaf_root_insertion.test";
        let expected_tree = "
            0: L[1, 2, 3, 4, 5] (0)
        ";
        let expected_tree = trim_lines(expected_tree);

        let mut tree = init_tree_in_file(filename);

        for i in 1..=5 {
            tree.insert(i, i).unwrap();
        }

        assert_eq!(&tree.to_description(), &expected_tree);
        assert_subtree_valid(&tree.root, &mut tree.pager_info());

        drop(tree);
        fs::remove_file(filename).unwrap();
    }

    #[test]
    fn leaf_root_split() {
        let filename = "leaf_root_split.test";
        let expected_tree = "
            0: [4] (2)
            0->0: L[1, 2, 3, 4] (0)
            0->1: L[5, 6, 7, 8] (0)
        ";
        let expected_tree = trim_lines(expected_tree);

        let mut tree = init_tree_in_file(filename);

        for i in 1..=8 {
            println!("inserting {i}");
            tree.insert(i, i).unwrap();
        }

        assert_eq!(&tree.to_description(), &expected_tree);
        assert_subtree_valid(&tree.root, &mut tree.pager_info());

        drop(tree);
        fs::remove_file(filename).unwrap();
    }

    #[test]
    fn node_root_split() {
        let filename = "node_root_split.test";
        let init_tree = "
            0: [5, 10, 15, 20, 25] (6)
            0->0: L[1, 2, 3, 4, 5] (0)
            0->1: L[6, 7, 8, 9, 10] (0)
            0->2: L[11, 12, 13, 14, 15] (0)
            0->3: L[16, 17, 18, 19, 20] (0)
            0->4: L[21, 22, 23, 24, 25] (0)
            0->5: L[26, 27, 28, 29, 30, 31, 32, 33, 34] (0)
        ";
        let init_tree = trim_lines(init_tree);

        let expected_tree = "
            0: [15] (2)
            0->0: [5, 10] (3)
            0->1: [20, 25, 30] (4)
            0->0->0: L[1, 2, 3, 4, 5] (0)
            0->0->1: L[6, 7, 8, 9, 10] (0)
            0->0->2: L[11, 12, 13, 14, 15] (0)
            0->1->0: L[16, 17, 18, 19, 20] (0)
            0->1->1: L[21, 22, 23, 24, 25] (0)
            0->1->2: L[26, 27, 28, 29, 30] (0)
            0->1->3: L[31, 32, 33, 34, 35] (0)
        ";
        let expected_tree = trim_lines(expected_tree);

        let mut tree = init_tree_from_description_in_file(filename, &init_tree);
        tree.insert(35, 35).unwrap();

        assert_eq!(tree.to_description(), expected_tree);
        assert_subtree_valid(&tree.root, &mut tree.pager_info());

        drop(tree);
        fs::remove_file(filename).unwrap();
    }

    #[test]
    fn split_as_leaf_insert_right() {
        let filename = "split_as_leaf_insert_right.test";
        let input_tree = "
            0: [3] (2)
            0->0: L[1, 2, 3] (0)
            0->1: L[4, 5, 6, 7, 8, 9, 10, 11, 12] (0)
            ";
        let input_tree = trim_lines(input_tree);

        let output_tree = "
            0: [3, 8] (3)
            0->0: L[1, 2, 3] (0)
            0->1: L[4, 5, 6, 7, 8] (0)
            0->2: L[9, 10, 11, 12, 13] (0)
            ";
        let output_tree = trim_lines(output_tree);

        let mut t = init_tree_from_description_in_file(filename, &input_tree);
        t.insert(13, 13).unwrap();

        assert_eq!(&t.to_description(), &output_tree);
        drop(t);
        fs::remove_file(filename).unwrap();
    }

    #[test]
    fn split_as_leaf_insert_left() {
        let filename = "split_as_leaf_insert_left.test";
        let input_tree = "
            0: [3] (2)
            0->0: L[1, 2, 3] (0)
            0->1: L[5, 6, 7, 8, 9, 10, 11, 12, 13] (0)
            ";
        let input_tree = trim_lines(input_tree);

        let output_tree = "
            0: [3, 9] (3)
            0->0: L[1, 2, 3] (0)
            0->1: L[4, 5, 6, 7, 8, 9] (0)
            0->2: L[10, 11, 12, 13] (0)
            ";
        let output_tree = trim_lines(output_tree);

        let mut t = init_tree_from_description_in_file(filename, &input_tree);
        t.insert(4, 4).unwrap();

        assert_eq!(&t.to_description(), &output_tree);
        drop(t);
        fs::remove_file(filename).unwrap();
    }

    #[test]
    fn split_as_node_insert_left() {
        let filename = "split_as_node_insert_left.test";
        let input_tree = "
            0: [12] (2)
            0->0: [3, 6, 9] (4)
            0->1: [15, 25, 28, 31, 34] (6)
            0->0->0: L[1, 2, 3] (0)
            0->0->1: L[4, 5, 6] (0)
            0->0->2: L[7, 8, 9] (0)
            0->0->3: L[10, 11, 12] (0)
            0->1->0: L[13, 14, 15] (0)
            0->1->1: L[16, 17, 18, 20, 21, 22, 23, 24, 25] (0)
            0->1->2: L[26, 27, 28] (0)
            0->1->3: L[29, 30, 31] (0)
            0->1->4: L[32, 33, 34] (0)
            0->1->5: L[35, 36, 37] (0)
        ";
        let input_tree = trim_lines(input_tree);

        let output_tree = "
            0: [12, 28] (3)
            0->0: [3, 6, 9] (4)
            0->1: [15, 21, 25] (4)
            0->2: [31, 34] (3)
            0->0->0: L[1, 2, 3] (0)
            0->0->1: L[4, 5, 6] (0)
            0->0->2: L[7, 8, 9] (0)
            0->0->3: L[10, 11, 12] (0)
            0->1->0: L[13, 14, 15] (0)
            0->1->1: L[16, 17, 18, 19, 20, 21] (0)
            0->1->2: L[22, 23, 24, 25] (0)
            0->1->3: L[26, 27, 28] (0)
            0->2->0: L[29, 30, 31] (0)
            0->2->1: L[32, 33, 34] (0)
            0->2->2: L[35, 36, 37] (0)
        ";
        let output_tree = trim_lines(output_tree);

        let mut t = init_tree_from_description_in_file(filename, &input_tree);
        t.insert(19, 19).unwrap();

        assert_eq!(&t.to_description(), &output_tree);
        assert_subtree_valid(&t.root, &mut t.pager_info());

        drop(t);
        fs::remove_file(filename).unwrap();
    }

    #[test]
    fn split_as_node_insert_right() {
        let filename = "split_as_node_insert_right.test";
        let input_tree = "
            0: [12] (2)
            0->0: [3, 6, 9] (4)
            0->1: [15, 18, 21, 24, 34] (6)
            0->0->0: L[1, 2, 3] (0)
            0->0->1: L[4, 5, 6] (0)
            0->0->2: L[7, 8, 9] (0)
            0->0->3: L[10, 11, 12] (0)
            0->1->0: L[13, 14, 15] (0)
            0->1->1: L[16, 17, 18] (0)
            0->1->2: L[19, 20, 21] (0)
            0->1->3: L[22, 23, 24] (0)
            0->1->4: L[25, 26, 27, 28, 29, 30, 32, 33, 34] (0)
            0->1->5: L[35, 36, 37] (0)
        ";
        let input_tree = trim_lines(input_tree);

        let output_tree = "
            0: [12, 21] (3)
            0->0: [3, 6, 9] (4)
            0->1: [15, 18] (3)
            0->2: [24, 29, 34] (4)
            0->0->0: L[1, 2, 3] (0)
            0->0->1: L[4, 5, 6] (0)
            0->0->2: L[7, 8, 9] (0)
            0->0->3: L[10, 11, 12] (0)
            0->1->0: L[13, 14, 15] (0)
            0->1->1: L[16, 17, 18] (0)
            0->1->2: L[19, 20, 21] (0)
            0->2->0: L[22, 23, 24] (0)
            0->2->1: L[25, 26, 27, 28, 29] (0)
            0->2->2: L[30, 31, 32, 33, 34] (0)
            0->2->3: L[35, 36, 37] (0)
        ";
        let output_tree = trim_lines(output_tree);

        let mut t = init_tree_from_description_in_file(filename, &input_tree);
        t.insert(31, 31).unwrap();

        assert_eq!(&t.to_description(), &output_tree);
        assert_subtree_valid(&t.root, &mut t.pager_info());

        drop(t);
        fs::remove_file(filename).unwrap();
    }
}
