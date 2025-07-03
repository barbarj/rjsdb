#![allow(dead_code)]

use std::{
    cell::RefCell,
    cmp::Ordering,
    fmt::{Debug, Display},
    marker::PhantomData,
    ops::RangeInclusive,
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

/*
 * TODO:
 * - I still have lots of operations that mix logical and physical position. I need to make it so
 * that they all operate only in one domain or the other
 */

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

    pub fn remove(&mut self, key: &K) -> Result<Option<V>> {
        let mut pager_info = self.pager_info();
        let res = self.root.remove(key, &mut pager_info)?;

        if self.root.key_count() == 0 && self.root.is_node() {
            // "replace" root with child by moving all data on child to root and dropping child
            let child = self
                .root
                .descendent_node_at_logical_pos(0, &mut pager_info)?;
            let child_page = child.page_ref.borrow();

            let mut root_page = self.root.page_ref.borrow_mut();
            assert_eq!(root_page.cell_count(), 1);
            root_page.remove_cell(0); // remove the one cell that should be here
            for (i, bytes) in child_page.cell_bytes_iter().enumerate() {
                root_page.insert_cell(i as u16, bytes)?;
            }

            root_page.set_kind(child_page.kind());

            let child_page_id = child_page.id();
            drop(root_page);
            drop(child_page);
            drop(child);
            pager_info.drop_page(child_page_id)?;
        }

        Ok(res)
    }

    pub fn iter(
        &self,
        min_key: KeyLimit<K>,
        max_key: KeyLimit<K>,
    ) -> Result<BTreeIter<PB, Fd, K, V>> {
        let mut pager_info = self.pager_info();
        let mut node: Node<PB, K, V> = pager_info.page_node(self.root.page_id())?;
        while !node.is_leaf() {
            node = match &min_key {
                KeyLimit::None => node.descendent_node_at_logical_pos(0, &mut pager_info)?,
                KeyLimit::Exclusive(k) => node.get_descendent_by_key(k, &mut pager_info)?.1,
                KeyLimit::Inclusive(k) => node.get_descendent_by_key(k, &mut pager_info)?.1,
            };
        }
        let starting_pos = match &min_key {
            KeyLimit::None => 0,
            KeyLimit::Exclusive(k) => match node.binary_search_keys(k) {
                Ok(pos) => pos + 1,
                Err(pos) => pos,
            },
            KeyLimit::Inclusive(k) => match node.binary_search_keys(k) {
                Ok(pos) => pos,
                Err(pos) => pos,
            },
        };

        let iter = BTreeIter::new(node, starting_pos, max_key, pager_info);
        Ok(iter)
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

    fn drop_page(&mut self, page_id: PageId) -> Result<()> {
        let mut pager = self.pager_ref.borrow_mut();
        pager.delete_page(self.backing_fd, page_id)?;
        Ok(())
    }
}

pub enum KeyLimit<K: Ord + Serialize + DeserializeOwned + Debug> {
    None,
    Inclusive(K),
    Exclusive(K),
}

pub struct BTreeIter<
    PB: PageBuffer,
    Fd: AsRawFd + Copy,
    K: Ord + Serialize + DeserializeOwned + Debug,
    V: Serialize + DeserializeOwned,
> {
    leaf: Node<PB, K, V>,
    logical_pos: u16,
    max_key: KeyLimit<K>,
    pager_info: PagerInfo<PB, Fd>,
}
impl<
        PB: PageBuffer,
        Fd: AsRawFd + Copy,
        K: Ord + Serialize + DeserializeOwned + Debug,
        V: Serialize + DeserializeOwned,
    > BTreeIter<PB, Fd, K, V>
{
    fn new(
        leftmost_leaf: Node<PB, K, V>,
        starting_pos: u16,
        max_key: KeyLimit<K>,
        pager_info: PagerInfo<PB, Fd>,
    ) -> Self {
        BTreeIter {
            leaf: leftmost_leaf,
            logical_pos: starting_pos,
            max_key,
            pager_info,
        }
    }
}

impl<
        PB: PageBuffer,
        Fd: AsRawFd + Copy,
        K: Ord + Serialize + DeserializeOwned + Debug,
        V: Serialize + DeserializeOwned,
    > Iterator for BTreeIter<PB, Fd, K, V>
{
    type Item = Result<(K, V)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.logical_pos == self.leaf.key_count() {
            // replace with next leaf
            let next_page_id = match self.leaf.leaf_right_sibling() {
                Ok(id) => id,
                Err(err) => return Some(Err(err)),
            };
            if next_page_id == 0 {
                return None;
            }
            self.leaf = match self.pager_info.page_node(next_page_id) {
                Ok(node) => node,
                Err(err) => return Some(Err(err)),
            };
            self.logical_pos = 0;
        }
        let (key, val) = match self.leaf.leaf_kv_at_pos(self.logical_pos) {
            Ok(kv) => kv,
            Err(err) => return Some(Err(err)),
        };
        match &self.max_key {
            KeyLimit::Exclusive(max) => {
                if &key >= max {
                    return None;
                }
            }
            KeyLimit::Inclusive(max) => {
                if &key > max {
                    return None;
                }
            }
            KeyLimit::None => {}
        }
        self.logical_pos += 1;
        Some(Ok((key, val)))
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

    fn page_used_space(&self) -> u16 {
        let page = self.page_ref.borrow();
        PB::buffer_size() - page.total_free_space()
    }

    fn key_from_leaf(&self, logical_pos: u16) -> Result<K> {
        assert!(self.is_leaf());
        let page = self.page_ref.borrow();
        let pos = Self::logical_leaf_key_pos_to_physical_pos(logical_pos);
        let (key, _): (K, V) = from_reader(page.cell_bytes(pos))?;
        Ok(key)
    }

    fn value_from_leaf<T: DeserializeOwned>(&self, logical: u16) -> Result<T> {
        assert!(self.is_leaf());
        let page = self.page_ref.borrow();
        let pos = Self::logical_leaf_key_pos_to_physical_pos(logical);
        let (_, val): (K, T) = from_reader(page.cell_bytes(pos))?;
        Ok(val)
    }

    fn leaf_kv_at_pos(&self, logical: u16) -> Result<(K, V)> {
        assert!(self.is_leaf());
        let page = self.page_ref.borrow();
        let pos = Self::logical_leaf_key_pos_to_physical_pos(logical);
        let kv = from_reader(page.cell_bytes(pos))?;
        Ok(kv)
    }

    fn leaf_left_sibling(&self) -> Result<PageId> {
        assert!(self.is_leaf());
        let page = self.page_ref.borrow();
        assert!(page.cell_count() >= 2);
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
        assert!(page.cell_count() >= 2);
        Ok(from_reader(page.cell_bytes(1))?)
    }

    fn logical_node_key_pos_to_physical_pos(key_pos: u16) -> u16 {
        (key_pos * 2) + 1
    }

    fn logical_leaf_key_pos_to_physical_pos(key_pos: u16) -> u16 {
        key_pos + 2
    }

    fn logical_id_pos_to_physical_pos(id_pos: u16) -> u16 {
        id_pos * 2
    }

    /// Returns None if this cell position will not contain a key
    fn node_physical_pos_to_logical_key_pos(physical_pos: u16) -> Option<u16> {
        if physical_pos % 2 == 0 {
            None
        } else {
            Some(physical_pos / 2) // integer division makes the division of an odd number (2n + 1) by
                                   // 2 result in the same number as if the input were the odd number's
                                   // even counterpart (2n)
        }
    }

    fn key_from_inner_node(&self, key_pos: u16) -> Result<K> {
        assert!(self.is_node());
        let pos = Self::logical_node_key_pos_to_physical_pos(key_pos);
        let page = self.page_ref.borrow();
        let key = from_reader(page.cell_bytes(pos))?;
        Ok(key)
    }

    fn page_id_from_inner_node(&self, id_pos: u16) -> Result<PageId> {
        assert!(self.is_node());
        let pos = Self::logical_id_pos_to_physical_pos(id_pos);
        let page = self.page_ref.borrow();
        let page_id = from_reader(page.cell_bytes(pos))?;
        Ok(page_id)
    }

    fn key_at_pos(&self, logical_pos: u16) -> Result<K> {
        if self.is_node() {
            self.key_from_inner_node(logical_pos)
        } else {
            self.key_from_leaf(logical_pos)
        }
    }

    fn binary_search_keys(&self, key: &K) -> std::result::Result<u16, u16> {
        if self.key_count() == 0 {
            return Err(0);
        }
        let mut low = 0;
        let mut high = self.key_count() - 1;
        while low < high {
            let mid = (low + high) / 2; // TODO: Rework to prevent overflow
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

    fn within_half_increment(goal: u16, increment: u16, current: u16) -> bool {
        if goal > current {
            goal - current <= increment / 2
        } else {
            current - goal <= increment / 2
        }
    }

    fn first_logical_pos_from_right_within_half_increment(
        &self,
        size: u16,
        initial_space_used: u16,
    ) -> u16 {
        let mut used_space = initial_space_used;
        let mut idx = self.key_count() - 1;
        let page = self.page_ref.borrow();

        let mut increment = 0;

        if self.is_node() {
            // Find the index of the first "position" at or past the halfway point
            let id_ptr = page.get_cell_pointer(Self::logical_id_pos_to_physical_pos(idx + 1));
            used_space += id_ptr.size;
            while !Self::within_half_increment(size, increment, used_space) {
                let id_ptr = page.get_cell_pointer(Self::logical_id_pos_to_physical_pos(idx));
                let key_ptr =
                    page.get_cell_pointer(Self::logical_node_key_pos_to_physical_pos(idx));
                increment = id_ptr.size + key_ptr.size + (2 * CELL_POINTER_SIZE);
                used_space += increment;
                idx -= 1;
            }
        } else {
            while !Self::within_half_increment(size, increment, used_space) {
                let ptr = page.get_cell_pointer(Self::logical_leaf_key_pos_to_physical_pos(idx));
                increment = ptr.size + CELL_POINTER_SIZE;
                used_space += increment;
                idx -= 1;
            }
        }
        idx + 1
    }

    fn first_logical_pos_past_size(&self, size: u16) -> u16 {
        let mut used_space = 0;
        let mut idx = 0;
        let mut increment = 0;
        let page = self.page_ref.borrow();

        if self.is_node() {
            // Find the index of the first "position" at or past the halfway point
            while !Self::within_half_increment(size, increment, used_space) {
                let id_ptr = page.get_cell_pointer(Self::logical_id_pos_to_physical_pos(idx));
                let key_ptr =
                    page.get_cell_pointer(Self::logical_node_key_pos_to_physical_pos(idx));
                increment = id_ptr.size + key_ptr.size + (2 * CELL_POINTER_SIZE);
                used_space += increment;
                idx += 1;
            }
        } else {
            used_space += Self::leaf_siblings_space_used();
            while !Self::within_half_increment(size, increment, used_space) {
                let ptr = page.get_cell_pointer(Self::logical_leaf_key_pos_to_physical_pos(idx));
                increment = ptr.size + CELL_POINTER_SIZE;
                used_space += increment;
                idx += 1;
            }
        }
        if used_space <= size {
            idx
        } else {
            idx - 1
        }
    }

    fn move_cells(
        from_node: &mut Self,
        to_node: &mut Self,
        from_logical_range: RangeInclusive<u16>,
        to_logical_start: u16,
    ) -> Result<()> {
        let physical_range = if from_node.is_leaf() {
            Self::logical_leaf_key_pos_to_physical_pos(*from_logical_range.start())
                ..=Self::logical_leaf_key_pos_to_physical_pos(*from_logical_range.end())
        } else {
            Self::logical_id_pos_to_physical_pos(*from_logical_range.start())
                ..=Self::logical_id_pos_to_physical_pos(*from_logical_range.end())
        };
        let physical_start = if to_node.is_leaf() {
            Self::logical_leaf_key_pos_to_physical_pos(to_logical_start)
        } else {
            Self::logical_id_pos_to_physical_pos(to_logical_start)
        };

        let from_page = from_node.page_ref.borrow();
        let mut to_page = to_node.page_ref.borrow_mut();

        // copy cells to to_page
        let skipped = (*physical_range.start()).into();
        let taken = physical_range.len();
        for (i, bytes) in from_page
            .cell_bytes_iter()
            .skip(skipped)
            .take(taken)
            .enumerate()
        {
            to_page.insert_cell(physical_start + i as u16, bytes)?;
        }

        drop(to_page);
        drop(from_page);
        // remove cells from from_page
        let mut from_page = from_node.page_ref.borrow_mut();
        // it's more efficient to remove in reverse, as doing so causes fewer-to-no cell pointers
        // to be moved in response
        for i in physical_range.rev() {
            from_page.remove_cell(i);
        }

        Ok(())
    }

    fn remove_leading_key(&mut self) {
        assert!(self.is_node());
        let mut page = self.page_ref.borrow_mut();
        page.remove_cell(0);
    }

    fn remove_trailing_key(&mut self, logical_key_pos: u16) {
        assert!(self.is_node());
        let physical_pos = Self::logical_node_key_pos_to_physical_pos(logical_key_pos);
        let mut page = self.page_ref.borrow_mut();
        assert_eq!(physical_pos, page.cell_count() - 1); // should always be the last cell
        page.remove_cell(physical_pos);
    }

    fn split_node<Fd: AsRawFd + Copy>(
        &mut self,
        pager_info: &mut PagerInfo<PB, Fd>,
    ) -> Result<(K, Node<PB, K, V>)> {
        assert!(self.page_free_space() < pager_info.buffer_size() / 2);
        let split_key_logical_pos = self.first_logical_pos_past_size(self.page_used_space() / 2);

        // self.key_from_inner_node uses the logical key position amongst other keys, so convert to
        // that before asking for the key
        let split_key = self.key_from_inner_node(split_key_logical_pos)?;

        // get new page
        let mut new_node = Self::init_node(pager_info)?;

        let key_count = self.key_count();
        Self::move_cells(
            self,
            &mut new_node,
            split_key_logical_pos + 1..=key_count,
            0,
        )?;

        self.remove_trailing_key(split_key_logical_pos);

        Ok((split_key, new_node))
    }

    fn leaf_siblings_space_used() -> u16 {
        let dummy_id: PageId = 0;
        (serialized_size(&dummy_id) as u16 + CELL_POINTER_SIZE) * 2
    }

    fn leaf_split_point(&self) -> u16 {
        assert!(self.is_leaf());
        (self.page_used_space() + Self::leaf_siblings_space_used()) / 2
    }

    fn split_leaf<Fd: AsRawFd + Copy>(
        &mut self,
        pager_info: &mut PagerInfo<PB, Fd>,
    ) -> Result<(K, Node<PB, K, V>)> {
        let first_logical_idx = self.first_logical_pos_past_size(self.leaf_split_point());
        assert!(first_logical_idx > 0);
        let split_key = self.key_from_leaf(first_logical_idx - 1)?;

        // get new page
        let mut new_node = Self::init_leaf(pager_info)?;

        // update sibling pointers
        let old_right = self.leaf_replace_right_sibling(&new_node.page_id())?;
        new_node.leaf_replace_left_sibling(&self.page_id())?;
        new_node.leaf_replace_right_sibling(&old_right)?;

        // copy cells to new page and remove cells from old page
        let key_count = self.key_count();
        Self::move_cells(self, &mut new_node, first_logical_idx..=key_count - 1, 0)?;

        Ok((split_key, new_node))
    }

    fn insert_as_leaf<Fd: AsRawFd + Copy>(
        &mut self,
        key: K,
        value: V,
        pager_info: &mut PagerInfo<PB, Fd>,
    ) -> Result<InsertResult<K>> {
        assert!(self.is_leaf());
        if !self.can_fit_leaf(&key, &value) {
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
                Ok(logical_pos) => {
                    let physical_pos = Self::logical_leaf_key_pos_to_physical_pos(logical_pos);
                    let mut page = self.page_ref.borrow_mut();
                    page.remove_cell(physical_pos);
                    page.insert_cell(physical_pos, &to_bytes(&(key, value))?)?;
                }
                Err(logical_pos) => {
                    let physical_pos = Self::logical_leaf_key_pos_to_physical_pos(logical_pos);
                    let mut page = self.page_ref.borrow_mut();
                    page.insert_cell(physical_pos, &to_bytes(&(key, value))?)?;
                }
            }
            Ok(InsertResult::Done)
        }
    }

    /// For node searches, we only care about which child to descend to,
    /// so an exact match doesn't provide any additional information.
    fn search_keys_as_node(&self, key: &K) -> u16 {
        match self.binary_search_keys(key) {
            Ok(pos) => pos,
            Err(pos) => pos,
        }
    }

    fn descendent_node_at_logical_pos<Fd: AsRawFd + Copy>(
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
        let logical_pos = self.search_keys_as_node(key);
        let descendent = self.descendent_node_at_logical_pos(logical_pos, pager_info)?;
        Ok((logical_pos, descendent))
    }

    fn insert_split_results_into_node(
        &mut self,
        logical_pos: u16,
        split_key: &K,
        new_page_id: PageId,
    ) -> Result<()> {
        let prior_key = self.replace_inner_node_key(logical_pos, split_key)?;

        let id_cell_physical_pos = Self::logical_id_pos_to_physical_pos(logical_pos + 1);
        let mut page = self.page_ref.borrow_mut();
        page.insert_cell(id_cell_physical_pos, &to_bytes(&new_page_id)?)?;

        if let Some(k) = prior_key {
            page.insert_cell(id_cell_physical_pos + 1, &to_bytes(&k)?)?;
        }
        Ok(())
    }

    /// replaces the key at key position pos with the new key, and returns the old key if there was
    /// one at that position
    fn replace_inner_node_key(&mut self, logical_key_pos: u16, new_key: &K) -> Result<Option<K>> {
        assert!(logical_key_pos <= self.key_count());
        let old_key = if logical_key_pos < self.key_count() {
            Some(self.key_from_inner_node(logical_key_pos)?)
        } else {
            None
        };
        let cell_idx = Self::logical_node_key_pos_to_physical_pos(logical_key_pos);
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
        let (logical_pos, mut child_node) = self.get_descendent_by_key(&key, pager_info)?;
        if let InsertResult::Split(split_key, new_page_id) =
            child_node.insert(key, value, pager_info)?
        {
            if !self.can_fit_node(&split_key) {
                let (parent_split_key, mut parent_new_node) = self.split_node(pager_info)?;
                assert!(parent_new_node.is_node());

                if split_key <= parent_split_key {
                    self.insert_split_results_into_node(logical_pos, &split_key, new_page_id)?
                } else {
                    // after the split, there's one less key between the two nodes, so account for
                    // that
                    println!("logical_pos: {logical_pos}");
                    println!("key_count: {}", self.key_count());
                    let pos = logical_pos - self.key_count() - 1;
                    parent_new_node.insert_split_results_into_node(pos, &split_key, new_page_id)?;
                }
                Ok(InsertResult::Split(
                    parent_split_key,
                    parent_new_node.page_id(),
                ))
            } else {
                self.insert_split_results_into_node(logical_pos, &split_key, new_page_id)?;
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
                Ok(logical_pos) => Ok(Some(self.value_from_leaf(logical_pos)?)),
                Err(_) => Ok(None),
            }
        } else {
            assert!(self.is_node());
            let (_, child_node) = self.get_descendent_by_key(key, pager_info)?;
            child_node.get(key, pager_info)
        }
    }

    fn can_fit_via_merge<Fd: AsRawFd + Copy>(
        &self,
        left_child_pos: u16,
        pager_info: &mut PagerInfo<PB, Fd>,
    ) -> Result<bool> {
        assert!(left_child_pos < self.descendent_count() - 1);
        let left_child = self.descendent_node_at_logical_pos(left_child_pos, pager_info)?;
        let right_child = self.descendent_node_at_logical_pos(left_child_pos + 1, pager_info)?;

        let fits = if left_child.is_node() {
            let merge_key = self.key_at_pos(left_child_pos)?;
            let key_size = serialized_size(&merge_key) as u16 + CELL_POINTER_SIZE;
            left_child.page_free_space() >= right_child.page_used_space() + key_size
        } else {
            left_child.page_free_space() >= right_child.page_used_space()
        };

        Ok(fits)
    }

    fn insert_trailing_key(&mut self, key: &K) -> Result<()> {
        assert!(self.is_node());
        let mut page = self.page_ref.borrow_mut();
        let key_pos = page.cell_count();
        page.insert_cell(key_pos, &to_bytes(key)?)?;
        Ok(())
    }

    fn remove_merged_key_and_id(&mut self, merged_logical_pos: u16) {
        let mut this_page = self.page_ref.borrow_mut();
        let key_cell_idx = Self::logical_node_key_pos_to_physical_pos(merged_logical_pos);
        this_page.remove_cell(key_cell_idx + 1); // remove page_id pointer
        this_page.remove_cell(key_cell_idx); // remove key
    }

    fn merge_children<Fd: AsRawFd + Copy>(
        &mut self,
        left_child_pos: u16,
        pager_info: &mut PagerInfo<PB, Fd>,
    ) -> Result<()> {
        assert!(left_child_pos < self.descendent_count() - 1);
        assert!(self.can_fit_via_merge(left_child_pos, pager_info)?);

        let mut left_child = self.descendent_node_at_logical_pos(left_child_pos, pager_info)?;
        let mut right_child =
            self.descendent_node_at_logical_pos(left_child_pos + 1, pager_info)?;

        let logical_start = if left_child.is_node() {
            // in the node case, we need to get the initial count because adding the split key will
            // mess up the calculation until the other cells are copied
            let initial_left_key_count = left_child.key_count();
            let key = self.key_from_inner_node(left_child_pos)?;
            left_child.insert_trailing_key(&key)?;
            initial_left_key_count + 1
        } else {
            left_child.key_count()
        };

        let from_range = if right_child.is_node() {
            0..=right_child.key_count()
        } else {
            0..=right_child.key_count() - 1
        };

        Self::move_cells(&mut right_child, &mut left_child, from_range, logical_start)?;

        if left_child.is_leaf() {
            // update right sibling pointer
            left_child.leaf_replace_right_sibling(&right_child.leaf_right_sibling()?)?;
        }

        // remove right page
        let right_page_id = right_child.page_id();
        // drop all references to right page so that the pager can safely mark it for deletion
        drop(right_child);
        pager_info.drop_page(right_page_id)?;

        self.remove_merged_key_and_id(left_child_pos);
        Ok(())
    }

    fn amount_to_steal(from: &Self, to: &Self) -> u16 {
        (from.page_used_space() - to.page_used_space()) / 2
    }

    fn insert_interior_split_key(&mut self, logical_key_pos: u16, key: &K) -> Result<()> {
        assert!(self.is_node());
        let mut page = self.page_ref.borrow_mut();
        let insert_pos = Self::logical_node_key_pos_to_physical_pos(logical_key_pos);
        page.insert_cell(insert_pos, &to_bytes(key)?)?;
        Ok(())
    }

    // TODO: far into the future, figure out how to handle cases where due to large cells, stealing
    // from a sibling doesn't bring the node being filled to the minimum size
    fn child_steal_from_left_sibling<Fd: AsRawFd + Copy>(
        &mut self,
        right_child_logical_pos: u16,
        pager_info: &mut PagerInfo<PB, Fd>,
    ) -> Result<()> {
        assert!(right_child_logical_pos > 0);

        let old_split_key = self.key_at_pos(right_child_logical_pos - 1)?;

        let mut left_child =
            self.descendent_node_at_logical_pos(right_child_logical_pos - 1, pager_info)?;
        let mut right_child =
            self.descendent_node_at_logical_pos(right_child_logical_pos, pager_info)?;

        let initial_size = if left_child.is_node() {
            // We need to account for the size of the split key we'll add. However, we don't yet
            // know what that key will be, or its size, so we'll use the old split key as a
            // stand-in to hopefully get us close.
            serialized_size(&old_split_key) as u16
        } else {
            0
        };
        let first_steal_pos = left_child.first_logical_pos_from_right_within_half_increment(
            Self::amount_to_steal(&left_child, &right_child),
            initial_size,
        );

        // get split key
        let new_split_key = left_child.key_at_pos(first_steal_pos - 1)?;

        // move cells
        let from_range = if left_child.is_node() {
            first_steal_pos..=left_child.key_count()
        } else {
            first_steal_pos..=left_child.key_count() - 1
        };

        Self::move_cells(&mut left_child, &mut right_child, from_range.clone(), 0)?;

        if right_child.is_node() {
            let key_insert_pos = from_range.len() - 1;
            right_child.insert_interior_split_key(key_insert_pos as u16, &old_split_key)?;
        }

        self.replace_inner_node_key(right_child_logical_pos - 1, &new_split_key)?;
        if left_child.is_node() {
            left_child.remove_trailing_key(first_steal_pos - 1);
        }
        Ok(())
    }

    fn child_steal_from_right_sibling<Fd: AsRawFd + Copy>(
        &mut self,
        left_child_logical_pos: u16,
        pager_info: &mut PagerInfo<PB, Fd>,
    ) -> Result<()> {
        assert!(left_child_logical_pos < self.descendent_count() - 1);

        let mut left_child =
            self.descendent_node_at_logical_pos(left_child_logical_pos, pager_info)?;
        let mut right_child =
            self.descendent_node_at_logical_pos(left_child_logical_pos + 1, pager_info)?;
        let keep_point = if right_child.is_leaf() {
            Self::leaf_siblings_space_used() + Self::amount_to_steal(&right_child, &left_child)
        } else {
            Self::amount_to_steal(&right_child, &left_child)
        };
        let first_keep_pos = right_child.first_logical_pos_past_size(keep_point);

        // get split key
        let new_split_key = right_child.key_at_pos(first_keep_pos - 1)?;

        // insert old split key
        if left_child.is_node() {
            let old_split_key = self.key_at_pos(left_child_logical_pos)?;
            left_child.insert_interior_split_key(left_child.key_count(), &old_split_key)?;
        }

        let logical_start = if left_child.is_node() {
            left_child.key_count() + 1
        } else {
            left_child.key_count()
        };
        Self::move_cells(
            &mut right_child,
            &mut left_child,
            0..=first_keep_pos - 1,
            logical_start,
        )?;

        self.replace_inner_node_key(left_child_logical_pos, &new_split_key)?;
        if right_child.is_node() {
            right_child.remove_leading_key();
        }
        Ok(())
    }

    fn remove<Fd: AsRawFd + Copy>(
        &mut self,
        key: &K,
        pager_info: &mut PagerInfo<PB, Fd>,
    ) -> Result<Option<V>> {
        if self.is_leaf() {
            if let Ok(logical) = self.binary_search_keys(key) {
                let val = self.value_from_leaf(logical)?;
                let mut page = self.page_ref.borrow_mut();
                page.remove_cell(Self::logical_leaf_key_pos_to_physical_pos(logical));
                Ok(Some(val))
            } else {
                Ok(None)
            }
        } else {
            assert!(self.is_node());
            let (logical_pos, mut child) = self.get_descendent_by_key(key, pager_info)?;
            let res = child.remove(key, pager_info)?;

            let space_used = child.page_used_space();
            drop(child);

            if space_used < PB::buffer_size() / 3 {
                if logical_pos > 0 && self.can_fit_via_merge(logical_pos - 1, pager_info)? {
                    // merge to left
                    self.merge_children(logical_pos - 1, pager_info)?;
                } else if logical_pos < self.descendent_count() - 1
                    && self.can_fit_via_merge(logical_pos, pager_info)?
                {
                    // merge right sibling into this one
                    self.merge_children(logical_pos, pager_info)?;
                } else if logical_pos == 0 {
                    // left edge case
                    self.child_steal_from_right_sibling(logical_pos, pager_info)?;
                } else if logical_pos == self.descendent_count() - 1 {
                    // right edge case
                    self.child_steal_from_left_sibling(logical_pos, pager_info)?;
                } else {
                    // steal from the smaller of siblings to, in theory, move less data around
                    let left_size = self
                        .descendent_node_at_logical_pos(logical_pos - 1, pager_info)?
                        .page_used_space();
                    let right_size = self
                        .descendent_node_at_logical_pos(logical_pos + 1, pager_info)?
                        .page_used_space();
                    if left_size < right_size {
                        self.child_steal_from_left_sibling(logical_pos, pager_info)?;
                    } else {
                        self.child_steal_from_right_sibling(logical_pos, pager_info)?;
                    }
                }
            }

            Ok(res)
        }
    }
}

#[cfg(test)]
/// This size allows for nodes with 5 keys and leaves with 7
/// - The min size for leaves is 2, and nodes is
const TEST_BUFFER_SIZE: u16 = 112;
#[cfg(test)]
pub struct TestPageBuffer {
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
impl<PB: PageBuffer> BTree<i32, PB, u32, u32> {
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
            .map(|s| DescriptionLine::from_str(s).unwrap())
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

    fn node_to_description(pager_info: &mut PagerInfo<PB, i32>, page_id: PageId) -> String {
        use std::collections::VecDeque;

        let mut description_lines = Vec::new();
        let mut queue = VecDeque::new();
        queue.push_back((vec![0], page_id));
        while let Some((ancestry, page_id)) = queue.pop_front() {
            let node: Node<PB, u32, u32> = pager_info.page_node(page_id).unwrap();
            let description_line = DescriptionLine::new(
                ancestry.clone(),
                node.is_leaf(),
                node.keys(),
                node.descendent_count().into(),
            );
            description_lines.push(description_line);
            queue.extend(node.descendent_page_ids().into_iter().enumerate().map(
                |(idx, page_id)| {
                    let mut child_ancestry = ancestry.clone();
                    child_ancestry.push(idx);
                    (child_ancestry, page_id)
                },
            ));
        }
        description_lines.into_iter().join("\n")
    }

    fn display_subtree(pager_info: &mut PagerInfo<PB, i32>, root_page_id: PageId) {
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
        range.map(|i| self.descendent_node_at_logical_pos(i, pager_info).unwrap())
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
            assert!(self.is_node());
            let page = self.page_ref.borrow();
            let count = page.cell_count() - self.key_count();
            assert_eq!(count - 1, self.key_count());
            count
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
    ) -> std::result::Result<Self, DescriptionLineError> {
        let mut new_node = pager_info.page_node(this_page_id).unwrap();
        let mut page = new_node.page_ref.borrow_mut();

        if this_node_line.is_leaf {
            // update sibling pointers
            for (i, key) in this_node_line.keys.iter().enumerate() {
                let bytes = to_bytes(&(key, key)).unwrap();
                // +2 to account for sibling pointers
                page.insert_cell(Self::logical_leaf_key_pos_to_physical_pos(i as u16), &bytes)
                    .unwrap();
            }
            drop(page);
            let (left_sibling, right_sibling) = sibling_page_ids;
            new_node.leaf_replace_left_sibling(&left_sibling).unwrap();
            new_node.leaf_replace_right_sibling(&right_sibling).unwrap();
        } else {
            let child_lines: Vec<_> = lines
                .peeking_take_while(|l| this_node_line.is_child_line(l))
                .collect();
            if child_lines.len() != this_node_line.child_count {
                return Err(DescriptionLineError::InvalidChildCount(
                    this_node_line.to_string(),
                ));
            }
            assert_eq!(child_lines.len(), this_node_line.child_count);
            assert_eq!(this_node_line.keys.len() + 1, this_node_line.child_count);
            let mut children = Vec::new();

            //this fills up with page ids, bookended with 0s, so that to know the siblings of
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
                page.insert_cell(
                    Self::logical_id_pos_to_physical_pos(idx as u16),
                    &page_id_bytes,
                )
                .unwrap();

                // Because we know that there is always 1 more child line that there is keys,
                // this will only be None on the last child line
                if idx < this_node_line.keys.len() {
                    let key = &this_node_line.keys[idx];
                    let key_bytes = to_bytes(key).unwrap();
                    page.insert_cell(
                        Self::logical_node_key_pos_to_physical_pos(idx as u16),
                        &key_bytes,
                    )
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
                )
                .unwrap();
            }
        }

        Ok(new_node)
    }
}

#[derive(Debug)]
enum DescriptionLineError {
    InvalidChildCount(String),
}
impl Display for DescriptionLineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidChildCount(s) => f.write_fmt(format_args!("InvalidChildCount - '{s}'")),
        }
    }
}
impl std::error::Error for DescriptionLineError {}

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
    fn new(traversal_path: Vec<usize>, is_leaf: bool, keys: Vec<u32>, child_count: usize) -> Self {
        DescriptionLine {
            traversal_path,
            is_leaf,
            keys,
            child_count,
        }
    }

    fn from_str(s: &str) -> std::result::Result<Self, DescriptionLineError> {
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

        let child_count = if is_leaf {
            0
        } else {
            second_half[closing_bracket_pos + 3..]
                .split(")")
                .next()
                .unwrap()
                .parse::<usize>()
                .unwrap()
        };

        if !is_leaf && keys.len() + 1 != child_count {
            return Err(DescriptionLineError::InvalidChildCount(s.to_string()));
        }

        Ok(DescriptionLine {
            traversal_path,
            is_leaf,
            keys,
            child_count,
        })
    }

    fn is_child_line(&self, candidate: &DescriptionLine) -> bool {
        let tvlen = self.traversal_path.len();
        candidate.traversal_path.len() == tvlen + 1
            && candidate.traversal_path[0..tvlen] == self.traversal_path
    }
}
#[cfg(test)]
impl Display for DescriptionLine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let path_parts: Vec<_> = self.traversal_path.iter().map(|x| x.to_string()).collect();
        let path = path_parts.join("->");
        if self.is_leaf {
            f.write_fmt(format_args!("{path}: L{:?}", self.keys))
        } else {
            f.write_fmt(format_args!(
                "{path}: {:?} ({})",
                self.keys, self.child_count
            ))
        }
    }
}

#[cfg(test)]
fn tree_keys_fully_ordered<PB: PageBuffer>(root: &Node<PB, u32, u32>) -> bool {
    let keys = root.keys();
    let mut sorted_keys = keys.clone();
    sorted_keys.sort();
    keys == sorted_keys
}

#[cfg(test)]
fn all_node_keys_ordered_and_deduped<PB: PageBuffer>(
    node: &Node<PB, u32, u32>,
    pager_info: &mut PagerInfo<PB, i32>,
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
fn all_keys_in_range<PB: PageBuffer>(node: &Node<PB, u32, u32>, min: u32, max: u32) -> bool {
    node.keys().iter().all(|k| (min..=max).contains(k))
}

#[cfg(test)]
fn all_subnode_keys_ordered_relative_to_node_keys<PB: PageBuffer>(
    node: &Node<PB, u32, u32>,
    pager_info: &mut PagerInfo<PB, i32>,
) -> bool {
    if node.is_leaf() {
        return true;
    }
    let mut min_key = u32::MIN;
    for (idx, k) in node.keys().iter().enumerate() {
        let max_key = *k;
        if !all_keys_in_range(
            &node
                .descendent_node_at_logical_pos(idx as u16, pager_info)
                .unwrap(),
            min_key,
            max_key,
        ) {
            return false;
        }
        min_key = k + 1;
    }
    all_keys_in_range(
        &node
            .descendent_node_at_logical_pos(node.key_count(), pager_info)
            .unwrap(),
        min_key,
        u32::MAX,
    )
}

#[cfg(test)]
fn all_nodes_sized_correctly<PB: PageBuffer>(
    root: &Node<PB, u32, u32>,
    pager_info: &mut PagerInfo<PB, i32>,
) -> bool {
    fn correct_cell_count<PB: PageBuffer>(node: &Node<PB, u32, u32>) -> bool {
        if node.is_leaf() {
            true
        } else {
            let page = node.page_ref.borrow();
            page.cell_count() % 2 == 1
        }
    }

    fn all_nodes_sized_correctly_not_root<PB: PageBuffer>(
        node: &Node<PB, u32, u32>,
        pager_info: &mut PagerInfo<PB, i32>,
    ) -> bool {
        let third_size = TestPageBuffer::buffer_size() / 3;
        let meets_minimum_size = node.page_free_space() >= third_size;

        let mut children_pass = || {
            if node.is_leaf() {
                true
            } else {
                let children: Vec<_> = node.descendent_iter(pager_info).collect();
                children
                    .iter()
                    .all(|node| all_nodes_sized_correctly_not_root(node, pager_info))
            }
        };

        meets_minimum_size && correct_cell_count(node) && children_pass()
    }

    let children: Vec<_> = root.descendent_iter(pager_info).collect();
    correct_cell_count(root)
        && children
            .iter()
            .all(|node| all_nodes_sized_correctly(node, pager_info))
}

#[cfg(test)]
fn all_leaves_same_level<PB: PageBuffer>(
    root: &Node<PB, u32, u32>,
    pager_info: &mut PagerInfo<PB, i32>,
) -> bool {
    fn leaf_levels<PB: PageBuffer>(
        node: &Node<PB, u32, u32>,
        level: usize,
        pager_info: &mut PagerInfo<PB, i32>,
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
fn assert_subtree_valid<PB: PageBuffer>(
    node: &Node<PB, u32, u32>,
    pager_info: &mut PagerInfo<PB, i32>,
) {
    assert!(all_nodes_sized_correctly(node, pager_info));
    assert!(tree_keys_fully_ordered(node));
    assert!(all_node_keys_ordered_and_deduped(node, pager_info));
    assert!(all_subnode_keys_ordered_relative_to_node_keys(
        node, pager_info
    ));
    assert!(all_leaves_same_level(node, pager_info));
}

#[cfg(test)]
mod tests {
    use std::{
        cell::RefCell,
        collections::BTreeMap,
        fmt::Debug,
        fs::{self, File, OpenOptions},
        os::fd::AsRawFd,
        rc::Rc,
    };

    use itertools::Itertools;
    use proptest::prelude::*;
    use proptest_state_machine::{prop_state_machine, ReferenceStateMachine, StateMachineTest};
    use serde::{de::DeserializeOwned, Serialize};
    use serialize::serialized_size;

    use super::{
        all_leaves_same_level, all_node_keys_ordered_and_deduped, all_nodes_sized_correctly,
        all_subnode_keys_ordered_relative_to_node_keys, assert_subtree_valid,
        tree_keys_fully_ordered, TEST_BUFFER_SIZE,
    };

    use crate::pager::{PageBuffer, PageId, Pager, CELL_POINTER_SIZE};

    use super::{BTree, KeyLimit, TestPageBuffer};

    fn trim_lines(s: &str) -> String {
        s.trim().lines().map(|l| l.trim()).join("\n")
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

        BTree::<i32, TestPageBuffer, u32, u32>::from_description(description, pager_ref, backing_fd)
    }

    fn init_tree_in_file(filename: &str) -> BTree<i32, TestPageBuffer, u32, u32> {
        let file = open_file(filename);
        let backing_fd = file.as_raw_fd();
        let pager_ref = Rc::new(RefCell::new(Pager::new(vec![file])));

        BTree::init(pager_ref, backing_fd).unwrap()
    }

    fn init_tree_in_file_with_pb<PB: PageBuffer>(filename: &str) -> BTree<i32, PB, u32, u32> {
        let file = open_file(filename);
        let backing_fd = file.as_raw_fd();
        let pager_ref = Rc::new(RefCell::new(Pager::new(vec![file])));

        BTree::init(pager_ref, backing_fd).unwrap()
    }

    #[test]
    fn sizing_proofs() {
        // These constants may change in the future. They're just tested here to prove that my
        // assumptions about tree construction during the tests are correct.

        let minimum_treshold: usize = TEST_BUFFER_SIZE as usize / 3;

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
        assert_eq!(leaf_entry_size, 12);

        let space_for_n_leaf_keys =
            |n: usize| sibling_pointers_size as usize + (n * leaf_entry_size);
        // fits 7 leaf keys
        assert!(TEST_BUFFER_SIZE as usize >= space_for_n_leaf_keys(7));
        assert!((TEST_BUFFER_SIZE as usize) < space_for_n_leaf_keys(8));

        // size below minimum threshold is 1
        assert!(minimum_treshold > space_for_n_leaf_keys(1));
        assert!(minimum_treshold <= space_for_n_leaf_keys(2));

        let node_key_entry_size = CELL_POINTER_SIZE as usize + node_key_size;
        let node_page_id_entry_size = CELL_POINTER_SIZE as usize + node_page_id_size;
        assert_eq!(node_key_entry_size, 8);
        assert_eq!(node_page_id_entry_size, 12);

        let space_for_n_node_keys =
            |n: usize| (node_key_entry_size * n) + (node_page_id_entry_size * (n + 1));

        // fits 5 node keys
        assert!(TEST_BUFFER_SIZE as usize >= space_for_n_node_keys(5));
        assert!((TEST_BUFFER_SIZE as usize) < space_for_n_node_keys(6));

        // size below minimum threshold is 1
        assert!(minimum_treshold > space_for_n_node_keys(1));
        assert!(minimum_treshold <= space_for_n_node_keys(2));
    }

    #[test]
    fn end_to_end_description() {
        let input_description = "
            0: [12, 23] (3)
            0->0: [3, 6, 9] (4)
            0->1: [15, 17, 20] (4)
            0->2: [28] (2)
            0->0->0: L[1, 2, 3] 
            0->0->1: L[4, 5, 6]
            0->0->2: L[7, 8, 9] 
            0->0->3: L[10, 11, 12] 
            0->1->0: L[13, 14, 15]
            0->1->1: L[16, 17] 
            0->1->2: L[18, 19, 20] 
            0->1->3: L[21, 22, 23]
            0->2->0: L[24, 25, 26, 27] 
            0->2->1: L[29, 30, 31]";
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
        let description = "0: L[2, 4, 6]";

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
        let description = "0: L[2, 4, 6, 8, 10]";

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
    fn replace_value_in_leaf() {
        let filename = "replace_value_in_leaf.test";
        let input_tree = trim_lines("0: L[1, 2, 3, 4]");
        let expected_tree = trim_lines("0: L[1, 2, 3, 4]");

        let mut t = init_tree_from_description_in_file(filename, &input_tree);
        t.insert(2, 42).unwrap();

        assert_eq!(t.to_description(), expected_tree);
        assert_eq!(t.get(&2).unwrap(), Some(42));
        assert_subtree_valid(&t.root, &mut t.pager_info());
    }

    #[test]
    fn single_insertion() {
        let filename = "single_insertion.test";
        let expected_tree = "0: L[1]";
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
            0: L[1, 2, 3, 4, 5]
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
            0: [3] (2)
            0->0: L[1, 2, 3]
            0->1: L[4, 5, 6, 7, 8] 
        ";
        let expected_tree = trim_lines(expected_tree);

        let mut tree = init_tree_in_file(filename);

        for i in 1..=8 {
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
            0: [3, 6, 9, 12, 15] (6)
            0->0: L[1, 2, 3]
            0->1: L[4, 5, 6] 
            0->2: L[7, 8, 9]
            0->3: L[10, 11, 12] 
            0->4: L[13, 14, 15] 
            0->5: L[16, 17, 18, 19, 20, 21, 22] 
        ";
        let init_tree = trim_lines(init_tree);

        let expected_tree = "
            0: [9] (2)
            0->0: [3, 6] (3)
            0->1: [12, 15, 18] (4)
            0->0->0: L[1, 2, 3] 
            0->0->1: L[4, 5, 6] 
            0->0->2: L[7, 8, 9]
            0->1->0: L[10, 11, 12] 
            0->1->1: L[13, 14, 15] 
            0->1->2: L[16, 17, 18] 
            0->1->3: L[19, 20, 21, 22, 23]
        ";
        let expected_tree = trim_lines(expected_tree);

        let mut tree = init_tree_from_description_in_file(filename, &init_tree);
        tree.insert(23, 23).unwrap();

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
            0->0: L[1, 2, 3] 
            0->1: L[4, 5, 6, 7, 8, 9, 10] 
            ";
        let input_tree = trim_lines(input_tree);

        let output_tree = "
            0: [3, 6] (3)
            0->0: L[1, 2, 3] 
            0->1: L[4, 5, 6] 
            0->2: L[7, 8, 9, 10, 11] 
            ";
        let output_tree = trim_lines(output_tree);

        let mut t = init_tree_from_description_in_file(filename, &input_tree);
        t.insert(11, 11).unwrap();

        assert_eq!(&t.to_description(), &output_tree);
        drop(t);
        fs::remove_file(filename).unwrap();
    }

    #[test]
    fn split_as_leaf_insert_left() {
        let filename = "split_as_leaf_insert_left.test";
        let input_tree = "
            0: [3] (2)
            0->0: L[1, 2, 3] 
            0->1: L[5, 6, 7, 8, 9, 10, 11] 
            ";
        let input_tree = trim_lines(input_tree);

        let output_tree = "
            0: [3, 7] (3)
            0->0: L[1, 2, 3] 
            0->1: L[4, 5, 6, 7] 
            0->2: L[8, 9, 10, 11] 
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
            0->1: [15, 23, 26, 29, 32] (6)
            0->0->0: L[1, 2, 3] 
            0->0->1: L[4, 5, 6] 
            0->0->2: L[7, 8, 9] 
            0->0->3: L[10, 11, 12] 
            0->1->0: L[13, 14, 15] 
            0->1->1: L[16, 17, 18, 20, 21, 22, 23] 
            0->1->2: L[24, 25, 26] 
            0->1->3: L[17, 28, 29] 
            0->1->4: L[30, 31, 32] 
            0->1->5: L[33, 34, 35] 
        ";
        let input_tree = trim_lines(input_tree);

        let output_tree = "
            0: [12, 26] (3)
            0->0: [3, 6, 9] (4)
            0->1: [15, 18, 23] (4)
            0->2: [29, 32] (3)
            0->0->0: L[1, 2, 3] 
            0->0->1: L[4, 5, 6] 
            0->0->2: L[7, 8, 9] 
            0->0->3: L[10, 11, 12] 
            0->1->0: L[13, 14, 15] 
            0->1->1: L[16, 17, 18] 
            0->1->2: L[19, 20, 21, 22, 23] 
            0->1->3: L[24, 25, 26] 
            0->2->0: L[17, 28, 29] 
            0->2->1: L[30, 31, 32] 
            0->2->2: L[33, 34, 35] 
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
            0->1: [15, 18, 21, 24, 32] (6)
            0->0->0: L[1, 2, 3] 
            0->0->1: L[4, 5, 6] 
            0->0->2: L[7, 8, 9] 
            0->0->3: L[10, 11, 12] 
            0->1->0: L[13, 14, 15] 
            0->1->1: L[16, 17, 18] 
            0->1->2: L[19, 20, 21] 
            0->1->3: L[22, 23, 24] 
            0->1->4: L[25, 26, 27, 28, 29, 30, 32] 
            0->1->5: L[33, 34, 35] 
        ";
        let input_tree = trim_lines(input_tree);

        let output_tree = "
            0: [12, 21] (3)
            0->0: [3, 6, 9] (4)
            0->1: [15, 18] (3)
            0->2: [24, 27, 32] (4)
            0->0->0: L[1, 2, 3] 
            0->0->1: L[4, 5, 6] 
            0->0->2: L[7, 8, 9] 
            0->0->3: L[10, 11, 12] 
            0->1->0: L[13, 14, 15] 
            0->1->1: L[16, 17, 18] 
            0->1->2: L[19, 20, 21] 
            0->2->0: L[22, 23, 24] 
            0->2->1: L[25, 26, 27] 
            0->2->2: L[28, 29, 30, 31, 32] 
            0->2->3: L[33, 34, 35] 
        ";
        let output_tree = trim_lines(output_tree);

        let mut t = init_tree_from_description_in_file(filename, &input_tree);
        t.insert(31, 31).unwrap();

        assert_eq!(&t.to_description(), &output_tree);
        assert_subtree_valid(&t.root, &mut t.pager_info());

        drop(t);
        fs::remove_file(filename).unwrap();
    }

    #[test]
    fn leaf_removal() {
        let filename = "leaf_removal.test";
        let input_tree = "0: L[1, 2, 3, 4, 5]";
        let output_tree = "0: L[1, 2, 4, 5]";

        let mut t = init_tree_from_description_in_file(filename, input_tree);
        let val = t.remove(&3).unwrap();

        assert_eq!(&t.to_description(), output_tree);
        assert_eq!(val, Some(3));
        assert_subtree_valid(&t.root, &mut t.pager_info());

        drop(t);
        fs::remove_file(filename).unwrap();
    }

    #[test]
    fn leaf_removal_noop() {
        let filename = "leaf_removal_noop.test";
        let input_tree = "0: L[1, 2, 3, 4, 5]";
        let output_tree = "0: L[1, 2, 3, 4, 5]";

        let mut t = init_tree_from_description_in_file(filename, input_tree);
        let val = t.remove(&42).unwrap();

        assert_eq!(&t.to_description(), output_tree);
        assert_eq!(val, None);
        assert_subtree_valid(&t.root, &mut t.pager_info());

        drop(t);
        fs::remove_file(filename).unwrap();
    }

    #[test]
    fn merge_left_leaf() {
        let filename = "merge_left_leaf.test";
        let input_tree = "
            0: [2, 6] (3)
            0->0: L[0, 1, 2]
            0->1: L[5, 6] 
            0->2: L[7, 8, 9, 10]
        ";
        let input_tree = trim_lines(input_tree);

        let output_tree = "
            0: [6] (2)
            0->0: L[0, 1, 2, 5] 
            0->1: L[7, 8, 9, 10] 
        ";
        let output_tree = trim_lines(output_tree);

        let mut t = init_tree_from_description_in_file(filename, &input_tree);
        let val = t.remove(&6).unwrap();

        assert_eq!(&t.to_description(), &output_tree);
        assert_eq!(val, Some(6));
        assert_subtree_valid(&t.root, &mut t.pager_info());

        drop(t);
        fs::remove_file(filename).unwrap();
    }

    #[test]
    fn merge_right_leaf() {
        let filename = "merge_right_leaf.test";
        let input_tree = "
            0: [5, 7] (3)
            0->0: L[0, 1, 2, 3, 4, 5] 
            0->1: L[6, 7] 
            0->2: L[8, 9, 10]
        ";
        let input_tree = trim_lines(input_tree);

        let output_tree = "
            0: [5] (2)
            0->0: L[0, 1, 2, 3, 4, 5] 
            0->1: L[6, 8, 9, 10] 
        ";
        let output_tree = trim_lines(output_tree);

        let mut t = init_tree_from_description_in_file(filename, &input_tree);
        let val = t.remove(&7).unwrap();

        assert_eq!(&t.to_description(), &output_tree);
        assert_eq!(val, Some(7));
        assert_subtree_valid(&t.root, &mut t.pager_info());

        drop(t);
        fs::remove_file(filename).unwrap();
    }

    #[test]
    fn merge_left_node() {
        let filename = "merge_left_node.test";
        let input_tree = "
            0: [7, 13] (3)
            0->0: [1, 3, 5] (4)
            0->1: [9, 11] (3)
            0->2: [15, 17, 19, 21] (5)
            0->0->0: L[0, 1] 
            0->0->1: L[2, 3] 
            0->0->2: L[4, 5] 
            0->0->3: L[6, 7] 
            0->1->0: L[8, 9] 
            0->1->1: L[10, 11] 
            0->1->2: L[12, 13] 
            0->2->0: L[14, 15] 
            0->2->1: L[16, 17] 
            0->2->2: L[18, 19] 
            0->2->3: L[20, 21] 
            0->2->4: L[22, 23] 
        ";
        let input_tree = trim_lines(input_tree);

        let output_tree = "
            0: [13] (2)
            0->0: [1, 3, 5, 7, 11] (6)
            0->1: [15, 17, 19, 21] (5)
            0->0->0: L[0, 1] 
            0->0->1: L[2, 3] 
            0->0->2: L[4, 5] 
            0->0->3: L[6, 7] 
            0->0->4: L[9, 10, 11] 
            0->0->5: L[12, 13] 
            0->1->0: L[14, 15] 
            0->1->1: L[16, 17] 
            0->1->2: L[18, 19] 
            0->1->3: L[20, 21] 
            0->1->4: L[22, 23] 
        ";
        let output_tree = trim_lines(output_tree);

        let mut t = init_tree_from_description_in_file(filename, &input_tree);
        let val = t.remove(&8).unwrap();

        assert_eq!(&t.to_description(), &output_tree);
        assert_eq!(val, Some(8));
        assert_subtree_valid(&t.root, &mut t.pager_info());

        drop(t);
        fs::remove_file(filename).unwrap();
    }

    #[test]
    fn merge_right_node() {
        // A merge-right only happens if the left node can not fit the node
        let filename = "merge_right_node.test";
        let input_tree = "
            0: [11, 17] (3)
            0->0: [1, 3, 5, 7, 9] (6)
            0->1: [13, 15] (3)
            0->2: [19, 21, 23] (4)
            0->0->0: L[0, 1] 
            0->0->1: L[2, 3] 
            0->0->2: L[4, 5] 
            0->0->3: L[6, 7] 
            0->0->4: L[8, 9] 
            0->0->5: L[10, 11] 
            0->1->0: L[12, 13] 
            0->1->1: L[14, 15] 
            0->1->2: L[16, 17] 
            0->2->0: L[18, 19] 
            0->2->1: L[20, 21] 
            0->2->2: L[22, 23] 
            0->2->3: L[24, 25] 
        ";
        let input_tree = trim_lines(input_tree);

        let output_tree = "
            0: [11] (2)
            0->0: [1, 3, 5, 7, 9] (6)
            0->1: [15, 17, 19, 21, 23] (6)
            0->0->0: L[0, 1] 
            0->0->1: L[2, 3] 
            0->0->2: L[4, 5] 
            0->0->3: L[6, 7] 
            0->0->4: L[8, 9] 
            0->0->5: L[10, 11] 
            0->1->0: L[12, 14, 15] 
            0->1->1: L[16, 17] 
            0->1->2: L[18, 19] 
            0->1->3: L[20, 21] 
            0->1->4: L[22, 23] 
            0->1->5: L[24, 25] 
        ";
        let output_tree = trim_lines(output_tree);

        let mut t = init_tree_from_description_in_file(filename, &input_tree);
        let val = t.remove(&13).unwrap();

        assert_eq!(&t.to_description(), &output_tree);
        assert_eq!(val, Some(13));
        assert_subtree_valid(&t.root, &mut t.pager_info());

        drop(t);
        fs::remove_file(filename).unwrap();
    }

    #[test]
    fn steal_from_left_leaf() {
        let filename = "steal_from_left_leaf.test";
        let input_tree = "
            0: [6, 8] (3)
            0->0: L[1, 2, 3, 4, 5, 6] 
            0->1: L[7, 8] 
            0->2: L[9, 10, 11, 12, 13, 14, 15] 
        ";
        let input_tree = trim_lines(input_tree);

        let output_tree = "
            0: [4, 8] (3)
            0->0: L[1, 2, 3, 4] 
            0->1: L[5, 6, 7] 
            0->2: L[9, 10, 11, 12, 13, 14, 15] 
        ";
        let output_tree = trim_lines(output_tree);

        let mut t = init_tree_from_description_in_file(filename, &input_tree);
        let val = t.remove(&8).unwrap();

        assert_eq!(&t.to_description(), &output_tree);
        assert_eq!(val, Some(8));
        assert_subtree_valid(&t.root, &mut t.pager_info());

        drop(t);
        fs::remove_file(filename).unwrap();
    }

    #[test]
    fn steal_from_left_leaf_edge() {
        let filename = "steal_from_left_leaf_edge.test";
        let input_tree = "
            0: [7] (2)
            0->0: L[1, 2, 3, 4, 5, 6, 7] 
            0->1: L[8, 9] 
        ";
        let input_tree = trim_lines(input_tree);

        let output_tree = "
            0: [4] (2)
            0->0: L[1, 2, 3, 4] 
            0->1: L[5, 6, 7, 8] 
        ";
        let output_tree = trim_lines(output_tree);

        let mut t = init_tree_from_description_in_file(filename, &input_tree);
        let val = t.remove(&9).unwrap();

        assert_eq!(&t.to_description(), &output_tree);
        assert_eq!(val, Some(9));
        assert_subtree_valid(&t.root, &mut t.pager_info());

        drop(t);
        fs::remove_file(filename).unwrap();
    }

    #[test]
    fn steal_from_right_leaf() {
        let filename = "steal_from_right_leaf.test";
        let input_tree = "
            0: [7, 9] (3)
            0->0: L[1, 2, 3, 4, 5, 6, 7] 
            0->1: L[8, 9] 
            0->2: L[10, 11, 12, 13, 14, 15] 
        ";
        let input_tree = trim_lines(input_tree);

        let output_tree = "
            0: [7, 11] (3)
            0->0: L[1, 2, 3, 4, 5, 6, 7] 
            0->1: L[8, 10, 11] 
            0->2: L[12, 13, 14, 15] 
        ";
        let output_tree = trim_lines(output_tree);

        let mut t = init_tree_from_description_in_file(filename, &input_tree);
        let val = t.remove(&9).unwrap();

        assert_eq!(&t.to_description(), &output_tree);
        assert_eq!(val, Some(9));
        assert_subtree_valid(&t.root, &mut t.pager_info());

        drop(t);
        fs::remove_file(filename).unwrap();
    }

    #[test]
    fn steal_from_right_leaf_edge() {
        let filename = "steal_from_right_leaf_edge.test";
        let input_tree = "
            0: [1] (2)
            0->0: L[0, 1] (0)
            0->1: L[2, 3, 4, 5, 6, 7, 8] 
        ";
        let input_tree = trim_lines(input_tree);

        let output_tree = "
            0: [4] (2)
            0->0: L[0, 2, 3, 4] 
            0->1: L[5, 6, 7, 8] 
        ";
        let output_tree = trim_lines(output_tree);

        let mut t = init_tree_from_description_in_file(filename, &input_tree);
        let val = t.remove(&1).unwrap();

        assert_eq!(&t.to_description(), &output_tree);
        assert_eq!(val, Some(1));
        assert_subtree_valid(&t.root, &mut t.pager_info());

        drop(t);
        fs::remove_file(filename).unwrap();
    }

    #[test]
    fn steal_from_left_node() {
        let filename = "steal_from_left_node.test";
        let input_tree = "
            0: [9, 15] (3)
            0->0: [1, 3, 5, 7] (5)
            0->1: [11, 13] (3)
            0->2: [17, 19, 21, 23, 25] (6)
            0->0->0: L[0, 1] 
            0->0->1: L[2, 3] 
            0->0->2: L[4, 5] 
            0->0->3: L[6, 7] 
            0->0->4: L[8, 9] 
            0->1->0: L[10, 11] 
            0->1->1: L[12, 13] 
            0->1->2: L[14, 15] 
            0->2->0: L[16, 17] 
            0->2->1: L[18, 19] 
            0->2->2: L[20, 21] 
            0->2->3: L[22, 23] 
            0->2->4: L[24, 25] 
            0->2->5: L[26, 27] 
        ";
        let input_tree = trim_lines(input_tree);

        let output_tree = "
            0: [5, 15] (3)
            0->0: [1, 3] (3)
            0->1: [7, 9, 13] (4)
            0->2: [17, 19, 21, 23, 25] (6)
            0->0->0: L[0, 1] 
            0->0->1: L[2, 3] 
            0->0->2: L[4, 5] 
            0->1->0: L[6, 7] 
            0->1->1: L[8, 9] 
            0->1->2: L[10, 11, 12] 
            0->1->3: L[14, 15] 
            0->2->0: L[16, 17] 
            0->2->1: L[18, 19] 
            0->2->2: L[20, 21] 
            0->2->3: L[22, 23] 
            0->2->4: L[24, 25] 
            0->2->5: L[26, 27] 
        ";
        let output_tree = trim_lines(output_tree);

        let mut t = init_tree_from_description_in_file(filename, &input_tree);
        let val = t.remove(&13).unwrap();

        assert_eq!(&t.to_description(), &output_tree);
        assert_eq!(val, Some(13));
        assert_subtree_valid(&t.root, &mut t.pager_info());

        drop(t);
        fs::remove_file(filename).unwrap();
    }

    #[test]
    fn steal_from_right_node() {
        let filename = "steal_from_right_node.test";
        let input_tree = "
            0: [11, 17] (3)
            0->0: [1, 3, 5, 7, 9] (6)
            0->1: [13, 15] (3)
            0->2: [19, 21, 23, 25] (5)
            0->0->0: L[0, 1] 
            0->0->1: L[2, 3] 
            0->0->2: L[4, 5] 
            0->0->3: L[6, 7] 
            0->0->4: L[8, 9] 
            0->0->5: L[10, 11] 
            0->1->0: L[12, 13] 
            0->1->1: L[14, 15] 
            0->1->2: L[16, 17] 
            0->2->0: L[18, 19] 
            0->2->1: L[20, 21] 
            0->2->2: L[22, 23] 
            0->2->3: L[24, 25] 
            0->2->4: L[26, 27] 
        ";
        let input_tree = trim_lines(input_tree);

        let output_tree = "
            0: [11, 19] (3)
            0->0: [1, 3, 5, 7, 9] (6)
            0->1: [15, 17] (3)
            0->2: [21, 23, 25] (4)
            0->0->0: L[0, 1] 
            0->0->1: L[2, 3] 
            0->0->2: L[4, 5] 
            0->0->3: L[6, 7] 
            0->0->4: L[8, 9] 
            0->0->5: L[10, 11] 
            0->1->0: L[12, 14, 15] 
            0->1->1: L[16, 17] 
            0->1->2: L[18, 19]
            0->2->0: L[20, 21] 
            0->2->1: L[22, 23] 
            0->2->2: L[24, 25] 
            0->2->3: L[26, 27] 
        ";
        let output_tree = trim_lines(output_tree);

        let mut t = init_tree_from_description_in_file(filename, &input_tree);
        let val = t.remove(&13).unwrap();

        assert_eq!(&t.to_description(), &output_tree);
        assert_eq!(val, Some(13));
        assert_subtree_valid(&t.root, &mut t.pager_info());

        drop(t);
        fs::remove_file(filename).unwrap();
    }

    #[test]
    fn steal_from_left_node_edge() {
        let filename = "steal_from_left_node_edge.test";
        let input_tree = "
            0: [11] (2)
            0->0: [1, 3, 5, 7, 9] (6)
            0->1: [13, 15] (3)
            0->0->0: L[0, 1] 
            0->0->1: L[2, 3] 
            0->0->2: L[4, 5] 
            0->0->3: L[6, 7] 
            0->0->4: L[8, 9] 
            0->0->5: L[10, 11] 
            0->1->0: L[12, 13] 
            0->1->1: L[14, 15] 
            0->1->2: L[16, 17] 
       ";
        let input_tree = trim_lines(input_tree);

        let output_tree = "
            0: [7] (2)
            0->0: [1, 3, 5] (4)
            0->1: [9, 11, 15] (4)
            0->0->0: L[0, 1] 
            0->0->1: L[2, 3] 
            0->0->2: L[4, 5] 
            0->0->3: L[6, 7] 
            0->1->0: L[8, 9] 
            0->1->1: L[10, 11] 
            0->1->2: L[12, 13, 15] 
            0->1->3: L[16, 17] 
        ";
        let output_tree = trim_lines(output_tree);

        let mut t = init_tree_from_description_in_file(filename, &input_tree);
        let val = t.remove(&14).unwrap();

        assert_eq!(&t.to_description(), &output_tree);
        assert_eq!(val, Some(14));
        assert_subtree_valid(&t.root, &mut t.pager_info());

        drop(t);
        fs::remove_file(filename).unwrap();
    }

    #[test]
    fn steal_from_right_node_edge() {
        let filename = "steal_from_right_node_edge.test";
        let input_tree = "
            0: [19] (2)
            0->0: [15, 17] (3)
            0->1: [21, 23, 25, 27, 29] (6)
            0->0->0: L[14, 15] 
            0->0->1: L[16, 17] 
            0->0->2: L[18, 19] 
            0->1->0: L[20, 21] 
            0->1->1: L[22, 23] 
            0->1->2: L[24, 25] 
            0->1->3: L[26, 27] 
            0->1->4: L[28, 29] 
            0->1->5: L[30, 31] 
        ";
        let input_tree = trim_lines(input_tree);

        let output_tree = "
            0: [23] (2)
            0->0: [17, 19, 21] (4)
            0->1: [25, 27, 29] (4)
            0->0->0: L[14, 15, 17] 
            0->0->1: L[18, 19] 
            0->0->2: L[20, 21] 
            0->0->3: L[22, 23] 
            0->1->0: L[24, 25] 
            0->1->1: L[26, 27] 
            0->1->2: L[28, 29] 
            0->1->3: L[30, 31] 
        ";
        let output_tree = trim_lines(output_tree);

        let mut t = init_tree_from_description_in_file(filename, &input_tree);
        let val = t.remove(&16).unwrap();

        assert_eq!(&t.to_description(), &output_tree);
        assert_eq!(val, Some(16));
        assert_subtree_valid(&t.root, &mut t.pager_info());

        drop(t);
        fs::remove_file(filename).unwrap();
    }

    #[test]
    fn basic_iter_test() {
        let filename = "basic_iter_test.test";
        let mut t = init_tree_in_file(filename);

        let mut expected = Vec::new();
        for i in 0..=50 {
            t.insert(i, i).unwrap();
            expected.push((i, i));
        }

        let actual: Vec<_> = t
            .iter(KeyLimit::None, KeyLimit::None)
            .unwrap()
            .map(|x| x.unwrap())
            .collect();
        assert_eq!(actual, expected);

        drop(t);
        fs::remove_file(filename).unwrap();
    }

    #[test]
    fn iter_test_inclusive_limits() {
        let filename = "iter_test_inclusive_limits.test";
        let mut t = init_tree_in_file(filename);

        let mut expected = Vec::new();
        for i in 0..=50 {
            t.insert(i, i).unwrap();
            expected.push((i, i));
        }
        expected.retain(|x| x.0 >= 10 && x.0 <= 40);

        let actual: Vec<_> = t
            .iter(KeyLimit::Inclusive(10), KeyLimit::Inclusive(40))
            .unwrap()
            .map(|x| x.unwrap())
            .collect();
        assert_eq!(actual, expected);

        drop(t);
        fs::remove_file(filename).unwrap();
    }

    #[test]
    fn iter_test_exclusive_limits() {
        let filename = "iter_test_exclusive_limits.test";
        let mut t = init_tree_in_file(filename);

        let mut expected = Vec::new();
        for i in 0..=50 {
            t.insert(i, i).unwrap();
            expected.push((i, i));
        }
        expected.retain(|x| x.0 > 10 && x.0 < 40);

        let actual: Vec<_> = t
            .iter(KeyLimit::Exclusive(10), KeyLimit::Exclusive(40))
            .unwrap()
            .map(|x| x.unwrap())
            .collect();
        assert_eq!(actual, expected);

        drop(t);
        fs::remove_file(filename).unwrap();
    }

    /*
     * Proptest stuff below here ---------------------------
     */

    fn first_nonretrievable_inserted_value<PB: PageBuffer>(
        tree: &BTree<i32, PB, u32, u32>,
        ref_tree: &BTreeMap<u32, u32>,
    ) -> Option<u32> {
        ref_tree
            .iter()
            .find(|(k, v)| tree.get(k).unwrap() != Some(**v))
            .map(|(k, v)| {
                println!("didn't find: ({k}, {v})");
                println!("actual value: {:?}", tree.get(k));
                *k
            })
    }

    #[derive(Debug, Clone)]
    pub enum TreeOperation {
        Insert(u32, u32),
        Remove(u32),
    }

    #[derive(Debug, Clone)]
    pub struct ReferenceBTree {
        ref_tree: BTreeMap<u32, u32>,
    }
    impl ReferenceStateMachine for ReferenceBTree {
        type State = Self;
        type Transition = TreeOperation;

        fn init_state() -> BoxedStrategy<Self::State> {
            let ref_tree = ReferenceBTree {
                ref_tree: BTreeMap::new(),
            };
            Just(ref_tree).boxed()
        }

        fn transitions(state: &Self::State) -> BoxedStrategy<Self::Transition> {
            if !state.ref_tree.is_empty() {
                let keys: Vec<_> = state.ref_tree.keys().cloned().collect();
                let removal_key = proptest::sample::select(keys);
                prop_oneof![
                    (any::<u32>(), any::<u32>()).prop_map(|(k, v)| TreeOperation::Insert(k, v)),
                    removal_key.prop_map(TreeOperation::Remove)
                ]
                .boxed()
            } else {
                (any::<u32>(), any::<u32>())
                    .prop_map(|(k, v)| TreeOperation::Insert(k, v))
                    .boxed()
            }
        }

        fn apply(mut state: Self::State, transition: &Self::Transition) -> Self::State {
            match transition {
                TreeOperation::Insert(k, v) => state.ref_tree.insert(*k, *v),
                TreeOperation::Remove(k) => state.ref_tree.remove(k),
            };
            state
        }

        fn preconditions(state: &Self::State, transition: &Self::Transition) -> bool {
            match transition {
                TreeOperation::Insert(_, _) => true,
                TreeOperation::Remove(k) => state.ref_tree.contains_key(k),
            }
        }
    }

    pub struct BTreeTestWrapper<
        PB: PageBuffer,
        K: Ord + Serialize + DeserializeOwned + Debug,
        V: Serialize + DeserializeOwned,
    > {
        tree: BTree<i32, PB, K, V>,
        filename: String,
    }
    impl<
            PB: PageBuffer,
            K: Ord + Serialize + DeserializeOwned + Debug,
            V: Serialize + DeserializeOwned,
        > BTreeTestWrapper<PB, K, V>
    {
        fn new(tree: BTree<i32, PB, K, V>, filename: String) -> Self {
            BTreeTestWrapper { tree, filename }
        }
    }

    impl<PB: PageBuffer> StateMachineTest for BTree<i32, PB, u32, u32> {
        type SystemUnderTest = BTreeTestWrapper<PB, u32, u32>;
        type Reference = ReferenceBTree;

        fn init_test(
            _ref_state: &<Self::Reference as ReferenceStateMachine>::State,
        ) -> Self::SystemUnderTest {
            let filename = "btree_state_machine_u32_u32.test";
            let t = init_tree_in_file_with_pb(filename);
            BTreeTestWrapper::new(t, filename.to_string())
        }

        fn apply(
            mut state: Self::SystemUnderTest,
            _ref_state: &<Self::Reference as ReferenceStateMachine>::State,
            transition: <Self::Reference as ReferenceStateMachine>::Transition,
        ) -> Self::SystemUnderTest {
            match transition {
                TreeOperation::Remove(k) => {
                    let res = state.tree.remove(&k).unwrap();
                    assert!(res.is_some());
                    println!("{}", state.tree.to_description());
                    assert!(state.tree.get(&k).unwrap().is_none());
                }
                TreeOperation::Insert(k, v) => {
                    state.tree.insert(k, v).unwrap();
                    println!("{}", state.tree.to_description());
                    assert_eq!(state.tree.get(&k).unwrap(), Some(v));
                }
            };
            state
        }

        fn check_invariants(
            state: &Self::SystemUnderTest,
            ref_state: &<Self::Reference as ReferenceStateMachine>::State,
        ) {
            assert!(tree_keys_fully_ordered(&state.tree.root));
            assert_eq!(
                first_nonretrievable_inserted_value(&state.tree, &ref_state.ref_tree),
                None
            );
            assert!(all_node_keys_ordered_and_deduped(
                &state.tree.root,
                &mut state.tree.pager_info()
            ));
            assert!(all_subnode_keys_ordered_relative_to_node_keys(
                &state.tree.root,
                &mut state.tree.pager_info()
            ));
            assert!(all_nodes_sized_correctly(
                &state.tree.root,
                &mut state.tree.pager_info()
            ));
            assert!(all_leaves_same_level(
                &state.tree.root,
                &mut state.tree.pager_info()
            ));
        }

        fn teardown(state: Self::SystemUnderTest) {
            drop(state.tree);
            fs::remove_file(state.filename).unwrap();
        }
    }

    prop_state_machine! {
        #![proptest_config(ProptestConfig {
             // Enable verbose mode to make the state machine test print the
             // transitions for each case.
             verbose: 1,
             max_shrink_iters: 8192,
             cases: 1024,
             .. ProptestConfig::default()
         })]

         #[test]
         fn full_tree_test(sequential 1..128 => BTree<i32, TestPageBuffer, u32, u32>);
    }

    #[test]
    fn failing_test_case_1() {
        let filename = "failing_test_case_1.test";
        let input = "
            0: [2] (2)
            0->0: L[0, 1, 2]
            0->1: L[3, 4]
        ";
        let input = trim_lines(input);
        let expected = "0: L[0, 1, 2, 3]";

        let mut t = init_tree_from_description_in_file(filename, &input);
        t.remove(&4).unwrap();

        println!("-------");
        assert_subtree_valid(&t.root, &mut t.pager_info());
        assert_eq!(&t.to_description(), expected);

        drop(t);
        fs::remove_file(filename).unwrap();
    }
}
