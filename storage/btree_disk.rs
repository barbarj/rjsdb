#![allow(dead_code)]

use std::{
    borrow::Cow,
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
#[cfg(test)]
use std::str::FromStr;

use crate::pager::{
    Page, PageBuffer, PageBufferOffset, PageError, PageId, PageKind, PageRef, Pager, PagerError,
    CELL_POINTER_SIZE,
};

#[cfg(test)]
use itertools::Itertools;

use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serialize::{from_bytes, serialized_size, to_bytes, Error as SerdeError};

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

pub struct BTree<Fd, PB, K, V>
where
    Fd: AsRawFd + Copy,
    PB: PageBuffer,
    K: Ord + Serialize + Debug + Clone + DeserializeOwned,
    V: Serialize + DeserializeOwned,
{
    pager_ref: Rc<RefCell<Pager<PB>>>,
    backing_fd: Fd,
    root: Node<PB, K, V>,
    _key: PhantomData<K>,
    _value: PhantomData<V>,
}
impl<Fd, PB, K, V> BTree<Fd, PB, K, V>
where
    Fd: AsRawFd + Copy,
    PB: PageBuffer,
    K: Ord + Serialize + Debug + Clone + DeserializeOwned,
    V: Serialize + DeserializeOwned,
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
        K: Ord + Serialize + Debug + Clone + DeserializeOwned,
        V: Serialize + DeserializeOwned,
    {
        let page = self.get_page(page_id)?;
        Ok(Node::new(page))
    }

    fn buffer_size(&self) -> PageBufferOffset {
        PB::buffer_size()
    }

    fn drop_page(&mut self, page_id: PageId) -> Result<()> {
        let mut pager = self.pager_ref.borrow_mut();
        pager.delete_page(self.backing_fd, page_id)?;
        Ok(())
    }
}

pub enum KeyLimit<K: Ord> {
    None,
    Inclusive(K),
    Exclusive(K),
}

pub struct BTreeIter<PB, Fd, K, V>
where
    PB: PageBuffer,
    Fd: AsRawFd + Copy,
    K: Ord + Serialize + Debug + Clone + DeserializeOwned,
    V: Serialize + DeserializeOwned,
{
    leaf: Node<PB, K, V>,
    logical_pos: u16,
    max_key: KeyLimit<K>,
    pager_info: PagerInfo<PB, Fd>,
}
impl<PB, Fd, K, V> BTreeIter<PB, Fd, K, V>
where
    PB: PageBuffer,
    Fd: AsRawFd + Copy,
    K: Ord + Serialize + Debug + Clone + DeserializeOwned,
    V: Serialize + DeserializeOwned,
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

impl<PB, Fd, K, V> Iterator for BTreeIter<PB, Fd, K, V>
where
    PB: PageBuffer,
    Fd: AsRawFd + Copy,
    K: Ord + Serialize + Debug + Clone + DeserializeOwned,
    V: Serialize + DeserializeOwned,
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
        let leaf_page = self.leaf.page_ref.borrow();
        let (key, val) = match self.leaf.leaf_kv_at_pos(self.logical_pos, &leaf_page) {
            Ok((k, v)) => (k, v),
            Err(err) => return Some(Err(err)),
        };
        match &self.max_key {
            KeyLimit::Exclusive(max) => {
                if key.key.as_ref() >= max {
                    return None;
                }
            }
            KeyLimit::Inclusive(max) => {
                if key.key.as_ref() > max {
                    return None;
                }
            }
            KeyLimit::None => {}
        }
        self.logical_pos += 1;
        Some(Ok((key.key.into_owned(), val)))
    }
}

enum InsertResult<K> {
    Split(K, PageId),
    Done,
}

/// contained value represents the logical position to split at
enum SplitDetermination {
    InsertLeft(u16),
    InsertRight(u16),
    DontInsert(u16),
}

#[derive(Deserialize)]
struct BorrowedKey<'a, K: Clone> {
    #[serde(borrow)]
    key: Cow<'a, K>,
}

struct Node<PB, K, V>
where
    PB: PageBuffer,
    K: Ord + Serialize + Debug + Clone + DeserializeOwned,
    V: Serialize + DeserializeOwned,
{
    page_ref: PageRef<PB>,
    _key: PhantomData<K>,
    _value: PhantomData<V>,
}
impl<PB, K, V> Node<PB, K, V>
where
    PB: PageBuffer,
    K: Ord + Debug + Serialize + DeserializeOwned + Clone,
    V: Serialize + DeserializeOwned,
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

    fn key_from_leaf<'page>(
        &self,
        logical_pos: u16,
        page: &'page Page<PB>,
    ) -> Result<BorrowedKey<'page, K>> {
        assert!(self.is_leaf());
        let pos = Self::logical_leaf_key_pos_to_physical_pos(logical_pos);
        let (key, _): (BorrowedKey<'page, K>, V) = from_bytes(page.cell_bytes(pos))?;
        Ok(key)
    }

    fn value_from_leaf(&self, logical: u16) -> Result<V> {
        assert!(self.is_leaf());
        let page = self.page_ref.borrow();
        let pos = Self::logical_leaf_key_pos_to_physical_pos(logical);
        let (_, val): (K, V) = from_bytes(page.cell_bytes(pos))?;
        Ok(val)
    }

    fn leaf_kv_at_pos<'page>(
        &self,
        logical: u16,
        page: &'page Page<PB>,
    ) -> Result<(BorrowedKey<'page, K>, V)> {
        assert!(self.is_leaf());
        let pos = Self::logical_leaf_key_pos_to_physical_pos(logical);
        let kv = from_bytes(page.cell_bytes(pos))?;
        Ok(kv)
    }

    #[allow(dead_code)]
    fn leaf_left_sibling(&self) -> Result<PageId> {
        assert!(self.is_leaf());
        let page = self.page_ref.borrow();
        assert!(page.cell_count() >= 2);
        Ok(from_bytes(page.cell_bytes(0))?)
    }

    /// Returns the prior sibling
    fn leaf_replace_left_sibling(&mut self, new_left: &PageId) -> Result<PageId> {
        assert!(self.is_leaf());
        let mut page = self.page_ref.borrow_mut();
        assert!(page.cell_count() >= 2);
        let prior_left = from_bytes(page.cell_bytes(0))?;
        page.remove_cell(0);
        page.insert_cell(0, &to_bytes(new_left)?)?;
        Ok(prior_left)
    }

    /// Returns the prior sibling
    fn leaf_replace_right_sibling(&mut self, new_right: &PageId) -> Result<PageId> {
        assert!(self.is_leaf());
        let mut page = self.page_ref.borrow_mut();
        assert!(page.cell_count() >= 2);
        let prior_right = from_bytes(page.cell_bytes(1))?;
        page.remove_cell(1);
        page.insert_cell(1, &to_bytes(new_right)?)?;
        Ok(prior_right)
    }

    fn leaf_right_sibling(&self) -> Result<PageId> {
        assert!(self.is_leaf());
        let page = self.page_ref.borrow();
        assert!(page.cell_count() >= 2);
        Ok(from_bytes(page.cell_bytes(1))?)
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

    fn key_from_inner_node<'page>(
        &self,
        key_pos: u16,
        page: &'page Page<PB>,
    ) -> Result<BorrowedKey<'page, K>> {
        assert!(self.is_node());
        let pos = Self::logical_node_key_pos_to_physical_pos(key_pos);
        let key = from_bytes(page.cell_bytes(pos))?;
        Ok(key)
    }

    fn page_id_from_inner_node(&self, id_pos: u16) -> Result<PageId> {
        assert!(self.is_node());
        let pos = Self::logical_id_pos_to_physical_pos(id_pos);
        let page = self.page_ref.borrow();
        let page_id = from_bytes(page.cell_bytes(pos))?;
        Ok(page_id)
    }

    fn key_at_pos<'page>(
        &self,
        logical_pos: u16,
        page: &'page Page<PB>,
    ) -> Result<BorrowedKey<'page, K>> {
        if self.is_node() {
            self.key_from_inner_node(logical_pos, page)
        } else {
            self.key_from_leaf(logical_pos, page)
        }
    }

    fn binary_search_keys(&self, key: &K) -> std::result::Result<u16, u16> {
        if self.key_count() == 0 {
            return Err(0);
        }
        let mut low = 0;
        let mut high = self.key_count() - 1;
        let page = self.page_ref.borrow();
        while low < high {
            let mid = (low + high) / 2; // TODO: Rework to prevent overflow
            let cell_key = self.key_at_pos(mid, &page).unwrap();
            match cell_key.key.as_ref().cmp(key) {
                Ordering::Less => {
                    low = mid + 1;
                }
                Ordering::Equal => return Ok(mid),
                Ordering::Greater => high = mid,
            }
        }
        let cell_key = self.key_at_pos(low, &page).unwrap();
        match cell_key.key.as_ref().cmp(key) {
            Ordering::Greater => Err(low),
            Ordering::Equal => Ok(low),
            Ordering::Less => Err(low + 1),
        }
    }

    fn move_cells(
        from_node: &mut Self,
        to_node: &mut Self,
        from_logical_range: RangeInclusive<u16>,
        to_logical_start: u16,
    ) -> Result<()> {
        Self::move_cells_with_physical_offset(
            from_node,
            to_node,
            from_logical_range,
            to_logical_start,
            0,
        )
    }

    fn move_cells_with_physical_offset(
        from_node: &mut Self,
        to_node: &mut Self,
        from_logical_range: RangeInclusive<u16>,
        to_logical_start: u16,
        physical_offset_to_from_range_start: u16,
    ) -> Result<()> {
        let physical_range = if from_node.is_leaf() {
            Self::logical_leaf_key_pos_to_physical_pos(*from_logical_range.start())
                ..=Self::logical_leaf_key_pos_to_physical_pos(*from_logical_range.end())
        } else {
            Self::logical_id_pos_to_physical_pos(*from_logical_range.start())
                ..=Self::logical_id_pos_to_physical_pos(*from_logical_range.end())
        };
        let physical_range: RangeInclusive<u16> =
            physical_range.start() + physical_offset_to_from_range_start..=*physical_range.end();

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

    // returns a SplitDetermination containing the logical position of the key to split on, or in
    // the case of a don't insert, the logical position of the first key to be in the right page
    fn determine_node_split_key_logical_pos(
        &self,
        key_to_be_inserted: &K,
        logical_insertion_pos: u16,
    ) -> Result<SplitDetermination> {
        let dummy_id: PageId = 0;
        let id_size = serialized_size(&dummy_id) as u16;
        let id_used_space = id_size + CELL_POINTER_SIZE;
        let key_size = serialized_size(key_to_be_inserted) as u16;
        let insertion_size = key_size + id_size + (CELL_POINTER_SIZE * 2);
        let mut used_space = 0;

        for i in 0..self.key_count() {
            if i == logical_insertion_pos {
                // at i == logical_insertion_pos, we need to try 2 positions.
                // - using the key_to_be_inserted as a split key
                // - using the key at i to be a split key with the key_to_be_inserted to the left of it

                // if this key to be inserted would be used as a split key, we only have a page id
                // to add to the right
                let size_goal = (self.page_used_space() + id_used_space) / 2;
                used_space += id_used_space;

                if used_space >= size_goal {
                    return Ok(SplitDetermination::DontInsert(i));
                }

                used_space -= id_used_space;
                used_space += insertion_size;
                // otherwise the key to be inserted will go left and all other considerations are
                // treated similarly to the rest
            }

            let this_key_used_space =
                serialized_size(self.key_at_pos(i, &self.page_ref.borrow())?.key.as_ref()) as u16
                    + CELL_POINTER_SIZE;
            let space_used_minus_this_key = self.page_used_space() - this_key_used_space;
            let size_goal = (space_used_minus_this_key + insertion_size) / 2;
            // determine if splitting here would put us at or past that page size goal
            used_space += id_used_space; // the id would stay, so consider that space

            if used_space >= size_goal {
                if i >= logical_insertion_pos {
                    return Ok(SplitDetermination::InsertLeft(i));
                } else {
                    return Ok(SplitDetermination::InsertRight(i));
                }
            }

            used_space += this_key_used_space; // this key stays now too
        }
        if logical_insertion_pos < self.key_count() {
            Ok(SplitDetermination::InsertLeft(self.key_count()))
        } else {
            Ok(SplitDetermination::InsertRight(self.key_count()))
        }
    }

    fn split_node_and_insert<Fd: AsRawFd + Copy>(
        &mut self,
        pager_info: &mut PagerInfo<PB, Fd>,
        logical_insertion_pos: u16,
        key_to_be_inserted: &K,
        new_page_id: PageId,
    ) -> Result<(K, Node<PB, K, V>)> {
        assert!(self.page_free_space() < pager_info.buffer_size() / 2);
        let split_determination =
            self.determine_node_split_key_logical_pos(key_to_be_inserted, logical_insertion_pos)?;

        // self.key_from_inner_node uses the logical key position amongst other keys, so convert to
        // that before asking for the key
        let (split_key, move_start_logical_pos, move_offset) = match split_determination {
            SplitDetermination::InsertLeft(pos) | SplitDetermination::InsertRight(pos) => (
                self.key_from_inner_node(pos, &self.page_ref.borrow())?
                    .key
                    .into_owned(),
                pos + 1,
                0,
            ),
            SplitDetermination::DontInsert(pos) => (key_to_be_inserted.clone(), pos, 1),
        };

        // get new page
        let mut new_node = Self::init_node(pager_info)?;

        let key_count = self.key_count();
        Self::move_cells_with_physical_offset(
            self,
            &mut new_node,
            move_start_logical_pos..=key_count,
            0,
            move_offset,
        )?;

        match split_determination {
            SplitDetermination::InsertLeft(split_logical_pos) => {
                self.remove_trailing_key(split_logical_pos);
                self.insert_split_key_and_page_id_into_node(
                    logical_insertion_pos,
                    key_to_be_inserted,
                    new_page_id,
                )?;
            }
            SplitDetermination::InsertRight(split_logical_pos) => {
                self.remove_trailing_key(split_logical_pos);
                let insert_pos = logical_insertion_pos - self.key_count() - 1;
                new_node.insert_split_key_and_page_id_into_node(
                    insert_pos,
                    key_to_be_inserted,
                    new_page_id,
                )?;
            }
            SplitDetermination::DontInsert(_) => {
                new_node.insert_split_page_id_into_node(0, new_page_id)?
            }
        }

        Ok((split_key, new_node))
    }

    fn leaf_space_used_ignoring_siblings(&self) -> u16 {
        self.page_used_space() - Self::leaf_siblings_space_used()
    }

    fn leaf_siblings_space_used() -> u16 {
        let dummy_id: PageId = 0;
        (serialized_size(&dummy_id) as u16 + CELL_POINTER_SIZE) * 2
    }

    fn split_leaf_and_insert<Fd: AsRawFd + Copy>(
        &mut self,
        key: K,
        value: V,
        pager_info: &mut PagerInfo<PB, Fd>,
    ) -> Result<(K, Node<PB, K, V>)> {
        println!("splitting leaf");
        let insertion_size = serialized_size(&(&key, &value)) as u16 + CELL_POINTER_SIZE;
        let size_goal_fn = |this_key: &K, _: &V| match key.cmp(this_key) {
            Ordering::Less => (self.leaf_space_used_ignoring_siblings() - insertion_size) / 2,
            Ordering::Equal => unreachable!("Existing keys shouldn't be inserted here"),
            Ordering::Greater => self.leaf_space_used_ignoring_siblings() / 2,
        };

        let split_key_pos = self
            .leaf_find_logical_position_meeting_size_goal(0, size_goal_fn)?
            .unwrap();

        let split_key = self
            .key_from_leaf(split_key_pos, &self.page_ref.borrow())?
            .key
            .into_owned();

        // get new page
        let mut new_node = Self::init_leaf(pager_info)?;

        // update sibling pointers
        let old_right = self.leaf_replace_right_sibling(&new_node.page_id())?;
        new_node.leaf_replace_left_sibling(&self.page_id())?;
        new_node.leaf_replace_right_sibling(&old_right)?;

        // copy cells to new page and remove cells from old page
        let key_count = self.key_count();
        println!("move range: {:?}", split_key_pos + 1..=key_count - 1);
        Self::move_cells(self, &mut new_node, split_key_pos + 1..=key_count - 1, 0)?;

        if key > split_key {
            new_node.insert_as_leaf(key, value, pager_info)?;
        } else {
            self.insert_as_leaf(key, value, pager_info)?;
        }

        Ok((split_key, new_node))
    }

    fn insert_as_leaf<Fd: AsRawFd + Copy>(
        &mut self,
        key: K,
        value: V,
        pager_info: &mut PagerInfo<PB, Fd>,
    ) -> Result<InsertResult<K>> {
        assert!(self.is_leaf());
        // if the key already exists, remove that entry before doing anything
        let existing_key_pos = self.binary_search_keys(&key);
        if let Ok(pos) = existing_key_pos {
            let physical_pos = Self::logical_leaf_key_pos_to_physical_pos(pos);
            let mut page = self.page_ref.borrow_mut();
            page.remove_cell(physical_pos);
        }

        if !self.can_fit_leaf(&key, &value) {
            let (split_key, new_node) = self.split_leaf_and_insert(key, value, pager_info)?;
            assert!(new_node.is_leaf());
            Ok(InsertResult::Split(split_key, new_node.page_id()))
        } else {
            let logical_pos = match existing_key_pos {
                Ok(logical_pos) => logical_pos,
                Err(logical_pos) => logical_pos,
            };
            let physical_pos = Self::logical_leaf_key_pos_to_physical_pos(logical_pos);
            let mut page = self.page_ref.borrow_mut();
            page.insert_cell(physical_pos, &to_bytes(&(key, value))?)?;
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

    fn insert_split_page_id_into_node(
        &mut self,
        logical_pos: u16,
        new_page_id: PageId,
    ) -> Result<()> {
        let physical_pos = Self::logical_id_pos_to_physical_pos(logical_pos);
        let mut page = self.page_ref.borrow_mut();
        page.insert_cell(physical_pos, &to_bytes(&new_page_id)?)?;
        Ok(())
    }

    fn insert_split_key_and_page_id_into_node(
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
            Some(
                self.key_from_inner_node(logical_key_pos, &self.page_ref.borrow())?
                    .key
                    .into_owned(),
            )
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
                let (parent_split_key, parent_new_node) =
                    self.split_node_and_insert(pager_info, logical_pos, &split_key, new_page_id)?;
                assert!(parent_new_node.is_node());
                Ok(InsertResult::Split(
                    parent_split_key,
                    parent_new_node.page_id(),
                ))
            } else {
                self.insert_split_key_and_page_id_into_node(logical_pos, &split_key, new_page_id)?;
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
        println!("inserting {key:?}");
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
            let page = self.page_ref.borrow();
            let merge_key = self.key_at_pos(left_child_pos, &page)?;
            let key_size = serialized_size(&merge_key.key) as u16 + CELL_POINTER_SIZE;
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

    #[allow(clippy::reversed_empty_ranges)]
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
            let page = self.page_ref.borrow();
            let key = self.key_from_inner_node(left_child_pos, &page)?;
            left_child.insert_trailing_key(&key.key)?;
            initial_left_key_count + 1
        } else {
            left_child.key_count()
        };

        let from_range = if right_child.is_node() {
            0..=right_child.key_count()
        } else if right_child.key_count() > 0 {
            0..=right_child.key_count() - 1
        } else {
            1..=0 // intentionally creating an empty range here, since the right child is empty in
                  // this case
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

    fn insert_interior_split_key(&mut self, logical_key_pos: u16, key: &K) -> Result<()> {
        assert!(self.is_node());
        let mut page = self.page_ref.borrow_mut();
        let insert_pos = Self::logical_node_key_pos_to_physical_pos(logical_key_pos);
        page.insert_cell(insert_pos, &to_bytes(key)?)?;
        Ok(())
    }

    fn leaf_find_logical_position_meeting_size_goal(
        &self,
        starting_size: u16,
        size_goal_fn: impl Fn(&K, &V) -> u16,
    ) -> Result<Option<u16>> {
        assert!(self.is_leaf());
        let mut used_space = starting_size;
        let page = self.page_ref.borrow();
        for i in 0..self.key_count() {
            let (k, v) = self.leaf_kv_at_pos(i, &page)?;
            let increment = serialized_size(&(&k.key, &v)) as u16 + CELL_POINTER_SIZE;
            used_space += increment;
            if used_space >= size_goal_fn(&k.key, &v) {
                return Ok(Some(i));
            }
        }
        Ok(None)
    }

    fn node_find_logical_position_meeting_size_goal(
        &self,
        starting_size: u16,
        size_goal_fn: impl Fn(&K, u16) -> u16, // takes key and index
    ) -> Result<Option<u16>> {
        let dummy_id: PageId = 0;
        let dummy_space_used = serialized_size(&dummy_id) as u16 + CELL_POINTER_SIZE;

        assert!(self.is_node());
        let mut used_space = starting_size;
        let page = self.page_ref.borrow();
        for i in 0..self.key_count() {
            used_space += dummy_space_used;
            let key = self.key_at_pos(i, &page)?;
            if used_space >= size_goal_fn(&key.key, i) {
                return Ok(Some(i));
            }
            used_space += serialized_size(&key.key) as u16 + CELL_POINTER_SIZE;
        }
        Ok(None)
    }

    fn child_leaf_steal_from_left_sibling<Fd: AsRawFd + Copy>(
        &mut self,
        right_child_logical_pos: u16,
        pager_info: &mut PagerInfo<PB, Fd>,
    ) -> Result<()> {
        assert!(right_child_logical_pos > 0);

        let mut left_child =
            self.descendent_node_at_logical_pos(right_child_logical_pos - 1, pager_info)?;
        let mut right_child =
            self.descendent_node_at_logical_pos(right_child_logical_pos, pager_info)?;
        assert!(left_child.is_leaf());

        let size_goal = (left_child.leaf_space_used_ignoring_siblings()
            + right_child.leaf_space_used_ignoring_siblings())
            / 2;

        let new_split_pos = left_child
            .leaf_find_logical_position_meeting_size_goal(0, |_, _| size_goal)?
            .expect("Should always have a value");

        let left_page = left_child.page_ref.borrow();
        let new_split_key = left_child.key_at_pos(new_split_pos, &left_page)?;
        self.replace_inner_node_key(right_child_logical_pos - 1, &new_split_key.key)?;
        drop(left_page);

        let from_range = new_split_pos + 1..=left_child.key_count() - 1;
        Self::move_cells(&mut left_child, &mut right_child, from_range, 0)?;

        Ok(())
    }

    fn child_leaf_steal_from_right_sibling<Fd: AsRawFd + Copy>(
        &mut self,
        left_child_logical_pos: u16,
        pager_info: &mut PagerInfo<PB, Fd>,
    ) -> Result<()> {
        assert!(left_child_logical_pos < self.descendent_count() - 1);

        let mut left_child =
            self.descendent_node_at_logical_pos(left_child_logical_pos, pager_info)?;
        let mut right_child =
            self.descendent_node_at_logical_pos(left_child_logical_pos + 1, pager_info)?;
        assert!(left_child.is_leaf());

        let size_goal = (left_child.leaf_space_used_ignoring_siblings()
            + right_child.leaf_space_used_ignoring_siblings())
            / 2;

        let new_split_pos = right_child
            .leaf_find_logical_position_meeting_size_goal(
                left_child.leaf_space_used_ignoring_siblings(),
                |_, _| size_goal,
            )?
            .expect("Should always have a value");

        let right_page = right_child.page_ref.borrow();
        let new_split_key = right_child.key_at_pos(new_split_pos, &right_page)?;
        self.replace_inner_node_key(left_child_logical_pos, &new_split_key.key)?;
        drop(right_page);

        let from_range = 0..=new_split_pos;
        let left_child_key_count = left_child.key_count();
        Self::move_cells(
            &mut right_child,
            &mut left_child,
            from_range,
            left_child_key_count,
        )?;

        Ok(())
    }

    fn child_node_steal_from_left_sibling<Fd: AsRawFd + Copy>(
        &mut self,
        right_child_logical_pos: u16,
        pager_info: &mut PagerInfo<PB, Fd>,
    ) -> Result<()> {
        assert!(right_child_logical_pos > 0);

        let page = self.page_ref.borrow();
        let old_split_key = self.key_at_pos(right_child_logical_pos - 1, &page)?;

        let mut left_child =
            self.descendent_node_at_logical_pos(right_child_logical_pos - 1, pager_info)?;
        let mut right_child =
            self.descendent_node_at_logical_pos(right_child_logical_pos, pager_info)?;
        assert!(left_child.is_node());

        let combined_size = left_child.page_used_space()
            + right_child.page_used_space()
            + serialized_size(&old_split_key.key) as u16
            + CELL_POINTER_SIZE;

        let new_split_pos = left_child
            .node_find_logical_position_meeting_size_goal(0, |key: &K, _: u16| {
                let key_space_used = serialized_size(&key) as u16 + CELL_POINTER_SIZE;
                (combined_size - key_space_used) / 2
            })?
            .expect("Should always have a value");

        let from_range = new_split_pos + 1..=left_child.key_count();
        Self::move_cells(&mut left_child, &mut right_child, from_range.clone(), 0)?;

        let key_insert_pos = from_range.len() - 1;
        right_child.insert_interior_split_key(key_insert_pos as u16, &old_split_key.key)?;
        drop(page);

        let left_page = left_child.page_ref.borrow();
        let new_split_key = left_child.key_at_pos(new_split_pos, &left_page)?;
        self.replace_inner_node_key(right_child_logical_pos - 1, &new_split_key.key)?;
        drop(left_page);

        left_child.remove_trailing_key(new_split_pos);
        Ok(())
    }

    fn child_node_steal_from_right_sibling<Fd: AsRawFd + Copy>(
        &mut self,
        left_child_logical_pos: u16,
        pager_info: &mut PagerInfo<PB, Fd>,
    ) -> Result<()> {
        assert!(left_child_logical_pos < self.descendent_count() - 1);

        let page = self.page_ref.borrow();
        let old_split_key = self.key_at_pos(left_child_logical_pos, &page)?;

        let mut left_child =
            self.descendent_node_at_logical_pos(left_child_logical_pos, pager_info)?;
        let mut right_child =
            self.descendent_node_at_logical_pos(left_child_logical_pos + 1, pager_info)?;
        assert!(left_child.is_node());

        let combined_size = left_child.page_used_space()
            + right_child.page_used_space()
            + serialized_size(&old_split_key.key) as u16
            + CELL_POINTER_SIZE;

        let starting_size = left_child.page_used_space()
            + serialized_size(&old_split_key.key) as u16
            + CELL_POINTER_SIZE;
        let new_split_pos = right_child
            .node_find_logical_position_meeting_size_goal(starting_size, |key: &K, _: u16| {
                let key_space_used = serialized_size(key) as u16 + CELL_POINTER_SIZE;
                (combined_size - key_space_used) / 2
            })?
            .expect("Should always have a value");

        let right_page = right_child.page_ref.borrow();
        let new_split_key = right_child.key_at_pos(new_split_pos, &right_page)?;

        let from_range = 0..=new_split_pos;
        let left_child_key_count = left_child.key_count();
        left_child.insert_interior_split_key(left_child_key_count, &old_split_key.key)?;
        drop(page);

        self.replace_inner_node_key(left_child_logical_pos, &new_split_key.key)?;
        drop(right_page);

        Self::move_cells(
            &mut right_child,
            &mut left_child,
            from_range.clone(),
            left_child_key_count + 1,
        )?;
        right_child.remove_leading_key();

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
        let right_child =
            self.descendent_node_at_logical_pos(right_child_logical_pos, pager_info)?;
        if right_child.is_node() {
            self.child_node_steal_from_left_sibling(right_child_logical_pos, pager_info)
        } else {
            self.child_leaf_steal_from_left_sibling(right_child_logical_pos, pager_info)
        }
    }

    fn child_steal_from_right_sibling<Fd: AsRawFd + Copy>(
        &mut self,
        left_child_logical_pos: u16,
        pager_info: &mut PagerInfo<PB, Fd>,
    ) -> Result<()> {
        assert!(left_child_logical_pos < self.descendent_count() - 1);
        let left_child = self.descendent_node_at_logical_pos(left_child_logical_pos, pager_info)?;
        if left_child.is_node() {
            self.child_node_steal_from_right_sibling(left_child_logical_pos, pager_info)
        } else {
            self.child_leaf_steal_from_right_sibling(left_child_logical_pos, pager_info)
        }
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
/// - The min size for leaves is 2, and nodes is 2
/// - This is the smallest buffer size we can write straitforward manual tests for
const TEST_BUFFER_SIZE: u16 = 112;
#[cfg(test)]
pub struct SmallBuffer {
    data: [u8; TEST_BUFFER_SIZE as usize],
}
#[cfg(test)]
impl PageBuffer for SmallBuffer {
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
// This is the smallest size you can construct a btree with when using u32 keys and u32 values
const SMALLEST_BUFFER_SIZE: u16 = 36;
#[cfg(test)]
pub struct SmallestBuffer {
    data: [u8; SMALLEST_BUFFER_SIZE as usize],
}
#[cfg(test)]
impl PageBuffer for SmallestBuffer {
    fn new() -> Self
    where
        Self: Sized,
    {
        Self {
            data: [0; SMALLEST_BUFFER_SIZE as usize],
        }
    }

    fn buffer_size() -> u16 {
        SMALLEST_BUFFER_SIZE
    }

    fn data(&self) -> &[u8] {
        &self.data
    }

    fn data_mut(&mut self) -> &mut [u8] {
        &mut self.data
    }
}

#[cfg(test)]
impl<PB: PageBuffer, T> BTree<i32, PB, T, T>
where
    T: Ord + Serialize + Debug + FromStr + Clone + DeserializeOwned,
{
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
        pager_ref: Rc<RefCell<Pager<SmallBuffer>>>,
        backing_fd: i32,
    ) -> BTree<i32, SmallBuffer, T, T> {
        let mut lines = description
            .trim()
            .split('\n')
            .map(|x| x.trim())
            .map(|s| DescriptionLine::<T>::from_str(s).unwrap())
            .peekable();

        assert!(lines.peek().is_some());

        // initalize pages
        let mut pager_info = PagerInfo::new(pager_ref.clone(), backing_fd);

        // init root page
        let first_line = lines.next().unwrap();
        let root: Node<SmallBuffer, T, T> = match first_line.is_leaf {
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
}

#[cfg(test)]
impl<PB: PageBuffer, T> BTree<i32, PB, T, T>
where
    T: Ord + Serialize + DeserializeOwned + Debug + Clone,
{
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
            let node: Node<PB, T, T> = pager_info.page_node(page_id).unwrap();
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

    #[allow(dead_code)]
    fn display_subtree(pager_info: &mut PagerInfo<PB, i32>, root_page_id: PageId) {
        let description = Self::node_to_description(pager_info, root_page_id);
        print!("{description}");
    }
}

impl<PB, K, V> Node<PB, K, V>
where
    PB: PageBuffer,
    K: Ord + Debug + Serialize + Clone + DeserializeOwned,
    V: Serialize + DeserializeOwned,
{
    #[allow(dead_code)]
    fn keys(&self) -> Vec<K> {
        if self.is_leaf() {
            let page = self.page_ref.borrow();
            (0..self.key_count())
                .map(|i| self.key_from_leaf(i, &page).unwrap().key.into_owned())
                .collect()
        } else {
            let page = self.page_ref.borrow();
            (0..self.key_count())
                .map(|i| self.key_from_inner_node(i, &page).unwrap().key.into_owned())
                .collect()
        }
    }

    #[allow(clippy::reversed_empty_ranges)]
    #[allow(dead_code)]
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

    #[allow(dead_code)]
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
impl<T> Node<SmallBuffer, T, T>
where
    T: Ord + Serialize + DeserializeOwned + Debug + FromStr + Clone,
{
    fn from_description_lines<Fd: AsRawFd + Copy, I: Iterator<Item = DescriptionLine<T>>>(
        pager_info: &mut PagerInfo<SmallBuffer, Fd>,
        this_node_line: DescriptionLine<T>,
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

#[cfg(test)]
#[derive(Debug)]
enum DescriptionLineError {
    InvalidChildCount(String),
}
#[cfg(test)]
impl Display for DescriptionLineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidChildCount(s) => f.write_fmt(format_args!("InvalidChildCount - '{s}'")),
        }
    }
}
#[cfg(test)]
impl std::error::Error for DescriptionLineError {}

#[cfg(test)]
#[derive(Debug, Clone)]
struct DescriptionLine<T> {
    traversal_path: Vec<usize>,
    is_leaf: bool,
    keys: Vec<T>,
    child_count: usize,
}
#[cfg(test)]
impl<T> DescriptionLine<T> {
    fn new(traversal_path: Vec<usize>, is_leaf: bool, keys: Vec<T>, child_count: usize) -> Self {
        DescriptionLine {
            traversal_path,
            is_leaf,
            keys,
            child_count,
        }
    }
}
#[cfg(test)]
impl<T: FromStr> DescriptionLine<T> {
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
        let keys: Vec<T> = num_strs
            .map(|x| match x.parse() {
                Ok(k) => k,
                Err(_) => panic!("failed to parse: '{}'", x),
            })
            .collect();

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

    fn is_child_line(&self, candidate: &DescriptionLine<T>) -> bool {
        let tvlen = self.traversal_path.len();
        candidate.traversal_path.len() == tvlen + 1
            && candidate.traversal_path[0..tvlen] == self.traversal_path
    }
}
#[cfg(test)]
impl<T: Debug> Display for DescriptionLine<T> {
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
fn assert_tree_keys_fully_ordered<PB, T>(root: &Node<PB, T, T>)
where
    PB: PageBuffer,
    T: Ord + Serialize + DeserializeOwned + Debug + Clone,
{
    let keys = root.keys();
    let mut sorted_keys = keys.clone();
    sorted_keys.sort();
    assert_eq!(keys, sorted_keys);
}

#[cfg(test)]
fn assert_all_node_keys_ordered_and_deduped<PB, T>(
    node: &Node<PB, T, T>,
    pager_info: &mut PagerInfo<PB, i32>,
) where
    PB: PageBuffer,
    T: Ord + Serialize + DeserializeOwned + Debug + Clone,
{
    let mut sorted_keys = node.keys();
    sorted_keys.sort();
    sorted_keys.dedup();
    let nodes: Vec<_> = node.descendent_iter(pager_info).collect();
    assert_eq!(sorted_keys, node.keys());
    nodes
        .into_iter()
        .for_each(|node| assert_all_node_keys_ordered_and_deduped(&node, pager_info));
}

#[cfg(test)]
fn assert_all_keys_in_range<PB, T>(
    node: &Node<PB, T, T>,
    min_exclusive: Option<&T>,
    max_inclusive: Option<&T>,
) where
    PB: PageBuffer,
    T: Ord + Serialize + DeserializeOwned + Debug + Clone,
{
    let res = match (min_exclusive, max_inclusive) {
        (Some(min), Some(max)) => node.keys().iter().all(|k| k > min && k <= max),
        (None, Some(max)) => node.keys().iter().all(|k| k <= max),
        (Some(min), None) => node.keys().iter().all(|k| k > min),
        (None, None) => unimplemented!("Should not ever happen"),
    };
    assert!(res);
}

#[cfg(test)]
fn assert_all_subnode_keys_ordered_relative_to_node_keys<PB, T>(
    node: &Node<PB, T, T>,
    pager_info: &mut PagerInfo<PB, i32>,
) where
    PB: PageBuffer,
    T: Ord + Serialize + DeserializeOwned + Debug + Clone,
{
    if node.is_leaf() {
        return;
    }
    let mut min_key_exclusive = None;
    for (idx, k) in node.keys().into_iter().enumerate() {
        let max_key = k.clone();
        assert_all_keys_in_range(
            &node
                .descendent_node_at_logical_pos(idx as u16, pager_info)
                .unwrap(),
            min_key_exclusive.as_ref(),
            Some(&max_key),
        );
        min_key_exclusive = Some(k);
    }
    assert_all_keys_in_range(
        &node
            .descendent_node_at_logical_pos(node.key_count(), pager_info)
            .unwrap(),
        min_key_exclusive.as_ref(),
        None,
    )
}

#[cfg(test)]
fn assert_all_nodes_sized_correctly<PB, T>(
    root: &Node<PB, T, T>,
    pager_info: &mut PagerInfo<PB, i32>,
) where
    PB: PageBuffer,
    T: Ord + Serialize + DeserializeOwned + Debug + Clone,
{
    fn correct_cell_count<PB, T>(node: &Node<PB, T, T>) -> bool
    where
        PB: PageBuffer,
        T: Ord + Serialize + DeserializeOwned + Debug + Clone,
    {
        if node.is_leaf() {
            true
        } else {
            let page = node.page_ref.borrow();
            page.cell_count() % 2 == 1
        }
    }

    fn assert_all_nodes_sized_correctly_not_root<PB, T>(
        node: &Node<PB, T, T>,
        pager_info: &mut PagerInfo<PB, i32>,
    ) where
        PB: PageBuffer,
        T: Ord + Serialize + DeserializeOwned + Debug + Clone,
    {
        let third_size = PB::buffer_size() / 3;
        let meets_minimum_size = node.page_used_space() >= third_size;
        //        println!("minimum_size: {third_size}");
        //       println!("actual size: {}", node.page_used_space());
        assert!(meets_minimum_size);
        let correct_cell_count = correct_cell_count(node);
        assert!(correct_cell_count);

        if node.is_node() {
            let children: Vec<_> = node.descendent_iter(pager_info).collect();
            children
                .iter()
                .for_each(|node| assert_all_nodes_sized_correctly_not_root(node, pager_info));
        }
    }

    let children: Vec<_> = root.descendent_iter(pager_info).collect();
    assert!(correct_cell_count(root));
    children
        .iter()
        .for_each(|node| assert_all_nodes_sized_correctly_not_root(node, pager_info));
}

#[cfg(test)]
fn assert_all_leaves_same_level<PB, T>(root: &Node<PB, T, T>, pager_info: &mut PagerInfo<PB, i32>)
where
    PB: PageBuffer,
    T: Ord + Serialize + DeserializeOwned + Debug + Clone,
{
    fn leaf_levels<PB, T>(
        node: &Node<PB, T, T>,
        level: usize,
        pager_info: &mut PagerInfo<PB, i32>,
    ) -> Vec<usize>
    where
        PB: PageBuffer,
        T: Ord + Serialize + DeserializeOwned + Debug + Clone,
    {
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
    assert!(levels.all(|x| x == first));
}

#[cfg(test)]
fn assert_subtree_valid<PB, T>(node: &Node<PB, T, T>, pager_info: &mut PagerInfo<PB, i32>)
where
    PB: PageBuffer,
    T: Ord + Serialize + DeserializeOwned + Debug + Clone,
{
    assert_all_nodes_sized_correctly(node, pager_info);
    assert_tree_keys_fully_ordered(node);
    assert_all_node_keys_ordered_and_deduped(node, pager_info);
    assert_all_subnode_keys_ordered_relative_to_node_keys(node, pager_info);
    assert_all_leaves_same_level(node, pager_info);
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
        assert_all_leaves_same_level, assert_all_node_keys_ordered_and_deduped,
        assert_all_nodes_sized_correctly, assert_all_subnode_keys_ordered_relative_to_node_keys,
        assert_subtree_valid, assert_tree_keys_fully_ordered, TEST_BUFFER_SIZE,
    };

    use crate::pager::{PageBuffer, PageId, Pager, CELL_POINTER_SIZE};

    use super::{BTree, KeyLimit, SmallBuffer, SmallestBuffer};

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
    ) -> BTree<i32, SmallBuffer, u32, u32> {
        let file = open_file(filename);
        let backing_fd = file.as_raw_fd();
        let pager_ref = Rc::new(RefCell::new(Pager::new(vec![file])));

        BTree::<i32, SmallBuffer, u32, u32>::from_description(description, pager_ref, backing_fd)
    }

    fn init_tree_in_file<PB: PageBuffer, T>(filename: &str) -> BTree<i32, PB, T, T>
    where
        T: Ord + Serialize + DeserializeOwned + Debug + Clone,
    {
        let file = open_file(filename);
        let backing_fd = file.as_raw_fd();
        let pager_ref = Rc::new(RefCell::new(Pager::new(vec![file])));

        BTree::init(pager_ref, backing_fd).unwrap()
    }

    fn init_tree_in_file_with_pb<PB, T>(filename: &str) -> BTree<i32, PB, T, T>
    where
        PB: PageBuffer,
        T: Ord + Serialize + DeserializeOwned + Debug + Clone,
    {
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
            0->2: [28, 31] (3)
            0->0->0: L[1, 2, 3] 
            0->0->1: L[4, 5, 6]
            0->0->2: L[7, 8, 9] 
            0->0->3: L[10, 11, 12] 
            0->1->0: L[13, 14, 15]
            0->1->1: L[16, 17] 
            0->1->2: L[18, 19, 20] 
            0->1->3: L[21, 22, 23]
            0->2->0: L[24, 25, 26, 27] 
            0->2->1: L[29, 30, 31]
            0->2->2: L[32, 33, 34]
        ";
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

        let tree: BTree<i32, SmallBuffer, _, _> = init_tree_in_file(filename);

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

        let mut tree: BTree<i32, SmallBuffer, u32, u32> = init_tree_in_file(filename);
        tree.insert(1, 1).unwrap();

        assert_eq!(&tree.to_description(), &expected_tree);
        assert_subtree_valid(&tree.root, &mut tree.pager_info());

        drop(tree);
        fs::remove_file(filename).unwrap();
    }

    #[test]
    fn leaf_root_insertion_u32() {
        let filename = "leaf_root_insertion_u32.test";
        let expected_tree = "
            0: L[1, 2, 3, 4, 5]
        ";
        let expected_tree = trim_lines(expected_tree);

        let mut tree: BTree<i32, SmallBuffer, u32, u32> = init_tree_in_file(filename);

        for i in 1..=5 {
            tree.insert(i, i).unwrap();
        }

        assert_eq!(&tree.to_description(), &expected_tree);
        assert_subtree_valid(&tree.root, &mut tree.pager_info());

        drop(tree);
        fs::remove_file(filename).unwrap();
    }

    #[test]
    fn leaf_root_insertion_i64() {
        let filename = "leaf_root_insertion_i64.test";
        let expected_tree = "
            0: L[1]
        ";
        let expected_tree = trim_lines(expected_tree);

        let mut tree: BTree<i32, SmallBuffer, i64, i64> = init_tree_in_file(filename);

        tree.insert(1i64, 1i64).unwrap();
        //for i in 1i64..=4 {
        //    tree.insert(i, i).unwrap();
        //}

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
            0->0: L[1, 2, 3, 4]
            0->1: L[5, 6, 7, 8] 
        ";
        let expected_tree = trim_lines(expected_tree);

        let mut tree: BTree<i32, SmallBuffer, u32, u32> = init_tree_in_file(filename);

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
            0: [12] (2)
            0->0: [3, 6, 9] (4)
            0->1: [15, 19] (3)
            0->0->0: L[1, 2, 3] 
            0->0->1: L[4, 5, 6] 
            0->0->2: L[7, 8, 9]
            0->0->3: L[10, 11, 12] 
            0->1->0: L[13, 14, 15] 
            0->1->1: L[16, 17, 18, 19] 
            0->1->2: L[20, 21, 22, 23]
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
            0: [3, 7] (3)
            0->0: L[1, 2, 3] 
            0->1: L[4, 5, 6, 7] 
            0->2: L[8, 9, 10, 11] 
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
            0->1: [15, 20, 23] (4)
            0->2: [29, 32] (3)
            0->0->0: L[1, 2, 3] 
            0->0->1: L[4, 5, 6] 
            0->0->2: L[7, 8, 9] 
            0->0->3: L[10, 11, 12] 
            0->1->0: L[13, 14, 15] 
            0->1->1: L[16, 17, 18, 19, 20] 
            0->1->2: L[21, 22, 23] 
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
            0: [12, 24] (3)
            0->0: [3, 6, 9] (4)
            0->1: [15, 18, 21] (4)
            0->2: [28, 32] (3)
            0->0->0: L[1, 2, 3] 
            0->0->1: L[4, 5, 6] 
            0->0->2: L[7, 8, 9] 
            0->0->3: L[10, 11, 12] 
            0->1->0: L[13, 14, 15] 
            0->1->1: L[16, 17, 18] 
            0->1->2: L[19, 20, 21] 
            0->1->3: L[22, 23, 24] 
            0->2->0: L[25, 26, 27, 28] 
            0->2->1: L[29, 30, 31, 32] 
            0->2->2: L[33, 34, 35] 
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
            0: [7, 12] (3)
            0->0: L[1, 2, 3, 4, 5, 6, 7] 
            0->1: L[8, 10, 11, 12] 
            0->2: L[13, 14, 15] 
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
            0: [7, 15] (3)
            0->0: [1, 3, 5] (4)
            0->1: [9, 13] (3)
            0->2: [17, 19, 21, 23, 25] (6)
            0->0->0: L[0, 1] 
            0->0->1: L[2, 3] 
            0->0->2: L[4, 5] 
            0->0->3: L[6, 7] 
            0->1->0: L[8, 9] 
            0->1->1: L[10, 11, 12] 
            0->1->2: L[14, 15] 
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
            0: [11, 21] (3)
            0->0: [1, 3, 5, 7, 9] (6)
            0->1: [15, 17, 19] (4)
            0->2: [23, 25] (3)
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
            0->2->0: L[22, 23] 
            0->2->1: L[24, 25] 
            0->2->2: L[26, 27] 
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
        let mut t: BTree<i32, SmallBuffer, u32, u32> = init_tree_in_file(filename);

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
        let mut t: BTree<i32, SmallBuffer, u32, u32> = init_tree_in_file(filename);

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
        let mut t: BTree<i32, SmallBuffer, u32, u32> = init_tree_in_file(filename);

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

    fn first_nonretrievable_inserted_value<PB, T>(
        tree: &BTree<i32, PB, T, T>,
        ref_tree: &BTreeMap<T, T>,
    ) -> Option<T>
    where
        PB: PageBuffer,
        T: Ord + Serialize + DeserializeOwned + Debug + PartialEq + Clone,
    {
        ref_tree
            .iter()
            .find(|(k, v)| tree.get(k).unwrap().as_ref() != Some(*v))
            .map(|(k, v)| {
                println!("didn't find: ({k:?}, {v:?})");
                println!("actual value: {:?}", tree.get(k));
                (*k).clone()
            })
    }

    #[derive(Debug, Clone)]
    pub enum TreeOperation<T: Ord + Serialize + DeserializeOwned + Debug> {
        Insert(T, T),
        Remove(T),
    }

    #[derive(Debug, Clone)]
    pub struct ReferenceBTree<T> {
        ref_tree: BTreeMap<T, T>,
        type_str: String,
    }
    impl<
            T: Ord + Serialize + DeserializeOwned + Debug + Clone + Arbitrary + 'static + ToString,
        > ReferenceStateMachine for ReferenceBTree<T>
    {
        type State = Self;
        type Transition = TreeOperation<T>;

        fn init_state() -> BoxedStrategy<Self::State> {
            T::arbitrary()
                .prop_map(|t| ReferenceBTree {
                    ref_tree: BTreeMap::new(),
                    type_str: t.to_string(),
                })
                .boxed()
        }

        fn transitions(state: &Self::State) -> BoxedStrategy<Self::Transition> {
            if !state.ref_tree.is_empty() {
                let keys: Vec<_> = state.ref_tree.keys().cloned().collect();
                let removal_key = proptest::sample::select(keys);
                prop_oneof![
                    (any::<T>(), any::<T>()).prop_map(|(k, v)| TreeOperation::Insert(k, v)),
                    removal_key.prop_map(TreeOperation::Remove)
                ]
                .boxed()
            } else {
                (any::<T>(), any::<T>())
                    .prop_map(|(k, v)| TreeOperation::Insert(k, v))
                    .boxed()
            }
        }

        fn apply(mut state: Self::State, transition: &Self::Transition) -> Self::State {
            match transition {
                TreeOperation::Insert(k, v) => state.ref_tree.insert(k.clone(), v.clone()),
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
        T: Ord + Serialize + DeserializeOwned + Debug + Clone,
    > {
        tree: BTree<i32, PB, T, T>,
        filename: String,
    }
    impl<PB: PageBuffer, T: Ord + Serialize + DeserializeOwned + Debug + Clone>
        BTreeTestWrapper<PB, T>
    {
        fn new(tree: BTree<i32, PB, T, T>, filename: String) -> Self {
            BTreeTestWrapper { tree, filename }
        }
    }

    impl<
            PB: PageBuffer,
            T: Ord + Serialize + DeserializeOwned + Debug + Clone + Arbitrary + 'static + ToString,
        > StateMachineTest for BTree<i32, PB, T, T>
    {
        type SystemUnderTest = BTreeTestWrapper<PB, T>;
        type Reference = ReferenceBTree<T>;

        fn init_test(
            ref_state: &<Self::Reference as ReferenceStateMachine>::State,
        ) -> Self::SystemUnderTest {
            let id = &ref_state.type_str;
            let filename = format!("btree_state_machine_{id}_{id}.test");
            let t = init_tree_in_file_with_pb(&filename);
            BTreeTestWrapper::new(t, filename)
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
                    state.tree.insert(k.clone(), v.clone()).unwrap();
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
            assert_tree_keys_fully_ordered(&state.tree.root);
            assert_eq!(
                first_nonretrievable_inserted_value(&state.tree, &ref_state.ref_tree),
                None
            );
            assert_all_node_keys_ordered_and_deduped(
                &state.tree.root,
                &mut state.tree.pager_info(),
            );
            assert_all_subnode_keys_ordered_relative_to_node_keys(
                &state.tree.root,
                &mut state.tree.pager_info(),
            );
            assert_all_nodes_sized_correctly(&state.tree.root, &mut state.tree.pager_info());
            assert_all_leaves_same_level(&state.tree.root, &mut state.tree.pager_info());
        }

        fn teardown(state: Self::SystemUnderTest) {
            drop(state.tree);
            fs::remove_file(state.filename).unwrap();
        }
    }

    prop_state_machine! {
        #![proptest_config(ProptestConfig {
            // When debugging, enable verbose mode to make the state machine test print the
            // transitions for each case.
            verbose: 1,
            max_shrink_iters: 8192,
            cases: 1024,
            .. ProptestConfig::default()
        })]

        #[test] #[ignore] // expensive test
        fn full_tree_test_u32(sequential 1..1024 => BTree<i32, SmallBuffer, u32, u32>);

        // TODO: Change this to do 1024 steps
        #[test] #[ignore]
        fn full_tree_test_i64(sequential 1..512 => BTree<i32, SmallBuffer, i64, i64>);

        #[test] #[ignore]
        fn smallest_buffer_tree_u32(sequential 1..3 => BTree<i32, SmallestBuffer, u32, u32>);
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

        assert_subtree_valid(&t.root, &mut t.pager_info());
        assert_eq!(&t.to_description(), expected);

        drop(t);
        fs::remove_file(filename).unwrap();
    }

    #[test]
    fn failing_test_case_2() {
        let filename = "failing_test_case_2.test";
        let input = "
            0: [3, 6, 13, 16] (5)
            0->0: L[0, 1, 2, 3]
            0->1: L[4, 5, 6]
            0->2: L[7, 8, 9, 10, 11, 12, 13]
            0->3: L[14, 15, 16]
            0->4: L[17, 18, 19]
        ";
        let input = trim_lines(input);

        let mut t: BTree<i32, SmallBuffer, u32, u32> =
            init_tree_from_description_in_file(filename, &input);
        t.insert(8, 0).unwrap();

        assert_subtree_valid(&t.root, &mut t.pager_info());

        drop(t);
        fs::remove_file(filename).unwrap();
    }

    #[test]
    fn failing_test_case_3() {
        let filename = "failing_test_case_3.test";
        let input = "
            0: [3, 6, 14, 17] (5)
            0->0: L[0, 1, 2, 3]
            0->1: L[4, 5, 6]
            0->2: L[7, 8, 9, 10, 11, 13, 14]
            0->3: L[15, 16, 17]
            0->4: L[18, 19, 20]
        ";
        let input = trim_lines(input);

        let mut t: BTree<i32, SmallBuffer, u32, u32> =
            init_tree_from_description_in_file(filename, &input);
        t.insert(12, 0).unwrap();

        assert_subtree_valid(&t.root, &mut t.pager_info());

        drop(t);
        fs::remove_file(filename).unwrap();
    }

    #[test]
    fn failing_test_case_4() {
        let filename = "failing_test_case_4.test";
        let input = "
            0: [2, 5, 8, 11] (5)
            0->0: L[0, 1, 2]
            0->1: L[3, 4, 5]
            0->2: L[6, 7, 8]
            0->3: L[9, 10, 11]
            0->4: L[12, 13, 14, 15, 16, 17, 19]
        ";
        let input = trim_lines(input);

        let mut t: BTree<i32, SmallBuffer, u32, u32> =
            init_tree_from_description_in_file(filename, &input);
        t.insert(18, 18).unwrap();

        assert_subtree_valid(&t.root, &mut t.pager_info());

        drop(t);
        fs::remove_file(filename).unwrap();
    }

    #[test]
    fn smallest_buffer_u32_insert() {
        let filename = "smallest_buffer_u32_insert.test";
        let mut t: BTree<i32, SmallestBuffer, u32, u32> = init_tree_in_file(filename);

        t.insert(0, 0).unwrap();
        assert_subtree_valid(&t.root, &mut t.pager_info());

        drop(t);
        fs::remove_file(filename).unwrap();
    }

    #[test]
    fn failing_test_case_5() {
        let filename = "failing_test_case_5.test";
        let mut t: BTree<i32, SmallestBuffer, u32, u32> = init_tree_in_file(filename);

        t.insert(1, 1).unwrap();
        t.insert(0, 0).unwrap();
        println!("finished inserting");
        assert_subtree_valid(&t.root, &mut t.pager_info());

        drop(t);
        fs::remove_file(filename).unwrap();
    }
}
