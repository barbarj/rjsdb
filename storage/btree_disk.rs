#![allow(dead_code)]

use std::{
    cell::RefCell,
    cmp::Ordering,
    fmt::{Debug, Display},
    marker::PhantomData,
    os::fd::AsRawFd,
    rc::Rc,
};

use crate::pager::{
    Page, PageError, PageId, PageKind, Pager, PagerError, CELL_POINTER_SIZE, PAGE_BUFFER_SIZE,
};
use serde::{de::DeserializeOwned, Serialize};
use serialize::{from_reader, serialized_size, to_bytes, Error as SerdeError};

///
/// # Notes on Page Structure
/// - Leaf node cells are (K, V)
/// - Internal nodes alternate PageIds and keys, so the cell order looks like:
///    PageId | Key | PageId | Key | PageId... etc.
///    The sequence always starts and end with PageIds. The Keys split the search space that the
///    PageIds represent.
///

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
    K: Ord + Serialize + DeserializeOwned + Debug,
    V: Serialize + DeserializeOwned,
> {
    pager_ref: Rc<RefCell<Pager>>,
    backing_fd: Fd,
    root: Node<K, V>,
    _key: PhantomData<K>,
    _value: PhantomData<V>,
}
impl<
        Fd: AsRawFd + Copy,
        K: Ord + Serialize + DeserializeOwned + Debug,
        V: Serialize + DeserializeOwned,
    > BTree<Fd, K, V>
{
    pub fn init(pager_ref: Rc<RefCell<Pager>>, backing_fd: Fd) -> Result<Self> {
        let mut pager = pager_ref.borrow_mut();
        let root_page = pager.get_page(backing_fd, 0)?; // page 0 is always the root
        drop(pager);
        let root = Node::new(root_page);
        Ok(BTree {
            pager_ref,
            backing_fd,
            root,
            _key: PhantomData,
            _value: PhantomData,
        })
    }

    fn pager_info(&self) -> PagerInfo<Fd> {
        PagerInfo::new(self.pager_ref.clone(), self.backing_fd)
    }

    pub fn insert(&mut self, key: K, value: V) -> Result<()> {
        let mut pager_info = self.pager_info();
        let insert_res = self.root.insert(key, value, &mut pager_info)?;
        if let InsertResult::Split(split_key, new_page_id_right) = insert_res {
            // move current root to a new page
            let new_page_left_ref = pager_info.new_page(self.root.page_kind())?;
            let mut new_page_left = new_page_left_ref.borrow_mut();
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
        }
        Ok(())
    }

    pub fn get(&self, key: &K) -> Result<Option<V>> {
        let mut pager_info = self.pager_info();
        self.root.get(key, &mut pager_info)
    }
}

struct PagerInfo<Fd: AsRawFd + Copy> {
    pager_ref: Rc<RefCell<Pager>>,
    backing_fd: Fd,
}
impl<Fd: AsRawFd + Copy> PagerInfo<Fd> {
    fn new(pager_ref: Rc<RefCell<Pager>>, backing_fd: Fd) -> Self {
        PagerInfo {
            pager_ref,
            backing_fd,
        }
    }

    fn new_page(&mut self, kind: PageKind) -> Result<Rc<RefCell<Page>>> {
        let mut pager = self.pager_ref.borrow_mut();
        let new_page = pager.new_page(self.backing_fd, kind)?;
        Ok(new_page)
    }

    fn get_page(&mut self, page_id: PageId) -> Result<Rc<RefCell<Page>>> {
        let mut pager = self.pager_ref.borrow_mut();
        let page = pager.get_page(self.backing_fd, page_id)?;
        Ok(page)
    }

    fn page_node<K, V>(&mut self, page_id: PageId) -> Result<Node<K, V>>
    where
        K: Ord + Debug + Serialize + DeserializeOwned,
        V: Serialize + DeserializeOwned,
    {
        let page = self.get_page(page_id)?;
        Ok(Node::new(page))
    }
}

enum InsertResult<K: Ord + Serialize + DeserializeOwned + Debug> {
    Split(K, PageId),
    Done,
}

// TODO: Convert the use of DeserializeOwned to a Deserialization of borrowed data (will need to
// get serialization format to support borrowed data
struct Node<K: Ord + Debug + Serialize + DeserializeOwned, V: Serialize + DeserializeOwned> {
    page_ref: Rc<RefCell<Page>>,
    _key: PhantomData<K>,
    _value: PhantomData<V>,
}
impl<K: Ord + Debug + Serialize + DeserializeOwned, V: Serialize + DeserializeOwned> Node<K, V> {
    fn new(page_ref: Rc<RefCell<Page>>) -> Self {
        Node {
            page_ref,
            _key: PhantomData,
            _value: PhantomData,
        }
    }

    fn key_count(&self) -> u16 {
        let page = self.page_ref.borrow();
        if self.is_leaf() {
            page.cell_count()
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
        let (key, _): (K, V) = from_reader(page.cell_bytes(pos))?;
        Ok(key)
    }

    fn value_from_leaf<T: DeserializeOwned>(&self, pos: u16) -> Result<T> {
        assert!(self.is_leaf());
        let page = self.page_ref.borrow();
        let (_, val): (K, T) = from_reader(page.cell_bytes(pos)).unwrap();
        Ok(val)
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

    // TODO: Test
    // TODO: Figure out if I should remove unwraps
    fn binary_search_keys(&self, key: &K) -> std::result::Result<u16, u16> {
        let mut low = 0;
        let mut high = self.key_count();
        while low < high {
            let mid = (low + high) / 2;
            let cell_key = self.key_at_pos(mid).unwrap();
            match &cell_key.cmp(key) {
                Ordering::Equal => return Ok(mid),
                Ordering::Less => {
                    low = mid + 1;
                }
                Ordering::Greater => {
                    high = mid - 1;
                }
            }
        }
        let cell_key = self.key_at_pos(low).unwrap();
        if &cell_key == key {
            Ok(low)
        } else {
            Err(low)
        }
    }

    fn split_inner_node<Fd: AsRawFd + Copy>(
        &mut self,
        pager_info: &mut PagerInfo<Fd>,
    ) -> Result<(K, Node<K, V>)> {
        let half = PAGE_BUFFER_SIZE / 2;
        assert!(self.page_free_space() < half);
        let mut used_space = 0;
        let mut idx = 0;
        let mut page = self.page_ref.borrow_mut();

        // Find the index of the first cell that begins past the halfway point
        while used_space < half {
            let ptr = page.get_cell_pointer(idx);
            used_space += ptr.size;
            idx += 1;
        }
        if idx % 2 == 0 {
            // cell at idx contains a pageId, so we actually want to use the key to the left of it.
            idx -= 1;
        }

        // self.key_from_inner_node uses the logical key position amongst other keys, so convert to
        // that before asking for the key
        let split_key = self.key_from_inner_node(Self::cell_pos_to_key_pos(idx).unwrap())?;

        // get new page
        let new_page_ref = pager_info.new_page(page.kind())?;
        let mut new_page = new_page_ref.borrow_mut();

        // copy cells to new page, starting with the cell after the split key
        for (i, bytes) in page.cell_bytes_iter().enumerate().skip((idx + 1).into()) {
            new_page.insert_cell(i as u16, bytes)?;
        }
        // remove moved cells, plus the now hanging right key from this node
        for i in page.cell_count() - 1..=idx {
            page.remove_cell(i);
        }
        drop(new_page);

        // remove the now hanging right key from this node
        page.remove_cell(idx);

        let new_node = Node::new(new_page_ref);
        Ok((split_key, new_node))
    }

    fn split_leaf<Fd: AsRawFd + Copy>(
        &mut self,
        pager_info: &mut PagerInfo<Fd>,
    ) -> Result<(K, Node<K, V>)> {
        let half = PAGE_BUFFER_SIZE / 2;
        let mut used_space = 0;
        let mut idx = 0;
        let mut page = self.page_ref.borrow_mut();

        // Find the index of the first cell that begins past the halfway point
        while used_space < half {
            let ptr = page.get_cell_pointer(idx);
            used_space += ptr.size;
            idx += 1;
        }
        // keys point left, and cell number idx is going to be the first cell in the new page,
        // so the split key should be one to the left.
        assert!(idx > 0);
        let split_key = self.key_from_leaf(idx - 1)?;

        // get new page
        let new_page_ref = pager_info.new_page(page.kind())?;
        let mut new_page = new_page_ref.borrow_mut();

        // copy cells to new page and remove cells from old page
        for i in idx..page.cell_count() {
            new_page.insert_cell(i, page.cell_bytes(idx))?;
            page.remove_cell(idx);
        }
        drop(new_page);

        let new_node = Node::new(new_page_ref);
        Ok((split_key, new_node))
    }

    fn insert_as_leaf<Fd: AsRawFd + Copy>(
        &mut self,
        key: K,
        value: V,
        pager_ref: &mut PagerInfo<Fd>,
    ) -> Result<InsertResult<K>> {
        assert!(self.is_leaf());
        if !self.can_fit_leaf(&key, &value) {
            let (split_key, mut new_node) = self.split_leaf(pager_ref)?;
            assert!(new_node.is_leaf());
            if key > split_key {
                new_node.insert_as_leaf(key, value, pager_ref)?;
            } else {
                self.insert_as_leaf(key, value, pager_ref)?;
            }
            Ok(InsertResult::Split(split_key, new_node.page_id()))
        } else {
            match self.binary_search_keys(&key) {
                Ok(pos) => {
                    let mut page = self.page_ref.borrow_mut();
                    // TODO: Add some replace cell function to page
                    page.remove_cell(pos);
                    page.insert_cell(pos, &to_bytes(&(key, value))?)?;
                }
                Err(pos) => {
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

    fn get_descendent_by_key<Fd: AsRawFd + Copy>(
        &self,
        key: &K,
        pager_ref: &mut PagerInfo<Fd>,
    ) -> Result<(u16, Node<K, V>)> {
        assert!(self.is_node());
        let pos = self.search_keys_as_node(key);
        let descendent = pager_ref.page_node(self.page_id_from_inner_node(pos)?)?;
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
        page.insert_cell(id_cell_pos + 1, &to_bytes(&prior_key)?)?;
        Ok(())
    }

    /// replaces the key at key position pos with the new key, and returns the old key
    fn replace_inner_node_key(&mut self, pos: u16, new_key: &K) -> Result<K> {
        let old_key = self.key_from_inner_node(pos)?;
        let cell_idx = Self::key_pos_to_cell_pos(pos);
        let mut page = self.page_ref.borrow_mut();
        page.remove_cell(cell_idx);
        page.insert_cell(cell_idx, &to_bytes(new_key)?)?;
        Ok(old_key)
    }

    fn insert_as_node<Fd: AsRawFd + Copy>(
        &mut self,
        key: K,
        value: V,
        pager_ref: &mut PagerInfo<Fd>,
    ) -> Result<InsertResult<K>> {
        assert!(self.is_node());
        let (pos, mut child_node) = self.get_descendent_by_key(&key, pager_ref)?;
        if let InsertResult::Split(split_key, new_page_id) =
            child_node.insert(key, value, pager_ref)?
        {
            if !self.can_fit_node(&split_key) {
                let (parent_split_key, mut parent_new_node) = self.split_inner_node(pager_ref)?;
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
        pager_ref: &mut PagerInfo<Fd>,
    ) -> Result<InsertResult<K>> {
        if self.is_leaf() {
            self.insert_as_leaf(key, value, pager_ref)
        } else {
            self.insert_as_node(key, value, pager_ref)
        }
    }

    fn get<Fd: AsRawFd + Copy>(&self, key: &K, pager_ref: &mut PagerInfo<Fd>) -> Result<Option<V>> {
        if self.is_leaf() {
            match self.binary_search_keys(key) {
                Ok(pos) => Ok(Some(self.value_from_leaf(pos)?)),
                Err(_) => Ok(None),
            }
        } else {
            assert!(self.is_node());
            let (_, child_node) = self.get_descendent_by_key(key, pager_ref)?;
            child_node.get(key, pager_ref)
        }
    }
}
