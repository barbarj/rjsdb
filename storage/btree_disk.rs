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
/// - Internal node cells are also (K, PageId), except the last cell, which is always just PageId.
/// This allows the keys to split the search space and avoid needing a max-key that complicates the
/// insert logic
///

#[derive(Debug)]
pub enum Error {
    PageError(PageError),
    PagerError(PagerError),
    SerdeError(SerdeError),
}
impl From<PageError> for Error {
    fn from(value: PageError) -> Self {
        Self::PageError(value)
    }
}
impl From<PagerError> for Error {
    fn from(value: PagerError) -> Self {
        Self::PagerError(value)
    }
}
impl From<SerdeError> for Error {
    fn from(value: SerdeError) -> Self {
        Self::SerdeError(value)
    }
}
impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::PageError(error) => std::fmt::Display::fmt(&error, f),
            Self::PagerError(error) => std::fmt::Display::fmt(&error, f),
            Self::SerdeError(error) => std::fmt::Display::fmt(&error, f),
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
            for i in 0..root_page.cell_count() {
                let cell = root_page.get_cell_owned(0);
                new_page_left.insert_cell(i, &cell[..])?;
                root_page.remove_cell(0);
            }
            let new_page_id_left = new_page_left.id();
            drop(new_page_left);
            // update root with new children
            root_page.insert_cell(0, &to_bytes(&(split_key, new_page_id_left))?)?;
            root_page.insert_cell(1, &to_bytes(&new_page_id_right)?)?;
        }
        Ok(())
    }

    // TODO: get
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
            assert!(page.cell_count() > 0);
            page.cell_count() - 1
        }
    }

    fn can_fit<T: Serialize>(&self, value: &T) -> bool {
        let needed_space: usize = serialized_size(value) + CELL_POINTER_SIZE as usize;
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

    fn key_from_cell_tuple(&self, pos: u16) -> K {
        assert!(self.is_leaf() || pos < self.key_count());
        let page = self.page_ref.borrow();
        let cell = page.get_cell_owned(pos);
        if self.is_leaf() {
            let (cell_key, _): (K, V) = from_reader(&cell[..]).unwrap();
            cell_key
        } else {
            let (cell_key, _): (K, PageId) = from_reader(&cell[..]).unwrap();
            cell_key
        }
    }

    fn value_from_cell_tuple<T: DeserializeOwned>(&self, pos: u16) -> Result<T> {
        let page = self.page_ref.borrow();
        let cell = page.get_cell_owned(pos);
        let (_, val): (K, T) = from_reader(&cell[..]).unwrap();
        Ok(val)
    }

    fn page_id_from_last_cell(&self) -> Result<PageId> {
        assert!(self.is_node());
        let page = self.page_ref.borrow();
        let cell = page.get_cell_owned(self.key_count());
        let page_id = from_reader(&cell[..]).unwrap();
        Ok(page_id)
    }

    // TODO: Remove unwraps
    fn page_id_from_cell(&self, pos: u16) -> PageId {
        assert!(self.is_node());
        if pos < self.key_count() {
            self.value_from_cell_tuple(pos).unwrap()
        } else {
            self.page_id_from_last_cell().unwrap()
        }
    }

    // TODO: Test
    fn binary_search_keys(&self, key: &K) -> std::result::Result<u16, u16> {
        let mut low = 0;
        let mut high = self.key_count();
        while low < high {
            let mid = (low + high) / 2;
            let cell_key = self.key_from_cell_tuple(mid);
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
        let cell_key = self.key_from_cell_tuple(low);
        if &cell_key == key {
            Ok(low)
        } else {
            Err(low)
        }
    }

    fn split<Fd: AsRawFd + Copy>(
        &mut self,
        pager_ref: &mut PagerInfo<Fd>,
    ) -> Result<(K, Node<K, V>)> {
        let half = PAGE_BUFFER_SIZE / 2;
        let mut used_space = 0;
        let mut idx = 0;
        let mut page = self.page_ref.borrow_mut();

        // determine split point
        while used_space < half {
            let ptr = page.get_cell_pointer(idx);
            used_space += ptr.size;
            idx += 1;
        }
        let split_key = self.key_from_cell_tuple(idx - 1);

        // get new page
        let new_page_ref = pager_ref.new_page(page.kind())?;
        let mut new_page = new_page_ref.borrow_mut();

        // copy cells to new page and remove cells from old page
        for i in idx..page.cell_count() {
            let cell = page.get_cell_owned(idx);
            new_page.insert_cell(i, &cell[..])?;
            page.remove_cell(idx);
        }
        drop(new_page);
        // inner nodes don't have a key on the rightmost cell, so we need to fix that
        if self.is_node() {
            let cell = page.get_cell_owned(idx - 1);
            let (_, page_id): (K, PageId) = from_reader(&cell[..])?;
            page.remove_cell(idx - 1);
            page.insert_cell(idx - 1, &to_bytes(&page_id)?)?;
        }

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
        if !self.can_fit(&(&key, &value)) {
            let (split_key, mut new_node) = self.split(pager_ref)?;
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
    /// so an exact match isn't necessary
    fn search_keys_as_node(&self, key: &K) -> u16 {
        match self.binary_search_keys(key) {
            Ok(pos) => pos,
            Err(pos) => pos,
        }
    }

    fn add_child_as_node(&mut self, key: K, page_id: PageId, pos: u16) -> Result<()> {
        assert!(self.is_node());
        assert!(self.can_fit(&(&key, &page_id)));
        let mut page = self.page_ref.borrow_mut();
        page.insert_cell(pos, &to_bytes(&(key, page_id))?)?;
        Ok(())
    }

    fn get_descendent_by_key<Fd: AsRawFd + Copy>(
        &self,
        key: &K,
        pager_ref: &mut PagerInfo<Fd>,
    ) -> Result<(u16, Node<K, V>)> {
        assert!(self.is_node());
        let pos = self.search_keys_as_node(&key);
        let descendent = pager_ref.page_node(self.page_id_from_cell(pos))?;
        Ok((pos, descendent))
    }

    fn as_node_insert_interior_postsplit_child(
        &self,
        pos: u16,
        split_key: K,
        new_page_id: PageId,
    ) -> Result<()> {
        let (prior_key, left_page_id): (K, PageId) = self.value_from_cell_tuple(pos)?;
        let mut page = self.page_ref.borrow_mut();
        page.remove_cell(pos);
        page.insert_cell(pos, &to_bytes(&(split_key, left_page_id))?)?;
        page.insert_cell(pos + 1, &to_bytes(&(prior_key, new_page_id))?)?;
        Ok(())
    }

    fn as_node_insert_rightmost_postsplit_child(
        &self,
        split_key: K,
        new_page_id: PageId,
    ) -> Result<()> {
        let cell_pos = self.key_count();
        let mut page = self.page_ref.borrow_mut();
        let cell_page_id: PageId = from_reader(&page.get_cell_owned(cell_pos)[..])?;
        page.remove_cell(cell_pos);
        page.insert_cell(cell_pos, &to_bytes(&(&split_key, &cell_page_id))?)?;
        page.insert_cell(cell_pos + 1, &to_bytes(&new_page_id)?)?;
        assert_eq!(cell_pos + 1, self.key_count());
        Ok(())
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
            // last item
            if pos == self.key_count() {
                // This isn't actually what the added bytes will be, but it does conveniently have
                // the same size, and this is easier to check.
                if !self.can_fit(&(&split_key, &new_page_id)) {
                    let (parent_split_key, parent_new_node) = self.split(pager_ref)?;
                    assert!(parent_new_node.is_node());

                    // in this case, we are always updating the rightmost cell in the new node
                    parent_new_node
                        .as_node_insert_rightmost_postsplit_child(split_key, new_page_id)?;
                    Ok(InsertResult::Split(
                        parent_split_key,
                        parent_new_node.page_id(),
                    ))
                } else {
                    self.as_node_insert_rightmost_postsplit_child(split_key, new_page_id)?;
                    Ok(InsertResult::Done)
                }
            } else {
                // This isn't actually what the added bytes will be, but it does conveniently have
                // the same size, and this is easier to check.
                if !self.can_fit(&(&split_key, &new_page_id)) {
                    let (parent_split_key, parent_new_node) = self.split(pager_ref)?;
                    assert!(parent_new_node.is_node());

                    match pos.cmp(&self.key_count()) {
                        Ordering::Less => {
                            self.as_node_insert_interior_postsplit_child(
                                pos,
                                split_key,
                                new_page_id,
                            )?;
                        }
                        Ordering::Equal => {
                            self.as_node_insert_rightmost_postsplit_child(split_key, new_page_id)?;
                        }
                        Ordering::Greater => {
                            let new_page_pos = pos - self.key_count();
                            assert!(new_page_pos <= parent_new_node.key_count());
                            if new_page_pos == parent_new_node.key_count() {
                                parent_new_node.as_node_insert_rightmost_postsplit_child(
                                    split_key,
                                    new_page_id,
                                )?;
                            } else {
                                parent_new_node.as_node_insert_interior_postsplit_child(
                                    new_page_pos,
                                    split_key,
                                    new_page_id,
                                )?;
                            };
                        }
                    }
                    Ok(InsertResult::Split(
                        parent_split_key,
                        parent_new_node.page_id(),
                    ))
                } else {
                    self.as_node_insert_interior_postsplit_child(pos, split_key, new_page_id)?;
                    Ok(InsertResult::Done)
                }
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
                Ok(pos) => Ok(Some(self.value_from_cell_tuple(pos)?)),
                Err(_) => Ok(None),
            }
        } else {
            assert!(self.is_node());
            let (_, child_node) = self.get_descendent_by_key(key, pager_ref)?;
            child_node.get(key, pager_ref)
        }
    }
}
