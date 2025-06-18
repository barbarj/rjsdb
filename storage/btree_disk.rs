#![allow(dead_code)]

use std::{
    cell::RefCell,
    cmp::Ordering,
    fmt::{Debug, Display},
    marker::PhantomData,
    os::fd::AsRawFd,
    rc::Rc,
};

use crate::pager::{Page, PageError, PageId, PageKind, Pager, PagerError, PAGE_BUFFER_SIZE};
use serde::{de::DeserializeOwned, Serialize};
use serialize::{from_reader, serialized_size, to_bytes, Error as SerdeError};

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

    fn pager_info(&self) -> PagerRef<Fd> {
        PagerRef::new(self.pager_ref.clone(), self.backing_fd)
    }
}

struct PagerRef<Fd: AsRawFd + Copy> {
    pager_ref: Rc<RefCell<Pager>>,
    backing_fd: Fd,
}
impl<Fd: AsRawFd + Copy> PagerRef<Fd> {
    fn new(pager_ref: Rc<RefCell<Pager>>, backing_fd: Fd) -> Self {
        PagerRef {
            pager_ref,
            backing_fd,
        }
    }

    fn new_page(&mut self, kind: PageKind) -> Result<Rc<RefCell<Page>>> {
        let mut pager = self.pager_ref.borrow_mut();
        let new_page = pager.new_page(self.backing_fd, kind)?;
        Ok(new_page)
    }
}

enum InsertResult<K: Ord + Serialize + DeserializeOwned + Debug, V: Serialize + DeserializeOwned> {
    Split(K, Node<K, V>),
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
        page.cell_count()
    }

    fn can_fit(&self, key: &K, value: &V) -> bool {
        let needed_space = serialized_size(&(key, value));
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

    fn key_from_cell(&self, pos: u16) -> K {
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

    // TODO: Test
    fn binary_search_keys(&self, key: &K) -> std::result::Result<u16, u16> {
        let mut low = 0;
        let mut high = self.key_count();
        while low < high {
            let mid = (low + high) / 2;
            let cell_key = self.key_from_cell(mid);
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
        let cell_key = self.key_from_cell(low);
        if &cell_key == key {
            Ok(low)
        } else {
            Err(low)
        }
    }

    fn split<Fd: AsRawFd + Copy>(
        &mut self,
        pager_ref: &mut PagerRef<Fd>,
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
        let split_key = self.key_from_cell(idx - 1);

        // get new page
        let new_page_ref = pager_ref.new_page(page.kind())?;
        let mut new_page = new_page_ref.borrow_mut();

        // copy cells to new page and remove cells from old page
        for i in 0..idx {
            let cell = page.get_cell_owned(0);
            new_page.insert_cell(i, &cell[..])?;
            page.remove_cell(0);
        }
        drop(new_page);

        let new_node = Node::new(new_page_ref);
        Ok((split_key, new_node))
    }

    fn insert_as_leaf<Fd: AsRawFd + Copy>(
        &mut self,
        key: K,
        value: V,
        pager_ref: &mut PagerRef<Fd>,
    ) -> Result<InsertResult<K, V>> {
        assert!(self.is_leaf());
        if !self.can_fit(&key, &value) {
            let (split_key, mut new_node) = self.split(pager_ref)?;
            assert!(new_node.is_leaf());
            if key > split_key {
                new_node.insert_as_leaf(key, value, pager_ref)?;
            } else {
                self.insert_as_leaf(key, value, pager_ref)?;
            }
            Ok(InsertResult::Split(split_key, new_node))
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

    fn insert_as_node<Fd: AsRawFd + Copy>(
        &mut self,
        key: K,
        value: V,
        pager_ref: &mut PagerRef<Fd>,
    ) -> Result<InsertResult<K, V>> {
        unimplemented!();
    }

    fn insert<Fd: AsRawFd + Copy>(
        &mut self,
        key: K,
        value: V,
        pager_ref: &mut PagerRef<Fd>,
    ) -> Result<InsertResult<K, V>> {
        if self.is_leaf() {
            self.insert_as_leaf(key, value, pager_ref)
        } else {
            self.insert_as_node(key, value, pager_ref)
        }
    }
}

// TODO: Page operations
