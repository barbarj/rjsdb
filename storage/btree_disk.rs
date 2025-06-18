#![allow(dead_code)]

use std::{
    cell::RefCell,
    cmp::Ordering,
    fmt::{Debug, Display},
    marker::PhantomData,
    rc::Rc,
};

use crate::pager::{Page, PageError, PageId, PageKind, PAGE_BUFFER_SIZE};
use serde::{de::DeserializeOwned, Serialize};
use serialize::{from_reader, serialized_size, to_bytes, Error as SerdeError};

#[derive(Debug)]
pub enum Error {
    PageError(PageError),
    SerdeError(SerdeError),
}
impl From<PageError> for Error {
    fn from(value: PageError) -> Self {
        Self::PageError(value)
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
            Self::PageError(page_error) => std::fmt::Display::fmt(&page_error, f),
            Self::SerdeError(serde_error) => std::fmt::Display::fmt(&serde_error, f),
        }
    }
}
impl std::error::Error for Error {}

type Result<T> = std::result::Result<T, Error>;

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
        let (cell_key, _): (K, V) = from_reader(&cell[..]).unwrap();
        cell_key
    }

    // TODO: Test
    fn binary_search_keys_leaf(&self, key: &K) -> std::result::Result<u16, u16> {
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

    // Probably only need one split function
    fn split(&mut self) -> (K, Node<K, V>) {
        /*
        let half = PAGE_BUFFER_SIZE / 2;
        let mut used_space = 0;
        let mut idx = 0;
        let mut page = self.page_ref.borrow_mut();
        while used_space < half {
            let ptr = page.get_cell_pointer(idx);
            used_space += ptr.size;
            idx += 1;
        }
        let split_key = self.key_from_cell(idx - 1);
        */
        unimplemented!();
    }

    fn insert_as_leaf(&mut self, key: K, value: V) -> Result<InsertResult<K, V>> {
        assert!(self.is_leaf());
        if !self.can_fit(&key, &value) {
            let (split_key, mut new_node) = self.split();
            assert!(new_node.is_leaf());
            if key > split_key {
                new_node.insert_as_leaf(key, value)?;
            } else {
                self.insert_as_leaf(key, value)?;
            }
            Ok(InsertResult::Split(split_key, new_node))
        } else {
            match self.binary_search_keys_leaf(&key) {
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

    fn insert_as_node(&mut self, key: K, value: V) -> Result<InsertResult<K, V>> {
        unimplemented!();
    }

    fn insert(&mut self, key: K, value: V) -> Result<InsertResult<K, V>> {
        if self.is_leaf() {
            self.insert_as_leaf(key, value)
        } else {
            self.insert_as_node(key, value)
        }
    }
}

// TODO: Page operations
