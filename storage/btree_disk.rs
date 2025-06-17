#![allow(dead_code)]

use std::{cell::RefCell, fmt::Debug, marker::PhantomData, rc::Rc};

use crate::pager::{Page, PageKind};
use serde::{de::DeserializeOwned, Serialize};

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

    fn can_fit(&self, _key: &K, _value: &V) -> bool {
        unimplemented!();
        // TODO: Add some way of knowing serialized size
        /*
        let page = self.page_ref.borrow();
        let needed_space = key.bytes_needed() + value.bytes_needed();
        assert!(needed_space <= u16::MAX.into());
        page.can_fit_data(needed_space as u16)
        */
    }

    fn is_leaf(&self) -> bool {
        let page = self.page_ref.borrow();
        matches!(page.kind(), PageKind::BTreeLeaf)
    }

    fn is_node(&self) -> bool {
        let page = self.page_ref.borrow();
        matches!(page.kind(), PageKind::BTreeNode)
    }
}

// TODO: Page operations
