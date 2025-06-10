#![allow(dead_code)]

use crate::serialize::{Deserialize, Serialize};
use std::{cell::RefCell, fmt::Debug, marker::PhantomData, rc::Rc};

use crate::pager::{Page, PageKind};

struct Node<K: Ord + Debug + Serialize + Deserialize, V: Serialize + Deserialize> {
    page_ref: Rc<RefCell<Page>>,
    _key: PhantomData<K>,
    _value: PhantomData<V>,
}
impl<K: Ord + Debug + Serialize + Deserialize, V: Serialize + Deserialize> Node<K, V> {
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
        let page = self.page_ref.borrow();
        let needed_space = key.bytes_needed() + value.bytes_needed();
        assert!(needed_space <= u16::MAX.into());
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
}

// TODO: Page operations
