#![allow(dead_code)]

use std::cmp::Ordering;

const FANOUT_FACTOR: usize = 512;

struct BTree<K: Ord, V> {
    root: Child<K, V>,
}
impl<K: Ord, V> BTree<K, V> {
    fn new() -> Self {
        BTree {
            root: Child::Leaf(Box::new(BTreeLeaf::new())),
        }
    }
}

enum BTreeError {
    NotEnoughSpace,
}

enum Child<K: Ord, V> {
    Node(Box<BTreeNode<K, V>>),
    Leaf(Box<BTreeLeaf<K, V>>),
}

struct BTreeNode<K: Ord, V> {
    keys: Vec<K>,
    children: Vec<Child<K, V>>,
}
impl<K: Ord, V> BTreeNode<K, V> {
    fn new() -> Self {
        BTreeNode {
            keys: Vec::new(),
            children: Vec::new(),
        }
    }
}

#[derive(Debug, PartialEq)]
enum SearchResult {
    Found(usize),
    NotFound(usize),
}

struct BTreeLeaf<K: Ord, V> {
    items: Vec<(K, V)>,
}
impl<K: Ord, V> BTreeLeaf<K, V> {
    fn new() -> Self {
        BTreeLeaf { items: Vec::new() }
    }

    fn from_items(items: Vec<(K, V)>) -> Self {
        BTreeLeaf { items }
    }

    fn insert(&mut self, key: K, value: V) -> Result<(), BTreeError> {
        // TODO: Requirements about item size
        if self.items.len() >= FANOUT_FACTOR {
            Err(BTreeError::NotEnoughSpace)
        } else {
            let position = match self.binary_search_for_key(&key) {
                SearchResult::Found(pos) => pos,
                SearchResult::NotFound(pos) => pos,
            };
            self.items.insert(position, (key, value));
            Ok(())
        }
    }

    fn remove(&mut self, key: &K) {
        let location = match self.binary_search_for_key(key) {
            SearchResult::Found(pos) => pos,
            SearchResult::NotFound(_) => return,
        };
        self.items.remove(location);
    }

    fn binary_search_for_key(&self, key: &K) -> SearchResult {
        let mut low = 0;
        let mut high = self.items.len() - 1;
        let mut cursor = (low + high) / 2;
        while low < high {
            let k = &self.items[cursor].0;
            match key.cmp(k) {
                Ordering::Less => {
                    if cursor == 0 {
                        return SearchResult::NotFound(cursor);
                    }
                    high = cursor - 1;
                    cursor = (low + high) / 2;
                }
                Ordering::Equal => {
                    return SearchResult::Found(cursor);
                }
                Ordering::Greater => {
                    low = cursor + 1;
                    cursor = (low + high) / 2;
                }
            }
        }
        cursor = low;
        let k = &self.items[cursor].0;
        match key.cmp(k) {
            Ordering::Less => SearchResult::NotFound(cursor),
            Ordering::Equal => SearchResult::Found(cursor),
            Ordering::Greater => SearchResult::NotFound(cursor + 1),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::btree2::SearchResult;

    use super::BTreeLeaf;

    #[test]
    fn leaf_binary_search() {
        let items = vec![(1, 10), (3, 30), (5, 50), (7, 70), (9, 90)];
        let leaf = BTreeLeaf::from_items(items);

        assert_eq!(leaf.binary_search_for_key(&0), SearchResult::NotFound(0));
        assert_eq!(leaf.binary_search_for_key(&1), SearchResult::Found(0));
        assert_eq!(leaf.binary_search_for_key(&2), SearchResult::NotFound(1));
        assert_eq!(leaf.binary_search_for_key(&3), SearchResult::Found(1));
        assert_eq!(leaf.binary_search_for_key(&4), SearchResult::NotFound(2));
        assert_eq!(leaf.binary_search_for_key(&5), SearchResult::Found(2));
        assert_eq!(leaf.binary_search_for_key(&6), SearchResult::NotFound(3));
        assert_eq!(leaf.binary_search_for_key(&7), SearchResult::Found(3));
        assert_eq!(leaf.binary_search_for_key(&8), SearchResult::NotFound(4));
        assert_eq!(leaf.binary_search_for_key(&9), SearchResult::Found(4));
        assert_eq!(leaf.binary_search_for_key(&10), SearchResult::NotFound(5));
    }
}
