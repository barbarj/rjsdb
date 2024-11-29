#![allow(dead_code)]

use std::fmt::Debug;

#[cfg(not(test))]
const FANOUT_FACTOR: usize = 512;
#[cfg(test)]
const FANOUT_FACTOR: usize = 5;

struct BTree<K: Ord + Clone, V: Clone> {
    root: Child<K, V>,
}
impl<K: Ord + Clone, V: Clone> BTree<K, V> {
    fn new() -> Self {
        BTree {
            root: Child::Leaf(Box::new(BTreeLeaf::new())),
        }
    }
}

enum BTreeError {
    NotEnoughSpace,
}

enum Child<K: Ord + Clone, V: Clone> {
    Node(Box<BTreeNode<K, V>>),
    Leaf(Box<BTreeLeaf<K, V>>),
}
impl<K: Ord + Clone, V: Clone> Child<K, V> {
    fn node(&self) -> &BTreeNode<K, V> {
        match self {
            Child::Node(node) => node,
            Child::Leaf(_) => unreachable!(),
        }
    }

    fn node_mut(&mut self) -> &BTreeNode<K, V> {
        match self {
            Child::Node(node) => node,
            Child::Leaf(_) => unreachable!(),
        }
    }

    fn leaf(&self) -> &BTreeLeaf<K, V> {
        match self {
            Child::Node(_) => unreachable!(),
            Child::Leaf(leaf) => leaf,
        }
    }

    fn leaf_mut(&mut self) -> &BTreeLeaf<K, V> {
        match self {
            Child::Node(_) => unreachable!(),
            Child::Leaf(leaf) => leaf,
        }
    }

    fn member_count(&self) -> usize {
        match self {
            Child::Node(node) => node.children.len(),
            Child::Leaf(leaf) => leaf.items.len(),
        }
    }
}

enum InsertionResult<K: Ord + Clone, V: Clone> {
    Done,
    Split(Child<K, V>),
}

struct BTreeNode<K: Ord + Clone, V: Clone> {
    keys: Vec<K>,
    children: Vec<Child<K, V>>,
}
impl<K: Ord + Clone + Debug, V: Clone> BTreeNode<K, V> {
    fn new() -> Self {
        BTreeNode {
            keys: Vec::with_capacity(FANOUT_FACTOR - 1),
            children: Vec::with_capacity(FANOUT_FACTOR),
        }
    }

    /// Only for testing purposes, and then only for testing functionality that
    /// only depends on keys. Technically this constructs an invalid node. This
    /// is convenient for some tests though.
    #[cfg(test)]
    fn with_keys_only(keys: Vec<K>) -> Self {
        BTreeNode {
            keys,
            children: Vec::new(),
        }
    }

    #[cfg(test)]
    fn child_sizes(&self) -> Vec<usize> {
        let mut sizes = Vec::new();
        for child in self.children.iter() {
            let size = match child {
                Child::Node(node) => node.children.len(),
                Child::Leaf(leaf) => leaf.items.len(),
            };
            sizes.push(size);
        }
        sizes
    }

    fn from_leaves(leaves: Vec<BTreeLeaf<K, V>>) -> Self {
        assert!(leaves.len() > 1);
        let keys = leaves[..leaves.len() - 1]
            .iter()
            .map(|leaf| &leaf.items.last().unwrap().0)
            .cloned()
            .collect();
        let children = leaves
            .into_iter()
            .map(|leaf| Child::Leaf(Box::new(leaf)))
            .collect();

        BTreeNode { keys, children }
    }

    /// returns the newly created node, representing the right side of the split
    fn split(&mut self) -> BTreeNode<K, V> {
        let midpoint = self.keys.len() / 2;
        let new_keys = self.keys.split_off(midpoint);
        // the resulting left node will have no children to the right of the last key, so we can
        // remove it.
        self.keys.pop();
        let new_children = self.children.split_off(midpoint);
        BTreeNode {
            keys: new_keys,
            children: new_children,
        }
    }

    fn insert(&mut self, key: K, value: V) -> InsertionResult<K, V> {
        assert_eq!(self.keys.len() + 1, self.children.len());
        let position = match self.binary_search_for_key(&key) {
            Ok(pos) => pos,
            Err(pos) => pos,
        };
        let child = self.children.get_mut(position).unwrap();
        let insertion_res = match child {
            Child::Node(node) => node.insert(key, value),
            Child::Leaf(leaf) => leaf.insert(key, value),
        };
        if let InsertionResult::Split(new_child_node) = insertion_res {
            let split_key = match child {
                Child::Node(node) => node.keys.last().unwrap().clone(),
                Child::Leaf(leaf) => leaf.last_key().unwrap().clone(),
            };
            if self.children.len() >= FANOUT_FACTOR {
                let mut new_node = self.split();
                if self.keys.last().unwrap() < &split_key {
                    // insert into new node (right one)
                    let pos = match new_node.binary_search_for_key(&split_key) {
                        Ok(pos) => pos,
                        Err(pos) => pos,
                    };
                    new_node.keys.insert(pos, split_key);
                    new_node.children.insert(pos + 1, new_child_node);
                } else {
                    // insert into existing node (this, the left one)
                    self.keys.insert(position, split_key);
                    self.children.insert(position + 1, new_child_node);
                }
                return InsertionResult::Split(Child::Node(Box::new(new_node)));
            } else {
                self.keys.insert(position, split_key);
                self.children.insert(position + 1, new_child_node);
                return InsertionResult::Done;
            }
        };
        InsertionResult::Done
    }

    fn get(&self, key: &K) -> Option<V> {
        let position = match self.binary_search_for_key(key) {
            Ok(pos) => pos,
            Err(pos) => pos,
        };
        match &self.children[position] {
            Child::Node(node) => node.get(key),
            Child::Leaf(leaf) => leaf.get(key),
        }
    }

    fn merge_children(&mut self, left_pos: usize, right_pos: usize) {
        assert_eq!(left_pos + 1, right_pos);
        let (left, right) = self.children.split_at_mut(right_pos);
        let left_child = &mut left[left_pos];
        let right_child = &mut right[0];

        match (left_child, right_child) {
            (Child::Node(left), Child::Node(right)) => {
                left.keys.append(&mut right.keys);
                left.children.append(&mut right.children);
            }
            (Child::Leaf(left), Child::Leaf(right)) => {
                left.items.append(&mut right.items);
            }
            _ => unreachable!(),
        }
        self.children.remove(right_pos);
        self.keys.remove(left_pos);
    }

    fn remove(&mut self, key: &K) {
        let position = match self.binary_search_for_key(key) {
            Ok(pos) => pos,
            Err(pos) => pos,
        };
        let child_node = &mut self.children[position];
        match child_node {
            Child::Node(child_node) => {
                child_node.remove(key);
            }
            Child::Leaf(leaf) => {
                leaf.remove(key);
            }
        };
        if child_node.member_count() <= (FANOUT_FACTOR / 3) {
            if position > 0 && self.children[position - 1].member_count() <= (FANOUT_FACTOR / 3) {
                self.merge_children(position - 1, position);
            } else if position < self.children.len() - 2
                && self.children[position + 1].member_count() <= (FANOUT_FACTOR / 3)
            {
                self.merge_children(position, position + 1);
            };
        }
    }

    fn binary_search_for_key(&self, key: &K) -> Result<usize, usize> {
        self.keys.binary_search(key)
    }
}

struct BTreeLeaf<K: Ord + Clone, V: Clone> {
    items: Vec<(K, V)>,
}
impl<K: Ord + Clone, V: Clone> BTreeLeaf<K, V> {
    fn new() -> Self {
        BTreeLeaf {
            items: Vec::with_capacity(FANOUT_FACTOR),
        }
    }

    fn split(&mut self) -> BTreeLeaf<K, V> {
        let right_items = self.items.split_off(self.items.len() / 2);
        BTreeLeaf { items: right_items }
    }

    fn last_key(&self) -> Option<&K> {
        self.items.last().map(|x| &x.0)
    }

    fn insert(&mut self, key: K, value: V) -> InsertionResult<K, V> {
        // TODO: Requirements about item size
        if self.items.len() >= FANOUT_FACTOR {
            let mut new_node = self.split();
            // resulting split key will be the last key in the left (not new) cell
            if key > *self.last_key().unwrap() {
                let _res = new_node.insert(key, value);
                let typed_done: InsertionResult<K, V> = InsertionResult::Done;
                assert!(matches!(typed_done, _res));
                return InsertionResult::Split(Child::Leaf(Box::new(new_node)));
            }
        }
        let position = match self.binary_search_for_key(&key) {
            Ok(pos) => pos,
            Err(pos) => pos,
        };
        self.items.insert(position, (key, value));
        InsertionResult::Done
    }

    fn get(&self, key: &K) -> Option<V> {
        match self.binary_search_for_key(key) {
            Ok(pos) => Some(self.items[pos].1.clone()),
            Err(_) => None,
        }
    }

    fn remove(&mut self, key: &K) {
        let location = match self.binary_search_for_key(key) {
            Ok(pos) => pos,
            Err(_) => return,
        };
        self.items.remove(location);
    }

    fn binary_search_for_key(&self, key: &K) -> Result<usize, usize> {
        self.items.binary_search_by_key(key, |item| item.0.clone())
    }
}

#[cfg(test)]
mod tests {
    use crate::btree2::BTreeNode;

    use super::{BTreeLeaf, Child, InsertionResult, FANOUT_FACTOR};

    #[test]
    fn leaf_binary_search() {
        let items = vec![(1, 10), (3, 30), (5, 50), (7, 70), (9, 90)];
        let leaf = BTreeLeaf { items };

        assert_eq!(leaf.binary_search_for_key(&0), Err(0));
        assert_eq!(leaf.binary_search_for_key(&1), Ok(0));
        assert_eq!(leaf.binary_search_for_key(&2), Err(1));
        assert_eq!(leaf.binary_search_for_key(&3), Ok(1));
        assert_eq!(leaf.binary_search_for_key(&4), Err(2));
        assert_eq!(leaf.binary_search_for_key(&5), Ok(2));
        assert_eq!(leaf.binary_search_for_key(&6), Err(3));
        assert_eq!(leaf.binary_search_for_key(&7), Ok(3));
        assert_eq!(leaf.binary_search_for_key(&8), Err(4));
        assert_eq!(leaf.binary_search_for_key(&9), Ok(4));
        assert_eq!(leaf.binary_search_for_key(&10), Err(5));
    }

    #[test]
    fn node_binary_search() {
        let items = vec![1, 3, 5, 7, 9];
        let node: BTreeNode<i32, ()> = BTreeNode::with_keys_only(items);

        assert_eq!(node.binary_search_for_key(&0), Err(0));
        assert_eq!(node.binary_search_for_key(&1), Ok(0));
        assert_eq!(node.binary_search_for_key(&2), Err(1));
        assert_eq!(node.binary_search_for_key(&3), Ok(1));
        assert_eq!(node.binary_search_for_key(&4), Err(2));
        assert_eq!(node.binary_search_for_key(&5), Ok(2));
        assert_eq!(node.binary_search_for_key(&6), Err(3));
        assert_eq!(node.binary_search_for_key(&7), Ok(3));
        assert_eq!(node.binary_search_for_key(&8), Err(4));
        assert_eq!(node.binary_search_for_key(&9), Ok(4));
        assert_eq!(node.binary_search_for_key(&10), Err(5));
    }

    #[test]
    fn leaf_insert_and_split() {
        let mut keys_iter = (0..).filter(|x| x % 2 == 1);
        let mut items: Vec<i32> = keys_iter.by_ref().take(FANOUT_FACTOR).collect();
        let mut leaf = BTreeLeaf::new();

        let insertion_done: InsertionResult<i32, i32> = InsertionResult::Done;
        // insert up to fanout factor
        for num in items.iter() {
            let _res = leaf.insert(*num, num * 10);
            assert!(matches!(&insertion_done, _res));
        }

        // prove all values are fetchable
        for num in items.iter() {
            assert_eq!(leaf.get(num), Some(num * 10));
        }

        // insert one more to trigger split
        let new_key = keys_iter.next().unwrap();
        let new_leaf = match leaf.insert(new_key, new_key * 10) {
            InsertionResult::Done => panic!("Should have had a new leaf"),
            InsertionResult::Split(new_child) => match new_child {
                Child::Node(_) => panic!("Child should be a leaf"),
                Child::Leaf(leaf) => leaf,
            },
        };
        items.push(new_key);
        let (left_items, right_items) = items.split_at(FANOUT_FACTOR / 2);

        // check the values of the resulting leaves
        for num in left_items {
            assert_eq!(leaf.get(num), Some(num * 10));
            assert!(new_leaf.get(num).is_none());
        }
        for num in right_items {
            assert_eq!(new_leaf.get(num), Some(num * 10));
            assert!(leaf.get(num).is_none());
        }
    }

    #[test]
    fn node_insert_and_split() {
        // TODO: update
        assert_eq!(
            FANOUT_FACTOR, 5,
            "This test needs updated if the test fanout factor changes"
        );

        let first_leaf_items = [100, 200, 300, 400, 500, 600];
        let split_right_leaf_items = [700, 800];
        let split_left_leaf_items = [110, 120, 130, 140];
        let split_interior_leaf_items = [310, 320, 330, 340];
        let fill_right_leaf_items = [900];
        // effects of insertions:
        // 100, 200, 300, 400, 500 // fill leaf
        // 100, 200 || 300, 400, 500, 600 // add one to split
        // 100, 200 || 300, 400 || 500, 600, 700, 800 // fill right leaf to split
        // 100, 110 || 120, 130, 140, 200 || 300, 400 || 500, 600, 700, 800 // fill left leaf to split
        // 100, 110 || 120, 130, 140, 200 || 300, 310 || 320, 330, 340, 400 || 500, 600, 700, 800 // fill interior leaf to split
        // 100, 110 || 120, 130, 140, 200 || 300, 310 || 320, 330, 340, 400 || 500, 600, 700, 800, 900 // fill one more leaf

        // add one more to right leaf to cause leaf split + node split
        // verify resulting nodes are correctly constructed
        //
        let insertion_done: InsertionResult<i32, i32> = InsertionResult::Done;
        let mut keys_present: Vec<i32> = Vec::new();
        let mut insertion_order = 0;

        // split first leaf
        let mut first_leaf = BTreeLeaf::new();
        for num in first_leaf_items[0..first_leaf_items.len() - 1].iter() {
            // all but last
            let _res = first_leaf.insert(*num, insertion_order);
            assert!(matches!(&insertion_done, _res));
            insertion_order += 1;
            keys_present.push(*num);
        }
        let second_leaf =
            match first_leaf.insert(*first_leaf_items.last().unwrap(), insertion_order) {
                InsertionResult::Split(Child::Leaf(new_leaf)) => new_leaf,
                _ => panic!("Should have split and made a leaf"),
            };
        insertion_order += 1;
        keys_present.push(*first_leaf_items.last().unwrap());

        assert_eq!(first_leaf.last_key(), Some(&200));

        let mut node = BTreeNode::from_leaves(vec![first_leaf, *second_leaf]);
        assert_eq!(node.children.len(), 2);
        assert_eq!(node.keys, vec![200]);
        assert_eq!(node.child_sizes(), vec![2, 4]);
        for (idx, key) in keys_present.iter().enumerate() {
            assert_eq!(node.get(key), Some(idx));
        }

        // split right leaf
        for num in split_right_leaf_items {
            node.insert(num, insertion_order);
            insertion_order += 1;
            keys_present.push(num);
        }
        assert_eq!(node.children.len(), 3);
        assert_eq!(node.keys, vec![200, 400]);
        assert_eq!(node.child_sizes(), vec![2, 2, 4]);
        for (idx, key) in keys_present.iter().enumerate() {
            assert_eq!(node.get(key), Some(idx));
        }

        // split left leaf
        for num in split_left_leaf_items {
            node.insert(num, insertion_order);
            insertion_order += 1;
            keys_present.push(num);
        }
        assert_eq!(node.children.len(), 4);
        assert_eq!(node.keys, vec![110, 200, 400]);
        assert_eq!(node.child_sizes(), vec![2, 4, 2, 4]);
        for (idx, key) in keys_present.iter().enumerate() {
            assert_eq!(node.get(key), Some(idx));
        }

        // split interior leaf
        for num in split_interior_leaf_items {
            node.insert(num, insertion_order);
            insertion_order += 1;
            keys_present.push(num);
        }
        assert_eq!(node.children.len(), 5);
        assert_eq!(node.keys, vec![110, 200, 310, 400]);
        assert_eq!(node.child_sizes(), vec![2, 4, 2, 4, 4]);
        for (idx, key) in keys_present.iter().enumerate() {
            assert_eq!(node.get(key), Some(idx));
        }

        // fill right leaf
        for num in fill_right_leaf_items {
            node.insert(num, insertion_order);
            insertion_order += 1;
            keys_present.push(num);
        }
        assert_eq!(node.child_sizes(), vec![2, 4, 2, 4, 5]);
        for (idx, key) in keys_present.iter().enumerate() {
            assert_eq!(node.get(key), Some(idx));
        }

        // add one more item to the full leaf on the right to cause leaf split then node split
        let new_node = match node.insert(910, insertion_order) {
            InsertionResult::Split(Child::Node(new_node)) => new_node,
            _ => panic!("Should have split into a new node"),
        };
        keys_present.push(910);
        // the new leaf will have ended up on the right node after the split
        assert_eq!(node.children.len(), 2);
        assert_eq!(node.keys, vec![110]);
        assert_eq!(new_node.children.len(), 4);
        assert_eq!(new_node.keys, vec![310, 400, 600]);
        for (idx, key) in keys_present.iter().enumerate() {
            let found_in_either = node.get(key).or(new_node.get(key));
            assert_eq!(found_in_either, Some(idx));
        }
        assert_eq!(node.child_sizes(), vec![2, 4]);
        assert_eq!(new_node.child_sizes(), vec![2, 4, 2, 4]);
    }
}
