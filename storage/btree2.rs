#![allow(dead_code)]

use std::{fmt::Debug, slice::Iter};

struct BTree<K: Ord + Clone + Debug, V: Clone> {
    root: Option<Child<K, V>>,
}
impl<K: Ord + Clone + Debug, V: Clone> BTree<K, V> {
    fn new(fanout_factor: usize) -> Self {
        BTree {
            root: Some(Child::Leaf(Box::new(BTreeLeaf::new(fanout_factor)))),
        }
    }

    fn insert(&mut self, key: K, value: V) {
        let result = match self.root.as_mut().unwrap() {
            Child::Leaf(leaf) => leaf.insert(key, value),
            Child::Node(node) => node.insert(key, value),
        };
        match result {
            InsertionResult::Done => (),
            InsertionResult::Split {
                child: new_child,
                key: split_key,
            } => match (self.root.take().unwrap(), new_child) {
                (Child::Leaf(leaf), Child::Leaf(new_leaf)) => {
                    let new_root = BTreeNode::from_leaves(vec![*leaf, *new_leaf], vec![split_key]);
                    self.root = Some(Child::Node(Box::new(new_root)));
                }
                (Child::Node(old_root), Child::Node(new_node)) => {
                    let new_root =
                        BTreeNode::from_nodes(vec![*old_root, *new_node], vec![split_key]);
                    self.root = Some(Child::Node(Box::new(new_root)));
                }
                _ => unreachable!(),
            },
        }
    }

    fn remove(&mut self, key: &K) -> Result<(), BTreeError> {
        match self.root.as_mut().unwrap() {
            Child::Leaf(leaf) => leaf.remove(key),
            Child::Node(node) => {
                node.remove(key)?;
                if node.children.len() == 1 {
                    self.root = node.children.pop();
                }
                Ok(())
            }
        }
    }

    fn get(&self, key: &K) -> Option<V> {
        match self.root.as_ref().unwrap() {
            Child::Leaf(leaf) => leaf.get(key),
            Child::Node(node) => node.get(key),
        }
    }

    fn iter(&self) -> BTreeIterator<K, V> {
        let mut node_stack = Vec::new();
        let mut idx_stack = Vec::new();
        let mut current = self.root.as_ref().unwrap();
        while !current.is_leaf() {
            let node = current.as_node();
            node_stack.push(node);
            current = &node.children[0];
            idx_stack.push(0);
        }
        let leaf_iter = current.as_leaf().items.iter();
        BTreeIterator {
            node_stack,
            idx_stack,
            leaf_iter,
        }
    }
}

struct BTreeIterator<'a, K: Ord + Clone + Debug, V: Clone> {
    node_stack: Vec<&'a BTreeNode<K, V>>,
    idx_stack: Vec<usize>,
    leaf_iter: Iter<'a, (K, V)>,
}
impl<'a, K: Ord + Clone + Debug, V: Clone> Iterator for BTreeIterator<'a, K, V> {
    type Item = &'a (K, V);

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(pair) = self.leaf_iter.next() {
            return Some(pair);
        }
        while !self.idx_stack.is_empty()
            && !self.node_stack.is_empty()
            && (*self.idx_stack.last().unwrap() + 1)
                == self.node_stack.last().map(|n| n.children.len()).unwrap()
        {
            self.node_stack.pop();
            self.idx_stack.pop();
        }
        if self.node_stack.is_empty() {
            return None;
        }
        let last_idx = self.idx_stack.last_mut().unwrap();
        *last_idx += 1;
        let mut current = self
            .node_stack
            .last()
            .map(|n| &n.children[*last_idx])
            .unwrap();
        while !current.is_leaf() {
            let node = current.as_node();
            self.node_stack.push(node);
            current = &node.children[0];
            self.idx_stack.push(0);
        }
        self.leaf_iter = current.as_leaf().items.iter();
        self.leaf_iter.next()
    }
}

#[derive(Debug)]
enum BTreeError {
    KeyNotFound,
}

enum Child<K: Ord + Clone + Debug, V: Clone> {
    Node(Box<BTreeNode<K, V>>),
    Leaf(Box<BTreeLeaf<K, V>>),
}
impl<K: Ord + Clone + Debug, V: Clone> Child<K, V> {
    fn as_node(&self) -> &BTreeNode<K, V> {
        match self {
            Child::Node(node) => node,
            Child::Leaf(_) => unreachable!(),
        }
    }

    fn as_node_mut(&mut self) -> &mut BTreeNode<K, V> {
        match self {
            Child::Node(node) => node,
            Child::Leaf(_) => unreachable!(),
        }
    }

    fn as_leaf(&self) -> &BTreeLeaf<K, V> {
        match self {
            Child::Node(_) => unreachable!(),
            Child::Leaf(leaf) => leaf,
        }
    }

    fn as_leaf_mut(&mut self) -> &mut BTreeLeaf<K, V> {
        match self {
            Child::Node(_) => unreachable!(),
            Child::Leaf(leaf) => leaf,
        }
    }

    fn into_node(self) -> BTreeNode<K, V> {
        match self {
            Child::Node(node) => *node,
            Child::Leaf(_) => unreachable!(),
        }
    }

    fn into_leaf(self) -> BTreeLeaf<K, V> {
        match self {
            Child::Node(_) => unreachable!(),
            Child::Leaf(leaf) => *leaf,
        }
    }

    fn member_count(&self) -> usize {
        match self {
            Child::Node(node) => node.children.len(),
            Child::Leaf(leaf) => leaf.items.len(),
        }
    }

    fn is_leaf(&self) -> bool {
        matches!(self, Child::Leaf(_))
    }

    fn is_node(&self) -> bool {
        matches!(self, Child::Node(_))
    }
}

enum InsertionResult<K: Ord + Clone + Debug, V: Clone> {
    Done,
    Split { child: Child<K, V>, key: K },
}

struct BTreeNode<K: Ord + Clone + Debug, V: Clone> {
    keys: Vec<K>,
    children: Vec<Child<K, V>>,
    fanout_factor: usize,
}
impl<K: Ord + Clone + Debug, V: Clone> BTreeNode<K, V> {
    fn new(fanout_factor: usize) -> Self {
        BTreeNode {
            keys: Vec::with_capacity(fanout_factor - 1),
            children: Vec::with_capacity(fanout_factor),
            fanout_factor,
        }
    }

    /// Only for testing purposes, and then only for testing functionality that
    /// only depends on keys. Technically this constructs an invalid node. This
    /// is convenient for some tests though.
    #[cfg(test)]
    fn with_keys_only(keys: Vec<K>, fanout_factor: usize) -> Self {
        BTreeNode {
            keys,
            children: Vec::new(),
            fanout_factor,
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

    fn from_nodes(nodes: Vec<BTreeNode<K, V>>, keys: Vec<K>) -> Self {
        assert!(nodes.len() > 1);
        let fanout_factor = nodes[0].fanout_factor;
        let children = nodes
            .into_iter()
            .map(|node| Child::Node(Box::new(node)))
            .collect();

        BTreeNode {
            keys,
            children,
            fanout_factor,
        }
    }

    fn from_leaves(leaves: Vec<BTreeLeaf<K, V>>, keys: Vec<K>) -> Self {
        assert!(leaves.len() > 1);
        let fanout_factor = leaves[0].fanout_factor;
        let children = leaves
            .into_iter()
            .map(|leaf| Child::Leaf(Box::new(leaf)))
            .collect();

        BTreeNode {
            keys,
            children,
            fanout_factor,
        }
    }

    /// returns the newly created node, representing the right side of the split
    fn split(&mut self) -> (BTreeNode<K, V>, K) {
        let midpoint = self.keys.len() / 2;
        let new_keys = self.keys.split_off(midpoint);
        // the resulting left node will have no children to the right of the last key, so we can
        // remove it.
        let split_key = self.keys.pop().unwrap();
        let new_children = self.children.split_off(midpoint);
        (
            BTreeNode {
                keys: new_keys,
                children: new_children,
                fanout_factor: self.fanout_factor,
            },
            split_key,
        )
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
        if let InsertionResult::Split {
            child: new_child_node,
            key: split_key,
        } = insertion_res
        {
            if self.children.len() >= self.fanout_factor {
                let (mut new_node, new_split_key) = self.split();
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
                return InsertionResult::Split {
                    child: Child::Node(Box::new(new_node)),
                    key: new_split_key,
                };
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
                if left.keys.is_empty() {
                    let new_key = match left.children.last().unwrap() {
                        Child::Node(node) => node.keys.last().unwrap(),
                        Child::Leaf(leaf) => leaf.last_key().unwrap(),
                    };
                    left.keys.push(new_key.clone());
                }
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

    fn replace_with_child(&mut self) {
        assert_eq!(self.children.len(), 1);
        if let Child::Node(_) = &self.children[0] {
            let lone_child = self.children.pop().unwrap().into_node();
            self.keys = lone_child.keys;
            self.children = lone_child.children;
        }
    }

    fn remove(&mut self, key: &K) -> Result<(), BTreeError> {
        let position = match self.binary_search_for_key(key) {
            Ok(pos) => pos,
            Err(pos) => pos,
        };
        let child_node = &mut self.children[position];
        match child_node {
            Child::Node(child_node) => {
                child_node.remove(key)?;
            }
            Child::Leaf(leaf) => {
                leaf.remove(key)?;
            }
        };
        if child_node.member_count() == 0 {
            self.children.remove(position);
            self.keys.remove(position);
        } else if child_node.member_count() <= (self.fanout_factor / 3) {
            if position > 0
                && self.children[position - 1].member_count() <= (self.fanout_factor / 3)
            {
                self.merge_children(position - 1, position);
            } else if position < self.children.len() - 1
                && self.children[position + 1].member_count() <= (self.fanout_factor / 3)
            {
                self.merge_children(position, position + 1);
            };
        }
        if self.children.len() == 1 {
            self.replace_with_child();
        }
        Ok(())
    }

    fn binary_search_for_key(&self, key: &K) -> Result<usize, usize> {
        self.keys.binary_search(key)
    }
}

#[derive(Debug)]
struct BTreeLeaf<K: Ord + Clone + Debug, V: Clone> {
    items: Vec<(K, V)>,
    fanout_factor: usize,
}
impl<K: Ord + Clone + Debug, V: Clone> BTreeLeaf<K, V> {
    fn new(fanout_factor: usize) -> Self {
        BTreeLeaf {
            items: Vec::with_capacity(fanout_factor),
            fanout_factor,
        }
    }

    fn split(&mut self) -> (BTreeLeaf<K, V>, K) {
        let right_items = self.items.split_off(self.items.len() / 2);
        (
            BTreeLeaf {
                items: right_items,
                fanout_factor: self.fanout_factor,
            },
            self.last_key().unwrap().clone(),
        )
    }

    fn last_key(&self) -> Option<&K> {
        self.items.last().map(|x| &x.0)
    }

    fn insert(&mut self, key: K, value: V) -> InsertionResult<K, V> {
        match self.binary_search_for_key(&key) {
            Ok(pos) => self.items[pos].1 = value,
            Err(pos) => {
                if self.items.len() >= self.fanout_factor {
                    let (mut new_node, split_key) = self.split();
                    // resulting split key will be the last key in the left (not new) cell
                    if key > *self.last_key().unwrap() {
                        let _res = new_node.insert(key, value);
                        let typed_done: InsertionResult<K, V> = InsertionResult::Done;
                        assert!(matches!(typed_done, _res));
                        return InsertionResult::Split {
                            child: Child::Leaf(Box::new(new_node)),
                            key: split_key,
                        };
                    }
                }
                self.items.insert(pos, (key, value));
            }
        }
        InsertionResult::Done
    }

    fn get(&self, key: &K) -> Option<V> {
        match self.binary_search_for_key(key) {
            Ok(pos) => Some(self.items[pos].1.clone()),
            Err(_) => None,
        }
    }

    fn remove(&mut self, key: &K) -> Result<(), BTreeError> {
        let location = match self.binary_search_for_key(key) {
            Ok(pos) => pos,
            Err(_) => return Err(BTreeError::KeyNotFound),
        };
        self.items.remove(location);
        Ok(())
    }

    fn binary_search_for_key(&self, key: &K) -> Result<usize, usize> {
        self.items.binary_search_by_key(key, |item| item.0.clone())
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, fmt::Debug};

    use proptest::prelude::*;

    use super::{BTree, BTreeError, BTreeLeaf, BTreeNode, Child, InsertionResult};

    const FANOUT_FACTOR: usize = 5;

    #[test]
    fn leaf_binary_search() {
        let items = vec![(1, 10), (3, 30), (5, 50), (7, 70), (9, 90)];
        let leaf = BTreeLeaf {
            items,
            fanout_factor: 5,
        };

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
        let node: BTreeNode<i32, ()> = BTreeNode::with_keys_only(items, FANOUT_FACTOR);

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
        let mut leaf = BTreeLeaf::new(FANOUT_FACTOR);

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
            InsertionResult::Split {
                child: new_child,
                key: _,
            } => match new_child {
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
        let mut first_leaf = BTreeLeaf::new(FANOUT_FACTOR);
        for num in first_leaf_items[0..first_leaf_items.len() - 1].iter() {
            // all but last
            let _res = first_leaf.insert(*num, insertion_order);
            assert!(matches!(&insertion_done, _res));
            insertion_order += 1;
            keys_present.push(*num);
        }
        let (second_leaf, split_key) =
            match first_leaf.insert(*first_leaf_items.last().unwrap(), insertion_order) {
                InsertionResult::Split {
                    child: Child::Leaf(new_leaf),
                    key,
                } => (new_leaf, key),
                _ => panic!("Should have split and made a leaf"),
            };
        insertion_order += 1;
        keys_present.push(*first_leaf_items.last().unwrap());

        assert_eq!(first_leaf.last_key(), Some(&200));

        let mut node = BTreeNode::from_leaves(vec![first_leaf, *second_leaf], vec![split_key]);
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
            InsertionResult::Split {
                child: Child::Node(new_node),
                key: _,
            } => new_node,
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

    #[test]
    fn node_remove_and_merge() {
        let entries = [
            [(10, 10), (20, 20)],
            [(30, 30), (40, 40)],
            [(50, 50), (60, 60)],
        ];
        let leaves: Vec<_> = entries
            .iter()
            .map(|leaf_entries| {
                let mut leaf = BTreeLeaf::new(FANOUT_FACTOR);
                for e in leaf_entries {
                    leaf.insert(e.0, e.1);
                }
                leaf
            })
            .collect();
        let keys = leaves
            .iter()
            .map(|l| l.last_key().unwrap())
            .take(leaves.len() - 1)
            .cloned()
            .collect();
        let mut node = BTreeNode::from_leaves(leaves, keys);

        // test merge left
        node.remove(&40).unwrap();
        assert_eq!(node.children.len(), 3); // no merge yet
        node.remove(&10).unwrap();
        assert_eq!(node.children.len(), 2);
        assert_eq!(node.keys.len(), 1);
        assert_eq!(node.children[0].as_leaf().items, [(20, 20), (30, 30)]);

        // test merge right
        node.remove(&50).unwrap();
        assert_eq!(node.children.len(), 2); // no merge yet
        node.remove(&20).unwrap();
        assert_eq!(node.children.len(), 1);
        assert_eq!(node.keys.len(), 0);
        assert_eq!(node.children[0].as_leaf().items, [(30, 30), (60, 60)]);
    }

    fn leaf_keys<K: Ord + Copy + Debug, V: Clone>(leaf: &BTreeLeaf<K, V>) -> Vec<K> {
        leaf.items.iter().map(|x| x.0).collect()
    }

    fn keys_of_leaf_children<K: Ord + Copy + Debug, V: Clone>(
        node: &BTreeNode<K, V>,
    ) -> Vec<Vec<K>> {
        node.children
            .iter()
            .map(|child| leaf_keys(child.as_leaf()))
            .collect()
    }

    #[test]
    fn tree_iter_check() {
        let mut tree = BTree::new(FANOUT_FACTOR);
        let kv_pairs: Vec<(i32, i32)> = (0..50).map(|v| (v, v)).collect();
        for (k, v) in kv_pairs.iter() {
            tree.insert(*k, *v);
        }

        let collected_iter: Vec<(i32, i32)> = tree.iter().cloned().collect();
        assert_eq!(collected_iter, kv_pairs);
    }

    #[test]
    fn full_tree_splits_and_merges() {
        let mut tree = BTree::new(FANOUT_FACTOR);
        // fill up with enough to make root a node with 5 leaves
        // leaves end up like:
        // (10, 20), (30, 40), (50, 60), (70, 80), (90, 100, 110, 120, 130)

        for i in (10..=130).step_by(10) {
            tree.insert(i, i);
        }
        assert_eq!(
            keys_of_leaf_children(tree.root.as_ref().unwrap().as_node()),
            [
                vec![10, 20],
                vec![30, 40],
                vec![50, 60],
                vec![70, 80],
                vec![90, 100, 110, 120, 130]
            ]
        );
        assert_eq!(tree.root.as_ref().unwrap().as_node().keys, [20, 40, 60, 80]);

        // make a leaf split, causing the node to split
        tree.insert(140, 140);
        assert_eq!(tree.root.as_ref().unwrap().as_node().children.len(), 2);
        assert_eq!(
            keys_of_leaf_children(tree.root.as_ref().unwrap().as_node().children[0].as_node()),
            [vec![10, 20], vec![30, 40],]
        );
        assert_eq!(
            keys_of_leaf_children(tree.root.as_ref().unwrap().as_node().children[1].as_node()),
            [
                vec![50, 60],
                vec![70, 80],
                vec![90, 100],
                vec![110, 120, 130, 140]
            ]
        );
        assert_eq!(tree.root.as_ref().unwrap().as_node().keys, [40]);

        // remove enough keys to cause the nodes to merge
        // left side
        tree.remove(&10).unwrap();
        tree.remove(&20).unwrap();
        assert_eq!(
            keys_of_leaf_children(tree.root.as_ref().unwrap().as_node().children[0].as_node()),
            [vec![30, 40],]
        );
        assert_eq!(tree.root.as_ref().unwrap().as_node().keys, [40]);

        // right side
        tree.remove(&50).unwrap();
        tree.remove(&60).unwrap();
        tree.remove(&70).unwrap();
        tree.remove(&80).unwrap();
        tree.remove(&90).unwrap();
        assert_eq!(
            keys_of_leaf_children(tree.root.as_ref().unwrap().as_node().children[1].as_node()),
            [vec![100], vec![110, 120, 130, 140]]
        );
        assert_eq!(tree.root.as_ref().unwrap().as_node().keys, [40]);

        tree.remove(&100).unwrap();

        assert_eq!(tree.root.as_ref().unwrap().as_node().children.len(), 2);
        assert_eq!(
            keys_of_leaf_children(tree.root.as_ref().unwrap().as_node()),
            [vec![30, 40], vec![110, 120, 130, 140]]
        );
        assert_eq!(tree.root.as_ref().unwrap().as_node().keys, [40]);

        tree.remove(&40).unwrap();
        assert_eq!(
            keys_of_leaf_children(tree.root.as_ref().unwrap().as_node()),
            [vec![30], vec![110, 120, 130, 140]]
        );
        assert_eq!(tree.root.as_ref().unwrap().as_node().keys, [40]);

        tree.remove(&110).unwrap();
        tree.remove(&120).unwrap();
        assert_eq!(
            keys_of_leaf_children(tree.root.as_ref().unwrap().as_node()),
            [vec![30], vec![130, 140]]
        );
        assert_eq!(tree.root.as_ref().unwrap().as_node().keys, [40]);

        tree.remove(&130).unwrap();
        assert_eq!(
            leaf_keys(tree.root.as_ref().unwrap().as_leaf()),
            vec![30, 140]
        );
    }

    // add test for leaf showing that inserting an existing key updates the value

    fn kv_pairs(len: usize) -> impl Strategy<Value = Vec<(i32, i32)>> {
        prop::collection::vec((any::<i32>(), any::<i32>()), len)
    }

    prop_compose! {
        fn arbitrary_leaf_with_keys(size: usize)(fanout_factor in (size..1000), pairs in kv_pairs(size)) -> (BTreeLeaf<i32, i32>, Vec<i32>) {
            let mut leaf = BTreeLeaf::new(fanout_factor);
            for (key, value) in pairs.iter() {
                leaf.insert(*key, *value);
            }
            let keys = pairs.into_iter().map(|p| p.0).collect();
            (leaf, keys)
        }
    }

    fn leaf_keys_are_ordered(leaf: &BTreeLeaf<i32, i32>) -> bool {
        let keys: Vec<_> = leaf.items.iter().map(|i| i.0).collect();
        let mut sorted_keys = keys.clone();
        sorted_keys.sort();
        sorted_keys == keys
    }

    proptest! {
        #[test]
        fn leaf_insert(pairs in kv_pairs(10)) {
            let mut leaf = BTreeLeaf::new(10);
            for (key, val) in pairs.iter() {
                leaf.insert(*key, *val);
                assert!(leaf_keys_are_ordered(&leaf));
                assert_eq!(leaf.get(key), Some(*val));
            }
        }

        #[test]
        fn leaf_deletion((mut leaf, keys) in arbitrary_leaf_with_keys(20)) {
            for k in keys {
                leaf.remove(&k).unwrap();
                assert!(leaf_keys_are_ordered(&leaf));
                assert!(leaf.get(&k).is_none());
            }
        }
    }

    #[derive(Debug)]
    enum TreeOperation {
        Insert(i32, i32),
        Remove(i32),
        Get(i32),
    }

    fn tree_operation() -> impl Strategy<Value = TreeOperation> {
        prop_oneof![
            (any::<i32>(), any::<i32>()).prop_map(|(k, v)| TreeOperation::Insert(k, v)),
            any::<i32>().prop_map(TreeOperation::Remove),
            any::<i32>().prop_map(TreeOperation::Get),
        ]
    }

    fn tree_operations(count: usize) -> impl Strategy<Value = Vec<TreeOperation>> {
        prop::collection::vec(tree_operation(), count)
    }

    fn tree_keys_fully_ordered(tree: &BTree<i32, i32>) -> bool {
        let keys: Vec<_> = tree.iter().collect();
        let mut sorted_keys = keys.clone();
        sorted_keys.sort();
        keys == sorted_keys
    }

    fn all_node_keys_ordered(root: &Child<i32, i32>) -> bool {
        match root {
            Child::Leaf(leaf) => {
                let keys: Vec<_> = leaf.items.iter().map(|x| x.0).collect();
                let mut sorted_keys = keys.clone();
                sorted_keys.sort();
                keys == sorted_keys
            }
            Child::Node(node) => {
                let mut sorted_keys = node.keys.clone();
                sorted_keys.sort();
                sorted_keys == node.keys && node.children.iter().all(all_node_keys_ordered)
            }
        }
    }

    fn all_keys_in_range(node: &Child<i32, i32>, min: i32, max: i32) -> bool {
        let keys: Vec<_> = match node {
            Child::Leaf(leaf) => leaf.items.iter().map(|x| x.0).collect(),
            Child::Node(node) => node.keys.clone(),
        };
        keys.iter().all(|k| (min..=max).contains(k))
    }

    fn all_subnode_keys_ordered_relative_to_node_keys(root: &Child<i32, i32>) -> bool {
        if root.is_leaf() {
            return true;
        }
        let node = root.as_node();
        let mut min_key = i32::MIN;
        for (idx, k) in node.keys.iter().enumerate() {
            let max_key = *k;
            if !all_keys_in_range(&node.children[idx], min_key, max_key) {
                return false;
            }
            min_key = k + 1;
        }
        all_keys_in_range(&node.children[node.keys.len()], min_key, i32::MAX)
    }

    fn all_inserted_values_are_retrievable(
        tree: &BTree<i32, i32>,
        kv_lookup: &HashMap<i32, i32>,
    ) -> bool {
        kv_lookup.iter().all(|(k, v)| tree.get(k) == Some(*v))
    }

    fn all_nodes_with_fanout_factor(root: &Child<i32, i32>) -> bool {
        match root {
            Child::Leaf(leaf) => leaf.items.len() <= leaf.fanout_factor,
            Child::Node(node) => {
                node.children.len() <= node.fanout_factor
                    && node.children.iter().all(all_nodes_with_fanout_factor)
            }
        }
    }

    fn no_empty_nodes(tree: &BTree<i32, i32>, root: &Child<i32, i32>) -> bool {
        match root {
            Child::Leaf(leaf) => {
                !leaf.items.is_empty() || tree.root.as_ref().unwrap().member_count() == 0
            }
            Child::Node(node) => {
                !node.children.is_empty()
                    && node
                        .children
                        .iter()
                        .all(|child| no_empty_nodes(tree, child))
            }
        }
    }

    fn no_mergeable_nodes(root: &Child<i32, i32>) -> bool {
        match root {
            Child::Leaf(_) => true,
            Child::Node(node) => {
                for i in 1..node.children.len() - 1 {
                    if node.children[i].member_count() <= node.fanout_factor / 3
                        && (node.children[i - 1].member_count() <= node.fanout_factor / 3
                            || node.children[i + 1].member_count() <= node.fanout_factor / 3)
                    {
                        return false;
                    }
                }
                node.children.iter().all(no_mergeable_nodes)
            }
        }
    }

    //#[test]
    fn failing_check() {
        use TreeOperation::*;

        let ops = [
            Insert(0, 0),
            Insert(0, 0),
            Insert(0, 0),
            Insert(0, 0),
            Insert(0, 0),
            Insert(0, 0),
            Insert(0, 0),
            Insert(0, 0),
            Insert(0, 0),
            Insert(0, 0),
            Insert(0, 0),
            Insert(0, 0),
            Insert(0, 0),
            Insert(0, 0),
            Insert(0, 0),
            Insert(0, 0),
            Insert(0, 0),
            Insert(0, 0),
            Insert(0, 0),
            Insert(0, 0),
            Insert(0, 0),
            Insert(0, 0),
            Insert(0, 0),
            Insert(0, 0),
            Insert(0, 0),
            Insert(0, 0),
            Insert(0, 0),
            Insert(0, 0),
            Insert(0, 0),
            Insert(0, 0),
            Insert(0, 0),
            Insert(0, 0),
            Insert(0, 0),
            Insert(0, 0),
            Insert(0, 0),
            Insert(0, 0),
            Insert(0, 0),
            Insert(0, 0),
            Insert(0, 0),
            Insert(0, 0),
            Insert(0, 0),
            Insert(0, 0),
            Insert(0, 0),
            Insert(0, 0),
            Insert(0, 0),
            Insert(0, 0),
            Insert(0, 0),
            Insert(0, 0),
            Insert(0, 0),
            Insert(0, 0),
            Insert(0, 0),
            Insert(0, 0),
            Insert(0, 0),
            Insert(0, 0),
            Insert(0, 0),
            Insert(0, 0),
            Insert(0, 0),
            Insert(0, 0),
            Insert(-1, 0),
            Insert(0, 0),
            Insert(-359134783, 0),
            Insert(1, 0),
            Insert(0, 0),
            Insert(0, 0),
            Insert(0, 0),
            Insert(0, 0),
            Insert(0, 0),
            Insert(0, 0),
            Insert(0, 0),
            Insert(-359134784, 0),
            Insert(128243536, 0),
            Remove(0),
            Insert(0, 0),
            Insert(128243536, 0),
            Insert(2, 0),
            Insert(0, 0),
        ];
        let tree = proccess_ops(5, &ops);
        let root = &tree.root.unwrap();
        println!("{:?}", root.as_node().keys);
        println!("<-: {:?}", root.as_node().children[0].as_leaf().items);
        println!("->: {:?}", root.as_node().children[1].as_leaf().items);
    }

    fn proccess_ops(fanout_factor: usize, ops: &[TreeOperation]) -> BTree<i32, i32> {
        let mut tree: BTree<i32, i32> = BTree::new(fanout_factor);
        let mut kv_lookup = HashMap::new();
        for op in ops {
            match op {
                TreeOperation::Get(k) => {
                    let res = tree.get(k);
                    let expected = kv_lookup.get(k).copied();
                    assert_eq!(res, expected);
                }
                TreeOperation::Insert(k, v) => {
                    kv_lookup.insert(*k, *v);
                    tree.insert(*k, *v);
                }
                TreeOperation::Remove(k) => {
                    kv_lookup.remove(k);
                    /*
                     * Here, I'm matching tree.remove exhaustively, because if we add other
                     * error types to BTreeError in the future, I want this to fail to compile.
                     * Only these two states are expected. Anything else is a bug.
                     */
                    match tree.remove(k) {
                        Ok(()) => (),
                        Err(BTreeError::KeyNotFound) => (),
                    }
                }
            }
            assert!(tree_keys_fully_ordered(&tree));
            assert!(all_inserted_values_are_retrievable(&tree, &kv_lookup));
            let root = tree.root.as_ref().unwrap();
            assert!(all_node_keys_ordered(root));
            assert!(all_subnode_keys_ordered_relative_to_node_keys(root));
            assert!(all_nodes_with_fanout_factor(root));
            assert!(no_empty_nodes(&tree, root));
            assert!(no_mergeable_nodes(root));
        }

        tree
    }

    proptest! {
        #![proptest_config(ProptestConfig {
            max_shrink_iters: 8192, .. ProptestConfig::default()
        })]
        #[test]
        fn full_tree_test(fanout_factor in (5usize..100), ops in tree_operations(100)) {
            let _ = proccess_ops(fanout_factor, &ops);
        }
    }
}
