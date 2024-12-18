#![allow(dead_code)]

use std::fmt::Debug;

#[cfg(not(test))]
const FANOUT_FACTOR: usize = 512;
#[cfg(test)]
const FANOUT_FACTOR: usize = 5;

struct BTree<K: Ord + Clone + Debug, V: Clone> {
    root: Option<Child<K, V>>,
}
impl<K: Ord + Clone + Debug, V: Clone> BTree<K, V> {
    fn new() -> Self {
        BTree {
            root: Some(Child::Leaf(Box::new(BTreeLeaf::new()))),
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
}

enum InsertionResult<K: Ord + Clone + Debug, V: Clone> {
    Done,
    Split { child: Child<K, V>, key: K },
}

struct BTreeNode<K: Ord + Clone + Debug, V: Clone> {
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

    fn from_nodes(nodes: Vec<BTreeNode<K, V>>, keys: Vec<K>) -> Self {
        assert!(nodes.len() > 1);
        let children = nodes
            .into_iter()
            .map(|node| Child::Node(Box::new(node)))
            .collect();

        BTreeNode { keys, children }
    }

    fn from_leaves(leaves: Vec<BTreeLeaf<K, V>>, keys: Vec<K>) -> Self {
        assert!(leaves.len() > 1);
        let children = leaves
            .into_iter()
            .map(|leaf| Child::Leaf(Box::new(leaf)))
            .collect();

        BTreeNode { keys, children }
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
            if self.children.len() >= FANOUT_FACTOR {
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
        } else if child_node.member_count() <= (FANOUT_FACTOR / 3) {
            if position > 0 && self.children[position - 1].member_count() <= (FANOUT_FACTOR / 3) {
                self.merge_children(position - 1, position);
            } else if position < self.children.len() - 1
                && self.children[position + 1].member_count() <= (FANOUT_FACTOR / 3)
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

struct BTreeLeaf<K: Ord + Clone + Debug, V: Clone> {
    items: Vec<(K, V)>,
}
impl<K: Ord + Clone + Debug, V: Clone> BTreeLeaf<K, V> {
    fn new() -> Self {
        BTreeLeaf {
            items: Vec::with_capacity(FANOUT_FACTOR),
        }
    }

    fn split(&mut self) -> (BTreeLeaf<K, V>, K) {
        let right_items = self.items.split_off(self.items.len() / 2);
        (
            BTreeLeaf { items: right_items },
            self.last_key().unwrap().clone(),
        )
    }

    fn last_key(&self) -> Option<&K> {
        self.items.last().map(|x| &x.0)
    }

    fn insert(&mut self, key: K, value: V) -> InsertionResult<K, V> {
        // TODO: Requirements about item size
        if self.items.len() >= FANOUT_FACTOR {
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
    use std::fmt::Debug;

    use super::{BTree, BTreeLeaf, BTreeNode, Child, InsertionResult, FANOUT_FACTOR};

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
        let mut first_leaf = BTreeLeaf::new();
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
                let mut leaf = BTreeLeaf::new();
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
    fn full_tree_splits_and_merges() {
        let mut tree = BTree::new();
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
}
