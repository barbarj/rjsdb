#![allow(dead_code)]

use std::{fmt::Debug, iter::Zip, mem, slice::Iter};

pub struct BTree<K: Ord + Clone + Debug, V: Clone> {
    root: Node<K, V>,
}
impl<K: Ord + Clone + Debug, V: Clone> BTree<K, V> {
    pub fn new(fanout_factor: usize) -> Self {
        BTree {
            root: Node::new(fanout_factor),
        }
    }

    pub fn insert(&mut self, key: K, value: V) {
        let insert_res = self.root.insert(key, value);
        if let InsertionResult::Split(new_node, split_key) = insert_res {
            let fanout_factor = self.root.fanout_factor;
            let old_root = mem::replace(&mut self.root, Node::new(fanout_factor));
            self.root.keys.push(split_key);
            self.root.children.push(old_root);
            self.root.children.push(new_node);
        }
    }

    pub fn get(&self, key: &K) -> Option<V> {
        self.root.get(key)
    }

    pub fn remove(&mut self, key: &K) -> Option<V> {
        self.root.remove(key)
    }

    pub fn iter(&self) -> BTreeIter<K, V> {
        BTreeIter::new(self)
    }
}

pub struct BTreeIter<'a, K: Ord + Clone + Debug, V: Clone> {
    stack: Vec<(&'a Node<K, V>, usize)>,
    leaf_iter: Zip<Iter<'a, K>, Iter<'a, V>>,
}
impl<'a, K: Ord + Clone + Debug, V: Clone> BTreeIter<'a, K, V> {
    fn new(tree: &'a BTree<K, V>) -> Self {
        let mut stack = Vec::new();

        let mut node = &tree.root;
        while !node.is_leaf() {
            assert!(!node.children.is_empty());
            let next = &node.children[0];
            stack.push((node, 0));
            node = next;
        }
        let leaf_iter = node.keys.iter().zip(node.values.iter());

        BTreeIter { stack, leaf_iter }
    }

    fn advance_to_next_leaf(&mut self) {
        if self.stack.is_empty() {
            return;
        }
        let (mut node, mut idx) = self.stack.pop().unwrap();
        while idx + 1 == node.children.len() && !self.stack.is_empty() {
            (node, idx) = self.stack.pop().unwrap();
        }
        if idx + 1 == node.children.len() {
            return;
        }
        let child = &node.children[idx + 1];
        self.stack.push((node, idx + 1));
        node = child;
        while !node.is_leaf() {
            let child = &node.children[0];
            self.stack.push((node, 0));
            node = child;
        }
        self.leaf_iter = node.keys.iter().zip(node.values.iter());
    }
}
impl<'a, K: Ord + Clone + Debug, V: Clone> Iterator for BTreeIter<'a, K, V> {
    type Item = (&'a K, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(pair) = self.leaf_iter.next() {
            return Some(pair);
        }
        self.advance_to_next_leaf();
        self.leaf_iter.next()
    }
}

pub struct Node<K: Ord + Clone + Debug, V: Clone> {
    keys: Vec<K>,
    children: Vec<Node<K, V>>,
    values: Vec<V>,
    fanout_factor: usize,
}
impl<K: Ord + Clone + Debug, V: Clone> Node<K, V> {
    pub fn new(fanout_factor: usize) -> Self {
        Node {
            keys: Vec::with_capacity(fanout_factor),
            children: Vec::new(),
            values: Vec::new(),
            fanout_factor,
        }
    }

    fn is_leaf(&self) -> bool {
        let res = self.children.is_empty();
        assert!(self.keys.is_empty() || self.values.is_empty() != res);
        res
    }

    // returns the new node, and the key to be brough up
    fn split(&mut self) -> (Node<K, V>, K) {
        let split_at = self.fanout_factor / 2;
        let new_keys = self.keys.split_off(split_at);
        let split_key: K = if self.is_leaf() {
            self.keys.last().unwrap().clone()
        } else {
            // on a node, keys split a keyspace. After splitting, the last key has
            // nothing right of it, so we pop it off
            self.keys.pop().unwrap()
        };
        let new_values = if self.is_leaf() {
            self.values.split_off(split_at)
        } else {
            Vec::new()
        };
        let new_children = if !self.is_leaf() {
            self.children.split_off(split_at)
        } else {
            Vec::new()
        };
        let node = Node {
            keys: new_keys,
            children: new_children,
            values: new_values,
            fanout_factor: self.fanout_factor,
        };
        (node, split_key)
    }

    /// Returns the newly-created node, which will contain values from the right-side of the split
    fn split_and_insert_as_leaf(&mut self, key: K, value: V) -> (Node<K, V>, K) {
        let (mut new_node, split_key) = self.split();
        if key > split_key {
            let insert_pos = match new_node.keys.binary_search(&key) {
                Ok(pos) => pos,
                Err(pos) => pos,
            };
            new_node.keys.insert(insert_pos, key);
            new_node.values.insert(insert_pos, value);
        } else {
            let insert_pos = match self.keys.binary_search(&key) {
                Ok(pos) => pos,
                Err(pos) => pos,
            };
            self.keys.insert(insert_pos, key);
            self.values.insert(insert_pos, value);
        }

        (new_node, split_key)
    }

    /// Returns the newly-created node, which will contain values from the right-side of the split
    ///
    /// The new node is inserted at insert_pos + because the key is to the left of it
    fn split_and_insert_as_node(&mut self, key: K, node: Node<K, V>) -> (Node<K, V>, K) {
        let (mut new_node, split_key) = self.split();
        if key > split_key {
            let insert_pos = match new_node.keys.binary_search(&key) {
                Ok(pos) => pos,
                Err(pos) => pos,
            };
            new_node.keys.insert(insert_pos, key);
            new_node.children.insert(insert_pos + 1, node);
        } else {
            let insert_pos = match self.keys.binary_search(&key) {
                Ok(pos) => pos,
                Err(pos) => pos,
            };
            self.keys.insert(insert_pos, key);
            self.children.insert(insert_pos + 1, node);
        }

        (new_node, split_key)
    }

    fn is_full(&self) -> bool {
        self.keys.len() == self.fanout_factor
    }

    fn insert_as_leaf(&mut self, key: K, value: V) -> InsertionResult<K, V> {
        assert!(self.is_leaf());
        match self.keys.binary_search(&key) {
            Ok(pos) => {
                self.values[pos] = value;
                InsertionResult::Done
            }
            Err(pos) => {
                if self.is_full() {
                    let (new_node, split_key) = self.split_and_insert_as_leaf(key, value);
                    InsertionResult::Split(new_node, split_key)
                } else {
                    self.keys.insert(pos, key);
                    self.values.insert(pos, value);
                    InsertionResult::Done
                }
            }
        }
    }

    fn insert_as_node(&mut self, key: K, value: V) -> InsertionResult<K, V> {
        assert!(!self.is_leaf());
        let pos = match self.keys.binary_search(&key) {
            Ok(pos) => pos,
            Err(pos) => pos,
        };
        if let InsertionResult::Split(new_node, split_key) = self.children[pos].insert(key, value) {
            if self.is_full() {
                // Otherwise, if this node is full, split and insert the new child node.
                let (new_new_node, new_split_key) =
                    self.split_and_insert_as_node(split_key, new_node);
                InsertionResult::Split(new_new_node, new_split_key)
            } else {
                // Otherwise just insert the new child node
                self.keys.insert(pos, split_key);
                self.children.insert(pos + 1, new_node);
                InsertionResult::Done
            }
        } else {
            InsertionResult::Done
        }
    }

    pub fn insert(&mut self, key: K, value: V) -> InsertionResult<K, V> {
        if self.is_leaf() {
            self.insert_as_leaf(key, value)
        } else {
            self.insert_as_node(key, value)
        }
    }

    pub fn get(&self, key: &K) -> Option<V> {
        if self.is_leaf() {
            self.keys
                .binary_search(key)
                .ok()
                .and_then(|pos| self.values.get(pos))
                .cloned()
        } else {
            let pos = match self.keys.binary_search(key) {
                Ok(pos) => pos,
                Err(pos) => pos,
            };
            self.children[pos].get(key)
        }
    }

    fn remove_as_leaf(&mut self, key: &K) -> Option<V> {
        match self.keys.binary_search(key) {
            Ok(pos) => {
                self.keys.remove(pos);
                let res = self.values.remove(pos);
                Some(res)
            }
            Err(_) => None,
        }
    }

    fn replace_with_only_child(&mut self) {
        assert_eq!(self.children.len(), 1);
        let child = self.children.pop().unwrap();
        assert_eq!(self.fanout_factor, child.fanout_factor);
        self.keys = child.keys;
        self.children = child.children;
        self.values = child.values;
    }

    fn is_empty(&self) -> bool {
        if self.is_leaf() {
            self.values.is_empty()
        } else {
            self.children.is_empty()
        }
    }

    fn is_below_minimum_size(&self) -> bool {
        assert!(!self.is_leaf());
        self.keys.len() < self.fanout_factor / 3
    }

    // we drop the boundary key because it becomes unecessary
    fn steal_for_child_from_child(&mut self, for_pos: usize, from_pos: usize) {
        assert_ne!(for_pos, from_pos);
        assert!(for_pos < self.children.len());
        assert!(from_pos < self.children.len());
        let len = self.children[from_pos].keys.len();
        let (split_idx, new_keys, new_children) = if for_pos > from_pos {
            // steal right half
            let split_idx = len / 2;
            let key_range = split_idx + 1..len;
            let child_range = split_idx + 1..len;

            let mut new_keys = Vec::with_capacity(self.fanout_factor);
            new_keys.extend(self.children[from_pos].keys.drain(key_range));
            new_keys.append(&mut self.children[for_pos].keys);

            let mut new_children = Vec::with_capacity(self.fanout_factor);
            new_children.extend(self.children[from_pos].children.drain(child_range));
            new_children.append(&mut self.children[for_pos].children);

            (split_idx, new_keys, new_children)
        } else {
            // steal left half
            let split_idx = len / 2;
            let key_range = 0..split_idx;
            let child_range = 0..split_idx + 1;

            // TODO: Once bugs fixed, improve this by removing the unecessary recreation of the
            // existing vec

            let mut new_keys = Vec::with_capacity(self.fanout_factor);
            new_keys.append(&mut self.children[for_pos].keys);
            new_keys.extend(self.children[from_pos].keys.drain(key_range));

            let mut new_children = Vec::with_capacity(self.fanout_factor);
            new_children.append(&mut self.children[for_pos].children);
            new_children.extend(self.children[from_pos].children.drain(child_range));

            (0, new_keys, new_children)
        };

        self.children[for_pos].keys = new_keys;
        self.children[for_pos].children = new_children;
        // drop key that now no longer splits any search space (as it's missing a side)
        self.children[from_pos].keys.remove(split_idx);
        if self.children[from_pos].keys.is_empty() {
            self.children.remove(from_pos);
        }
    }

    fn remove_as_node(&mut self, key: &K) -> Option<V> {
        let pos = match self.keys.binary_search(key) {
            Ok(pos) => pos,
            Err(pos) => pos,
        };
        let res = self.children[pos].remove(key);
        if self.children[pos].is_empty() {
            self.children.remove(pos);
            if pos < self.keys.len() {
                self.keys.remove(pos);
            } else {
                self.keys.pop();
            }
        } else if !self.children[pos].is_leaf() && self.children[pos].is_below_minimum_size() {
            // steal from the larger sibling
            let left_size = if pos > 0 {
                self.children[pos - 1].keys.len()
            } else {
                0
            };
            let right_size = if pos < self.keys.len() {
                self.children[pos + 1].keys.len()
            } else {
                0
            };
            let steal_from_pos = if left_size > right_size {
                pos - 1
            } else {
                pos + 1
            };
            self.steal_for_child_from_child(pos, steal_from_pos);
        }
        if self.children.len() == 1 && !self.children[0].is_leaf() {
            self.replace_with_only_child();
        }
        res
    }

    pub fn remove(&mut self, key: &K) -> Option<V> {
        if self.is_leaf() {
            self.remove_as_leaf(key)
        } else {
            self.remove_as_node(key)
        }
    }

    pub fn last_key(&self) -> K {
        assert!(!self.keys.is_empty());
        self.keys.last().unwrap().clone()
    }

    fn member_count(&self) -> usize {
        self.keys.len()
    }
}

pub enum InsertionResult<K: Ord + Clone + Debug, V: Clone> {
    Done,
    Split(Node<K, V>, K),
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, VecDeque};

    use proptest::prelude::*;
    use proptest_state_machine::{prop_state_machine, ReferenceStateMachine, StateMachineTest};

    use super::{BTree, Node};

    // TODO: Figure out how to stick an extra seperator between grandchildren
    fn display_tree(tree: &BTree<u32, u32>) {
        let mut queue = VecDeque::new();
        queue.push_back((vec![0], &tree.root));
        while let Some((ancestry, node)) = queue.pop_front() {
            let path_parts: Vec<_> = ancestry.iter().map(|x| x.to_string()).collect();
            let path = path_parts.join("->");
            if node.is_leaf() {
                println!("{path}: L{:?}", node.keys);
            } else {
                println!("{path}: {:?}", node.keys);
            }
            queue.extend(node.children.iter().enumerate().map(|(idx, node)| {
                let mut child_ancestry = ancestry.clone();
                child_ancestry.push(idx);
                (child_ancestry, node)
            }));
        }
    }

    #[derive(Debug, Clone)]
    pub struct ReferenceBTree {
        ref_tree: BTreeMap<u32, u32>,
        fanout_factor: usize,
    }
    impl ReferenceStateMachine for ReferenceBTree {
        type State = Self;
        type Transition = TreeOperation;

        fn init_state() -> BoxedStrategy<Self::State> {
            (5usize..50)
                .prop_map(|x| ReferenceBTree {
                    ref_tree: BTreeMap::new(),
                    fanout_factor: x,
                })
                .boxed()
        }

        fn transitions(state: &Self::State) -> BoxedStrategy<Self::Transition> {
            if !state.ref_tree.is_empty() {
                let keys: Vec<_> = state.ref_tree.keys().cloned().collect();
                let removal_key = proptest::sample::select(keys);
                prop_oneof![
                    (any::<u32>(), any::<u32>()).prop_map(|(k, v)| TreeOperation::Insert(k, v)),
                    removal_key.prop_map(TreeOperation::Remove)
                ]
                .boxed()
            } else {
                (any::<u32>(), any::<u32>())
                    .prop_map(|(k, v)| TreeOperation::Insert(k, v))
                    .boxed()
            }
        }

        fn apply(mut state: Self::State, transition: &Self::Transition) -> Self::State {
            match transition {
                TreeOperation::Insert(k, v) => state.ref_tree.insert(*k, *v),
                TreeOperation::Remove(k) => state.ref_tree.remove(k),
            };
            state
        }

        fn preconditions(state: &Self::State, transition: &Self::Transition) -> bool {
            match transition {
                TreeOperation::Insert(_, _) => true,
                TreeOperation::Remove(k) => state.ref_tree.contains_key(k),
            }
        }
    }

    impl StateMachineTest for BTree<u32, u32> {
        type SystemUnderTest = Self;
        type Reference = ReferenceBTree;

        fn init_test(
            ref_state: &<Self::Reference as ReferenceStateMachine>::State,
        ) -> Self::SystemUnderTest {
            Self::new(ref_state.fanout_factor)
        }

        fn apply(
            mut state: Self::SystemUnderTest,
            _ref_state: &<Self::Reference as ReferenceStateMachine>::State,
            transition: <Self::Reference as ReferenceStateMachine>::Transition,
        ) -> Self::SystemUnderTest {
            match transition {
                TreeOperation::Remove(k) => {
                    let res = state.remove(&k);
                    assert!(res.is_some());
                    assert!(state.get(&k).is_none());
                }
                TreeOperation::Insert(k, v) => {
                    state.insert(k, v);
                    assert_eq!(state.get(&k), Some(v));
                }
            };
            display_tree(&state);
            state
        }

        fn check_invariants(
            state: &Self::SystemUnderTest,
            ref_state: &<Self::Reference as ReferenceStateMachine>::State,
        ) {
            assert!(tree_keys_fully_ordered(state));
            assert_eq!(
                first_nonretrievable_inserted_value(state, &ref_state.ref_tree),
                None
            );
            assert!(all_nodes_properly_structured(&state.root));
            assert!(all_node_keys_ordered(&state.root));
            assert!(all_subnode_keys_ordered_relative_to_node_keys(&state.root));
            assert!(all_nodes_with_fanout_factor(
                &state.root,
                ref_state.fanout_factor
            ));

            assert!(all_nodes_sized_correctly(&state.root));
            assert!(root_is_sized_correctly(&state.root));
            assert!(all_leaves_same_level(&state.root));
        }
    }

    #[derive(Debug, Clone)]
    pub enum TreeOperation {
        Insert(u32, u32),
        Remove(u32),
    }

    fn all_leaves_same_level(root: &Node<u32, u32>) -> bool {
        fn leaf_levels(node: &Node<u32, u32>, level: usize) -> Vec<usize> {
            if node.is_leaf() {
                return vec![level];
            }
            node.children
                .iter()
                .flat_map(|c| leaf_levels(c, level + 1))
                .collect()
        }

        let mut levels = leaf_levels(root, 0).into_iter();
        let first = levels.next().unwrap();
        levels.all(|x| x == first)
    }

    fn root_is_sized_correctly(root: &Node<u32, u32>) -> bool {
        root.is_leaf() || root.children.len() > 1
    }

    fn all_nodes_sized_correctly(root: &Node<u32, u32>) -> bool {
        fn all_nodes_sized_correctly_not_root(node: &Node<u32, u32>) -> bool {
            if node.is_leaf() {
                return true;
            }
            node.keys.len() >= node.fanout_factor / 3
                && node.children.iter().all(all_nodes_sized_correctly_not_root)
        }

        root.children.iter().all(all_nodes_sized_correctly)
    }

    fn all_nodes_properly_structured(node: &Node<u32, u32>) -> bool {
        if node.is_leaf() {
            node.keys.len() == node.values.len()
        } else {
            node.keys.len() == node.children.len() - 1
        }
    }

    fn tree_keys_fully_ordered(tree: &BTree<u32, u32>) -> bool {
        let keys: Vec<_> = tree.iter().collect();
        let mut sorted_keys = keys.clone();
        sorted_keys.sort();
        keys == sorted_keys
    }

    fn all_node_keys_ordered(node: &Node<u32, u32>) -> bool {
        let mut sorted_keys = node.keys.clone();
        sorted_keys.sort();
        sorted_keys == node.keys && node.children.iter().all(all_node_keys_ordered)
    }

    fn all_keys_in_range(node: &Node<u32, u32>, min: u32, max: u32) -> bool {
        node.keys.iter().all(|k| (min..=max).contains(k))
    }

    fn all_subnode_keys_ordered_relative_to_node_keys(node: &Node<u32, u32>) -> bool {
        if node.is_leaf() {
            return true;
        }
        let mut min_key = u32::MIN;
        for (idx, k) in node.keys.iter().enumerate() {
            let max_key = *k;
            if !all_keys_in_range(&node.children[idx], min_key, max_key) {
                return false;
            }
            min_key = k + 1;
        }
        all_keys_in_range(&node.children[node.keys.len()], min_key, u32::MAX)
    }

    fn first_nonretrievable_inserted_value(
        tree: &BTree<u32, u32>,
        ref_tree: &BTreeMap<u32, u32>,
    ) -> Option<u32> {
        ref_tree
            .iter()
            .find(|(k, v)| tree.get(k) != Some(**v))
            .map(|(k, v)| {
                println!("didn't find: ({k}, {v})");
                println!("actual value: {:?}", tree.get(k));
                *k
            })
    }

    fn all_nodes_with_fanout_factor(node: &Node<u32, u32>, fanout_factor: usize) -> bool {
        node.member_count() <= node.fanout_factor
            && node.fanout_factor == fanout_factor
            && node
                .children
                .iter()
                .all(|child| all_nodes_with_fanout_factor(child, fanout_factor))
    }

    prop_state_machine! {
       #![proptest_config(ProptestConfig {
            // Enable verbose mode to make the state machine test print the
            // transitions for each case.
            verbose: 1,
            max_shrink_iters: 8192,
            .. ProptestConfig::default()
        })]

        #[test]
        fn full_tree_test(sequential 1..500 => BTree<u32, u32>);
    }
}
