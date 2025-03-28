#![allow(dead_code)]

use std::{fmt::Debug, mem};

pub enum InsertResult<K: Ord + Clone + Debug, V: Clone> {
    Split(K, Node<K, V>),
    Done,
}

pub struct BTree<K: Ord + Clone + Debug, V: Clone> {
    root: Node<K, V>,
}
impl<K: Ord + Clone + Debug, V: Clone> BTree<K, V> {
    pub fn new(fanout_factor: usize) -> Self {
        let root = Node::new(fanout_factor);
        BTree { root }
    }

    pub fn insert(&mut self, key: K, value: V) {
        let insert_res = self.root.insert(key, value);
        if let InsertResult::Split(split_key, new_node) = insert_res {
            let fanout_factor = self.root.fanout_factor;
            let old_root = mem::replace(&mut self.root, Node::new(fanout_factor));
            self.root.keys.push(split_key);
            self.root.children.extend([old_root, new_node]);
        }
    }

    pub fn get(&self, key: &K) -> Option<&V> {
        self.root.get(key)
    }

    pub fn remove(&mut self, key: &K) -> Option<V> {
        self.root.remove(key)
    }

    pub fn iter(&self) -> BTreeIterator<K, V> {
        BTreeIterator::new(self)
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

    fn member_count(&self) -> usize {
        self.keys.len()
    }

    fn is_full(&self) -> bool {
        self.member_count() == self.fanout_factor
    }

    fn is_leaf(&self) -> bool {
        let res = self.children.is_empty();
        assert!(self.keys.is_empty() || self.values.is_empty() != res);
        res
    }

    fn is_node(&self) -> bool {
        !self.is_leaf()
    }

    fn split_as_leaf(&mut self) -> (K, Node<K, V>) {
        let half = self.fanout_factor / 2;
        let new_node = Node {
            keys: self.keys.drain(half..).collect(),
            children: Vec::new(),
            values: self.values.drain(half..).collect(),
            fanout_factor: self.fanout_factor,
        };
        let split_key = self.keys.last().unwrap().clone();
        (split_key, new_node)
    }

    fn split_as_node(&mut self) -> (K, Node<K, V>) {
        let half = self.fanout_factor / 2;
        let new_node = Node {
            keys: self.keys.drain(half + 1..).collect(),
            children: self.children.drain(half + 1..).collect(),
            values: Vec::new(),
            fanout_factor: self.fanout_factor,
        };
        let split_key = self.keys.pop().unwrap();
        (split_key, new_node)
    }

    fn insert_as_leaf(&mut self, key: K, value: V) -> InsertResult<K, V> {
        assert!(self.is_leaf());
        if self.is_full() {
            let (split_key, mut new_node) = self.split_as_leaf();
            assert!(new_node.is_leaf());
            if key > split_key {
                new_node.insert_as_leaf(key, value);
            } else {
                self.insert_as_leaf(key, value);
            }
            InsertResult::Split(split_key, new_node)
        } else {
            match self.keys.binary_search(&key) {
                Ok(pos) => {
                    self.values[pos] = value;
                }
                Err(pos) => {
                    self.keys.insert(pos, key);
                    self.values.insert(pos, value);
                }
            }
            InsertResult::Done
        }
    }

    fn insert_as_node(&mut self, key: K, value: V) -> InsertResult<K, V> {
        assert!(self.is_node());
        let pos = match self.keys.binary_search(&key) {
            Ok(pos) => pos,
            Err(pos) => pos,
        };
        if let InsertResult::Split(split_key, new_node) = self.children[pos].insert(key, value) {
            if self.is_full() {
                let (parent_split_key, mut parent_new_node) = self.split_as_node();
                assert!(parent_new_node.is_node());
                if split_key > parent_split_key {
                    let parent_pos = match parent_new_node.keys.binary_search(&split_key) {
                        Ok(pos) => pos,
                        Err(pos) => pos,
                    };
                    parent_new_node.keys.insert(parent_pos, split_key);
                    parent_new_node.children.insert(parent_pos + 1, new_node);
                } else {
                    let parent_pos = match self.keys.binary_search(&split_key) {
                        Ok(pos) => pos,
                        Err(pos) => pos,
                    };
                    self.keys.insert(parent_pos, split_key);
                    self.children.insert(parent_pos + 1, new_node);
                }
                InsertResult::Split(parent_split_key, parent_new_node)
            } else {
                self.keys.insert(pos, split_key);
                self.children.insert(pos + 1, new_node);
                InsertResult::Done
            }
        } else {
            InsertResult::Done
        }
    }

    pub fn insert(&mut self, key: K, value: V) -> InsertResult<K, V> {
        if self.is_leaf() {
            self.insert_as_leaf(key, value)
        } else {
            self.insert_as_node(key, value)
        }
    }

    pub fn get(&self, key: &K) -> Option<&V> {
        if self.is_leaf() {
            match self.keys.binary_search(key) {
                Ok(pos) => Some(&self.values[pos]),
                Err(_) => None,
            }
        } else {
            let pos = match self.keys.binary_search(key) {
                Ok(pos) => pos,
                Err(pos) => pos,
            };
            self.children[pos].get(key)
        }
    }

    fn below_min_size(&self) -> bool {
        self.member_count() < self.fanout_factor / 3
    }

    fn can_fit(&self, count: usize) -> bool {
        self.fanout_factor - self.member_count() >= count
    }

    fn last_key(&self) -> &K {
        if self.is_node() {
            self.children.last().unwrap().last_key()
        } else {
            self.keys.last().unwrap()
        }
    }

    fn merge_children(&mut self, left_child_idx: usize) {
        assert!(left_child_idx < self.children.len() - 1);
        assert!(
            self.children[left_child_idx].can_fit(self.children[left_child_idx + 1].member_count())
        );
        let mut right_child = self.children.remove(left_child_idx + 1);
        if self.children[left_child_idx].is_node() {
            let join_key = self.keys.remove(left_child_idx);
            let left_child = &mut self.children[left_child_idx];
            left_child.keys.push(join_key);
            left_child.keys.append(&mut right_child.keys);
            left_child.children.append(&mut right_child.children);
        } else {
            // is leaf
            let left_child = &mut self.children[left_child_idx];
            assert!(left_child.is_leaf());
            left_child.keys.append(&mut right_child.keys);
            left_child.values.append(&mut right_child.values);
            self.keys.remove(left_child_idx);
        }
    }

    fn child_steal_from_left_sibling(&mut self, pos: usize) -> K {
        assert!(pos > 0);
        assert!(self.children[pos - 1].member_count() > self.children[pos].member_count());
        let amount_to_steal =
            (self.children[pos - 1].member_count() - self.children[pos].member_count()) / 2;
        let start_idx = self.children[pos - 1].member_count() - amount_to_steal;

        if self.children[pos].is_leaf() {
            let mut new_keys = Vec::new();
            new_keys.extend(self.children[pos - 1].keys.drain(start_idx..));
            new_keys.append(&mut self.children[pos].keys);

            let mut new_values = Vec::new();
            new_values.extend(self.children[pos - 1].values.drain(start_idx..));
            new_values.append(&mut self.children[pos].values);

            self.children[pos].keys = new_keys;
            self.children[pos].values = new_values;
            self.children[pos - 1].keys.last().unwrap().clone()
        } else {
            let join_key = self.children[pos - 1].last_key().clone();
            let mut new_keys = Vec::new();
            new_keys.extend(self.children[pos - 1].keys.drain(start_idx + 1..));
            new_keys.push(join_key);
            new_keys.append(&mut self.children[pos].keys);

            let mut new_children = Vec::new();
            new_children.extend(self.children[pos - 1].children.drain(start_idx + 1..));
            new_children.append(&mut self.children[pos].children);

            self.children[pos].keys = new_keys;
            self.children[pos].children = new_children;
            self.children[pos - 1].keys.pop().unwrap()
        }
    }

    // TODO: Figure out how to avoid copying the destination node's contents unecissarilly
    fn child_steal_from_right_sibling(&mut self, pos: usize) -> K {
        assert!(pos < self.children.len() - 1);
        assert!(self.children[pos + 1].member_count() > self.children[pos].member_count());
        let end_idx =
            (self.children[pos + 1].member_count() - self.children[pos].member_count()) / 2;

        if self.children[pos].is_leaf() {
            let mut new_keys = Vec::new();
            new_keys.append(&mut self.children[pos].keys);
            new_keys.extend(self.children[pos + 1].keys.drain(..end_idx));

            let mut new_values = Vec::new();
            new_values.append(&mut self.children[pos].values);
            new_values.extend(self.children[pos + 1].values.drain(..end_idx));

            self.children[pos].keys = new_keys;
            self.children[pos].values = new_values;
            self.children[pos].last_key().clone()
        } else {
            let join_key = self.children[pos].last_key().clone();

            let mut new_keys = Vec::new();
            new_keys.append(&mut self.children[pos].keys);
            new_keys.push(join_key);
            new_keys.extend(self.children[pos + 1].keys.drain(..end_idx));

            let mut new_children = Vec::new();
            new_children.append(&mut self.children[pos].children);
            new_children.extend(self.children[pos + 1].children.drain(..end_idx));

            self.children[pos].keys = new_keys;
            self.children[pos].children = new_children;
            self.children[pos + 1].keys.remove(0)
        }
    }

    pub fn remove(&mut self, key: &K) -> Option<V> {
        if self.is_leaf() {
            if let Ok(pos) = self.keys.binary_search(key) {
                self.keys.remove(pos);
                let val = self.values.remove(pos);
                Some(val)
            } else {
                None
            }
        } else {
            let pos = match self.keys.binary_search(key) {
                Ok(pos) => pos,
                Err(pos) => pos,
            };
            let res = self.children[pos].remove(key);
            if self.children[pos].below_min_size() {
                if pos > 0 && self.children[pos - 1].can_fit(self.children[pos].member_count()) {
                    // merge to left
                    self.merge_children(pos - 1);
                } else if pos < self.children.len() - 1
                    && self.children[pos].can_fit(self.children[pos + 1].member_count())
                {
                    // merge right sibling into this one
                    self.merge_children(pos);
                } else if pos == 0 {
                    // left-edge case
                    self.keys[pos] = self.child_steal_from_right_sibling(pos);
                } else if pos == self.children.len() - 1 {
                    // right edge case
                    self.keys[pos - 1] = self.child_steal_from_left_sibling(pos);
                } else {
                    // steal from smaller of siblings
                    let left_size = if pos > 0 {
                        self.children[pos - 1].member_count()
                    } else {
                        0
                    };
                    let right_size = if pos < self.children.len() - 1 {
                        self.children[pos + 1].member_count()
                    } else {
                        0
                    };
                    if left_size < right_size {
                        self.keys[pos - 1] = self.child_steal_from_left_sibling(pos);
                    } else {
                        self.keys[pos] = self.child_steal_from_right_sibling(pos);
                    }
                }
            }

            if self.keys.is_empty() {
                assert_eq!(self.children.len(), 1);
                let child = self.children.pop().unwrap();
                let _ = mem::replace(self, child);
            }

            res
        }
    }
}

pub struct BTreeIterator<'a, K: Ord + Clone + Debug, V: Clone> {
    queue: Vec<&'a Node<K, V>>,
    queue_indices: Vec<usize>,
    leaf: &'a Node<K, V>,
    leaf_idx: usize,
}
impl<'a, K: Ord + Clone + Debug, V: Clone> BTreeIterator<'a, K, V> {
    pub fn new(tree: &'a BTree<K, V>) -> Self {
        let mut queue = Vec::new();
        let mut queue_indices = Vec::new();
        let mut node = &tree.root;
        while node.is_node() {
            let next = &node.children[0];
            queue.push(node);
            queue_indices.push(0);
            node = next;
        }
        BTreeIterator {
            queue,
            queue_indices,
            leaf: node,
            leaf_idx: 0,
        }
    }
}
impl<'a, K: Ord + Clone + Debug, V: Clone> Iterator for BTreeIterator<'a, K, V> {
    type Item = (&'a K, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        if self.leaf_idx >= self.leaf.member_count() {
            // traverse up as needed
            while let Some(node) = self.queue.pop() {
                let idx = self.queue_indices.pop().unwrap();
                if idx < node.member_count() {
                    self.queue.push(node);
                    self.queue_indices.push(idx + 1);
                }
            }
            if self.queue.is_empty() {
                return None;
            }
            // traverse down as needed
            while self.queue.last().unwrap().is_node() {
                let next = &self.queue.last().unwrap().children[0];
                self.queue.push(next);
                self.queue_indices.push(0);
            }
            let idx = self.queue_indices.last().unwrap();
            self.leaf = &self.queue.last().unwrap().children[*idx];
            self.leaf_idx = 0;
        }
        let out = (
            &self.leaf.keys[self.leaf_idx],
            &self.leaf.values[self.leaf_idx],
        );
        self.leaf_idx += 1;
        Some(out)
    }
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
                    display_tree(&state);
                    assert!(state.get(&k).is_none());
                }
                TreeOperation::Insert(k, v) => {
                    state.insert(k, v);
                    display_tree(&state);
                    assert_eq!(state.get(&k), Some(&v));
                }
            };
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
            .find(|(k, v)| tree.get(k) != Some(*v))
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
