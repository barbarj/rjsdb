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
            println!("inserting right");
            let insert_pos = match new_node.keys.binary_search(&key) {
                Ok(pos) => pos,
                Err(pos) => pos,
            };
            new_node.keys.insert(insert_pos, key);
            new_node.children.insert(insert_pos + 1, node);
        } else {
            println!("inserting left");
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
        if self.is_leaf() {
            self.values.len() == self.fanout_factor
        } else {
            self.children.len() == self.fanout_factor
        }
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

    fn are_mergeable(left: &Node<K, V>, right: &Node<K, V>) -> bool {
        ((left.is_leaf() && right.is_leaf()) || (!left.is_leaf() && !right.is_leaf()))
            && left.is_mergeable()
            && right.is_mergeable()
    }

    fn insert_as_node(&mut self, key: K, value: V) -> InsertionResult<K, V> {
        assert!(!self.is_leaf());
        let mut pos = match self.keys.binary_search(&key) {
            Ok(pos) => pos,
            Err(pos) => pos,
        };
        if let InsertionResult::Split(mut new_node, split_key) =
            self.children[pos].insert(key, value)
        {
            if pos > 0 && Self::are_mergeable(&self.children[pos - 1], &self.children[pos]) {
                println!("merging left");
                println!("keys before merge: {:?}", self.keys);
                self.merge_children(pos - 1);
                println!("keys aftermerge:   {:?}", self.keys);
                pos -= 1;
            }
            let insert_res = if pos < self.children.len() - 1
                && Self::are_mergeable(&new_node, &self.children[pos + 1])
            {
                println!("merging new node right");
                // If the new node is can be merged into its right-sibling to be, just do that and
                // avoid manipulating the children otherwise

                // That means,
                // - split key goes at pos,
                // - merge_key is key at pos currently
                // - new_node goes to pos + 1
                Self::merge_nodes(
                    self.keys[pos].clone(),
                    &mut new_node,
                    self.children.get_mut(pos + 1).unwrap(),
                );
                self.children[pos + 1] = new_node;
                self.keys[pos] = split_key;
                InsertionResult::Done
            } else if self.is_full() {
                println!(
                    "splitting and inserting at node with end key: {:?} at pos {}",
                    self.last_key(),
                    pos + 1
                );
                // Otherwise, if this node is full, split and insert the new child node.
                let (new_new_node, new_split_key) =
                    self.split_and_insert_as_node(split_key, new_node);
                InsertionResult::Split(new_new_node, new_split_key)
            } else {
                println!("inserting new node");
                // Otherwise just insert the new child node
                self.keys.insert(pos, split_key);
                self.children.insert(pos + 1, new_node);
                InsertionResult::Done
            };
            insert_res
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

    fn is_mergeable(&self) -> bool {
        self.keys.len() <= self.fanout_factor / 3
    }

    fn remove_as_leaf(&mut self, key: &K) -> Option<V> {
        match self.keys.binary_search(key) {
            Ok(pos) => {
                println!("remove 6");
                self.keys.remove(pos);
                println!("remove 7");
                let res = self.values.remove(pos);
                Some(res)
            }
            Err(_) => None,
        }
    }

    /// This will always merge the contents of the right node into the left node,
    /// The right node will be empty after this operation
    fn merge_nodes(merge_key: K, left: &mut Node<K, V>, right: &mut Node<K, V>) {
        if left.is_leaf() {
            left.values.append(&mut right.values);
        } else {
            left.children.append(&mut right.children);
            left.keys.push(merge_key);
        }
        left.keys.append(&mut right.keys);
    }

    /// merges the child at left_child_idx with the child to its right by moving the contents of
    /// the right child into the left child
    fn merge_children(&mut self, left_child_idx: usize) {
        assert!(left_child_idx < self.children.len() - 1);

        println!("remove 1");
        let merge_key = self.keys.remove(left_child_idx);
        let (left_side, right_side) = self.children.split_at_mut(left_child_idx + 1);
        let left_child = &mut left_side[left_child_idx];
        let right_child = &mut right_side[0];

        Self::merge_nodes(merge_key, left_child, right_child);

        println!("remove 2");
        // now clean up the now duplicate data of the right node
        self.children.remove(left_child_idx + 1);
        println!("did remove 2");
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

    fn remove_as_node(&mut self, key: &K) -> Option<V> {
        let pos = match self.keys.binary_search(key) {
            Ok(pos) => pos,
            Err(pos) => pos,
        };
        println!("remove 3");
        let res = self.children[pos].remove(key);
        if self.children[pos].is_empty() {
            println!("remove 4");
            self.children.remove(pos);
            println!("remove 5");
            if pos < self.keys.len() {
                self.keys.remove(pos);
            } else {
                self.keys.pop();
            }
        } else if pos > 0 && Self::are_mergeable(&self.children[pos - 1], &self.children[pos]) {
            self.merge_children(pos - 1);
        } else if pos < self.children.len() - 1
            && Self::are_mergeable(&self.children[pos], &self.children[pos + 1])
        {
            self.merge_children(pos);
        }
        if self.children.len() == 1 {
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
        if self.is_leaf() {
            self.values.len()
        } else {
            self.children.len()
        }
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
            assert!(no_empty_nodes(state, &state.root));
            assert!(no_mergeable_nodes(&state.root));
        }
    }

    #[derive(Debug, Clone)]
    pub enum TreeOperation {
        Insert(u32, u32),
        Remove(u32),
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

    fn no_empty_nodes(tree: &BTree<u32, u32>, node: &Node<u32, u32>) -> bool {
        if node.is_leaf() {
            !node.is_empty() || tree.root.is_empty()
        } else {
            !node.is_empty()
                && node
                    .children
                    .iter()
                    .all(|child| no_empty_nodes(tree, child))
        }
    }

    fn no_mergeable_nodes(node: &Node<u32, u32>) -> bool {
        if node.is_leaf() {
            return true;
        }
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
