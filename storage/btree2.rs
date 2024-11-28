#![allow(dead_code)]

use std::fmt::Debug;

#[cfg(not(test))]
const FANOUT_FACTOR: usize = 512;
#[cfg(test)]
const FANOUT_FACTOR: usize = 8;

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
            keys: Vec::with_capacity(FANOUT_FACTOR),
            children: Vec::with_capacity(FANOUT_FACTOR + 1),
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

    fn from_leaves(leaves: Vec<BTreeLeaf<K, V>>) -> Self {
        assert!(leaves.len() > 1);
        let keys = leaves[1..]
            .iter()
            .map(|leaf| &leaf.items[0].0)
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
                    println!("{split_key:?}");
                    let pos = match self.binary_search_for_key(&split_key) {
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

    fn remove(&mut self, _key: &K) {
        unimplemented!();
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
        assert_eq!(
            FANOUT_FACTOR, 8,
            "This test needs updated if the test fanout factor changes"
        );

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
        //100, 200, 300, 400, 500, 600, 700, 800 // fill leaf
        //100, 200, 300, 400 || 500, 600, 700, 800, 900 // add one to split
        //100, 200, 300, 400 || 500, 600, 700, 800 || 900, 910, 920, 930, 940 // fill right leaf to split
        //100, 200, 300, 400 || 410, 420, 430, 440, 450 || 500, 600, 700, 800 || 900, 910, 920, 930, 940 // fill first leaf to split
        //100, 200, 300, 400 || 410, 420, 430, 440 || 450, 451, 452, 453, 454 || 500, 600, 700, 800 || 900, 910, 920, 930, 940 // fill interior leaf so it splits
        // add enough to right side to cause node to split
        //100, 200, 300, 400 || 410, 420, 430, 440 || 450, 451, 452, 453, 454 || 500, 600, 700, 800 || 900, 910, 920, 930 || 940, 941, 942, 943, 944 // 6 leaves
        //100, 200, 300, 400 || 410, 420, 430, 440 || 450, 451, 452, 453, 454 || 500, 600, 700, 800 || 900, 910, 920, 930 || 940, 941, 942, 943, || 944, 945, 946, 947, 948 // 7 leaves
        //100, 200, 300, 400 || 410, 420, 430, 440 || 450, 451, 452, 453, 454 || 500, 600, 700, 800 || 900, 910, 920, 930 || 940, 941, 942, 943, || 944, 945, 946, 947 || 948, 949, 950, 951, 952 // 8 leaves
        //100, 200, 300, 400 || 410, 420, 430, 440 || 450, 451, 452, 453, 454 || 500, 600, 700, 800 || 900, 910, 920, 930 || 940, 941, 942, 943, || 944, 945, 946, 947 || 948, 949, 950, 951, 952, 953, 954, 955 // fill last leaf
        // insert one more to cause node split
        // confirm resulting nodes constructed correctly

        let insertion_done: InsertionResult<i32, i32> = InsertionResult::Done;
        let mut keys_present: Vec<i32> = Vec::new();

        // split first leaf
        let mut insertion_order = 0;
        let mut first_leaf = BTreeLeaf::new();
        for i in 1..=8 {
            println!("{i}");
            first_leaf.insert(i * 100, insertion_order);
            insertion_order += 1;
            keys_present.push(i * 100);
        }
        let new_leaf = match first_leaf.insert(900, 9) {
            InsertionResult::Split(Child::Leaf(new_leaf)) => new_leaf,
            _ => panic!(),
        };
        let mut node = BTreeNode::from_leaves(vec![first_leaf, *new_leaf]);

        // fill right leaf to split
        for i in [910, 920, 930, 940].iter() {
            println!("{i}");
            let _res = node.insert(*i, insertion_order);
            assert!(matches!(&insertion_done, _res));
            insertion_order += 1;
            keys_present.push(*i);
        }

        // fill left leaf to split
        for i in [410, 420, 430, 440, 450].iter() {
            println!("{i}");
            let _res = node.insert(*i, insertion_order);
            assert!(matches!(&insertion_done, _res));
            insertion_order += 1;
            keys_present.push(*i);
        }

        // fill interior leaf to split
        for i in [451, 452, 453, 454].iter() {
            println!("{i}");
            let _res = node.insert(*i, insertion_order);
            assert!(matches!(&insertion_done, _res));
            insertion_order += 1;
            keys_present.push(*i);
        }

        // fill leaves to the right to fill this node
        for i in 941..=955 {
            println!("{i}");
            let _res = node.insert(i, insertion_order);
            assert!(matches!(&insertion_done, _res));
            insertion_order += 1;
            keys_present.push(i);
        }

        // insert one more, node splits
        println!("956");
        let new_node = match node.insert(956, insertion_order) {
            InsertionResult::Split(Child::Node(new_node)) => new_node,
            _ => panic!("Should have split into a new node"),
        };
        assert_eq!(node.keys, vec![410, 450, 500]);
        assert_eq!(new_node.keys, vec![940, 944, 948]);
    }
}
