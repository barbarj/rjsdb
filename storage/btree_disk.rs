#![allow(dead_code)]

use std::{
    cell::RefCell,
    cmp::Ordering,
    fmt::{Debug, Display},
    marker::PhantomData,
    os::fd::AsRawFd,
    rc::Rc,
};

#[cfg(test)]
use std::iter::Peekable;

use crate::pager::{
    PageBuffer, PageBufferOffset, PageError, PageId, PageKind, PageRef, Pager, PagerError,
    CELL_POINTER_SIZE,
};

#[cfg(test)]
use itertools::Itertools;

use serde::{de::DeserializeOwned, Serialize};
use serialize::{from_reader, serialized_size, to_bytes, Error as SerdeError};

/// # Notes on Page Structure
/// - Leaf node cells are (K, V)
/// - Internal nodes alternate PageIds and keys, so the cell order looks like:
///    PageId | Key | PageId | Key | PageId... etc.
///    The sequence always starts and end with PageIds. The Keys split the search space that the
///    PageIds represent.

#[derive(Debug)]
pub enum Error {
    Page(PageError),
    Pager(PagerError),
    Serde(SerdeError),
}
impl From<PageError> for Error {
    fn from(value: PageError) -> Self {
        Self::Page(value)
    }
}
impl From<PagerError> for Error {
    fn from(value: PagerError) -> Self {
        Self::Pager(value)
    }
}
impl From<SerdeError> for Error {
    fn from(value: SerdeError) -> Self {
        Self::Serde(value)
    }
}
impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Page(error) => std::fmt::Display::fmt(&error, f),
            Self::Pager(error) => std::fmt::Display::fmt(&error, f),
            Self::Serde(error) => std::fmt::Display::fmt(&error, f),
        }
    }
}
impl std::error::Error for Error {}

type Result<T> = std::result::Result<T, Error>;

pub struct BTree<
    Fd: AsRawFd,
    PB: PageBuffer,
    K: Ord + Serialize + DeserializeOwned + Debug,
    V: Serialize + DeserializeOwned,
> {
    pager_ref: Rc<RefCell<Pager<PB>>>,
    backing_fd: Fd,
    root: Node<PB, K, V>,
    _key: PhantomData<K>,
    _value: PhantomData<V>,
}
impl<
        Fd: AsRawFd + Copy,
        PB: PageBuffer,
        K: Ord + Serialize + DeserializeOwned + Debug,
        V: Serialize + DeserializeOwned,
    > BTree<Fd, PB, K, V>
{
    pub fn init(pager_ref: Rc<RefCell<Pager<PB>>>, backing_fd: Fd) -> Result<Self> {
        let mut pager = pager_ref.borrow_mut();
        let root_page_ref = if pager.file_has_page(&backing_fd, 0) {
            pager.get_page(backing_fd, 0)?
        } else {
            pager.new_page(backing_fd, PageKind::BTreeLeaf)?
        };
        drop(pager);

        let root = Node::new(root_page_ref);
        assert_eq!(root.page_id(), 0);
        Ok(BTree {
            pager_ref,
            backing_fd,
            root,
            _key: PhantomData,
            _value: PhantomData,
        })
    }

    fn pager_info(&self) -> PagerInfo<PB, Fd> {
        PagerInfo::new(self.pager_ref.clone(), self.backing_fd)
    }

    pub fn insert(&mut self, key: K, value: V) -> Result<()> {
        let mut pager_info = self.pager_info();
        let insert_res = self.root.insert(key, value, &mut pager_info)?;
        if let InsertResult::Split(split_key, new_page_id_right) = insert_res {
            // get a new page to move data to, representing the left side of the split
            let new_page_left_ref = pager_info.new_page(self.root.page_kind())?;
            let mut new_page_left = new_page_left_ref.borrow_mut();

            // move data currently on the root page to the new page
            let mut root_page = self.root.page_ref.borrow_mut();
            for (i, bytes) in root_page.cell_bytes_iter().enumerate() {
                new_page_left.insert_cell(i as u16, bytes)?;
            }
            root_page.clear_data();

            let new_page_id_left = new_page_left.id();
            drop(new_page_left);

            // update root with new children
            root_page.insert_cell(0, &to_bytes(&new_page_id_left)?)?;
            root_page.insert_cell(1, &to_bytes(&split_key)?)?;
            root_page.insert_cell(2, &to_bytes(&new_page_id_right)?)?;
        }
        Ok(())
    }

    pub fn get(&self, key: &K) -> Result<Option<V>> {
        let mut pager_info = self.pager_info();
        self.root.get(key, &mut pager_info)
    }
}

struct PagerInfo<PB: PageBuffer, Fd: AsRawFd + Copy> {
    pager_ref: Rc<RefCell<Pager<PB>>>,
    backing_fd: Fd,
}
impl<PB: PageBuffer, Fd: AsRawFd + Copy> PagerInfo<PB, Fd> {
    fn new(pager_ref: Rc<RefCell<Pager<PB>>>, backing_fd: Fd) -> Self {
        PagerInfo {
            pager_ref,
            backing_fd,
        }
    }

    fn new_page(&mut self, kind: PageKind) -> Result<PageRef<PB>> {
        let mut pager = self.pager_ref.borrow_mut();
        let new_page = pager.new_page(self.backing_fd, kind)?;
        Ok(new_page)
    }

    fn get_page(&mut self, page_id: PageId) -> Result<PageRef<PB>> {
        let mut pager = self.pager_ref.borrow_mut();
        let page = pager.get_page(self.backing_fd, page_id)?;
        Ok(page)
    }

    fn page_node<K, V>(&mut self, page_id: PageId) -> Result<Node<PB, K, V>>
    where
        K: Ord + Debug + Serialize + DeserializeOwned,
        V: Serialize + DeserializeOwned,
    {
        let page = self.get_page(page_id)?;
        Ok(Node::new(page))
    }

    fn new_page_node<K, V>(&mut self, kind: PageKind) -> Result<Node<PB, K, V>>
    where
        K: Ord + Debug + Serialize + DeserializeOwned,
        V: Serialize + DeserializeOwned,
    {
        let page = self.new_page(kind)?;
        Ok(Node::new(page))
    }

    fn buffer_size(&self) -> PageBufferOffset {
        PB::buffer_size()
    }
}

enum InsertResult<K: Ord + Serialize + DeserializeOwned + Debug> {
    Split(K, PageId),
    Done,
}

// TODO: Convert the use of DeserializeOwned to a Deserialization of borrowed data (will need to
// get serialization format to support borrowed data
struct Node<
    PB: PageBuffer,
    K: Ord + Debug + Serialize + DeserializeOwned,
    V: Serialize + DeserializeOwned,
> {
    page_ref: PageRef<PB>,
    _key: PhantomData<K>,
    _value: PhantomData<V>,
}
impl<
        PB: PageBuffer,
        K: Ord + Debug + Serialize + DeserializeOwned,
        V: Serialize + DeserializeOwned,
    > Node<PB, K, V>
{
    fn new(page_ref: PageRef<PB>) -> Self {
        Node {
            page_ref,
            _key: PhantomData,
            _value: PhantomData,
        }
    }

    fn key_count(&self) -> u16 {
        let page = self.page_ref.borrow();
        if self.is_leaf() {
            page.cell_count()
        } else {
            (page.cell_count() - 1) / 2
        }
    }

    fn can_fit_leaf(&self, key: &K, value: &V) -> bool {
        assert!(self.is_leaf());
        let needed_space: usize = serialized_size(&(key, value)) + CELL_POINTER_SIZE as usize;
        assert!(needed_space <= u16::MAX.into());
        let page = self.page_ref.borrow();
        page.can_fit_data(needed_space as u16)
    }

    fn can_fit_node(&self, key: &K) -> bool {
        assert!(self.is_node());
        let dummy_id: PageId = 42;
        let needed_space =
            serialized_size(&key) + serialized_size(&dummy_id) + (2 * CELL_POINTER_SIZE as usize);
        assert!(needed_space <= u16::MAX.into());
        let page = self.page_ref.borrow();
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

    fn page_id(&self) -> PageId {
        let page = self.page_ref.borrow();
        page.id()
    }

    fn page_kind(&self) -> PageKind {
        let page = self.page_ref.borrow();
        page.kind()
    }

    fn page_free_space(&self) -> u16 {
        let page = self.page_ref.borrow();
        page.total_free_space()
    }

    fn key_from_leaf(&self, pos: u16) -> Result<K> {
        assert!(self.is_leaf());
        let page = self.page_ref.borrow();
        let (key, _): (K, V) = from_reader(page.cell_bytes(pos))?;
        Ok(key)
    }

    fn value_from_leaf<T: DeserializeOwned>(&self, pos: u16) -> Result<T> {
        assert!(self.is_leaf());
        let page = self.page_ref.borrow();
        let (_, val): (K, T) = from_reader(page.cell_bytes(pos))?;
        Ok(val)
    }

    fn key_pos_to_cell_pos(key_pos: u16) -> u16 {
        (key_pos * 2) + 1
    }

    fn id_pos_to_cell_pos(id_pos: u16) -> u16 {
        id_pos * 2
    }

    /// Returns None if this cell position will not contain a key
    fn cell_pos_to_key_pos(cell_pos: u16) -> Option<u16> {
        if cell_pos % 2 == 0 {
            None
        } else {
            Some(cell_pos / 2) // integer division makes the division of an odd number (2n + 1) by
                               // 2 result in the same number as if the input were the odd number's
                               // even counterpart (2n)
        }
    }

    fn key_from_inner_node(&self, key_pos: u16) -> Result<K> {
        assert!(self.is_node());
        let pos = Self::key_pos_to_cell_pos(key_pos);
        let page = self.page_ref.borrow();
        let key = from_reader(page.cell_bytes(pos))?;
        Ok(key)
    }

    fn page_id_from_inner_node(&self, id_pos: u16) -> Result<PageId> {
        assert!(self.is_node());
        let pos = Self::id_pos_to_cell_pos(id_pos);
        let page = self.page_ref.borrow();
        let page_id = from_reader(page.cell_bytes(pos))?;
        Ok(page_id)
    }

    fn key_at_pos(&self, pos: u16) -> Result<K> {
        if self.is_node() {
            self.key_from_inner_node(pos)
        } else {
            self.key_from_leaf(pos)
        }
    }

    // TODO: Test
    // TODO: Figure out if I should remove unwraps
    fn binary_search_keys(&self, key: &K) -> std::result::Result<u16, u16> {
        if self.key_count() == 0 {
            return Err(0);
        }
        let mut low = 0;
        let mut high = self.key_count() - 1;
        while low < high {
            let mid = (low + high) / 2;
            let cell_key = self.key_at_pos(mid).unwrap();
            match &cell_key.cmp(key) {
                Ordering::Less => {
                    low = mid + 1;
                }
                Ordering::Equal => return Ok(mid),
                Ordering::Greater => high = mid,
            }
        }
        let cell_key = self.key_at_pos(low).unwrap();
        match &cell_key.cmp(key) {
            Ordering::Greater => Err(low),
            Ordering::Equal => Ok(low),
            Ordering::Less => Err(low + 1),
        }
    }

    fn split_inner_node<Fd: AsRawFd + Copy>(
        &mut self,
        pager_info: &mut PagerInfo<PB, Fd>,
    ) -> Result<(K, Node<PB, K, V>)> {
        let half = pager_info.buffer_size() / 2;
        assert!(self.page_free_space() < half);
        let mut used_space = 0;
        let mut idx = 0;
        let mut page = self.page_ref.borrow_mut();

        // Find the index of the first cell that begins past the halfway point
        while used_space < half {
            let ptr = page.get_cell_pointer(idx);
            used_space += ptr.size;
            idx += 1;
        }
        if idx % 2 == 0 {
            // cell at idx contains a pageId, so we actually want to use the key to the left of it.
            idx -= 1;
        }

        // self.key_from_inner_node uses the logical key position amongst other keys, so convert to
        // that before asking for the key
        let split_key = self.key_from_inner_node(Self::cell_pos_to_key_pos(idx).unwrap())?;

        // get new page
        let new_node = pager_info.new_page_node(page.kind())?;
        let mut new_page = new_node.page_ref.borrow_mut();

        // copy cells to new page, starting with the cell after the split key
        for (i, bytes) in page.cell_bytes_iter().enumerate().skip((idx + 1).into()) {
            new_page.insert_cell(i as u16, bytes)?;
        }
        // remove moved cells, plus the now hanging right key from this node
        for i in page.cell_count() - 1..=idx {
            page.remove_cell(i);
        }
        drop(new_page);

        // remove the now hanging right key from this node
        page.remove_cell(idx);

        Ok((split_key, new_node))
    }

    fn split_leaf<Fd: AsRawFd + Copy>(
        &mut self,
        pager_info: &mut PagerInfo<PB, Fd>,
    ) -> Result<(K, Node<PB, K, V>)> {
        let half = pager_info.buffer_size();
        let mut used_space = 0;
        let mut idx = 0;
        let mut page = self.page_ref.borrow_mut();

        // Find the index of the first cell that begins past the halfway point
        while used_space < half {
            let ptr = page.get_cell_pointer(idx);
            used_space += ptr.size;
            idx += 1;
        }
        // keys point left, and cell number idx is going to be the first cell in the new page,
        // so the split key should be one to the left.
        assert!(idx > 0);
        let split_key = self.key_from_leaf(idx - 1)?;

        // get new page
        let new_page_ref = pager_info.new_page(page.kind())?;
        let mut new_page = new_page_ref.borrow_mut();

        // copy cells to new page and remove cells from old page
        for i in idx..page.cell_count() {
            new_page.insert_cell(i, page.cell_bytes(idx))?;
            page.remove_cell(idx);
        }
        drop(new_page);

        let new_node = Node::new(new_page_ref);
        Ok((split_key, new_node))
    }

    fn insert_as_leaf<Fd: AsRawFd + Copy>(
        &mut self,
        key: K,
        value: V,
        pager_info: &mut PagerInfo<PB, Fd>,
    ) -> Result<InsertResult<K>> {
        assert!(self.is_leaf());
        if !self.can_fit_leaf(&key, &value) {
            let (split_key, mut new_node) = self.split_leaf(pager_info)?;
            assert!(new_node.is_leaf());
            if key > split_key {
                new_node.insert_as_leaf(key, value, pager_info)?;
            } else {
                self.insert_as_leaf(key, value, pager_info)?;
            }
            Ok(InsertResult::Split(split_key, new_node.page_id()))
        } else {
            match self.binary_search_keys(&key) {
                Ok(pos) => {
                    let mut page = self.page_ref.borrow_mut();
                    // TODO: Add some replace cell function to page
                    page.remove_cell(pos);
                    page.insert_cell(pos, &to_bytes(&(key, value))?)?;
                }
                Err(pos) => {
                    let mut page = self.page_ref.borrow_mut();
                    page.insert_cell(pos, &to_bytes(&(key, value))?)?;
                }
            }
            Ok(InsertResult::Done)
        }
    }

    /// For node searches, we only care about which child to descend to,
    /// so an exact match doesn't provide any additional information
    fn search_keys_as_node(&self, key: &K) -> u16 {
        match self.binary_search_keys(key) {
            Ok(pos) => pos,
            Err(pos) => pos,
        }
    }

    fn get_descendent_by_key<Fd: AsRawFd + Copy>(
        &self,
        key: &K,
        pager_info: &mut PagerInfo<PB, Fd>,
    ) -> Result<(u16, Node<PB, K, V>)> {
        assert!(self.is_node());
        let pos = self.search_keys_as_node(key);
        let descendent = pager_info.page_node(self.page_id_from_inner_node(pos)?)?;
        Ok((pos, descendent))
    }

    fn insert_split_results_into_node(
        &mut self,
        pos: u16,
        split_key: &K,
        new_page_id: PageId,
    ) -> Result<()> {
        let prior_key = self.replace_inner_node_key(pos, split_key)?;
        let id_cell_pos = Self::id_pos_to_cell_pos(pos + 1);
        let mut page = self.page_ref.borrow_mut();
        page.insert_cell(id_cell_pos, &to_bytes(&new_page_id)?)?;
        page.insert_cell(id_cell_pos + 1, &to_bytes(&prior_key)?)?;
        Ok(())
    }

    /// replaces the key at key position pos with the new key, and returns the old key
    fn replace_inner_node_key(&mut self, pos: u16, new_key: &K) -> Result<K> {
        let old_key = self.key_from_inner_node(pos)?;
        let cell_idx = Self::key_pos_to_cell_pos(pos);
        let mut page = self.page_ref.borrow_mut();
        page.remove_cell(cell_idx);
        page.insert_cell(cell_idx, &to_bytes(new_key)?)?;
        Ok(old_key)
    }

    fn insert_as_node<Fd: AsRawFd + Copy>(
        &mut self,
        key: K,
        value: V,
        pager_info: &mut PagerInfo<PB, Fd>,
    ) -> Result<InsertResult<K>> {
        assert!(self.is_node());
        let (pos, mut child_node) = self.get_descendent_by_key(&key, pager_info)?;
        if let InsertResult::Split(split_key, new_page_id) =
            child_node.insert(key, value, pager_info)?
        {
            if !self.can_fit_node(&split_key) {
                let (parent_split_key, mut parent_new_node) = self.split_inner_node(pager_info)?;
                assert!(parent_new_node.is_node());

                if pos < self.key_count() {
                    self.insert_split_results_into_node(pos, &split_key, new_page_id)?
                } else {
                    // after the split, there's one less key between the two nodes, so account for
                    // that
                    let pos = pos - self.key_count() - 1;
                    parent_new_node.insert_split_results_into_node(pos, &split_key, new_page_id)?;
                }
                Ok(InsertResult::Split(
                    parent_split_key,
                    parent_new_node.page_id(),
                ))
            } else {
                self.insert_split_results_into_node(pos, &split_key, new_page_id)?;
                Ok(InsertResult::Done)
            }
        } else {
            Ok(InsertResult::Done)
        }
    }

    fn insert<Fd: AsRawFd + Copy>(
        &mut self,
        key: K,
        value: V,
        pager_info: &mut PagerInfo<PB, Fd>,
    ) -> Result<InsertResult<K>> {
        if self.is_leaf() {
            self.insert_as_leaf(key, value, pager_info)
        } else {
            self.insert_as_node(key, value, pager_info)
        }
    }

    fn get<Fd: AsRawFd + Copy>(
        &self,
        key: &K,
        pager_info: &mut PagerInfo<PB, Fd>,
    ) -> Result<Option<V>> {
        if self.is_leaf() {
            match self.binary_search_keys(key) {
                Ok(pos) => Ok(Some(self.value_from_leaf(pos)?)),
                Err(_) => Ok(None),
            }
        } else {
            assert!(self.is_node());
            let (_, child_node) = self.get_descendent_by_key(key, pager_info)?;
            child_node.get(key, pager_info)
        }
    }
}

#[cfg(test)]
const TEST_BUFFER_SIZE: u16 = 60;
#[cfg(test)]
struct TestPageBuffer {
    data: [u8; TEST_BUFFER_SIZE as usize],
}
#[cfg(test)]
impl PageBuffer for TestPageBuffer {
    fn new() -> Self
    where
        Self: Sized,
    {
        Self {
            data: [0; TEST_BUFFER_SIZE as usize],
        }
    }

    fn buffer_size() -> u16 {
        TEST_BUFFER_SIZE
    }

    fn data(&self) -> &[u8] {
        &self.data
    }

    fn data_mut(&mut self) -> &mut [u8] {
        &mut self.data
    }
}

#[cfg(test)]
impl<Fd: AsRawFd + Copy> BTree<Fd, TestPageBuffer, u32, u32> {
    /*
     * An example description looks something like this:
    0: [12, 23] (3)
    0->0: [3, 6, 9] (4)
    0->1: [15, 17, 20] (4)
    0->2: [28] (2)
    0->0->0: L[1, 2, 3] (0)
    0->0->1: L[4, 5, 6] (0)
    0->0->2: L[7, 8, 9] (0)
    0->0->3: L[10, 11, 12] (0)
    0->1->0: L[13, 14, 15] (0)
    0->1->1: L[16, 17] (0)
    0->1->2: L[18, 19, 20] (0)
    0->1->3: L[21, 22, 23] (0)
    0->2->0: L[24, 25, 26, 27] (0)
    0->2->1: L[29, 30, 31] (0)
        */
    pub fn from_description(
        description: &str,
        pager_ref: Rc<RefCell<Pager<TestPageBuffer>>>,
        backing_fd: Fd,
    ) -> BTree<Fd, TestPageBuffer, u32, u32> {
        let mut lines = description
            .trim()
            .split('\n')
            .map(|x| x.trim())
            .map(DescriptionLine::from_str)
            .peekable();

        assert!(lines.peek().is_some());

        // initalize pages
        let mut pager_info = PagerInfo::new(pager_ref.clone(), backing_fd);

        // init root page
        let first_line = lines.next().unwrap();
        let root_kind = match first_line.is_leaf {
            true => PageKind::BTreeLeaf,
            false => PageKind::BTreeNode,
        };
        let root: Node<TestPageBuffer, u32, u32> = pager_info.new_page_node(root_kind).unwrap();
        let first_page_id = root.page_id();
        assert_eq!(first_page_id, 0);
        drop(root);

        let _root =
            Node::from_description_lines(&mut pager_info, first_line, &mut lines, first_page_id);

        BTree::init(pager_ref, backing_fd).unwrap()
    }

    fn to_description(&self) -> String {
        let mut pager_info = self.pager_info();
        Self::node_to_description(&mut pager_info, self.root.page_id())
    }

    fn node_to_description(
        pager_info: &mut PagerInfo<TestPageBuffer, Fd>,
        page_id: PageId,
    ) -> String {
        use std::collections::VecDeque;

        let mut description = String::new();
        let mut queue = VecDeque::new();
        queue.push_back((vec![0], page_id));
        while let Some((ancestry, page_id)) = queue.pop_front() {
            let node = pager_info.page_node(page_id).unwrap();
            let path_parts: Vec<_> = ancestry.iter().map(|x| x.to_string()).collect();
            let path = path_parts.join("->");
            if node.is_leaf() {
                let s = format!("{path}: L{:?} ({})\n", node.keys(), node.descendent_count());
                description.push_str(&s);
            } else {
                let s = format!("{path}: {:?} ({})\n", node.keys(), node.descendent_count());
                description.push_str(&s);
            }
            queue.extend(node.descendent_page_ids().into_iter().enumerate().map(
                |(idx, page_id)| {
                    let mut child_ancestry = ancestry.clone();
                    child_ancestry.push(idx);
                    (child_ancestry, page_id)
                },
            ));
        }
        description
    }

    fn display_subtree(pager_info: &mut PagerInfo<TestPageBuffer, Fd>, root_page_id: PageId) {
        let description = Self::node_to_description(pager_info, root_page_id);
        print!("{description}");
    }
}

#[cfg(test)]
impl Node<TestPageBuffer, u32, u32> {
    fn from_description_lines<Fd: AsRawFd + Copy, I: Iterator<Item = DescriptionLine>>(
        pager_info: &mut PagerInfo<TestPageBuffer, Fd>,
        this_node_line: DescriptionLine,
        lines: &mut Peekable<I>,
        this_page_id: PageId,
    ) -> Self {
        let new_node = pager_info.page_node(this_page_id).unwrap();
        let mut page = new_node.page_ref.borrow_mut();

        if this_node_line.is_leaf {
            for (i, key) in this_node_line.keys.iter().enumerate() {
                let bytes = to_bytes(&(key, key)).unwrap();
                page.insert_cell(i as u16, &bytes).unwrap();
            }
        } else {
            let child_lines: Vec<_> = lines
                .peeking_take_while(|l| this_node_line.is_child_line(l))
                .collect();
            assert_eq!(child_lines.len(), this_node_line.child_count);
            assert_eq!(this_node_line.keys.len() + 1, this_node_line.child_count);
            let mut children = Vec::new();

            for (idx, child_line) in child_lines.into_iter().enumerate() {
                // init child page so we can get the page id
                let kind = match child_line.is_leaf {
                    true => PageKind::BTreeLeaf,
                    false => PageKind::BTreeNode,
                };
                let child_node: Self = pager_info.new_page_node(kind).unwrap();
                let page_id = child_node.page_id();
                children.push((child_line, page_id)); // store for later
                drop(child_node);

                let page_id_bytes = to_bytes(&page_id).unwrap();
                page.insert_cell(Self::id_pos_to_cell_pos(idx as u16), &page_id_bytes)
                    .unwrap();

                // Because we know that there is always 1 more child line that there is keys,
                // this will only be None on the last child line
                if idx < this_node_line.keys.len() {
                    let key = &this_node_line.keys[idx];
                    let key_bytes = to_bytes(key).unwrap();
                    page.insert_cell(Self::key_pos_to_cell_pos(idx as u16), &key_bytes)
                        .unwrap();
                }
            }

            // Process the children we set aside earlier
            for (child_line, page_id) in children.into_iter() {
                Self::from_description_lines(pager_info, child_line, lines, page_id);
            }
        }

        drop(page);
        new_node
    }

    fn keys(&self) -> Vec<u32> {
        if self.is_leaf() {
            (0..self.key_count())
                .map(|i| self.key_from_leaf(i).unwrap())
                .collect()
        } else {
            (0..self.key_count())
                .map(|i| self.key_from_inner_node(i).unwrap())
                .collect()
        }
    }

    fn descendent_page_ids(&self) -> Vec<PageId> {
        if self.is_leaf() {
            Vec::new()
        } else {
            (0..=self.key_count())
                .map(|i| self.page_id_from_inner_node(i).unwrap())
                .collect()
        }
    }

    fn descendent_count(&self) -> u16 {
        let page = self.page_ref.borrow();
        page.cell_count() - self.key_count()
    }
}

#[cfg(test)]
#[derive(Debug, Clone)]
struct DescriptionLine {
    traversal_path: Vec<usize>,
    is_leaf: bool,
    keys: Vec<u32>,
    child_count: usize,
}
#[cfg(test)]
impl DescriptionLine {
    fn from_str(s: &str) -> Self {
        let mut parts = s.split(": ");
        let traversal_path = parts
            .next()
            .unwrap()
            .split("->")
            .map(|x| x.parse::<usize>().unwrap())
            .collect();

        let second_half = parts.next().unwrap();
        assert!(second_half.starts_with("L[") || second_half.starts_with("["));
        let is_leaf = second_half.starts_with("L");
        let skip_num = if is_leaf { 2 } else { 1 };

        let closing_bracket_pos = second_half.chars().position(|c| c == ']').unwrap();
        let num_strs = second_half[skip_num..closing_bracket_pos].split(", ");
        let keys: Vec<u32> = num_strs.map(|x| x.parse::<u32>().unwrap()).collect();

        let child_count = second_half[closing_bracket_pos + 3..]
            .split(")")
            .next()
            .unwrap()
            .parse::<usize>()
            .unwrap();

        if is_leaf {
            assert_eq!(child_count, 0);
        } else {
            assert_eq!(keys.len() + 1, child_count);
        }

        DescriptionLine {
            traversal_path,
            is_leaf,
            keys,
            child_count,
        }
    }

    fn is_child_line(&self, candidate: &DescriptionLine) -> bool {
        let tvlen = self.traversal_path.len();
        candidate.traversal_path.len() == tvlen + 1
            && candidate.traversal_path[0..tvlen] == self.traversal_path
    }
}

#[cfg(test)]
mod tests {
    use std::{
        cell::RefCell,
        fs::{self, File, OpenOptions},
        os::fd::AsRawFd,
        rc::Rc,
    };

    use itertools::Itertools;

    use crate::pager::Pager;

    use super::{BTree, TestPageBuffer};

    fn trim_lines(s: &str) -> String {
        s.trim().lines().map(|l| l.trim()).join("\n") + "\n"
    }

    fn open_file(filename: &str) -> File {
        OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(filename)
            .unwrap()
    }

    fn init_tree_from_description_in_file(
        filename: &str,
        description: &str,
    ) -> BTree<i32, TestPageBuffer, u32, u32> {
        let file = open_file(filename);
        let backing_fd = file.as_raw_fd();
        let pager_ref = Rc::new(RefCell::new(Pager::new(vec![file])));

        BTree::from_description(description, pager_ref, backing_fd)
    }

    fn init_tree_in_file(filename: &str) -> BTree<i32, TestPageBuffer, u32, u32> {
        let file = open_file(filename);
        let backing_fd = file.as_raw_fd();
        let pager_ref = Rc::new(RefCell::new(Pager::new(vec![file])));

        BTree::init(pager_ref, backing_fd).unwrap()
    }

    #[test]
    fn end_to_end_description() {
        let input_description = "
            0: [12, 23] (3)
            0->0: [3, 6, 9] (4)
            0->1: [15, 17, 20] (4)
            0->2: [28] (2)
            0->0->0: L[1, 2, 3] (0)
            0->0->1: L[4, 5, 6] (0)
            0->0->2: L[7, 8, 9] (0)
            0->0->3: L[10, 11, 12] (0)
            0->1->0: L[13, 14, 15] (0)
            0->1->1: L[16, 17] (0)
            0->1->2: L[18, 19, 20] (0)
            0->1->3: L[21, 22, 23] (0)
            0->2->0: L[24, 25, 26, 27] (0)
            0->2->1: L[29, 30, 31] (0)";
        let input_description = trim_lines(input_description);

        let filename = "end_to_end_description.test";
        let tree = init_tree_from_description_in_file(filename, &input_description);

        assert_eq!(tree.root.page_id(), 0);
        assert_eq!(&tree.to_description(), &input_description);

        drop(tree);
        fs::remove_file(filename).unwrap();
    }

    #[test]
    fn binary_search_keys_empty() {
        let filename = "binary_search_keys_empty.test";

        let tree = init_tree_in_file(filename);

        assert!(matches!(tree.root.binary_search_keys(&42), Err(0)));

        drop(tree);
        fs::remove_file(filename).unwrap();
    }

    #[test]
    fn binary_search_keys_single() {
        let filename = "binary_search_keys_single.test";
        let description = "0: L[2] (0)";

        let tree = init_tree_from_description_in_file(filename, description);

        // less
        assert_eq!(tree.root.binary_search_keys(&1), Err(0));
        // equal
        assert_eq!(tree.root.binary_search_keys(&2), Ok(0));
        // greater
        assert_eq!(tree.root.binary_search_keys(&3), Err(1));

        drop(tree);
        fs::remove_file(filename).unwrap();
    }

    #[test]
    fn binary_search_keys_multiple() {
        // smaller
        let filename = "binary_search_keys_multiple.test";
        let description = "0: L[2, 4, 6] (0)";

        let tree = init_tree_from_description_in_file(filename, description);

        assert_eq!(tree.root.binary_search_keys(&1), Err(0));
        assert_eq!(tree.root.binary_search_keys(&2), Ok(0));
        assert_eq!(tree.root.binary_search_keys(&3), Err(1));
        assert_eq!(tree.root.binary_search_keys(&4), Ok(1));
        assert_eq!(tree.root.binary_search_keys(&5), Err(2));
        assert_eq!(tree.root.binary_search_keys(&6), Ok(2));
        assert_eq!(tree.root.binary_search_keys(&7), Err(3));

        drop(tree);
        fs::remove_file(filename).unwrap();

        // bigger
        let filename = "binary_search_keys_multiple.test";
        let description = "0: L[2, 4, 6, 8, 10] (0)";

        let tree = init_tree_from_description_in_file(filename, description);

        assert_eq!(tree.root.binary_search_keys(&1), Err(0));
        assert_eq!(tree.root.binary_search_keys(&2), Ok(0));
        assert_eq!(tree.root.binary_search_keys(&3), Err(1));
        assert_eq!(tree.root.binary_search_keys(&4), Ok(1));
        assert_eq!(tree.root.binary_search_keys(&5), Err(2));
        assert_eq!(tree.root.binary_search_keys(&6), Ok(2));
        assert_eq!(tree.root.binary_search_keys(&7), Err(3));
        assert_eq!(tree.root.binary_search_keys(&8), Ok(3));
        assert_eq!(tree.root.binary_search_keys(&9), Err(4));
        assert_eq!(tree.root.binary_search_keys(&10), Ok(4));
        assert_eq!(tree.root.binary_search_keys(&11), Err(5));

        drop(tree);
        fs::remove_file(filename).unwrap();
    }

    #[test]
    fn single_insertion() {
        let filename = "single_insertion.test";
        let expected_tree = "0: L[1] (0)";
        let expected_tree = trim_lines(expected_tree);

        let mut tree = init_tree_in_file(filename);
        tree.insert(1, 1).unwrap();

        assert_eq!(&tree.to_description(), &expected_tree);

        drop(tree);
        fs::remove_file(filename).unwrap();
    }

    #[test]
    fn leaf_root_insertion() {
        let filename = "leaf_root_insertion.test";
        let expected_tree = "
            0: L[1, 2, 3, 4, 5] (0)
        ";
        let expected_tree = trim_lines(expected_tree);

        let mut tree = init_tree_in_file(filename);

        for i in 1..=5 {
            tree.insert(i, i).unwrap();
            println!("inserted {i}");
        }

        assert_eq!(&tree.to_description(), &expected_tree);

        drop(tree);
        fs::remove_file(filename).unwrap();
    }

    #[test]
    fn leaf_root_split() {
        let filename = "leaf_root_insertion.test";
        let expected_tree = "
            0: [3] (2)
            0->0: L[1, 2, 3] (0)
            0->1: L[4, 5, 6] (0)
        ";
        let expected_tree = trim_lines(expected_tree);

        let mut tree = init_tree_in_file(filename);

        for i in 1..=6 {
            tree.insert(i, i).unwrap();
            println!("inserted {i}");
        }

        assert_eq!(&tree.to_description(), &expected_tree);

        drop(tree);
        fs::remove_file(filename).unwrap();
    }
}
