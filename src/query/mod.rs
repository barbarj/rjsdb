use tokenize::Tokenizer;

use crate::storage::{Row, StorageError};

mod parse;
mod tokenize;

enum QueryError {
    StorageError(StorageError),
}
enum QueryResult {
    Ok,
    Rows(QueryRows),
}

type Result<T> = std::result::Result<T, QueryError>;

pub fn execute(command: &str) -> Result<QueryResult> {
    let tokenizer = Tokenizer::new(command);
    // let logical_plan = parse::parse(tokens);
    // parse sql into query plan
    // use tree-sitter for now to generate the AST?
    // run query plan

    Ok(QueryResult::Ok)
}

// type QueryIterator = Iterator<Item = Row>;

struct QueryRows {
    rows: Vec<Row>,
}
impl<Iter> From<Iter> for QueryRows
where
    Iter: Iterator<Item = Row>,
{
    fn from(value: Iter) -> Self {
        QueryRows {
            rows: value.collect(),
        }
    }
}

struct Select<Iter>
where
    Iter: Iterator<Item = Row>,
{
    input: Iter,
}
impl<Iter> Iterator for Select<Iter>
where
    Iter: Iterator<Item = Row>,
{
    type Item = Row;

    fn next(&mut self) -> Option<Self::Item> {
        self.input.next()
    }
}

struct Filter<Iter>
where
    Iter: Iterator<Item = Row>,
{
    input: Iter,
    predicate: fn(&Row) -> bool,
}
impl<Iter> Iterator for Filter<Iter>
where
    Iter: Iterator<Item = Row>,
{
    type Item = Row;

    fn next(&mut self) -> Option<Self::Item> {
        self.input.find(self.predicate)
    }
}

/// Stores rows in reverse order, so first item is at end
/// of the vec, giving O(1) access with pop()
struct Sort<Iter, K>
where
    Iter: Iterator<Item = Row>,
    K: Ord,
{
    input: Iter,
    rows: Vec<Row>,
    is_sorted: bool,
    key_fn: fn(&Row) -> K,
    desc: bool,
}
impl<Iter, K> Sort<Iter, K>
where
    Iter: Iterator<Item = Row>,
    K: Ord,
{
    fn new(input: Iter, key_fn: fn(&Row) -> K, desc: bool) -> Self {
        Sort {
            input,
            rows: Vec::new(),
            is_sorted: false,
            key_fn,
            desc,
        }
    }
}
impl<Iter, K> Iterator for Sort<Iter, K>
where
    Iter: Iterator<Item = Row>,
    K: Ord,
{
    type Item = Row;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.is_sorted {
            for row in &mut self.input {
                self.rows.push(row);
            }
            self.rows.sort_by_key(self.key_fn);
            if !self.desc {
                self.rows.reverse();
            }
            self.is_sorted = true;
        }

        self.rows.pop()
    }
}
