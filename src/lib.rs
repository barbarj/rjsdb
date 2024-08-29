use std::{
    collections::HashSet,
    fmt::Display,
    hash::Hash,
    path::Path,
    sync::{Mutex, MutexGuard, PoisonError},
};

use generate::Generate;
use query::{QueryError, QueryResult};
use serde::{self, Deserialize, Serialize};
use storage::{Row, StorageError, StorageLayer};

pub mod generate;
pub mod query;
pub mod repl;
pub mod storage;

const DB_TYPE_COUNT: u32 = 3;
#[derive(Serialize, Deserialize, Debug, PartialEq, Copy, Clone)]
pub enum DbType {
    String,
    Integer,
    Float,
}
impl DbType {
    pub fn generate_val(&self, rng: &mut generate::RNG) -> DbValue {
        match self {
            Self::Float => DbValue::Float(DbFloat::generate(rng)),
            Self::Integer => DbValue::Integer(i32::generate(rng)),
            Self::String => DbValue::String(String::generate(rng)),
        }
    }
}
impl Generate for DbType {
    fn generate(rng: &mut generate::RNG) -> Self {
        assert_eq!(DB_TYPE_COUNT, 3);
        let choice = rng.next_value() % DB_TYPE_COUNT;
        match choice {
            0 => Self::String,
            1 => Self::Integer,
            2 => Self::Float,
            _ => panic!("Somehow got a number out of range!"),
        }
    }
}

/// Gaurantees that this float is finite, which means we
/// can enforce equality and total order on it.
#[derive(Serialize, Deserialize, Debug, PartialEq, Clone, PartialOrd)]
struct PrivateDbFloat {
    f: f32,
}
impl PrivateDbFloat {
    fn new(f: f32) -> Self {
        assert!(f.is_finite());
        PrivateDbFloat { f }
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct DbFloat {
    inner: PrivateDbFloat,
}
impl DbFloat {
    pub fn new(f: f32) -> Self {
        DbFloat {
            inner: PrivateDbFloat::new(f),
        }
    }
}
impl Display for DbFloat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.inner.f.fmt(f)
    }
}
impl Eq for DbFloat {
    fn assert_receiver_is_total_eq(&self) {}
}
#[allow(clippy::non_canonical_partial_ord_impl)]
impl PartialOrd for DbFloat {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.inner.f.partial_cmp(&other.inner.f)
    }
}
impl Ord for DbFloat {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match self.partial_cmp(other) {
            Some(ord) => ord,
            None => panic!("This should be impossible, since all DbFloats must be finite"),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone, PartialOrd, Eq, Ord)]
pub enum DbValue {
    String(String),
    Integer(i32),
    Float(DbFloat),
}
impl DbValue {
    pub fn db_type(&self) -> DbType {
        match self {
            Self::Float(_) => DbType::Float,
            Self::Integer(_) => DbType::Integer,
            Self::String(_) => DbType::String,
        }
    }

    pub fn as_insertable_sql_str(&self) -> String {
        match self {
            Self::Float(v) => format!("{v:.1}"),
            Self::Integer(v) => format!("{v}"),
            Self::String(v) => format!("'{v}'"),
        }
    }
}
impl Display for DbValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Float(v) => v.fmt(f),
            Self::Integer(v) => v.fmt(f),
            Self::String(v) => {
                let str = format!("\"{v}\"");
                str.fmt(f)
            }
        }
    }
}
// impl Ord for DbValue {
//     fn cmp(&self, other: &Self) -> std::cmp::Ordering {
//     }
// }

fn has_duplicates<I, T>(seq: T) -> bool
where
    I: Eq + Hash,
    T: Iterator<Item = I>,
{
    let mut seen = HashSet::new();
    for i in seq {
        if seen.contains(&i) {
            return true;
        }
        seen.insert(i);
    }
    false
}

pub enum DatabaseError {
    StorageError(StorageError),
    QueryError(QueryError),
    MutexError,
}
impl From<StorageError> for DatabaseError {
    fn from(value: StorageError) -> Self {
        Self::StorageError(value)
    }
}
impl From<QueryError> for DatabaseError {
    fn from(value: QueryError) -> Self {
        Self::QueryError(value)
    }
}
impl From<PoisonError<MutexGuard<'_, StorageLayer>>> for DatabaseError {
    fn from(_: PoisonError<MutexGuard<'_, StorageLayer>>) -> Self {
        Self::MutexError
    }
}

type Result<T> = std::result::Result<T, DatabaseError>;

struct Database {
    storage: Mutex<StorageLayer>,
}
impl Database {
    pub fn new(db_file: &Path) -> Result<Self> {
        let storage = StorageLayer::init(db_file)?;
        Ok(Database {
            storage: Mutex::new(storage),
        })
    }

    pub fn prepare<'stmt>(&'stmt mut self, stmt: &'stmt str) -> Result<Statement<'stmt>> {
        Ok(Statement {
            storage: self.storage.lock()?,
            statement: stmt,
        })
    }

    pub fn execute(&mut self, command: &str) -> Result<()> {
        _ = self.prepare(command)?.execute()?;
        Ok(())
    }
}

struct Statement<'stmt> {
    storage: MutexGuard<'stmt, StorageLayer>,
    statement: &'stmt str,
}
impl<'stmt> Statement<'stmt> {
    pub fn execute(&mut self) -> Result<Vec<Row>> {
        let query_res = query::execute(self.statement, &mut self.storage)?;
        let res = match query_res {
            QueryResult::NothingToDo => Vec::new(),
            QueryResult::Ok => Vec::new(),
            QueryResult::Rows(rows) => rows.map(|x| x.into_owned()).collect(),
        };
        Ok(res)
    }
}
