use std::{
    collections::HashSet,
    fmt,
    hash::Hash,
    path::Path,
    sync::{Mutex, MutexGuard, PoisonError},
};

use generate::Generate;
use query::{QueryError, QueryResult, ResultRows};
use serde::{self, Deserialize, Serialize};
use storage::{Row, Schema, StorageError, StorageLayer};

pub mod generate;
pub mod query;
pub mod repl;
pub mod storage;

const DB_TYPE_COUNT: u32 = 4;
#[derive(Serialize, Deserialize, Debug, PartialEq, Copy, Clone)]
pub enum DbType {
    String,
    Integer,
    Float,
    UnsignedInt,
}
impl DbType {
    pub fn generate_val(&self, rng: &mut generate::RNG) -> DbValue {
        match self {
            Self::Float => DbValue::Float(DbFloat::generate(rng)),
            Self::Integer => DbValue::Integer(i64::generate(rng)),
            Self::String => DbValue::String(String::generate(rng)),
            Self::UnsignedInt => DbValue::UnsignedInt(u64::generate(rng)),
        }
    }

    pub fn coerceable_to(&self, other: &DbType) -> bool {
        matches!(
            (self, other),
            (DbType::Float, DbType::Float)
                | (DbType::Float, DbType::Integer)
                | (DbType::Float, DbType::UnsignedInt)
                | (DbType::Integer, DbType::Float)
                | (DbType::Integer, DbType::Integer)
                | (DbType::Integer, DbType::UnsignedInt)
                | (DbType::UnsignedInt, DbType::Float)
                | (DbType::UnsignedInt, DbType::Integer)
                | (DbType::UnsignedInt, DbType::UnsignedInt)
                | (DbType::String, DbType::String)
        )
    }
}
impl Generate for DbType {
    fn generate(rng: &mut generate::RNG) -> Self {
        assert_eq!(DB_TYPE_COUNT, 4);
        let choice = rng.next_value() % DB_TYPE_COUNT;
        match choice {
            0 => Self::String,
            1 => Self::Integer,
            2 => Self::Float,
            3 => Self::UnsignedInt,
            _ => panic!("Somehow got a number out of range!"),
        }
    }
}

/// Gaurantees that this float is finite, which means we
/// can enforce equality and total order on it.
#[derive(Serialize, Deserialize, Debug, PartialEq, Clone, PartialOrd)]
struct PrivateDbFloat {
    f: f64,
}
impl PrivateDbFloat {
    fn new(f: f64) -> Self {
        assert!(f.is_finite());
        PrivateDbFloat { f }
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct DbFloat {
    inner: PrivateDbFloat,
}
impl DbFloat {
    pub fn new(f: f64) -> Self {
        DbFloat {
            inner: PrivateDbFloat::new(f),
        }
    }
}
impl fmt::Display for DbFloat {
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
impl fmt::LowerExp for DbFloat {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.inner.f.fmt(formatter)
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone, PartialOrd, Eq, Ord)]
pub enum DbValue {
    String(String),
    Integer(i64),
    Float(DbFloat),
    UnsignedInt(u64),
}
impl DbValue {
    pub fn db_type(&self) -> DbType {
        match self {
            Self::Float(_) => DbType::Float,
            Self::Integer(_) => DbType::Integer,
            Self::String(_) => DbType::String,
            Self::UnsignedInt(_) => DbType::UnsignedInt,
        }
    }

    pub fn as_insertable_sql_str(&self) -> String {
        match self {
            Self::Float(v) => format!("{v:}"),
            Self::Integer(v) => format!("{v}"),
            Self::String(v) => format!("'{v}'"),
            Self::UnsignedInt(v) => format!("{v}"),
        }
    }

    /// Returns Some(_) if the coercion is possible,
    /// otherwise returns None. This coercion may be lossy.
    /// Does not coerce non-strings to strings
    fn coerced_to(&self, t: DbType) -> Option<Self> {
        match (t, self) {
            (DbType::Float, DbValue::Float(_)) => Some(self.clone()),
            (DbType::Float, DbValue::Integer(i)) => Some(DbValue::Float(DbFloat::new(*i as f64))),
            (DbType::Float, DbValue::UnsignedInt(i)) => {
                Some(DbValue::Float(DbFloat::new(*i as f64)))
            }
            (DbType::Integer, DbValue::Float(f)) => Some(DbValue::Integer(f.inner.f as i64)),
            (DbType::Integer, DbValue::Integer(_)) => Some(self.clone()),
            (DbType::Integer, DbValue::UnsignedInt(i)) => Some(DbValue::Integer(*i as i64)),
            (DbType::UnsignedInt, DbValue::Float(f)) => {
                Some(DbValue::UnsignedInt(f.inner.f as u64))
            }
            (DbType::UnsignedInt, DbValue::Integer(i)) => Some(DbValue::UnsignedInt(*i as u64)),
            (DbType::UnsignedInt, DbValue::UnsignedInt(_)) => Some(self.clone()),
            (DbType::String, DbValue::String(_)) => Some(self.clone()),
            _ => None,
        }
    }
}
impl fmt::Display for DbValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Float(v) => v.fmt(f),
            Self::Integer(v) => v.fmt(f),
            Self::String(v) => {
                let str = format!("\"{v}\"");
                str.fmt(f)
            }
            Self::UnsignedInt(v) => v.fmt(f),
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

#[derive(Debug)]
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

pub trait TableKnowledge {
    fn table_exists(&self, name: &str) -> bool;
    fn table_schema(&self, name: &str) -> Result<Schema>;
}

pub struct Database {
    storage: Mutex<StorageLayer>,
}
impl Database {
    pub fn init(db_file: &Path) -> Result<Self> {
        let storage = StorageLayer::init(db_file)?;
        Ok(Database {
            storage: Mutex::new(storage),
        })
    }

    pub fn execute(&mut self, command: &str) -> Result<()> {
        let mut tx = self.transaction()?;
        tx.prepare(command)?.execute()?;
        tx.commit()?;
        Ok(())
    }

    pub fn transaction(&mut self) -> Result<Transaction> {
        Ok(Transaction {
            storage: self.storage.lock()?,
        })
    }
}
impl TableKnowledge for Database {
    fn table_exists(&self, name: &str) -> bool {
        self.storage.lock().unwrap().table_exists(name)
    }

    fn table_schema(&self, name: &str) -> Result<Schema> {
        let schema = self.storage.lock().unwrap().table_schema(name)?.clone();
        Ok(schema)
    }
}

pub struct Transaction<'tx> {
    storage: MutexGuard<'tx, StorageLayer>,
}
impl<'tx> Transaction<'tx> {
    pub fn prepare<'a>(&'a mut self, stmt: &'a str) -> Result<Statement<'a>> {
        Ok(Statement {
            storage: &mut self.storage,
            statement: stmt,
        })
    }

    pub fn commit(mut self) -> Result<()> {
        self.storage.flush()?;
        Ok(())
    }

    pub fn execute<'a>(&'a mut self, command: &'a str) -> Result<()> {
        self.prepare(command)?.execute()?;
        Ok(())
    }
}
impl<'tx> TableKnowledge for Transaction<'tx> {
    fn table_exists(&self, name: &str) -> bool {
        self.storage.table_exists(name)
    }

    fn table_schema(&self, name: &str) -> Result<Schema> {
        let schema = self.storage.table_schema(name)?;
        Ok(schema.clone())
    }
}

#[derive(Debug)]
pub struct ReturnedRows {
    rows: Vec<Row>,
    schema: Schema,
}
impl From<ResultRows<'_>> for ReturnedRows {
    fn from(value: ResultRows) -> Self {
        let schema = value.schema().into_owned();
        let rows = value.map(|r| r.into_owned()).collect();
        ReturnedRows { rows, schema }
    }
}

#[derive(Debug)]
pub enum DatabaseResult {
    NothingToDo,
    Ok,
    Rows(ReturnedRows),
}

pub struct Statement<'stmt> {
    storage: &'stmt mut StorageLayer,
    statement: &'stmt str,
}
impl<'stmt> Statement<'stmt> {
    pub fn execute(&mut self) -> Result<DatabaseResult> {
        let query_res = query::execute(self.statement, self.storage)?;
        let res = match query_res {
            QueryResult::NothingToDo => DatabaseResult::NothingToDo,
            QueryResult::Ok => DatabaseResult::Ok,
            QueryResult::Rows(rows) => DatabaseResult::Rows(ReturnedRows::from(rows)),
        };
        Ok(res)
    }

    pub fn commit(&mut self) -> Result<()> {
        self.storage.flush()?;
        Ok(())
    }
}
impl<'stmt> TableKnowledge for Statement<'stmt> {
    fn table_exists(&self, name: &str) -> bool {
        self.storage.table_exists(name)
    }

    fn table_schema(&self, name: &str) -> Result<Schema> {
        let schema = self.storage.table_schema(name)?;
        Ok(schema.clone())
    }
}
