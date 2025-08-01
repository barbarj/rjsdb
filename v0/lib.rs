use std::{
    borrow::Cow,
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
    InvalidTypeMapping,
    RowPositionInvalid,
    QueryDidNotReturnRows,
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

    pub fn execute(&mut self, command: &str) -> Result<usize> {
        let affected = self.prepare(command)?.execute([])?;
        Ok(affected)
    }

    pub fn transaction(&mut self) -> Result<Transaction> {
        let lock = self.storage.lock()?;
        Ok(Transaction { storage: lock })
    }

    pub fn commit(&mut self) -> Result<()> {
        self.storage.lock()?.flush()?;
        Ok(())
    }

    pub fn abort(&mut self) -> Result<()> {
        self.storage.lock()?.reload()?;
        Ok(())
    }

    pub fn prepare<'a>(&'a mut self, stmt: &'a str) -> Result<PreparedStatement<'a>> {
        Ok(PreparedStatement {
            storage: MaybeLockedStorage::HoldingLock(self.storage.lock()?),
            statement: stmt,
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
    pub fn prepare<'a>(&'a mut self, stmt: &'a str) -> PreparedStatement<'a> {
        PreparedStatement {
            storage: MaybeLockedStorage::NotHoldingLock(&mut self.storage),
            statement: stmt,
        }
    }

    pub fn commit(mut self) -> Result<()> {
        self.storage.flush()?;
        Ok(())
    }

    pub fn abort(mut self) -> Result<()> {
        self.storage.reload()?;
        Ok(())
    }

    pub fn execute(&mut self, command: &str) -> Result<usize> {
        let affected = self.prepare(command).execute([])?;
        Ok(affected)
    }
}
impl TableKnowledge for Transaction<'_> {
    fn table_exists(&self, name: &str) -> bool {
        self.storage.table_exists(name)
    }

    fn table_schema(&self, name: &str) -> Result<Schema> {
        let schema = self.storage.table_schema(name)?;
        Ok(schema.clone())
    }
}

enum RowContents<'a> {
    Filled(ResultRows<'a>),
    Empty,
}

pub struct Rows<'a> {
    rows: RowContents<'a>,
}
impl<'a> Rows<'a> {
    fn new(rows: RowContents<'a>) -> Self {
        Rows { rows }
    }

    pub fn mapped<F>(self, map_fn: F) -> MappedResults<'a, F> {
        MappedResults::new(self.rows, map_fn)
    }
}
impl<'a> Iterator for Rows<'a> {
    type Item = Cow<'a, Row>;

    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.rows {
            RowContents::Empty => None,
            RowContents::Filled(rows) => rows.next(),
        }
    }
}

pub struct MappedResults<'a, F> {
    rows: RowContents<'a>,
    map_fn: F,
}
impl<'a, F> MappedResults<'a, F> {
    fn new(rows: RowContents<'a>, map_fn: F) -> Self {
        MappedResults { rows, map_fn }
    }
}
impl<T, F> Iterator for MappedResults<'_, F>
where
    F: Fn(&Row) -> Result<T>,
{
    type Item = Result<T>;

    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.rows {
            RowContents::Empty => None,
            RowContents::Filled(rows) => rows.next().map(|r| (self.map_fn)(&r)),
        }
    }
}

enum MaybeLockedStorage<'stmt> {
    HoldingLock(MutexGuard<'stmt, StorageLayer>),
    NotHoldingLock(&'stmt mut StorageLayer),
}

pub struct PreparedStatement<'stmt> {
    storage: MaybeLockedStorage<'stmt>,
    statement: &'stmt str,
}
impl PreparedStatement<'_> {
    pub fn execute<P: Params>(&mut self, params: P) -> Result<usize> {
        let bound_statement = params.bind_to(self.statement);
        match &mut self.storage {
            MaybeLockedStorage::HoldingLock(lock) => {
                let res = match query::execute(&bound_statement, lock)? {
                    QueryResult::NothingToDo => 0,
                    QueryResult::Ok(affected) => affected,
                    QueryResult::Rows(_) => 0,
                };
                lock.flush()?;
                Ok(res)
            }
            MaybeLockedStorage::NotHoldingLock(storage) => {
                match query::execute(&bound_statement, storage)? {
                    QueryResult::NothingToDo => Ok(0),
                    QueryResult::Ok(affected) => Ok(affected),
                    QueryResult::Rows(_) => Ok(0),
                }
            }
        }
    }

    pub fn query(&mut self) -> Result<Rows<'_>> {
        let res = match &mut self.storage {
            MaybeLockedStorage::HoldingLock(lock) => query::execute(self.statement, lock)?,
            MaybeLockedStorage::NotHoldingLock(storage) => query::execute(self.statement, storage)?,
        };
        match res {
            QueryResult::NothingToDo => Ok(Rows::new(RowContents::Empty)),
            QueryResult::Ok(_) => Ok(Rows::new(RowContents::Empty)),
            QueryResult::Rows(rows) => Ok(Rows::new(RowContents::Filled(rows))),
        }
    }
}
impl TableKnowledge for PreparedStatement<'_> {
    fn table_exists(&self, name: &str) -> bool {
        match &self.storage {
            MaybeLockedStorage::HoldingLock(lock) => lock.table_exists(name),
            MaybeLockedStorage::NotHoldingLock(storage) => storage.table_exists(name),
        }
    }

    fn table_schema(&self, name: &str) -> Result<Schema> {
        let schema = match &self.storage {
            MaybeLockedStorage::HoldingLock(lock) => lock.table_schema(name)?,
            MaybeLockedStorage::NotHoldingLock(storage) => storage.table_schema(name)?,
        };
        Ok(schema.clone())
    }
}

pub trait Params {
    fn bind_to(&self, target: &str) -> String;
}
impl<T: ToSql> Params for &[(&str, T)] {
    fn bind_to(&self, target: &str) -> String {
        let mut bound = target.to_string();
        for (target, replacement) in *self {
            bound = bound.replace(target, replacement.to_sql().as_ref());
        }
        bound
    }
}
impl Params for &[(&str, &dyn ToSql)] {
    fn bind_to(&self, target: &str) -> String {
        let mut bound = target.to_string();
        for (target, replacement) in *self {
            bound = bound.replace(target, replacement.to_sql().as_ref());
        }
        bound
    }
}
impl Params for [&dyn ToSql; 0] {
    fn bind_to(&self, target: &str) -> String {
        target.to_string()
    }
}
// TODO: Figure out how to write a macro to generate code for abitrary tuple sizes
impl<T, U, V, W> Params for ((&str, T), (&str, U), (&str, V), (&str, W))
where
    T: ToSql,
    U: ToSql,
    V: ToSql,
    W: ToSql,
{
    fn bind_to(&self, target: &str) -> String {
        // TODO: Do this in a smarter way with a smarter type/function
        let mut bound = target.replace(self.0 .0, self.0 .1.to_sql().as_ref());
        bound = bound.replace(self.1 .0, self.1 .1.to_sql().as_ref());
        bound = bound.replace(self.2 .0, self.2 .1.to_sql().as_ref());
        bound = bound.replace(self.3 .0, self.3 .1.to_sql().as_ref());
        bound
    }
}

trait ToSql {
    fn to_sql(&self) -> String;
}
impl ToSql for String {
    fn to_sql(&self) -> String {
        format!("\"{}\"", escape_str(self))
    }
}
impl ToSql for &String {
    fn to_sql(&self) -> String {
        format!("\"{}\"", escape_str(self))
    }
}
impl ToSql for &str {
    fn to_sql(&self) -> String {
        format!("\"{}\"", escape_str(self))
    }
}
impl ToSql for f64 {
    fn to_sql(&self) -> String {
        format!("{:}", self)
    }
}
impl ToSql for i64 {
    fn to_sql(&self) -> String {
        self.to_string()
    }
}
impl ToSql for u64 {
    fn to_sql(&self) -> String {
        self.to_string()
    }
}
impl ToSql for usize {
    fn to_sql(&self) -> String {
        self.to_string()
    }
}

pub trait FromSql: Sized {
    fn from_sql(sql_val: &DbValue) -> Result<Self>;
}
impl FromSql for String {
    fn from_sql(sql_val: &DbValue) -> Result<Self> {
        match sql_val {
            DbValue::String(s) => Ok(s.clone()),
            _ => Err(DatabaseError::InvalidTypeMapping),
        }
    }
}
impl FromSql for f64 {
    fn from_sql(sql_val: &DbValue) -> Result<Self> {
        match sql_val {
            DbValue::Float(f) => Ok(f.inner.f),
            _ => Err(DatabaseError::InvalidTypeMapping),
        }
    }
}
impl FromSql for u64 {
    fn from_sql(sql_val: &DbValue) -> Result<Self> {
        match sql_val {
            DbValue::UnsignedInt(i) => Ok(*i),
            _ => Err(DatabaseError::InvalidTypeMapping),
        }
    }
}
impl FromSql for i64 {
    fn from_sql(sql_val: &DbValue) -> Result<Self> {
        match sql_val {
            DbValue::Integer(i) => Ok(*i),
            _ => Err(DatabaseError::InvalidTypeMapping),
        }
    }
}
impl FromSql for usize {
    fn from_sql(sql_val: &DbValue) -> Result<Self> {
        match sql_val {
            DbValue::UnsignedInt(i) => Ok(usize::try_from(*i).unwrap()),
            _ => Err(DatabaseError::InvalidTypeMapping),
        }
    }
}

pub trait DataAccess {
    fn get<T: FromSql>(&self, idx: usize) -> Result<T>;
}
impl DataAccess for Row {
    fn get<T: FromSql>(&self, idx: usize) -> Result<T> {
        match self.data.get(idx) {
            None => Err(DatabaseError::RowPositionInvalid),
            Some(v) => T::from_sql(v),
        }
    }
}

fn escape_str(input: &str) -> String {
    let mut lookbehind = '\0';
    let mut parts = input.split(|c| {
        let mut res = false;
        if c == '"' && lookbehind != '\\' {
            res = true;
        }
        lookbehind = c;
        res
    });

    let mut escaped = String::with_capacity(input.len());

    // add first part so we can add escaped quotes between the rest easily
    escaped += match parts.next() {
        Some(part) => part,
        None => return escaped,
    };
    parts.fold(escaped, |mut accum, part| {
        accum += "\\\"";
        accum += part;
        accum
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn escape_str_escapes() {
        let input = "a \" b \" c \" d";
        let expected = "a \\\" b \\\" c \\\" d";
        let actual = escape_str(input);
        assert_eq!(expected, actual);
    }

    #[test]
    fn escaped_str_doesnt_escaped_already_escaped() {
        let input = "foo \\\" bar";
        let expected = "foo \\\" bar";
        let actual = escape_str(input);
        assert_eq!(expected, actual);
    }
}
