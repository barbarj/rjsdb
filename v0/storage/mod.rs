use std::{
    borrow::Cow,
    collections::{BTreeSet, HashMap},
    fmt::{Display, Write as FmtWrite},
    fs::{File, OpenOptions},
    io::{self, Read, Seek, Write},
    iter::zip,
    path::Path,
    str::Utf8Error,
};

use chrono::{DateTime, Utc};
use serde::{de, ser, Deserialize, Serialize};

use crate::{
    generate::{Generate, RNG},
    has_duplicates, DbFloat, DbType, DbValue,
};

pub mod read;
pub mod write;

// NOTE: This implementation is intenationally stupid right now. We re-write the entire db file on every commit!.
// Good first change would be to figure out how to make that partial

#[derive(Debug)]
pub enum StorageError {
    SerdeError(SerdeError),
    TableAlreadyExists,
    TableDoesNotExist,
    DuplicateColumnNames,
    EmptyTableName,
    EmptySchemaProvided,
    SchemaDoesntMatch,
    UniquenessConstraintViolated,
    UnkownPrimaryKeyColumn,
    UnknownColumnNameProvided,
    NonIndexedConflictColumn,
    ReservedColumnName,
}
impl Display for StorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SerdeError(serde_err) => serde_err.fmt(f),
            Self::TableAlreadyExists => f.write_str("Table already exists"),
            Self::TableDoesNotExist => f.write_str("The requested table does not exist"),
            Self::DuplicateColumnNames => f.write_str("Duplicate column names found"),
            Self::EmptyTableName => f.write_str("An empty table name was provided"),
            Self::EmptySchemaProvided => f.write_str("Empty schema provided"),
            Self::SchemaDoesntMatch => f.write_str("Non-matching schema provided"),
            Self::UniquenessConstraintViolated => {
                f.write_str("A uniqueness constraint was violated")
            }
            Self::UnkownPrimaryKeyColumn => f.write_str("Unknown primary key column provided"),
            Self::UnknownColumnNameProvided => f.write_str("Unknown column name provided"),
            Self::NonIndexedConflictColumn => {
                f.write_str("A non-indexed column name was provided as part of a conlict rule")
            }
            Self::ReservedColumnName => f.write_str("A column using a reserved name was provided"),
        }
    }
}
impl From<SerdeError> for StorageError {
    fn from(value: SerdeError) -> Self {
        Self::SerdeError(value)
    }
}
impl From<io::Error> for StorageError {
    fn from(value: io::Error) -> Self {
        Self::SerdeError(SerdeError::from(value))
    }
}

type Result<T> = std::result::Result<T, StorageError>;

#[derive(Deserialize, Debug)]
struct DeserializableStorageLayer {
    db_header: DbHeader,
    tables: Vec<Table>,
}
impl DeserializableStorageLayer {
    fn into_storage_layer(self, file: File) -> StorageLayer {
        StorageLayer {
            file,
            db_header: self.db_header,
            tables: self.tables,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct StorageLayer {
    #[serde(skip)]
    file: File,
    pub db_header: DbHeader,
    tables: Vec<Table>,
}
impl StorageLayer {
    pub fn init(db_file: &Path) -> Result<Self> {
        if db_file.exists() {
            StorageLayer::from_file(db_file)
        } else {
            StorageLayer::new(db_file)
        }
    }

    fn from_file(db_file: &Path) -> Result<Self> {
        let mut file = OpenOptions::new().read(true).write(true).open(db_file)?;
        let mut buff = Vec::new();
        file.read_to_end(&mut buff)?;
        let ser_db: DeserializableStorageLayer = read::from_bytes(&buff)?;
        let db = ser_db.into_storage_layer(file);
        Ok(db)
    }

    fn new(db_file: &Path) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(db_file)?;
        let db = StorageLayer {
            file,
            db_header: DbHeader::new(),
            tables: Vec::new(),
        };
        Ok(db)
    }

    pub fn flush(&mut self) -> Result<()> {
        // temporary file reference to allow borrow of self in to_writer
        let mut file = self.file.try_clone()?;
        file.rewind()?;
        file.set_len(0)?;
        self.db_header.last_modified = Utc::now();
        write::to_writer(&mut file, self)?;
        file.flush()?;
        Ok(())
    }

    pub fn reload(&mut self) -> Result<()> {
        let mut buff = Vec::new();
        self.file.rewind()?;
        self.file.read_to_end(&mut buff)?;
        let ser_db: DeserializableStorageLayer = read::from_bytes(&buff)?;
        self.db_header = ser_db.db_header;
        self.tables = ser_db.tables;
        Ok(())
    }

    pub fn table_exists(&self, name: &str) -> bool {
        self.tables.iter().any(|t| t.header.table_name == name)
    }

    pub fn create_table(
        &mut self,
        name: String,
        schema: Schema,
        primary_key_col: PrimaryKey,
    ) -> Result<()> {
        if self.table_exists(&name) {
            return Err(StorageError::TableAlreadyExists);
        }
        if name.is_empty() {
            return Err(StorageError::EmptyTableName);
        }
        if schema.schema.is_empty() {
            return Err(StorageError::EmptySchemaProvided);
        }
        if has_duplicates(schema.columns().map(|c| c.name.as_str())) {
            return Err(StorageError::DuplicateColumnNames);
        }
        if schema
            .schema
            .keys()
            .map(|x| x.to_lowercase())
            .any(|x| x == "rowid")
        {
            return Err(StorageError::ReservedColumnName);
        }
        let table = Table::build(name, schema, primary_key_col)?;
        self.tables.push(table);
        Ok(())
    }

    pub fn destroy_table(&mut self, name: &str) -> Result<()> {
        let idx = self.tables.iter().position(|t| t.header.table_name == name);
        let idx = match idx {
            Some(idx) => idx,
            None => return Err(StorageError::TableDoesNotExist),
        };

        self.tables.swap_remove(idx);
        Ok(())
    }

    pub fn show_table_info(&self) {
        for t in self.tables.iter() {
            println!("{}", t.info());
        }
        println!("------------");
    }

    pub fn table_row_count(&self, table_name: &str) -> Result<usize> {
        match self.table(table_name) {
            None => Err(StorageError::TableDoesNotExist),
            Some(table) => Ok(table.rows.len()),
        }
    }

    fn table_mut(&mut self, table_name: &str) -> Option<&mut Table> {
        self.tables
            .iter_mut()
            .find(|t| t.header.table_name == table_name)
    }

    fn table(&self, table_name: &str) -> Option<&Table> {
        self.tables
            .iter()
            .find(|t| t.header.table_name == table_name)
    }

    pub fn insert_rows(
        &mut self,
        table_name: &str,
        rows: &[Row],
        conflict_rule: Option<ConflictRule>,
    ) -> Result<usize> {
        let table = match self.table_mut(table_name) {
            Some(table) => table,
            None => return Err(StorageError::TableDoesNotExist),
        };
        table.insert_rows(rows, conflict_rule)
    }

    pub fn delete_rows(&mut self, table_name: &str, ids: &[usize]) -> Result<usize> {
        let table = match self.table_mut(table_name) {
            Some(table) => table,
            None => return Err(StorageError::TableDoesNotExist),
        };
        table.delete_rows(ids)
    }

    pub fn table_scan(&self, table_name: &str, with_row_id: bool) -> Result<Rows> {
        let table = match self.table(table_name) {
            Some(table) => table,
            None => return Err(StorageError::TableDoesNotExist),
        };
        Ok(table.rows(with_row_id))
    }

    pub fn table_schema(&self, table_name: &str) -> Result<&Schema> {
        let table = match self.table(table_name) {
            Some(table) => table,
            None => return Err(StorageError::TableDoesNotExist),
        };
        Ok(&table.header.schema)
    }
}

const DB_HEADER_VERSION: u16 = 0;
#[derive(Serialize, Deserialize, Debug)]
pub struct DbHeader {
    header_version: u16,
    pub last_modified: DateTime<Utc>,
}
impl DbHeader {
    pub fn new() -> Self {
        DbHeader {
            header_version: DB_HEADER_VERSION,
            last_modified: Utc::now(),
        }
    }
}
impl Default for DbHeader {
    fn default() -> Self {
        DbHeader::new()
    }
}

const TABLE_HEADER_VERSION: u16 = 0;
const ROW_HEADER_VERSION: u16 = 0;
#[derive(Serialize, Deserialize, Debug)]
pub struct TableHeader {
    header_version: u16,
    row_header_version: u16,
    table_name: String,
    schema: Schema,
}
impl TableHeader {
    pub fn new(table_name: String, schema: Schema) -> Self {
        TableHeader {
            header_version: TABLE_HEADER_VERSION,
            row_header_version: ROW_HEADER_VERSION,
            table_name,
            schema,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Column {
    pub name: String,
    pub _type: DbType,
}
impl Column {
    pub fn new(name: String, _type: DbType) -> Self {
        Column { name, _type }
    }

    pub fn with_name(&self, name: String) -> Self {
        Column {
            name,
            _type: self._type,
        }
    }
}
impl Display for Column {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{} ({:?})", self.name, self._type))
    }
}
impl Generate for Column {
    fn generate(rng: &mut crate::generate::RNG) -> Self {
        let mut name = String::generate(rng);
        while name.is_empty() {
            name = String::generate(rng);
        }
        name.truncate(6);
        Column {
            name,
            _type: DbType::generate(rng),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ColumnWithIndex {
    pub column: Column,
    pub index: usize,
}
impl ColumnWithIndex {
    fn new(column: Column, index: usize) -> Self {
        ColumnWithIndex { column, index }
    }
}

// TODO: Need to consider storing column order explicitly somewhere
//      so that we're not re-sorting it every time, or consider how to do
//      differently the things `columns()` is currently being used for.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Schema {
    schema: HashMap<String, ColumnWithIndex>,
}
impl Schema {
    pub fn new(schema: Vec<Column>) -> Self {
        let mut map = HashMap::new();
        for (index, col) in schema.into_iter().enumerate() {
            map.insert(col.name.clone(), ColumnWithIndex::new(col, index));
        }
        Schema { schema: map }
    }

    pub fn column_position(&self, name: &str) -> Option<usize> {
        self.schema.get(name).map(|ci| ci.index)
    }

    pub fn column(&self, name: &str) -> Option<&Column> {
        self.schema.get(name).map(|ci| &ci.column)
    }

    pub fn get(&self, name: &str) -> Option<&ColumnWithIndex> {
        self.schema.get(name)
    }

    pub fn matches(&self, row: &Row) -> bool {
        let our_count = self.schema.len();
        if row.data.len() != our_count {
            return false;
        }
        let our_types = self.columns().map(|c| c._type);
        let their_types = row.data.iter().map(|v| v.db_type());
        zip(our_types, their_types).all(|(a, b)| a == b)
    }

    pub fn columns(&self) -> impl Iterator<Item = &Column> {
        SchemaColumns::new(self)
    }

    pub fn gen_row(&self, rng: &mut RNG) -> Row {
        let mut data = Vec::new();
        for col in self.columns() {
            data.push(col._type.generate_val(rng));
        }
        Row::new(data)
    }

    pub fn column_value<'a>(&self, name: &str, row: &'a Row) -> Result<&'a DbValue> {
        let pos = match self.column_position(name) {
            Some(p) => p,
            None => return Err(StorageError::UnknownColumnNameProvided),
        };
        let val = match row.data.get(pos) {
            Some(v) => v,
            None => return Err(StorageError::SchemaDoesntMatch),
        };
        Ok(val)
    }

    pub fn remove(&mut self, name: &str) {
        let removed = self.schema.remove(name);
        match removed {
            None => (),
            Some(ci) => self
                .schema
                .iter_mut()
                .map(|(_, col_index)| {
                    if col_index.index > ci.index {
                        col_index.index -= 1;
                    }
                })
                .collect(),
        }
    }
}
impl Display for Schema {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_char('[')?;
        let mut first = true;
        for c in self.columns() {
            if !first {
                f.write_str(", ")?;
            }
            c.fmt(f)?;
            first = false;
        }
        f.write_char(']')
    }
}
impl Generate for Schema {
    fn generate(rng: &mut crate::generate::RNG) -> Self {
        let mut col_count = rng.next_value() % 5;
        while col_count == 0 {
            col_count = rng.next_value() % 5;
        }
        let col_count = col_count;
        let mut cols = Vec::new();
        for _ in 0..col_count {
            cols.push(Column::generate(rng));
        }
        Schema::new(cols)
    }
}

struct SchemaColumns<'a> {
    columns: Vec<&'a Column>,
    cursor: usize,
}
impl<'a> SchemaColumns<'a> {
    fn new(schema: &'a Schema) -> Self {
        let mut columns_with_index: Vec<&ColumnWithIndex> = schema.schema.values().collect();
        columns_with_index.sort_by_key(|ci| ci.index);
        let columns = columns_with_index.iter().map(|ci| &ci.column).collect();
        SchemaColumns { columns, cursor: 0 }
    }
}
impl<'a> Iterator for SchemaColumns<'a> {
    type Item = &'a Column;

    fn next(&mut self) -> Option<Self::Item> {
        if self.cursor >= self.columns.len() {
            return None;
        }
        let res = self.columns.get(self.cursor).copied();
        self.cursor += 1;
        res
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub enum PrimaryKey {
    Rowid,
    Column { col: Column, keyset: KeySet },
}

#[derive(Serialize, Deserialize, Debug)]
pub enum KeySet {
    Strings(BTreeSet<String>),
    Integers(BTreeSet<i64>),
    Floats(BTreeSet<DbFloat>),
    UnsignedInts(BTreeSet<u64>),
}
impl KeySet {
    pub fn contains(&self, v: &DbValue) -> bool {
        match (self, v) {
            (Self::Strings(set), DbValue::String(v)) => set.contains(v.as_str()),
            (Self::Integers(set), DbValue::Integer(v)) => set.contains(v),
            (Self::Floats(set), DbValue::Float(v)) => set.contains(v),
            (Self::UnsignedInts(set), DbValue::UnsignedInt(v)) => set.contains(v),
            _ => panic!("This assumes matching types"),
        }
    }

    pub fn insert(&mut self, v: DbValue) {
        match (self, v) {
            (Self::Strings(set), DbValue::String(v)) => set.insert(v),
            (Self::Integers(set), DbValue::Integer(v)) => set.insert(v),
            (Self::Floats(set), DbValue::Float(v)) => set.insert(v),
            (Self::UnsignedInts(set), DbValue::UnsignedInt(v)) => set.insert(v),
            _ => panic!("This assumes matching types"),
        };
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Table {
    header: TableHeader,
    rows: Vec<StorageRow>,
    next_id: usize,
    primary_key: PrimaryKey,
}
impl Table {
    pub fn build(table_name: String, schema: Schema, primary_key: PrimaryKey) -> Result<Self> {
        match &primary_key {
            PrimaryKey::Rowid => (),
            PrimaryKey::Column { col, keyset: _ } => {
                if schema.column(&col.name).is_none() {
                    return Err(StorageError::UnkownPrimaryKeyColumn);
                }
            }
        }
        Ok(Table {
            header: TableHeader::new(table_name, schema),
            rows: Vec::new(),
            next_id: 0,
            primary_key,
        })
    }

    pub fn info(&self) -> String {
        format!(
            "{}: {} || {} rows",
            self.header.table_name,
            self.header.schema,
            self.rows.len()
        )
    }

    fn primary_key_constraint_passes(&self, row: &Row) -> Result<bool> {
        match &self.primary_key {
            PrimaryKey::Rowid => Ok(true),
            PrimaryKey::Column { col, keyset } => {
                let val = self.header.schema.column_value(&col.name, row)?;
                Ok(!keyset.contains(val))
            }
        }
    }

    fn insert_rows(&mut self, rows: &[Row], conflict_rule: Option<ConflictRule>) -> Result<usize> {
        match (&conflict_rule, &self.primary_key) {
            (Some(rule), PrimaryKey::Column { col, keyset: _ }) if rule.column != col.name => {
                return Err(StorageError::NonIndexedConflictColumn);
            }
            _ => (),
        };
        let conflict_action = conflict_rule
            .map(|r| r.action)
            .unwrap_or(ConflictAction::Abort);

        let mut affected_rows = 0;
        for row in rows {
            if !self.header.schema.matches(row) {
                return Err(StorageError::SchemaDoesntMatch);
            }
            // verify constraint based on conflict rule
            if !self.primary_key_constraint_passes(row)? {
                match conflict_action {
                    ConflictAction::Nothing => continue,
                    ConflictAction::Abort => {
                        return Err(StorageError::UniquenessConstraintViolated)
                    }
                }
            }
            let storage_row = StorageRow {
                row: row.clone(),
                id: self.next_id,
            };
            self.next_id += 1;
            match &mut self.primary_key {
                PrimaryKey::Rowid => (),
                PrimaryKey::Column { col, keyset } => {
                    let v = self.header.schema.column_value(&col.name, row)?;
                    keyset.insert(v.clone());
                }
            }

            self.rows.push(storage_row);
            affected_rows += 1;
        }
        Ok(affected_rows)
    }

    fn delete_rows(&mut self, ids: &[usize]) -> Result<usize> {
        let initial_len = self.rows.len();
        self.rows.retain(|row| !ids.contains(&row.id));
        let after_len = self.rows.len();
        Ok(initial_len - after_len)
    }

    pub fn rows(&self, with_rowid: bool) -> Rows {
        Rows::new(&self.rows, with_rowid, &self.header.schema)
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
struct StorageRow {
    row: Row,
    id: usize,
}

// TODO: Add reference to column list, and a way to get a specific columns value
#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct Row {
    // id: usize,
    pub data: Vec<DbValue>,
}
impl Row {
    pub fn new(data: Vec<DbValue>) -> Self {
        Row { data }
    }

    pub fn schema(&self) -> Vec<DbType> {
        self.data.iter().map(|r| r.db_type()).collect()
    }
}
impl Display for Row {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_char('(')?;
        for v in self.data.iter() {
            v.fmt(f)?;
            f.write_char(',')?;
        }
        f.write_char(')')?;
        Ok(())
    }
}

pub struct Rows<'a> {
    rows: &'a [StorageRow],
    with_id: bool,
    cursor: usize,
    pub schema: Cow<'a, Schema>,
}
impl<'a> Rows<'a> {
    fn new(rows: &'a [StorageRow], with_id: bool, schema: &'a Schema) -> Self {
        let schema = if with_id {
            let mut schema = schema.clone();
            schema.schema.insert(
                String::from("rowid"),
                ColumnWithIndex {
                    column: Column::new(String::from("rowid"), DbType::UnsignedInt),
                    index: schema.schema.len(),
                },
            );
            Cow::Owned(schema)
        } else {
            Cow::Borrowed(schema)
        };
        Rows {
            rows,
            with_id,
            cursor: 0,
            schema,
        }
    }
}
impl<'a> Iterator for Rows<'a> {
    type Item = Cow<'a, Row>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.cursor >= self.rows.len() {
            return None;
        }
        let row = self.rows.get(self.cursor).map(|r| {
            if self.with_id {
                let mut row = r.row.clone();
                row.data.push(DbValue::UnsignedInt(r.id as u64));
                Cow::Owned(row)
            } else {
                Cow::Borrowed(&r.row)
            }
        });
        self.cursor += 1;
        row
    }
}

#[derive(Debug)]
pub enum SerdeError {
    Message(String),
    IoError(io::Error),
    TrailingBytes,
    Eof,
    UnparseableValue,
    Utf8ParsingError(std::str::Utf8Error),
}
impl std::error::Error for SerdeError {}
impl ser::Error for SerdeError {
    fn custom<T>(msg: T) -> Self
    where
        T: std::fmt::Display,
    {
        Self::Message(msg.to_string())
    }
}
impl de::Error for SerdeError {
    fn custom<T>(msg: T) -> Self
    where
        T: std::fmt::Display,
    {
        Self::Message(msg.to_string())
    }
}
impl Display for SerdeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Message(msg) => f.write_str(msg),
            Self::IoError(err) => err.fmt(f),
            Self::TrailingBytes => f.write_str("Trailing bytes left over."),
            Self::Eof => f.write_str("Reached end of input"),
            Self::UnparseableValue => f.write_str("Unparseable value"),
            Self::Utf8ParsingError(err) => err.fmt(f),
        }
    }
}
impl From<io::Error> for SerdeError {
    fn from(value: io::Error) -> Self {
        Self::IoError(value)
    }
}
impl From<Utf8Error> for SerdeError {
    fn from(value: Utf8Error) -> Self {
        Self::Utf8ParsingError(value)
    }
}

pub enum ConflictAction {
    Nothing,
    Abort,
}

pub struct ConflictRule {
    pub column: String,
    pub action: ConflictAction,
}
