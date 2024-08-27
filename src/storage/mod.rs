use std::{
    collections::HashMap,
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
    has_duplicates, DbType, DbValue,
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

    pub fn table_exists(&self, name: &str) -> bool {
        self.tables.iter().any(|t| t.header.table_name == name)
    }

    pub fn create_table(&mut self, name: &str, schema: &Schema) -> Result<()> {
        if self.table_exists(name) {
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
        let table = Table::new(name.to_string(), schema.clone());
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

    pub fn insert_rows(&mut self, table_name: &str, rows: Vec<Row>) -> Result<()> {
        let table = match self.table_mut(table_name) {
            Some(table) => table,
            None => return Err(StorageError::TableDoesNotExist),
        };
        table.insert_rows(rows)
    }

    pub fn delete_rows(&mut self, table_name: &str, ids: &[usize]) -> Result<()> {
        let table = match self.table_mut(table_name) {
            Some(table) => table,
            None => return Err(StorageError::TableDoesNotExist),
        };
        table.delete_rows(ids)
    }

    pub fn table_scan(&self, table_name: &str) -> Result<Rows> {
        let table = match self.table(table_name) {
            Some(table) => table,
            None => return Err(StorageError::TableDoesNotExist),
        };
        Ok(table.rows())
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
pub struct Table {
    header: TableHeader,
    rows: Vec<Row>,
    next_id: usize,
}
impl Table {
    pub fn new(table_name: String, schema: Schema) -> Self {
        Table {
            header: TableHeader::new(table_name, schema),
            rows: Vec::new(),
            next_id: 0,
        }
    }

    pub fn info(&self) -> String {
        format!(
            "{}: {} || {} rows",
            self.header.table_name,
            self.header.schema,
            self.rows.len()
        )
    }

    fn insert_rows(&mut self, rows: Vec<Row>) -> Result<()> {
        for mut row in rows {
            if !self.header.schema.matches(&row) {
                return Err(StorageError::SchemaDoesntMatch);
            }
            row.id = self.next_id;
            self.next_id += 1;
            self.rows.push(row);
        }
        Ok(())
    }

    fn delete_rows(&mut self, ids: &[usize]) -> Result<()> {
        self.rows.retain(|row| !ids.contains(&row.id));
        Ok(())
    }

    pub fn rows(&self) -> Rows {
        Rows {
            rows: &self.rows,
            schema: self.header.schema.clone(),
        }
    }
}

// TODO: Add reference to column list, and a way to get a specific columns value
#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct Row {
    id: usize,
    pub data: Vec<DbValue>,
}
impl Row {
    pub fn new(data: Vec<DbValue>) -> Self {
        Row { id: 0, data }
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
    pub rows: &'a [Row],
    pub schema: Schema,
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
