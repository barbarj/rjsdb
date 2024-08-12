use std::{
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

#[derive(Serialize, Deserialize, Debug)]
pub struct Database {
    #[serde(skip)]
    file: Option<File>,
    pub db_header: DbHeader,
    tables: Vec<Table>,
}
impl Database {
    pub fn init(db_file: &Path) -> Result<Self, SerdeError> {
        if db_file.exists() {
            Database::from_file(db_file)
        } else {
            Database::new(db_file)
        }
    }

    fn from_file(db_file: &Path) -> Result<Self, SerdeError> {
        let mut file = OpenOptions::new().read(true).write(true).open(db_file)?;
        let mut buff = Vec::new();
        file.read_to_end(&mut buff)?;
        let mut db: Database = read::from_bytes(&buff)?;
        db.file = Some(file);
        Ok(db)
    }

    fn new(db_file: &Path) -> Result<Self, SerdeError> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(db_file)?;
        let mut db = Database {
            file: Some(file),
            db_header: DbHeader::new(),
            tables: Vec::new(),
        };
        db.flush()?;
        Ok(db)
    }

    pub fn flush(&mut self) -> Result<(), SerdeError> {
        let mut file = self.file.as_ref().expect("File should be set by now");
        file.rewind()?;
        file.set_len(0)?;
        self.db_header.last_modified = Utc::now();
        write::to_writer(&mut file, self)?;
        file.flush()?;
        Ok(())
    }

    fn table_exists(&self, name: &str) -> bool {
        self.tables
            .iter()
            .find(|t| t.header.table_name == name)
            .is_some()
    }

    pub fn create_table(&mut self, name: &str, schema: &Schema) -> Result<(), SerdeError> {
        if self.table_exists(&name) {
            return Err(SerdeError::TableAlreadyExists);
        }
        if name.len() == 0 {
            return Err(SerdeError::EmptyTableName);
        }
        if schema.schema.len() == 0 {
            return Err(SerdeError::EmptySchemaProvided);
        }
        if has_duplicates(schema.schema.iter().map(|c| &c.name)) {
            return Err(SerdeError::DuplicateColumnNames);
        }
        let table = Table::new(name.to_string(), schema.clone());
        self.tables.push(table);
        self.flush()
    }

    pub fn destroy_table(&mut self, name: &str) -> Result<(), SerdeError> {
        if !self.table_exists(name) {
            return Err(SerdeError::TableDoesNotExist);
        }

        // find table index
        let idx = || -> usize {
            for index in 0..self.tables.len() {
                if self.tables[index].header.table_name == name {
                    return index;
                }
            }
            panic!("Should never happen");
        }();

        self.tables.swap_remove(idx);
        self.flush()
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

    pub fn insert_rows(&mut self, table_name: &str, rows: Vec<Row>) -> Result<(), SerdeError> {
        let table = match self.table_mut(table_name) {
            Some(table) => table,
            None => return Err(SerdeError::TableDoesNotExist),
        };
        table.insert_rows(rows)
    }

    pub fn table_scan(&self, table_name: &str) -> Result<impl Iterator<Item = &Row>, SerdeError> {
        let table = match self.table(table_name) {
            Some(table) => table,
            None => return Err(SerdeError::TableDoesNotExist),
        };
        Ok(table.rows())
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
    name: String,
    _type: DbType,
}
impl Display for Column {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{} ({:?})", self.name, self._type))
    }
}
impl Generate for Column {
    fn generate(rng: &mut crate::generate::RNG) -> Self {
        let mut name = String::generate(rng);
        while name.len() == 0 {
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
pub struct Schema {
    schema: Vec<Column>,
}
impl Schema {
    pub fn new(cols: Vec<Column>) -> Self {
        Schema { schema: cols }
    }

    pub fn matches(&self, row: &Row) -> bool {
        let our_types = self.schema.iter().map(|c| &c._type);
        let their_types = row.data.iter().map(|v| v.db_type());
        zip(our_types, their_types).all(|(a, b)| *a == b)
    }

    pub fn gen_row(&self, rng: &mut RNG) -> Row {
        let mut data = Vec::new();
        for col in self.schema.iter() {
            data.push(col._type.generate_val(rng));
        }
        Row {
            id: usize::generate(rng),
            data,
        }
    }
}
impl Display for Schema {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_char('[')?;
        let mut first = true;
        for c in self.schema.iter() {
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
        Schema { schema: cols }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Table {
    header: TableHeader,
    rows: Vec<Row>,
}
impl Table {
    pub fn new(table_name: String, schema: Schema) -> Self {
        Table {
            header: TableHeader::new(table_name, schema),
            rows: Vec::new(),
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

    fn insert_rows(&mut self, rows: Vec<Row>) -> Result<(), SerdeError> {
        for row in rows {
            if !self.header.schema.matches(&row) {
                return Err(SerdeError::SchemaDoesntMatch);
            }
            self.rows.push(row);
        }
        Ok(())
    }

    pub fn rows(&self) -> impl Iterator<Item = &Row> {
        self.rows.iter()
    }
}

// TODO: Privatize row
#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct Row {
    pub id: usize,
    pub data: Vec<DbValue>,
}
impl Row {
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

#[derive(Debug)]
pub enum SerdeError {
    Message(String),
    IoError(io::Error),
    TrailingBytes,
    Eof,
    UnparseableValue,
    Utf8ParsingError(std::str::Utf8Error),

    // These probably should be a different error type
    TableAlreadyExists,
    TableDoesNotExist,
    DuplicateColumnNames,
    EmptyTableName,
    EmptySchemaProvided,
    SchemaDoesntMatch,
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
            Self::TableAlreadyExists => f.write_str("Table already exists"),
            Self::TableDoesNotExist => f.write_str("The requested table does not exist"),
            Self::DuplicateColumnNames => f.write_str("Duplicate column names found"),
            Self::EmptyTableName => f.write_str("An empty table name was provided"),
            Self::EmptySchemaProvided => f.write_str("Empty schema provided"),
            Self::SchemaDoesntMatch => f.write_str("Non-matching schema provided"),
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
