use std::{
    fmt::{Display, Write as FmtWrite},
    fs::{File, OpenOptions},
    io::{self, Read, Seek, Write},
    path::Path,
    str::Utf8Error,
};

use chrono::{DateTime, Utc};
use serde::{de, ser, Deserialize, Serialize};

use crate::{generate::Generate, DbType, DbValue};

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

    pub fn create_table(&mut self, name: String, schema: Schema) -> Result<(), SerdeError> {
        if self.table_exists(&name) {
            return Err(SerdeError::TableAlreadyExists);
        }
        // TODO: Verify no column name duplicates
        let table = Table::new(name, schema);
        self.tables.push(table);
        self.flush()
    }

    pub fn show_table_info(&self) {
        for t in self.tables.iter() {
            println!("{}", t.info());
        }
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

#[derive(Serialize, Deserialize, Debug)]
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
        name.truncate(6);
        Column {
            name,
            _type: DbType::generate(rng),
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Schema {
    schema: Vec<Column>,
}
impl Schema {
    pub fn new(cols: Vec<Column>) -> Self {
        Schema { schema: cols }
    }
}
impl Display for Schema {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_char('[')?;
        for c in self.schema.iter() {
            c.fmt(f)?;
        }
        f.write_char(']')
    }
}
impl Generate for Schema {
    fn generate(rng: &mut crate::generate::RNG) -> Self {
        let col_count = rng.next_value() % 5;
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
        format!("{}: {}", self.header.table_name, self.header.schema)
    }
}

// TODO: Privatize row
#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct Row {
    pub id: usize,
    pub data: Vec<DbValue>,
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
    TableAlreadyExists,
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
