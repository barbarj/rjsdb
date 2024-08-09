use std::{
    fmt::{Display, Write as FmtWrite},
    fs::{self, File, OpenOptions},
    io::{self, Read, Seek, Write},
    path::Path,
    str::Utf8Error,
};

use chrono::{DateTime, Utc};
use serde::{de, ser, Deserialize, Serialize};

use crate::DbType;

pub mod read;
pub mod write;

// NOTE: This implementation is intenationally stupid right now. We re-write the entire db file on every commit!.
// Good first change would be to figure out how to make that partial

#[derive(Serialize, Deserialize, Debug)]
pub struct Database {
    #[serde(skip)]
    file: Option<File>,
    pub db_header: DbHeader,
    metadata_table: Table,
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
        let db_header = DbHeader {
            header_version: DB_HEADER_VERSION,
            last_modified: Utc::now(),
        };
        let metadata_table = Table {
            header: TableHeader {
                header_version: TABLE_HEADER_VERSION,
                row_header_version: ROW_HEADER_VERSION,
                table_name: String::from("__metadata"),
            },
            rows: Vec::new(),
        };
        let mut db = Database {
            file: Some(file),
            db_header,
            metadata_table,
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
}

const DB_HEADER_VERSION: u16 = 0;
#[derive(Serialize, Deserialize, Debug)]
pub struct DbHeader {
    header_version: u16,
    pub last_modified: DateTime<Utc>,
}

const TABLE_HEADER_VERSION: u16 = 0;
const ROW_HEADER_VERSION: u16 = 0;
#[derive(Serialize, Deserialize, Debug)]
pub struct TableHeader {
    header_version: u16,
    row_header_version: u16,
    table_name: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Table {
    header: TableHeader,
    rows: Vec<Row>,
}
// TODO: Privatize row
#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct Row {
    pub id: usize,
    pub data: Vec<DbType>,
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

pub fn write_to_table(db_file: &Path, rows: &Vec<Row>) -> Result<(), SerdeError> {
    let mut file = fs::OpenOptions::new()
        .write(true)
        .truncate(true)
        .create(true)
        .open(db_file)?;
    write::to_writer(&mut file, &rows)?;
    file.flush()?;
    Ok(())
}
