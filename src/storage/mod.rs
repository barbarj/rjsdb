use std::{
    fmt::{Display, Write as FmtWrite},
    fs,
    io::{self, Write},
    path::Path,
    str::Utf8Error,
};

use serde::{de, ser, Deserialize, Serialize};

use crate::DbType;

pub mod read;
pub mod write;

#[derive(Debug)]
pub enum SerdeError {
    Message(String),
    WritingError(io::Error),
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
            Self::WritingError(err) => err.fmt(f),
            Self::TrailingBytes => f.write_str("Trailing bytes left over."),
            Self::Eof => f.write_str("Reached end of input"),
            Self::UnparseableValue => f.write_str("Unparseable value"),
            Self::Utf8ParsingError(err) => err.fmt(f),
        }
    }
}
impl From<io::Error> for SerdeError {
    fn from(value: io::Error) -> Self {
        Self::WritingError(value)
    }
}
impl From<Utf8Error> for SerdeError {
    fn from(value: Utf8Error) -> Self {
        Self::Utf8ParsingError(value)
    }
}

// TODO: Privatize row
#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct Row {
    pub vals: Vec<DbType>,
}
impl Display for Row {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_char('(')?;
        for v in self.vals.iter() {
            v.fmt(f)?;
            f.write_char(',')?;
        }
        f.write_char(')')?;
        Ok(())
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
