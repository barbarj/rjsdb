use std::{fmt::Display, string::FromUtf8Error};

use serde::{de, ser};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    Message(String),
    ExpectedBool,
    ExpectedUtf8String(FromUtf8Error),
    ExpectedChar,
    ExpectedOption,
}
impl ser::Error for Error {
    fn custom<T>(msg: T) -> Self
    where
        T: std::fmt::Display,
    {
        Error::Message(msg.to_string())
    }
}
impl de::Error for Error {
    fn custom<T>(msg: T) -> Self
    where
        T: std::fmt::Display,
    {
        Error::Message(msg.to_string())
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Message(msg) => f.write_str(msg),
            Self::ExpectedBool => f.write_str("Expected a boolean"),
            Self::ExpectedUtf8String(err) => f.write_str(&format!(
                "Expected a valid utf8 string. The bytes were valid up to index: {}",
                err.utf8_error().valid_up_to()
            )),
            Self::ExpectedChar => f.write_str("Expected a char"),
            Self::ExpectedOption => f.write_str("Expected an Option"),
        }
    }
}

impl std::error::Error for Error {}

impl From<std::io::Error> for Error {
    fn from(value: std::io::Error) -> Self {
        Self::Message(value.to_string())
    }
}
