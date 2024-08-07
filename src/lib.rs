use core::str;
use std::error::Error;

pub mod generate;

enum PersistedDataParsingError {
    ConversionErr(Box<dyn Error>),
    DBTypeError,
}

struct ParseResult<T> {
    res: T,
    bytes_read: usize,
}

trait Persistable {
    fn to_persistent_bytes(&self) -> Vec<u8>;
    // TODO: Try to do this withouth needing to box the original error
    fn from_peristent_bytes(bytes: &[u8]) -> Result<ParseResult<Self>, PersistedDataParsingError>
    where
        Self: Sized;
}

enum DbType {
    String(String),
    Integer(i32),
    Float(f32),
}
impl DbType {
    fn persistent_int(&self) -> u8 {
        match self {
            Self::String(_) => 1,
            Self::Integer(_) => 2,
            Self::Float(_) => 3,
        }
    }
}
impl Persistable for DbType {
    fn to_persistent_bytes(&self) -> Vec<u8> {
        // TODO: Make this do fewer passes over the output bytes
        let type_bytes = Vec::from(self.persistent_int().to_ne_bytes());
        let val_bytes = match self {
            Self::String(str) => str.to_persistent_bytes(),
            Self::Integer(i) => i.to_persistent_bytes(),
            Self::Float(f) => f.to_persistent_bytes(),
        };
        [type_bytes, val_bytes].concat()
    }

    fn from_peristent_bytes(
        bytes: &[u8],
    ) -> Result<ParseResult<DbType>, PersistedDataParsingError> {
        let type_int = bytes[0];
        match type_int {
            1 => {
                let s = String::from_peristent_bytes(&bytes[1..])?;
                Ok(ParseResult {
                    res: DbType::String(s.res),
                    bytes_read: s.bytes_read + 1,
                })
            }
            2 => {
                let i = i32::from_peristent_bytes(&bytes[1..])?;
                Ok(ParseResult {
                    res: DbType::Integer(i.res),
                    bytes_read: i.bytes_read + 1,
                })
            }
            3 => {
                let f = f32::from_peristent_bytes(&bytes[1..])?;
                Ok(ParseResult {
                    res: DbType::Float(f.res),
                    bytes_read: f.bytes_read + 1,
                })
            }
            _ => return Err(PersistedDataParsingError::DBTypeError),
        }
    }
}

impl Persistable for String {
    fn to_persistent_bytes(&self) -> Vec<u8> {
        let len_bytes = Vec::from(self.len().to_ne_bytes());
        let val_bytes = Vec::from(self.as_bytes());
        [len_bytes, val_bytes].concat()
    }

    fn from_peristent_bytes(
        bytes: &[u8],
    ) -> Result<ParseResult<String>, PersistedDataParsingError> {
        let len_byte_array: [u8; 8] = bytes[0..8]
            .try_into()
            .map_err(|err| PersistedDataParsingError::ConversionErr(Box::new(err)))?;
        let len = usize::from_ne_bytes(len_byte_array);

        let str_bytes = Vec::from(&bytes[8..8 + len]);
        assert_eq!(str_bytes.len(), len);
        let s = String::from_utf8(str_bytes)
            .map_err(|err| PersistedDataParsingError::ConversionErr(Box::new(err)))?;
        Ok(ParseResult {
            res: s,
            bytes_read: len + 8,
        })
    }
}

impl Persistable for i32 {
    fn to_persistent_bytes(&self) -> Vec<u8> {
        Vec::from(self.to_ne_bytes())
    }

    fn from_peristent_bytes(bytes: &[u8]) -> Result<ParseResult<i32>, PersistedDataParsingError> {
        let val_bytes = bytes[0..4]
            .try_into()
            .map_err(|err| PersistedDataParsingError::ConversionErr(Box::new(err)))?;
        Ok(ParseResult {
            res: i32::from_ne_bytes(val_bytes),
            bytes_read: 4,
        })
    }
}

impl Persistable for f32 {
    fn to_persistent_bytes(&self) -> Vec<u8> {
        Vec::from(self.to_ne_bytes())
    }

    fn from_peristent_bytes(bytes: &[u8]) -> Result<ParseResult<f32>, PersistedDataParsingError> {
        let val_bytes = bytes[0..4]
            .try_into()
            .map_err(|err| PersistedDataParsingError::ConversionErr(Box::new(err)))?;
        Ok(ParseResult {
            res: f32::from_ne_bytes(val_bytes),
            bytes_read: 4,
        })
    }
}

/// # Table Layout Notes
///
/// These are layed out in this order on disk
///
/// ## DB Header
/// - header_version: u16
/// - header_size: u32
///
/// ## Metadata Table
/// ### Table header
/// - header_version: u16
/// - header_size: u32
/// - row_header_version: u16
/// - row_header_size: u32
/// - table_name_len: u32
/// - table_name: str
/// ### Rows (per-row)
/// #### Row Header
/// - row_size: u64
/// - row_id: u64
/// #### Row contents (in row-schema)
///
///

pub mod storage {
    use std::{
        fs,
        io::{Error, Write},
        path::Path,
    };

    use crate::{DbType, Persistable};

    // fn init_db(db_file: &Path) -> Result<(), Error> {
    //     let file = fs::OpenOptions::new()
    //         .read(true)
    //         .write(true)
    //         .truncate(true)
    //         .open(db_file)?;

    //     // look for _metadata table. Create if does not exist.

    //     Ok(())
    // }

    // fn create_table(name: &str, schema: Vec<DbType>) -> Result {

    // }

    struct Row {
        vals: Vec<DbType>,
    }
    impl Persistable for Row {
        fn to_persistent_bytes(&self) -> Vec<u8> {
            let mut buffer = Vec::new();

            for v in self.vals.iter() {
                let mut res = v.to_persistent_bytes();
                buffer.append(&mut res);
            }

            buffer
        }

        fn from_peristent_bytes(
            bytes: &[u8],
        ) -> Result<ParseResult<Row>, PersistedDataParsingError> {
            // TODO: WIP
        }
    }

    fn write_to_table(db_file: &Path, rows: Vec<Row>) -> Result<(), Error> {
        let mut file = fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(db_file)?;

        for row in rows {
            file.write(row.to_persistent_bytes().as_ref())?;
        }

        file.flush()?;

        Ok(())
    }
}
