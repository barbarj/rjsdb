use std::{
    io::{self, Read, Write},
    rc::Rc,
};

/*
 * Goal: Paging System
 * Subgoals:
 * - Get row disk format figured out, and be able to read and write rows
 *   to a byte buffer arbitrarily
 * - Get page data structure working. Be able to interact with data in page
 *   - header fields
 *   - slotted page setup
 *   - see if I can map bytes directly to/from struct (will need to think about struct padding)
 * - Read/Write pages from/to disk
 *   - block-device interface interaction
 * - For a page cache smaller than total number of pages, implement LRU rules
 * - Ability to pin pages
 */

mod serialize;

pub enum StorageError {
    IoError(io::Error),
}
impl From<io::Error> for StorageError {
    fn from(value: io::Error) -> Self {
        Self::IoError(value)
    }
}

type Result<T> = std::result::Result<T, StorageError>;

trait Serialize {
    fn write_to_bytes(&self, dest: &mut impl Write) -> Result<()>;
}

pub struct NumericCfg {
    // TODO: Both of these values should probably be a smaller type.
    // Figure out what that type should be.
    max_precision: usize,
    max_scale: usize,
}

pub enum NumericValueSign {
    Positive,
    Negative,
    NaN,
}

// TODO: Postgres also supports Inf, -Inf, and NAN as Numeric values. Add support for them
pub struct NumericValue {
    total_digits: u16,
    first_group_weight: u16,
    sign: NumericValueSign,
    digits: Vec<u16>, // each member is a group of 4 digits. Stored in base 10000. Most significant
                      // to least significant
}

pub struct Char {
    v: String,
}
impl Char {
    pub fn value(&self) -> &str {
        &self.v
    }
}

// TODO: Make it so:
// both date and time (no time zone)
// Low value: 4713 BC
// High value: 294276 AD
// Resolution: 1 microsecond
pub struct Timestamp {
    v: u64,
}

// TODO: Maybe convert these to boxed types to decrease aggregate memory usage
pub enum DbValue {
    Numeric(NumericValue),
    Integer(i32),
    Varchar(String),
    Char(Char),
    Double(f64),
    Timestamp(Timestamp),
}

#[derive(Clone)]
pub enum DbType {
    Numeric(Rc<NumericCfg>),
    Integer,
    Varchar,
    Char(u32),
    Double,
    Timestamp,
}

type Schema = Vec<DbType>;

struct Row {
    data: Vec<DbValue>,
    schema: Rc<Schema>,
}

/*impl Serialize for DbValue {
    fn write_to_bytes(&self, dest: impl Write) {
        match self {
        }
    }
*/
