use std::{
    io::{self, Write},
    rc::Rc,
};

use generate::{Generate, RNG};

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

mod generate; // TODO: This should probably be its own crate??
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

#[derive(Debug, PartialEq)]
pub struct NumericCfg {
    // TODO: Both of these values should probably be a smaller type.
    // Figure out what that type should be.
    max_precision: usize,
    max_scale: usize,
}

#[derive(Debug, PartialEq)]
pub enum NumericValueSign {
    Positive,
    Negative,
    NaN,
}
impl NumericValueSign {
    fn from_number(num: u8) -> Option<Self> {
        match num {
            0 => Some(Self::Negative),
            1 => Some(Self::Positive),
            2 => Some(Self::NaN),
            _ => None,
        }
    }
}
impl Generate for NumericValueSign {
    fn generate(rng: &mut RNG) -> Self {
        let num: u8 = (rng.next_value() % 3).try_into().unwrap();
        NumericValueSign::from_number(num).unwrap()
    }
}

// TODO: Postgres also supports Inf, -Inf, and NAN as Numeric values. Add support for them
#[derive(Debug, PartialEq)]
pub struct NumericValue {
    total_digits: u16,
    first_group_weight: u16,
    sign: NumericValueSign,
    digits: Vec<u16>, // each member is a group of 4 digits. Stored in base 10000. Most significant
                      // to least significant
}
impl Generate for NumericValue {
    // TODO: Make this take into account a NumericCfg
    fn generate(rng: &mut RNG) -> Self {
        let total_digits = u16::generate(rng);
        let first_group_weight = u16::generate(rng);
        let sign = NumericValueSign::generate(rng);
        let mut digits = Vec::with_capacity(total_digits.into());

        let digit_groups_count = if total_digits % 4 == 0 {
            total_digits / 4
        } else {
            (total_digits / 4) + 1
        };
        for _ in 0..digit_groups_count {
            digits.push(u16::generate(rng));
        }
        NumericValue {
            total_digits,
            first_group_weight,
            sign,
            digits,
        }
    }
}

#[derive(Debug, PartialEq)]
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
#[derive(Debug, PartialEq)]
pub struct Timestamp {
    v: u64,
}

// TODO: Maybe convert these to boxed types to decrease aggregate memory usage
#[derive(Debug, PartialEq)]
pub enum DbValue {
    Numeric(NumericValue),
    Integer(i32),
    Varchar(String),
    Char(Char),
    Double(f64),
    Timestamp(Timestamp),
}

#[derive(Debug, Clone, PartialEq)]
pub enum DbType {
    Numeric(Rc<NumericCfg>),
    Integer,
    Varchar,
    Char(u32),
    Double,
    Timestamp,
}
impl DbType {
    fn as_generated_value(&self, rng: &mut RNG) -> DbValue {
        match self {
            DbType::Numeric(_) => DbValue::Numeric(NumericValue::generate(rng)),
            DbType::Integer => DbValue::Integer(i32::generate(rng)),
            DbType::Varchar => DbValue::Varchar(String::generate(rng)),
            DbType::Char(size) => {
                let mut s = String::generate(rng);
                s.truncate(*size as usize);
                DbValue::Char(Char { v: s })
            }
            DbType::Double => DbValue::Double(f64::generate(rng)),
            DbType::Timestamp => DbValue::Timestamp(Timestamp {
                v: u64::generate(rng),
            }),
        }
    }
}

type Schema = Vec<DbType>;

#[derive(Debug, PartialEq)]
struct Row {
    data: Vec<DbValue>,
    schema: Rc<Schema>,
}
