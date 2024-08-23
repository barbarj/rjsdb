use std::{cmp::Ordering, collections::HashSet, fmt::Display, hash::Hash};

use generate::Generate;
use serde::{self, Deserialize, Serialize};

pub mod generate;
pub mod query;
pub mod storage;

const DB_TYPE_COUNT: u32 = 3;
#[derive(Serialize, Deserialize, Debug, PartialEq, Copy, Clone)]
pub enum DbType {
    String,
    Integer,
    Float,
}
impl DbType {
    pub fn generate_val(&self, rng: &mut generate::RNG) -> DbValue {
        match self {
            Self::Float => DbValue::Float(f32::generate(rng)),
            Self::Integer => DbValue::Integer(i32::generate(rng)),
            Self::String => DbValue::String(String::generate(rng)),
        }
    }
}
impl Generate for DbType {
    fn generate(rng: &mut generate::RNG) -> Self {
        assert_eq!(DB_TYPE_COUNT, 3);
        let choice = rng.next_value() % DB_TYPE_COUNT;
        match choice {
            0 => Self::String,
            1 => Self::Integer,
            2 => Self::Float,
            _ => panic!("Somehow got a number out of range!"),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone, PartialOrd)]
pub enum DbValue {
    String(String),
    Integer(i32),
    Float(f32),
}
impl DbValue {
    pub fn db_type(&self) -> DbType {
        match self {
            Self::Float(_) => DbType::Float,
            Self::Integer(_) => DbType::Integer,
            Self::String(_) => DbType::String,
        }
    }
}
impl Display for DbValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Float(v) => v.fmt(f),
            Self::Integer(v) => v.fmt(f),
            Self::String(v) => f.write_fmt(format_args!("\"{v}\"")),
        }
    }
}
impl Eq for DbValue {}
// TODO: Handle this issue
#[allow(clippy::derive_ord_xor_partial_ord)]
impl Ord for DbValue {
    // TODO: Do the setup to make this ordering actually safe
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Self::String(s1), Self::String(s2)) => s1.cmp(s2),
            (Self::Integer(i1), Self::Integer(i2)) => i1.cmp(i2),
            (Self::Float(f1), Self::Float(f2)) => {
                f1.partial_cmp(f2).expect("Should always have valid order")
            }
            _ => panic!("Non-supported comparison!"),
        }
    }
}

fn has_duplicates<I, T>(seq: T) -> bool
where
    I: Eq + Hash,
    T: Iterator<Item = I>,
{
    let mut seen = HashSet::new();
    for i in seq {
        if seen.contains(&i) {
            return true;
        }
        seen.insert(i);
    }
    false
}
