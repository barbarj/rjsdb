use std::{collections::HashSet, fmt::Display, hash::Hash};

use generate::Generate;
use serde::{self, Deserialize, Serialize};

pub mod generate;
pub mod storage;

const DB_TYPE_COUNT: u32 = 3;
#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub enum DbType {
    String,
    Integer,
    Float,
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

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub enum DbValue {
    String(String),
    Integer(i32),
    Float(f32),
}
impl DbValue {
    pub fn db_type(self) -> DbType {
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
            Self::String(v) => v.fmt(f),
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
