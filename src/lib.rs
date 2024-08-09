use std::fmt::Display;

use serde::{self, Deserialize, Serialize};

pub mod generate;
pub mod storage;

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub enum DbType {
    String(String),
    Integer(i32),
    Float(f32),
}
impl Display for DbType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Float(v) => v.fmt(f),
            Self::Integer(v) => v.fmt(f),
            Self::String(v) => v.fmt(f),
        }
    }
}
