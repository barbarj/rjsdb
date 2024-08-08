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

// / # Table Layout Notes
// /
// / These are layed out in this order on disk
// /
// / ## DB Header
// / - header_version: u16
// / - header_size: u32
// /
// / ## Metadata Table
// / ### Table header
// / - header_version: u16
// / - header_size: u32
// / - row_header_version: u16
// / - row_header_size: u32
// / - table_name_len: u32
// / - table_name: str
// / ### Rows (per-row)
// / #### Row Header
// / - row_size: u64
// / - row_id: u64
// / #### Row contents (in row-schema)
// /
