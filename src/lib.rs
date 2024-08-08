use serde::{self, Deserialize, Serialize};

pub mod generate;
pub mod storage;

#[derive(Serialize, Deserialize)]
enum DbType {
    String(String),
    Integer(i32),
    Float(f32),
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
