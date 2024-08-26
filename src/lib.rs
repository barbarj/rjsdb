use std::{
    borrow::Cow,
    cmp::{max, Ordering},
    collections::HashSet,
    fmt::Display,
    hash::Hash,
    io::{stdin, stdout, Write},
    iter::zip,
};

use generate::Generate;
use query::{execute, QueryResult, ResultRows};
use serde::{self, Deserialize, Serialize};
use storage::{Row, StorageLayer};

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
            Self::String(v) => {
                let str = format!("\"{v}\"");
                str.fmt(f)
            }
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

pub fn repl(storage: &mut StorageLayer) {
    let mut s = String::new();
    while !s.contains(';') {
        print!("> ");
        stdout().flush().unwrap();
        stdin().read_line(&mut s).unwrap();
        if s.trim() == "exit" {
            break;
        }
        match execute(s.trim(), storage) {
            Err(err) => println!("{err:?}"),
            Ok(QueryResult::Ok) => println!("ok"),
            Ok(QueryResult::Rows(rows)) => display_rows(rows),
        }
        s.clear();
    }
    storage.flush().unwrap();
}

fn print_row(col_widths: &[usize], row: &Row) {
    for (val, width) in zip(row.data.iter(), col_widths) {
        print!("| {:<width$} ", val);
    }
    println!("|");
}

fn row_width(col_widths: &[usize]) -> usize {
    let row_width: usize = col_widths.iter().sum(); // string widths themselves
    let row_width = row_width + (col_widths.len() * 3); // to account for spacing and dividers;
    row_width + 1 // last dividider;
}

fn display_rows(rows: ResultRows) {
    // limit to 20 rows, mainly to not dump a crazy amount of
    // data on the user.
    let schema = rows.schema();
    let all_rows: Vec<Cow<Row>> = rows.take(20).collect();
    let name_widths: Vec<usize> = schema.columns().map(|c| c.name.len()).collect();
    let col_widths = all_rows.iter().fold(name_widths, |widths, row| {
        let row_widths = row.data.iter().map(|x| format!("{x}").len());
        zip(widths, row_widths).map(|(a, b)| max(a, b)).collect()
    });

    let divider = "-".repeat(row_width(&col_widths));

    // header
    println!("{}", divider);
    for (col, width) in zip(schema.columns(), col_widths.iter()) {
        print!("| {:<width$} ", col.name);
    }
    println!("|");
    println!("{}", divider);

    // body
    for row in all_rows {
        print_row(&col_widths, &row);
    }

    println!("{}", divider);
}
