use std::{
    fs::{self, OpenOptions},
    io::Read,
    iter::zip,
    path::Path,
};

use rjsdb::{
    generate::{Generate, RNG},
    storage::{read::from_bytes, write_to_table, Row},
    DbType,
};

fn gen_row(rng: &mut RNG) -> Row {
    let a = DbType::Float(f32::generate(rng));
    let b = DbType::Integer(i32::generate(rng));
    let c = DbType::String(String::generate(rng));

    Row {
        vals: Vec::from([a, b, c]),
    }
}

fn main() {
    let mut rng = RNG::new();
    let mut rows = Vec::new();
    println!("IN:");
    for _ in 0..10 {
        let row = gen_row(&mut rng);
        println!("{row}");
        rows.push(row);
    }

    let db_file = Path::new("./db.db");
    write_to_table(db_file, &rows).unwrap();

    let mut file = OpenOptions::new().read(true).open(db_file).unwrap();
    let mut buff = Vec::new();
    file.read_to_end(&mut buff).unwrap();

    let read_rows: Vec<Row> = from_bytes(&buff).unwrap();

    println!("OUT:");
    for row in &read_rows {
        println!("{row}");
    }

    for (written, read) in zip(rows, read_rows) {
        assert_eq!(written, read);
    }
    println!("THERE THE SAME!!!!");
}
