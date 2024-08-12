use std::{
    fs::{self, OpenOptions},
    io::Read,
    iter::zip,
    os::unix::fs::MetadataExt,
    path::Path,
    thread::sleep,
    time::Duration,
};

use rjsdb::{
    generate::{Generate, RNG},
    storage::{self, read::from_bytes, Row, Schema},
    DbValue,
};

// TODO:
// - Delete rows (how do other systems do this??)
// - table scan access

fn main() {
    let mut rng = RNG::new();

    let path = Path::new("db.db");
    if path.exists() {
        fs::remove_file(path).unwrap();
        println!("db file removed");
    }
    let mut db = storage::Database::init(path).unwrap();
    let mut name = String::generate(&mut rng);
    name.truncate(5);
    let schema = Schema::generate(&mut rng);
    db.create_table(&name, &schema).unwrap();

    let mut rows = Vec::new();
    for _ in 0..20 {
        rows.push(schema.gen_row(&mut rng))
    }

    db.insert_rows(&name, rows).unwrap();
    db.show_table_info();
    db.flush().unwrap();

    drop(db);

    let db = storage::Database::init(path).unwrap();
    db.show_table_info();
}
