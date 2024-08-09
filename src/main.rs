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

fn gen_row(rng: &mut RNG) -> Row {
    let a = DbValue::Float(f32::generate(rng));
    let b = DbValue::Integer(i32::generate(rng));
    let c = DbValue::String(String::generate(rng));

    Row {
        id: usize::generate(rng),
        data: Vec::from([a, b, c]),
    }
}

fn main() {
    let mut rng = RNG::new();

    let path = Path::new("db.db");
    if path.exists() {
        fs::remove_file(path).unwrap();
        println!("db file removed");
    }
    let mut db = storage::Database::init(path).unwrap();
    for _ in 0..3 {
        let mut name = String::generate(&mut rng);
        name.truncate(5);
        db.create_table(name, Schema::generate(&mut rng)).unwrap();
    }
    db.show_table_info();
    drop(db);

    let db = storage::Database::init(path).unwrap();
    db.show_table_info();
}
