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
    storage::{self, read::from_bytes, write_to_table, Row},
    DbType,
};

fn gen_row(rng: &mut RNG) -> Row {
    let a = DbType::Float(f32::generate(rng));
    let b = DbType::Integer(i32::generate(rng));
    let c = DbType::String(String::generate(rng));

    Row {
        id: usize::generate(rng),
        data: Vec::from([a, b, c]),
    }
}

fn main() {
    // let mut rng = RNG::new();
    // let mut rows = Vec::new();
    // println!("IN:");
    // for _ in 0..10 {
    //     let row = gen_row(&mut rng);
    //     println!("{row}");
    //     rows.push(row);
    // }

    // let db_file = Path::new("./db.db");
    // write_to_table(db_file, &rows).unwrap();

    // let mut file = OpenOptions::new().read(true).open(db_file).unwrap();
    // let mut buff = Vec::new();
    // file.read_to_end(&mut buff).unwrap();

    // let read_rows: Vec<Row> = from_bytes(&buff).unwrap();

    // println!("OUT:");
    // for row in &read_rows {
    //     println!("{row}");
    // }

    // for (written, read) in zip(rows, read_rows) {
    //     assert_eq!(written, read);
    // }
    // println!("THERE THE SAME!!!!");

    let path = Path::new("db.db");
    if path.exists() {
        fs::remove_file(path).unwrap();
        println!("db file removed");
    }
    let mut db = storage::Database::init(path).unwrap();
    db.flush().unwrap();
    let t1 = db.db_header.last_modified;
    drop(db);

    sleep(Duration::from_secs(2));

    let db = storage::Database::init(path).unwrap();
    let t2 = db.db_header.last_modified;

    assert_eq!(t1, t2);
    println!("IT WORKS")
}
