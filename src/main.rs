use std::path::Path;

use rjsdb::{
    generate::{Generate, RNG},
    storage,
};

fn main() {
    let mut rng = RNG::new();

    let path = Path::new("db.db");
    // if path.exists() {
    //     fs::remove_file(path).unwrap();
    //     println!("db file removed");
    // }
    let mut db = storage::Database::init(path).unwrap();
    let mut name = String::generate(&mut rng);
    name.truncate(5);
    let name = String::from("test_table");
    // let schema = Schema::generate(&mut rng);
    let schema = db.table_schema(&name).unwrap();
    // db.create_table(&name, &schema).unwrap();

    let mut rows = Vec::new();
    for _ in 0..20 {
        rows.push(schema.gen_row(&mut rng))
    }

    db.insert_rows(&name, rows).unwrap();
    db.show_table_info();
    db.flush().unwrap();

    drop(db);

    let mut db = storage::Database::init(path).unwrap();
    db.show_table_info();

    // assert_eq!(db.table_scan(&name).unwrap().count(), 20);
    let removed_ids: Vec<usize> = db
        .table_scan(&name)
        .unwrap()
        .map(|row| row.id)
        .filter(|id| id % 2 == 0)
        .collect();
    db.delete_rows(&name, &removed_ids).unwrap();
    // assert_eq!(db.table_scan(&name).unwrap().count(), 10);
    db.show_table_info();
    let ids: Vec<usize> = db.table_scan(&name).unwrap().map(|row| row.id).collect();
    println!("{ids:?}");
    db.flush().unwrap();
}
