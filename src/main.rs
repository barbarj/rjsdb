use std::path::Path;

use rjsdb::{
    generate::{Generate, RNG},
    repl::Repl,
    storage::Row,
    DataAccess, Database, DatabaseError, TableKnowledge, Transaction,
};

// TODO:
// - fix possible integer overflow during type coercion
//   - research how other dbs handle this
// - transactions in repl
//   - requires table locks,
// - host repl on a my website
// - add tests for parser, execution
// - missing options for trawler testing
// - figure out how to do read-only stuff with unmutable references
// - "stackable"/"traversable" errors when in dev build
// - better data structure for representing schema/primary key/etc, to allow:
//    - showing which col is the pk in repl
// - Figure out how to manage database connections and accept requests
//   - This'll be a client/server model, and the server probably will
//     need something like tokio to manage threads/requests
// - unsigned type (for ids, etc) (will require some schema-aware type coercion)

fn wrapped_join<'a>(input: impl Iterator<Item = &'a str>) -> String {
    let mut str = String::from("(");
    for item in input {
        str += item;
        str += ", ";
    }
    // remove the last ', '
    _ = str.pop();
    _ = str.pop();
    str += ")";
    str
}

fn gen_rows(count: usize, table_name: &str, tx: &mut Transaction, rng: &mut RNG) {
    let schema = tx.table_schema(table_name).unwrap().clone();
    for row in (0..count).map(|_| schema.gen_row(rng)) {
        let columns_str = wrapped_join(schema.columns().map(|c| c.name.as_str()));
        let values: Vec<String> = row.data.iter().map(|v| v.as_insertable_sql_str()).collect();
        let values_str = wrapped_join(values.iter().map(|s| s.as_str()));
        let stmt = format!(
            "INSERT INTO {table_name} {} values {};",
            columns_str, values_str
        );
        println!("{stmt}");
        tx.execute(&stmt).unwrap();
    }
}

fn test_prepare_gen_rows(count: usize, tx: &mut Transaction, rng: &mut RNG) {
    for _ in 0..count {
        let stmt = "INSERT INTO the_mf_table (id, foo, bar, baz) VALUES (:id, :foo, :bar, :baz);";
        // let params = [
        //     (":id", i64::generate(rng)),
        //     (":foo", String::generate(rng)),
        //     (":bar", i64::generate(rng)),
        //     (":baz", f64::generate(rng)),
        // ];
        tx.prepare(stmt)
            .execute((
                (":id", i64::generate(rng)),
                (":foo", String::generate(rng)),
                (":bar", i64::generate(rng)),
                (":baz", f64::generate(rng)),
            ))
            .unwrap();
    }
}

fn main() {
    let path = Path::new("db.db");
    let mut db = Database::init(path).unwrap();

    // let mut rng = RNG::new();
    // if !db.table_exists("the_mf_table") {
    //     let create_table =
    //         "CREATE TABLE IF NOT EXISTS the_mf_table (id integer primary key, foo string, bar integer, baz float);";
    //     db.execute(create_table).unwrap();
    //     let mut tx = db.transaction().unwrap();
    //     gen_rows(30, "the_mf_table", &mut tx, &mut rng);
    //     tx.commit().unwrap();
    // }
    // // println!("{}", db.table_schema("the_mf_table").unwrap());
    // let mut tx = db.transaction().unwrap();
    // test_prepare_gen_rows(10, &mut tx, &mut rng);
    // tx.commit().unwrap();
    // println!("added");

    // let mut tx = db.transaction().unwrap();
    // let mut prepped = tx.prepare("select foo, bar, rowid from the_mf_table;");
    // let results: Result<Vec<(String, i64, usize)>, DatabaseError> = prepped
    //     .query()
    //     .unwrap()
    //     .mapped(|r: &Row| {
    //         let foo = r.get(0);
    //         println!("foo: {:?}", foo);
    //         let bar = r.get(1);
    //         println!("bar: {:?}", bar);
    //         let id = r.get(2);
    //         println!("id: {:?}", id);
    //         Ok((foo?, bar?, id?))
    //     })
    //     .collect();
    // for row in results.unwrap() {
    //     println!("{:?}", row);
    // }
    // drop(prepped);
    // tx.abort().unwrap();

    let mut repl = Repl::new();
    repl.run(&mut db).unwrap();

    // db.execute("CREATE TABLE IF NOT EXISTS _metadata(version UNSIGNED INT);")
    //     .unwrap();
    // let mut tx = db.transaction().unwrap();
    // // create table
    // let rows_changed = tx
    //     .execute(
    //         "CREATE TABLE IF NOT EXISTS posts( \
    //             link STRING PRIMARY KEY, \
    //             title STRING, \
    //             date STRING, \
    //             author STRING \
    //         );",
    //     )
    //     .unwrap();
    // assert_eq!(rows_changed, 0);

    // tx.execute("INSERT INTO _metadata(version) VALUES(1);")
    //     .unwrap();
    // tx.commit().unwrap();

    // db.execute("CREATE TABLE IF NOT EXISTS _metadata(version UNSIGNED INT);")
    //     .unwrap();

    // let version: Option<usize> = db
    //     .prepare("SELECT version FROM _metadata ORDER BY version DESC LIMIT 1;")
    //     .unwrap()
    //     .query()
    //     .unwrap()
    //     .mapped(|row: &Row| {
    //         let version: usize = row.get(0).unwrap();
    //         Ok(version)
    //     })
    //     .flatten()
    //     .next();

    // println!("version: {version:?}");
}
