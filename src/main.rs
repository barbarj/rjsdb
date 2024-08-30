use std::path::Path;

use rjsdb::{generate::RNG, repl::Repl, Database, TableKnowledge, Transaction};

// TODO:
// - missing stuff to support my RSS feed
//   - wrapper 'library'
//      - prepared statements (with replacement tags)
//      - better 'swizzling' (basically, have some fromSQL trait to convert from DbValue to inferred destitnation type),
//        wrap that in row.extract_val or something
//      - try and make ReturnedRows still an iterable somehow.
// - disallow use of reserved column names ("rowid")
// - add tests for parser, execution
// - missing options for trawler testing
// - better data structure for representing schema/primary key/etc, to allow:
//    - showing which col is the pk in repl
// - Figure out how to manage database connections and accept requests
//   - This'll be a client/server model, and the server probably will
//     need something like tokio to manage threads/requests
// - unsigned type (for ids, etc) (will require some schema-aware type coercion)
// - Do type coercion based on schema if allowed (i.e. int->float)

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

fn main() {
    let path = Path::new("db.db");
    let mut db = Database::init(path).unwrap();

    if !db.table_exists("the_mf_table") {
        let create_table =
            "CREATE TABLE IF NOT EXISTS the_mf_table (id integer primary key, foo string, bar integer, baz float);";
        db.execute(create_table).unwrap();
        let mut rng = RNG::new();
        let mut tx = db.transaction().unwrap();
        gen_rows(30, "the_mf_table", &mut tx, &mut rng);
        tx.commit().unwrap();
    }

    let mut repl = Repl::new();
    repl.run(&mut db).unwrap();
}
