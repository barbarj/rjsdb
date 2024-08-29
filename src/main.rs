use std::path::Path;

use rjsdb::{
    generate::RNG,
    query::execute,
    repl::Repl,
    storage::{self, StorageLayer},
};

// TODO:
// - left, right arrows interaction in repl
// - unsigned type (for ids, etc)
// - missing stuff to support my RSS feed
//   - upsert (ON CONFLICT)
//   - "parallel" access (probably just put the db being a mutex)
//   - DELETE
//      - will require subqueries
//   - db wrapper
//   - "transactions"
//   - wrapper 'library'
//      - prepared statements
// - add tests for parser, execution
// - missing options for trawler testing
// - Figure out how to manage database connections and accept requests
//   - This'll be a client/server model, and the server probably will
//     need something like tokio to manage threads/requests
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

fn gen_rows(count: usize, table_name: &str, storage: &mut StorageLayer, rng: &mut RNG) {
    let schema = storage.table_schema(table_name).unwrap().clone();
    for row in (0..count).map(|_| schema.gen_row(rng)) {
        let columns_str = wrapped_join(schema.columns().map(|c| c.name.as_str()));
        let values: Vec<String> = row.data.iter().map(|v| v.as_insertable_sql_str()).collect();
        let values_str = wrapped_join(values.iter().map(|s| s.as_str()));
        let stmt = format!(
            "INSERT INTO {table_name} {} values {};",
            columns_str, values_str
        );
        println!("{stmt}");
        execute(&stmt, storage).unwrap();
    }
}

fn main() {
    let path = Path::new("db.db");
    let mut storage = storage::StorageLayer::init(path).unwrap();

    if !storage.table_exists("the_mf_table") {
        let create_table =
            "CREATE TABLE IF NOT EXISTS the_mf_table (id integer primary key, foo string, bar integer, baz float);";
        _ = execute(create_table, &mut storage).unwrap();
        storage.flush().unwrap();
        let mut rng = RNG::new();
        gen_rows(30, "the_mf_table", &mut storage, &mut rng);
        storage.flush().unwrap();
    }
    let mut repl = Repl::new(&mut storage);
    repl.run().unwrap();
}
