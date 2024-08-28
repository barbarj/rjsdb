use std::path::Path;

use rjsdb::{query::execute, repl::Repl, storage};

// TODO:
// - missing stuff to support my RSS feed
//   - PRIMARY KEY
//      - allowed once per table create, need to mark as such in the table.
//      - during insertion, check for uniqueness of this column
//          - that means I need an index for this value
//          - For now I'll just use a BTreeSet to check the uniqueness constraint.
//            Building an actual index will require some substantial reworking of the storage engine
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

fn main() {
    let path = Path::new("db.db");
    let mut storage = storage::StorageLayer::init(path).unwrap();

    let create_table =
        "CREATE TABLE IF NOT EXISTS the_mf_table (foo string, bar integer, baz float);";
    _ = execute(create_table, &mut storage).unwrap();
    let mut repl = Repl::new(&mut storage);
    repl.run().unwrap();
}
