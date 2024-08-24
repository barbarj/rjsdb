use std::path::Path;

use rjsdb::{
    generate::{Generate, RNG},
    query::{
        execute,
        tokenize::{Token, Tokenizer},
        QueryResult, ResultRows,
    },
    storage,
};

// TODO:
// - basic test in main
// - repl
// - actual filtering
// - Figure out how to manage database connections and accept requests
//   - This'll be a client/server model, and the server probably will
//     need something like tokio to manage threads/requests
// - Do type coercion based on schema if allowed (i.e. int->float)

fn main() {
    let mut rng = RNG::new();

    let path = Path::new("db.db");
    let mut storage = storage::StorageLayer::init(path).unwrap();

    let create_table =
        "CREATE TABLE IF NOT EXISTS the_mf_table (foo string, bar integer, baz float);";
    let insert_expr = |foo: String, bar: i32, baz: f32| {
        println!("bar: {bar}");
        println!("baz: {baz:?}");
        format!(
            "INSERT INTO the_mf_table (foo, bar, baz) VALUES ('{}', {}, {:?});",
            foo, bar, baz
        )
    };
    let select_expr = "SELECT * from the_mf_table;";

    _ = execute(create_table, &mut storage).unwrap();
    for _ in 0..10 {
        let foo = String::generate(&mut rng);
        let bar = i32::generate(&mut rng);
        let baz = f32::generate(&mut rng);
        let stmt = insert_expr(foo, bar, baz);
        println!("{:?}", Tokenizer::new(&stmt).iter().collect::<Vec<Token>>());
        _ = execute(&stmt, &mut storage).unwrap();
    }

    {
        let rows = execute(select_expr, &mut storage).unwrap();
        match rows {
            QueryResult::Ok => println!("uh oh"),
            QueryResult::Rows(rows) => print_result(rows),
        }
    }
    storage.flush().unwrap();
}

fn print_result(rows: ResultRows) {
    for row in rows {
        println!("{row}");
    }
}
