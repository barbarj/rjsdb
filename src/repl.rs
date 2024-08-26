use std::{
    borrow::Cow,
    cmp::max,
    io::{stdin, stdout, Write},
    iter::zip,
};

use crate::{
    query::{execute, QueryResult, ResultRows},
    storage::{Row, StorageLayer},
};

pub struct Repl<'a> {
    storage: &'a mut StorageLayer,
    history: Vec<String>,
}
impl<'a> Repl<'a> {
    pub fn new(storage: &'a mut StorageLayer) -> Self {
        Repl {
            storage,
            history: Vec::new(),
        }
    }

    fn get_next_line() -> String {
        print!("> ");
        stdout().flush().unwrap();
        let mut line = String::new();

        /*
        cursor = 0
        while fill_buff (enough bytes for escape codes, 3?) {
            if buff == 'UP' {
                cursor = min(0, cursor - 1);
                display_history(cursor)
                clear_line()
            }
            // down
            if buff == '\n' {
                return line
            }

        }
         */

        while !line.contains(';') {
            stdin().read_line(&mut line).unwrap();
        }
        line
    }

    pub fn run(&mut self) {
        loop {
            let line = Repl::get_next_line();
            if line.trim() == "exit;" {
                break;
            }
            match execute(line.trim(), self.storage) {
                Err(err) => println!("{err:?}"),
                Ok(QueryResult::Ok) => println!("ok"),
                Ok(QueryResult::Rows(rows)) => Repl::display_rows(rows),
            }
            self.history.push(line);
        }
        self.storage.flush().unwrap();
    }

    fn print_row(col_widths: &[usize], row: &Row) {
        for (val, width) in zip(row.data.iter(), col_widths) {
            print!("| {:<width$} ", val);
        }
        println!("|");
    }

    fn row_width(col_widths: &[usize]) -> usize {
        let row_width: usize = col_widths.iter().sum(); // string widths themselves
        let row_width = row_width + (col_widths.len() * 3); // to account for spacing and dividers;
        row_width + 1 // last dividider;
    }

    fn display_rows(rows: ResultRows) {
        // limit to 20 rows, mainly to not dump a crazy amount of
        // data on the user.
        let schema = rows.schema();
        let all_rows: Vec<Cow<Row>> = rows.take(20).collect();
        let name_widths: Vec<usize> = schema.columns().map(|c| c.name.len()).collect();
        let col_widths = all_rows.iter().fold(name_widths, |widths, row| {
            let row_widths = row.data.iter().map(|x| format!("{x}").len());
            zip(widths, row_widths).map(|(a, b)| max(a, b)).collect()
        });

        let divider = "-".repeat(Repl::row_width(&col_widths));

        // header
        println!("{}", divider);
        for (col, width) in zip(schema.columns(), col_widths.iter()) {
            print!("| {:<width$} ", col.name);
        }
        println!("|");
        println!("{}", divider);

        // body
        for row in all_rows {
            Repl::print_row(&col_widths, &row);
        }

        println!("{}", divider);
    }
}
