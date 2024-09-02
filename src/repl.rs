use std::{
    cmp::max,
    io::{Error as IoError, Write},
    iter::zip,
    string::FromUtf8Error,
};

use console::{Key, Term};

use crate::{query::ResultRows, storage::Row, Database, DatabaseError, DatabaseResult, DbValue};

#[derive(Debug)]
pub enum ReplError {
    DatabaseError(DatabaseError),
    IoError(IoError),
    FromUtf8Error(FromUtf8Error),
}
impl From<DatabaseError> for ReplError {
    fn from(value: DatabaseError) -> Self {
        Self::DatabaseError(value)
    }
}
impl From<IoError> for ReplError {
    fn from(value: IoError) -> Self {
        Self::IoError(value)
    }
}
impl From<FromUtf8Error> for ReplError {
    fn from(value: FromUtf8Error) -> Self {
        Self::FromUtf8Error(value)
    }
}

type Result<T> = std::result::Result<T, ReplError>;

struct DisplayState {
    new_line: String,
    display_line: String,
    showing_new_line: bool,
    cursor: usize,
    display_chars: usize,
    should_rerender: bool,
}
impl DisplayState {
    fn new() -> Self {
        DisplayState {
            new_line: String::new(),
            display_line: String::new(),
            showing_new_line: true,
            cursor: 0,
            display_chars: 0,
            should_rerender: false,
        }
    }

    fn use_new_line_as_display(&mut self) {
        self.replace_display_line(self.new_line.clone());
        self.showing_new_line = true;
    }

    fn replace_display_line(&mut self, replacement: String) {
        self.display_line = replacement;
        self.display_chars = self.display_line.chars().count();
        self.cursor = self.display_chars;
    }

    fn backspace(&mut self) {
        _ = self.display_line.pop();
        if self.showing_new_line {
            _ = self.new_line.pop();
        }
        self.display_chars -= 1;
        self.cursor -= 1;
    }

    fn left(&mut self) {
        if self.cursor > 0 {
            self.cursor -= 1;
        }
    }

    fn right(&mut self) {
        if self.cursor < self.display_chars {
            self.cursor += 1;
        }
    }

    fn insert_char(&mut self, ch: char) {
        if self.showing_new_line {
            match self.new_line.char_indices().nth(self.cursor) {
                Some((idx, _)) => {
                    self.new_line.insert(idx, ch);
                    self.should_rerender = true;
                }
                None => self.new_line.push(ch),
            }
        }
        match self.display_line.char_indices().nth(self.cursor) {
            Some((idx, _)) => {
                self.display_line.insert(idx, ch);
                self.should_rerender = true;
            }
            None => self.display_line.push(ch),
        }
        self.display_chars += 1;
        self.cursor += 1;
    }

    fn reset(&mut self) {
        self.new_line = String::new();
        self.display_line = String::new();
        self.showing_new_line = true;
        self.cursor = 0;
        self.should_rerender = false;
    }
}

pub struct Repl {
    history: Vec<String>,
    history_cursor: usize,
    term: Term,
    display: DisplayState,
}
impl Default for Repl {
    fn default() -> Self {
        Repl::new()
    }
}
impl Repl {
    pub fn new() -> Self {
        Repl {
            history: Vec::new(),
            history_cursor: 0,
            term: Term::buffered_stdout(),
            display: DisplayState::new(),
        }
    }

    fn prompt(&mut self) -> Result<()> {
        // self.term
        // .write_fmt(format_args!("{}", self.display.cursor))?;
        self.term.write_all("> ".as_bytes())?;
        self.term.write_all(self.display.display_line.as_bytes())?;
        self.term.move_cursor_left(usize::MAX)?;
        self.term.move_cursor_right(self.display.cursor + 2)?;
        self.term.flush()?;
        self.display.should_rerender = false;
        Ok(())
    }

    fn echo_char(&mut self, ch: char) -> Result<()> {
        self.term.write_all(ch.to_string().as_bytes())?;
        self.term.flush()?;
        Ok(())
    }

    fn show_previous_line(&mut self) -> Result<()> {
        if !self.history.is_empty() && self.history_cursor > 0 {
            if self.display.showing_new_line {
                self.display.showing_new_line = false;
            }
            self.history_cursor -= 1;
            let line = self
                .history
                .get(self.history_cursor)
                .expect("Should always be something here")
                .clone();
            self.display.replace_display_line(line);
            self.term.clear_line()?;
            self.prompt()?;
        }
        Ok(())
    }

    fn show_next_line(&mut self) -> Result<()> {
        if self.history_cursor < self.history.len() {
            self.term.clear_line()?;
            self.history_cursor += 1;
            if self.history_cursor < self.history.len() {
                self.display.showing_new_line = false;
                let line = self
                    .history
                    .get(self.history_cursor)
                    .expect("Should always be something here")
                    .clone();
                self.display.replace_display_line(line);
            } else {
                self.display.use_new_line_as_display();
            }
            self.prompt()?;
        }
        Ok(())
    }

    fn get_user_input(&mut self) -> Result<String> {
        self.display.reset();
        self.prompt()?;
        loop {
            let key = self.term.read_key()?;
            match key {
                Key::ArrowUp => self.show_previous_line()?,
                Key::ArrowDown => self.show_next_line()?,
                Key::ArrowLeft => {
                    if self.display.cursor > 0 {
                        self.display.left();
                        self.term.move_cursor_left(1)?;
                        self.term.flush()?;
                    }
                }
                Key::ArrowRight => {
                    if self.display.cursor < self.display.display_chars {
                        self.display.right();
                        self.term.move_cursor_right(1)?;
                        self.term.flush()?;
                    }
                }
                Key::Backspace => {
                    self.term.clear_chars(1)?;
                    self.display.backspace();
                    self.term.flush()?;
                }
                Key::Char(ch) => {
                    self.display.insert_char(ch);
                    if self.display.should_rerender {
                        self.term.clear_line()?;
                        self.prompt()?;
                    } else {
                        self.echo_char(ch)?;
                    }
                }
                Key::Enter => {
                    self.display.display_line.push('\n');
                    self.echo_char('\n')?;
                    break;
                }
                _ => (),
            }
        }
        self.history
            .push(self.display.display_line.trim().to_string());
        self.history_cursor = self.history.len();
        Ok(self.display.display_line.clone())
    }

    pub fn run(&mut self, db: &mut Database) -> Result<()> {
        let mut tx = db.transaction()?;
        loop {
            let line = self.get_user_input()?;
            if line.trim() == "exit;" {
                break;
            }
            let mut prepped = tx.prepare(&line)?;
            let res = prepped.execute([]);
            match res {
                Err(err) => println!("{err:?}"),
                Ok(DatabaseResult::Ok(affected)) => match affected {
                    1 => println!("1 row affected"),
                    _ => println!("{affected} rows affected"),
                },
                Ok(DatabaseResult::NothingToDo) => (),
                Ok(DatabaseResult::Rows(rows)) => Repl::display_rows(rows),
            }
        }
        tx.commit()?;
        Ok(())
    }

    fn value_len(val: &DbValue) -> usize {
        match val {
            DbValue::Float(f) => format!("| {:+<e} ", f).len(),
            _ => format!("| {} ", val).len(),
        }
    }

    fn value_format(val: &DbValue, width: usize) -> String {
        match val {
            DbValue::Float(f) => format!("| {:<width$e} ", f),
            _ => format!("| {:<width$} ", val),
        }
    }

    fn print_row(col_widths: &[usize], row: &Row) {
        for (val, width) in zip(row.data.iter(), col_widths) {
            print!("{}", Repl::value_format(val, *width));
        }
        println!("|");
    }

    fn row_width(col_widths: &[usize]) -> usize {
        let row_width: usize = col_widths.iter().sum(); // string widths themselves
        let row_width = row_width + (col_widths.len() * 3); // to account for spacing and dividers;
        row_width + 1 // last dividider;
    }

    fn display_rows(rows: ResultRows) {
        let schema = rows.schema();
        let name_widths: Vec<usize> = schema
            .columns()
            .map(|c: &crate::storage::Column| c.name.len())
            .collect();
        let all_rows: Vec<Row> = rows.map(|r| r.into_owned()).collect();
        let col_widths = all_rows.iter().fold(name_widths, |widths, row| {
            let row_widths = row.data.iter().map(Repl::value_len);
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
