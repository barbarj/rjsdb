use std::{
    borrow::Cow,
    cmp::max,
    io::{Error as IoError, Write},
    iter::zip,
    string::FromUtf8Error,
};

use console::{Key, Term};

use crate::{
    query::{execute, QueryResult, ResultRows},
    storage::{Row, StorageError, StorageLayer},
};

#[derive(Debug)]
pub enum ReplError {
    StorageError(StorageError),
    IoError(IoError),
    FromUtf8Error(FromUtf8Error),
}
impl From<StorageError> for ReplError {
    fn from(value: StorageError) -> Self {
        Self::StorageError(value)
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
}
impl DisplayState {
    fn new() -> Self {
        DisplayState {
            new_line: String::new(),
            display_line: String::new(),
            showing_new_line: true,
        }
    }

    fn use_new_line_as_display(&mut self) {
        self.display_line = self.new_line.clone();
        self.showing_new_line = true;
    }

    fn backspace(&mut self) {
        _ = self.display_line.pop();
        if self.showing_new_line {
            _ = self.new_line.pop();
        }
    }

    fn push_char(&mut self, ch: char) {
        if self.showing_new_line {
            self.new_line.push(ch);
        }
        self.display_line.push(ch);
    }

    fn reset(&mut self) {
        self.new_line = String::new();
        self.display_line = String::new();
        self.showing_new_line = true;
    }
}

pub struct Repl<'strg> {
    storage: &'strg mut StorageLayer,
    history: Vec<String>,
    cursor: usize,
    term: Term,
    display: DisplayState,
}
impl<'strg> Repl<'strg> {
    pub fn new(storage: &'strg mut StorageLayer) -> Self {
        Repl {
            storage,
            history: Vec::new(),
            cursor: 0,
            term: Term::buffered_stdout(),
            display: DisplayState::new(),
        }
    }

    fn prompt(&mut self) -> Result<()> {
        self.term.write_all("> ".as_bytes())?;
        self.term.write_all(self.display.display_line.as_bytes())?;
        self.term.flush()?;
        Ok(())
    }

    fn echo_char(&mut self, ch: char) -> Result<()> {
        self.term.write_all(ch.to_string().as_bytes())?;
        self.term.flush()?;
        Ok(())
    }

    fn show_previous_line(&mut self) -> Result<()> {
        if !self.history.is_empty() && self.cursor > 0 {
            if self.display.showing_new_line {
                self.display.showing_new_line = false;
            }
            self.cursor -= 1;
            self.display.display_line = self
                .history
                .get(self.cursor)
                .expect("Should always be something here")
                .clone();
            self.term.clear_line()?;
            self.prompt()?;
        }
        Ok(())
    }

    fn show_next_line(&mut self) -> Result<()> {
        if self.cursor < self.history.len() {
            self.term.clear_line()?;
            self.cursor += 1;
            if self.cursor < self.history.len() {
                self.display.showing_new_line = false;
                self.display.display_line = self
                    .history
                    .get(self.cursor)
                    .expect("Should always be something here")
                    .clone();
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
                Key::Backspace => {
                    self.term.clear_chars(1)?;
                    self.display.backspace();
                    self.term.flush()?;
                }
                Key::Char(ch) => {
                    self.display.push_char(ch);
                    self.echo_char(ch)?;
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
        self.cursor = self.history.len();
        Ok(self.display.display_line.clone())
    }

    pub fn run(&mut self) -> Result<()> {
        loop {
            let line = self.get_user_input()?;
            if line.trim() == "exit;" {
                break;
            }
            match execute(line.trim(), self.storage) {
                Err(err) => println!("{err:?}"),
                Ok(QueryResult::Ok) => println!("ok"),
                Ok(QueryResult::NothingToDo) => (),
                Ok(QueryResult::Rows(rows)) => Repl::display_rows(rows),
            }
        }
        self.storage.flush()?;
        Ok(())
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
