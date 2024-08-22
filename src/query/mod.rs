use execute::{ExecutablePlan, ExecutionError, QueryResult};
use parse::{Parser, ParsingError};
use tokenize::Tokenizer;

use crate::storage::{Row, StorageError, StorageLayer};

mod execute;
mod parse;
mod tokenize;

enum QueryError {
    StorageError(StorageError),
    ParsingError(ParsingError),
    ExecutionError(ExecutionError),
}
impl From<ParsingError> for QueryError {
    fn from(value: ParsingError) -> Self {
        Self::ParsingError(value)
    }
}
impl From<ExecutionError> for QueryError {
    fn from(value: ExecutionError) -> Self {
        Self::ExecutionError(value)
    }
}

type Result<T> = std::result::Result<T, QueryError>;

pub fn execute(command: &str, storage: &mut StorageLayer) -> Result<QueryResult> {
    let tokenizer = Tokenizer::new(command);
    let plan = Parser::new(tokenizer).parse()?;
    let mut executable_plan = ExecutablePlan::new(&plan, storage);
    let res = executable_plan.execute()?;
    Ok(res)
}
