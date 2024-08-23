use execute::{ExecutablePlan, ExecutionError, QueryResult};
use parse::{Parser, ParsingError};
use tokenize::Tokenizer;

use crate::storage::{StorageError, StorageLayer};

mod execute;
mod parse;
mod tokenize;

pub enum QueryError {
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

pub fn execute<'strg>(
    command: &str,
    storage: &'strg mut StorageLayer,
) -> Result<QueryResult<'strg>> {
    let tokenizer = Tokenizer::new(command);
    let plan = Parser::new(tokenizer).parse()?;
    let executable_plan = ExecutablePlan::new(plan);
    let res = executable_plan.execute(storage)?;
    Ok(res)
}
