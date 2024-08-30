use std::{
    collections::BTreeSet,
    num::{ParseFloatError, ParseIntError},
};

use crate::{
    storage::{self, ConflictRule, KeySet, Schema},
    DbFloat, DbType, DbValue,
};

use super::tokenize::{Token, TokenKind, Tokenizer, TokenizerError, Tokens};

#[derive(Debug)]
pub enum ParsingError {
    UnexpectedEndOfStatement,
    UnexpectedTokenType,
    ParseFloatError(ParseFloatError),
    ParseIntError(ParseIntError),
    TokenizerError(TokenizerError),
    MultiplePrimaryKeys,
    UnknownPrimaryKeyProvided,
}
impl From<ParseFloatError> for ParsingError {
    fn from(value: ParseFloatError) -> Self {
        ParsingError::ParseFloatError(value)
    }
}
impl From<ParseIntError> for ParsingError {
    fn from(value: ParseIntError) -> Self {
        ParsingError::ParseIntError(value)
    }
}
impl From<TokenizerError> for ParsingError {
    fn from(value: TokenizerError) -> Self {
        ParsingError::TokenizerError(value)
    }
}

type Result<T> = std::result::Result<T, ParsingError>;

pub struct Parser<'a> {
    tokens: Tokens<'a>,
    lookahead: Option<Token<'a>>,
}
impl<'a> Parser<'a> {
    pub fn build(tokenizer: Tokenizer<'a>) -> Result<Self> {
        let mut tokens = tokenizer.tokens();
        let lookahead = tokens.next_token()?;
        Ok(Parser { tokens, lookahead })
    }

    fn done_parsing(&self) -> bool {
        self.lookahead.is_none()
    }

    fn consume(&mut self, tk: TokenKind) -> Result<Token<'a>> {
        let token = self.lookahead.take();
        match token {
            Some(t) if t.kind() == tk => {
                self.lookahead = self.tokens.next_token()?;
                Ok(t)
            }
            Some(_) => Err(ParsingError::UnexpectedTokenType),
            None => Err(ParsingError::UnexpectedEndOfStatement),
        }
    }

    fn consume_type_token(&mut self) -> Result<Token<'a>> {
        let token = match self.lookahead.take() {
            Some(t) => t,
            None => return Err(ParsingError::UnexpectedEndOfStatement),
        };
        if matches!(
            token.kind(),
            TokenKind::TypeString
                | TokenKind::TypeInteger
                | TokenKind::TypeFloat
                | TokenKind::TypeUnsignedInt
        ) {
            self.lookahead = self.tokens.next_token()?;
            return Ok(token);
        }
        Err(ParsingError::UnexpectedTokenType)
    }

    fn consume_value_token(&mut self) -> Result<Token<'a>> {
        let token = match self.lookahead.take() {
            Some(t) => t,
            None => return Err(ParsingError::UnexpectedEndOfStatement),
        };
        if matches!(
            token.kind(),
            TokenKind::String | TokenKind::Integer | TokenKind::Float
        ) {
            self.lookahead = self.tokens.next_token()?;
            return Ok(token);
        }
        Err(ParsingError::UnexpectedTokenType)
    }

    fn peek_kind(&self) -> Option<TokenKind> {
        self.lookahead.as_ref().map(|t| t.kind())
    }

    pub fn parse(&mut self) -> Result<Vec<Statement>> {
        self.expression_list()
    }

    fn expression_list(&mut self) -> Result<Vec<Statement>> {
        let mut expressions = Vec::new();

        while !self.done_parsing() {
            expressions.push(self.statement()?);
        }

        Ok(expressions)
    }

    fn statement(&mut self) -> Result<Statement> {
        let expr = match self.peek_kind() {
            None => return Err(ParsingError::UnexpectedEndOfStatement),
            Some(TokenKind::Select) => Statement::Select(self.select_statement()?),
            Some(TokenKind::Create) => Statement::Create(self.create_statement()?),
            Some(TokenKind::Insert) => Statement::Insert(self.insert_statement()?),
            Some(TokenKind::Destroy) => Statement::Destroy(self.destroy_statement()?),
            Some(TokenKind::Delete) => Statement::Delete(self.delete_statement()?),
            Some(_) => return Err(ParsingError::UnexpectedTokenType),
        };
        self.end_of_statement()?;
        Ok(expr)
    }

    fn end_of_statement(&mut self) -> Result<()> {
        _ = self.consume(TokenKind::Semicolon)?;
        Ok(())
    }

    fn column_name(&mut self) -> Result<String> {
        let name = self.consume(TokenKind::Identifier)?.contents().to_string();
        Ok(name)
    }

    fn column_projection(&mut self) -> Result<ColumnProjection> {
        let in_name = self.column_name()?;
        if self.peek_kind() == Some(TokenKind::As) {
            _ = self.consume(TokenKind::As)?;
            let out_name = self.consume(TokenKind::Identifier)?.contents().to_string();
            Ok(ColumnProjection::new(in_name, out_name))
        } else {
            Ok(ColumnProjection::no_projection(in_name))
        }
    }

    fn select_columns(&mut self) -> Result<SelectColumns> {
        if self.peek_kind() == Some(TokenKind::Star) {
            _ = self.consume(TokenKind::Star)?;
            return Ok(SelectColumns::All);
        }
        let first = self.column_projection()?;
        let mut cols = vec![first];

        while self.peek_kind() == Some(TokenKind::Comma) {
            _ = self.consume(TokenKind::Comma)?;
            cols.push(self.column_projection()?);
        }

        Ok(SelectColumns::Only(cols))
    }

    fn nested_select_statement(&mut self) -> Result<SelectStatement> {
        _ = self.consume(TokenKind::LeftParen)?;
        let statement = self.select_statement()?;
        _ = self.consume(TokenKind::RightParen)?;
        Ok(statement)
    }

    fn select_statement(&mut self) -> Result<SelectStatement> {
        _ = self.consume(TokenKind::Select)?;

        let columns = self.select_columns()?;

        _ = self.consume(TokenKind::From)?;
        let source = match self.peek_kind() {
            Some(TokenKind::Identifier) => {
                let table = self.consume(TokenKind::Identifier)?.contents().to_string();
                SelectSource::Table(table)
            }
            Some(TokenKind::LeftParen) => SelectSource::Expression(self.nested_select_statement()?),
            Some(_) => return Err(ParsingError::UnexpectedEndOfStatement),
            None => return Err(ParsingError::UnexpectedTokenType),
        };

        let where_clause = if self.peek_kind() == Some(TokenKind::Where) {
            Some(self.where_clause()?)
        } else {
            None
        };
        let order_by_clause = if self.peek_kind() == Some(TokenKind::Order) {
            Some(self.order_by_clause()?)
        } else {
            None
        };
        let limit = if self.peek_kind() == Some(TokenKind::Limit) {
            Some(self.limit()?)
        } else {
            None
        };

        Ok(SelectStatement {
            columns,
            source: Box::new(source),
            where_clause,
            order_by_clause,
            limit,
        })
    }

    fn is_where_clause_member_kind(tk: TokenKind) -> bool {
        matches!(
            tk,
            TokenKind::Identifier | TokenKind::String | TokenKind::Integer | TokenKind::Float
        )
    }

    fn where_token_to_where_member(token: Token) -> Result<WhereMember> {
        match token.kind() {
            TokenKind::Identifier => Ok(WhereMember::Column(token.contents().to_string())),
            TokenKind::String => Ok(WhereMember::Value(DbValue::String(
                token.contents().to_string(),
            ))),
            TokenKind::Integer => Ok(WhereMember::Value(DbValue::Integer(
                token.contents().parse::<i64>()?,
            ))),
            TokenKind::Float => Ok(WhereMember::Value(DbValue::Float(DbFloat::new(
                token.contents().parse::<f64>()?,
            )))),
            _ => Err(ParsingError::UnexpectedTokenType),
        }
    }

    fn where_clause(&mut self) -> Result<WhereClause> {
        _ = self.consume(TokenKind::Where)?;
        let left = match self.peek_kind() {
            Some(k) if Parser::is_where_clause_member_kind(k) => {
                let token = self.consume(k)?;
                Parser::where_token_to_where_member(token)?
            }
            Some(_) => return Err(ParsingError::UnexpectedTokenType),
            None => return Err(ParsingError::UnexpectedEndOfStatement),
        };
        let cmp = match self.peek_kind() {
            Some(TokenKind::EqualsSign) => {
                _ = self.consume(TokenKind::EqualsSign)?;
                WhereCmp::Eq
            }
            Some(TokenKind::LeftAngleBracket) => {
                _ = self.consume(TokenKind::LeftAngleBracket)?;
                WhereCmp::LessThan
            }
            Some(TokenKind::RightAngleBracket) => {
                _ = self.consume(TokenKind::RightAngleBracket)?;
                WhereCmp::GreaterThan
            }
            Some(TokenKind::LessThanEquals) => {
                _ = self.consume(TokenKind::LessThanEquals)?;
                WhereCmp::LessThanEquals
            }
            Some(TokenKind::GreaterThanEquals) => {
                _ = self.consume(TokenKind::GreaterThanEquals)?;
                WhereCmp::GreaterThanEquals
            }
            Some(_) => return Err(ParsingError::UnexpectedTokenType),
            None => return Err(ParsingError::UnexpectedEndOfStatement),
        };
        let right = match self.peek_kind() {
            Some(k) if Parser::is_where_clause_member_kind(k) => {
                let token = self.consume(k)?;
                Parser::where_token_to_where_member(token)?
            }
            Some(_) => return Err(ParsingError::UnexpectedTokenType),
            None => return Err(ParsingError::UnexpectedEndOfStatement),
        };
        Ok(WhereClause { left, cmp, right })
    }

    fn order_by_clause(&mut self) -> Result<OrderByClause> {
        _ = self.consume(TokenKind::Order)?;
        _ = self.consume(TokenKind::By)?;
        let sort_column = self.column_name()?;
        let desc = self.peek_kind().filter(|k| *k == TokenKind::Desc).is_some();
        if desc {
            _ = self.consume(TokenKind::Desc)?;
        }
        Ok(OrderByClause { sort_column, desc })
    }

    fn limit(&mut self) -> Result<usize> {
        _ = self.consume(TokenKind::Limit)?;
        let token = self.consume(TokenKind::Integer)?;
        let limit = token.contents().parse::<usize>()?;
        Ok(limit)
    }

    fn create_statement(&mut self) -> Result<CreateStatement> {
        _ = self.consume(TokenKind::Create)?;
        _ = self.consume(TokenKind::Table)?;
        let if_not_exists = self.peek_kind().filter(|k| *k == TokenKind::If).is_some();
        if if_not_exists {
            _ = self.consume(TokenKind::If)?;
            _ = self.consume(TokenKind::Not)?;
            _ = self.consume(TokenKind::Exists)?;
        }
        let table = self.consume(TokenKind::Identifier)?.contents().to_string();
        let columns = self.create_columns()?;

        Ok(CreateStatement {
            table,
            if_not_exists,
            columns,
        })
    }

    fn create_columns(&mut self) -> Result<CreateColumns> {
        _ = self.consume(TokenKind::LeftParen)?;
        let mut names = Vec::new();
        let mut types = Vec::new();
        let mut primary_key_col: Option<String> = None;
        while self.peek_kind().is_some() && self.peek_kind() != Some(TokenKind::RightParen) {
            let name = self.consume(TokenKind::Identifier)?.contents().to_string();
            let this_type = match self.consume_type_token()?.kind() {
                TokenKind::TypeString => DbType::String,
                TokenKind::TypeInteger => DbType::Integer,
                TokenKind::TypeFloat => DbType::Float,
                TokenKind::TypeUnsignedInt => DbType::UnsignedInt,
                _ => panic!("Got a non-type token!"),
            };

            if self.peek_kind() == Some(TokenKind::Primary) {
                if primary_key_col.is_none() {
                    primary_key_col = Some(name.clone());
                } else {
                    return Err(ParsingError::MultiplePrimaryKeys);
                }
                _ = self.consume(TokenKind::Primary)?;
                _ = self.consume(TokenKind::Key)?;
            }

            names.push(name);
            types.push(this_type);

            if self.peek_kind() != Some(TokenKind::RightParen) {
                _ = self.consume(TokenKind::Comma)?;
            }
        }
        _ = self.consume(TokenKind::RightParen)?;

        let primary_key_col = primary_key_col
            .map(KeyColumn::Column)
            .unwrap_or(KeyColumn::Rowid);
        Ok(CreateColumns {
            names,
            types,
            primary_key_col,
        })
    }

    fn conflict_clause(&mut self) -> Result<ConflictClause> {
        _ = self.consume(TokenKind::On)?;
        _ = self.consume(TokenKind::Conflict)?;
        _ = self.consume(TokenKind::LeftParen)?;
        let mut target_columns = Vec::new();
        while self.peek_kind().is_some() && self.peek_kind() != Some(TokenKind::RightParen) {
            let name = self.consume(TokenKind::Identifier)?.contents().to_string();
            target_columns.push(name);
            if self.peek_kind() != Some(TokenKind::RightParen) {
                _ = self.consume(TokenKind::Comma)?;
            }
        }
        _ = self.consume(TokenKind::RightParen)?;

        _ = self.consume(TokenKind::Do)?;
        let action = match self.peek_kind() {
            Some(TokenKind::Nothing) => {
                _ = self.consume(TokenKind::Nothing)?;
                ConflictAction::Nothing
            }
            Some(_) => return Err(ParsingError::UnexpectedTokenType),
            _ => return Err(ParsingError::UnexpectedEndOfStatement),
        };
        Ok(ConflictClause {
            target_columns,
            action,
        })
    }

    fn insert_statement(&mut self) -> Result<InsertStatement> {
        _ = self.consume(TokenKind::Insert)?;
        _ = self.consume(TokenKind::Into)?;

        let table = self.consume(TokenKind::Identifier)?.contents().to_string();

        let mut columns = Vec::new();
        _ = self.consume(TokenKind::LeftParen)?;
        while self.peek_kind().is_some() && self.peek_kind() != Some(TokenKind::RightParen) {
            let name = self.consume(TokenKind::Identifier)?.contents().to_string();
            columns.push(name);
            if self.peek_kind() != Some(TokenKind::RightParen) {
                _ = self.consume(TokenKind::Comma)?;
            }
        }
        _ = self.consume(TokenKind::RightParen)?;

        _ = self.consume(TokenKind::Values)?;
        let mut values = Vec::new();
        _ = self.consume(TokenKind::LeftParen)?;
        while self.peek_kind().is_some() && self.peek_kind() != Some(TokenKind::RightParen) {
            let token = self.consume_value_token()?;
            let val = match token.kind() {
                TokenKind::String => DbValue::String(token.contents().to_string()),
                TokenKind::Float => DbValue::Float(DbFloat::new(token.contents().parse::<f64>()?)),
                TokenKind::Integer => DbValue::Integer(token.contents().parse::<i64>()?),
                _ => panic!("Should not happen!"),
            };

            values.push(val);
            if self.peek_kind() != Some(TokenKind::RightParen) {
                _ = self.consume(TokenKind::Comma)?;
            }
        }
        _ = self.consume(TokenKind::RightParen)?;

        let conflict_clause = if self.peek_kind() == Some(TokenKind::On) {
            Some(self.conflict_clause()?)
        } else {
            None
        };

        Ok(InsertStatement {
            table,
            columns,
            values,
            conflict_clause,
        })
    }

    fn destroy_statement(&mut self) -> Result<DestroyStatement> {
        _ = self.consume(TokenKind::Destroy)?;
        _ = self.consume(TokenKind::Table)?;
        let table = self.consume(TokenKind::Identifier)?.contents().to_string();
        Ok(DestroyStatement { table })
    }

    fn delete_statement(&mut self) -> Result<DeleteStatement> {
        _ = self.consume(TokenKind::Delete)?;
        _ = self.consume(TokenKind::From)?;
        let table = self.consume(TokenKind::Identifier)?.contents().to_string();
        let where_clause = self.where_clause()?;
        Ok(DeleteStatement {
            table,
            where_clause,
        })
    }
}

#[derive(PartialEq, Debug)]
pub struct ColumnProjection {
    pub in_name: String,
    pub out_name: String,
}
impl ColumnProjection {
    fn new(in_name: String, out_name: String) -> Self {
        ColumnProjection { in_name, out_name }
    }

    fn no_projection(name: String) -> Self {
        ColumnProjection {
            in_name: name.clone(),
            out_name: name,
        }
    }
}

#[derive(PartialEq, Debug)]
pub enum SelectColumns {
    All,
    Only(Vec<ColumnProjection>),
}

#[derive(PartialEq, Debug)]
pub enum KeyColumn {
    Rowid,
    Column(String),
}
impl KeyColumn {
    pub fn as_storage_key_column(&self, schema: &Schema) -> Result<storage::PrimaryKey> {
        match self {
            Self::Rowid => Ok(storage::PrimaryKey::Rowid),
            Self::Column(name) => {
                let col = match schema.column(name) {
                    Some(col) => col.clone(),
                    None => return Err(ParsingError::UnknownPrimaryKeyProvided),
                };
                let keyset = match col._type {
                    DbType::Float => KeySet::Floats(BTreeSet::new()),
                    DbType::Integer => KeySet::Integers(BTreeSet::new()),
                    DbType::String => KeySet::Strings(BTreeSet::new()),
                    DbType::UnsignedInt => KeySet::UnsignedInts(BTreeSet::new()),
                };
                Ok(storage::PrimaryKey::Column { col, keyset })
            }
        }
    }
}

#[derive(PartialEq, Debug)]
pub struct CreateColumns {
    pub names: Vec<String>,
    pub types: Vec<DbType>,
    pub primary_key_col: KeyColumn,
}

#[derive(PartialEq, Debug)]
pub enum Statement {
    Select(SelectStatement),
    Create(CreateStatement),
    Insert(InsertStatement),
    Destroy(DestroyStatement),
    Delete(DeleteStatement),
}

#[derive(PartialEq, Debug)]
pub enum SelectSource {
    Table(String),
    Expression(SelectStatement),
}

#[derive(PartialEq, Debug)]
pub struct SelectStatement {
    pub columns: SelectColumns,
    pub source: Box<SelectSource>,
    pub where_clause: Option<WhereClause>,
    pub order_by_clause: Option<OrderByClause>,
    pub limit: Option<usize>,
}
impl SelectStatement {
    pub fn uses_row_id(&self) -> bool {
        if let SelectColumns::Only(cols) = &self.columns {
            if cols.iter().any(|p| p.in_name == "rowid") {
                return true;
            }
        }
        if let Some(WhereClause {
            left: WhereMember::Column(col),
            cmp: _,
            right: _,
        }) = &self.where_clause
        {
            if col == "rowid" {
                return true;
            };
        }
        if let Some(WhereClause {
            left: _,
            cmp: _,
            right: WhereMember::Column(col),
        }) = &self.where_clause
        {
            if col == "rowid" {
                return true;
            };
        }
        if let Some(clause) = &self.order_by_clause {
            if clause.sort_column() == "rowid" {
                return true;
            }
        }
        false
    }
}

#[derive(PartialEq, Debug)]
pub struct CreateStatement {
    pub table: String,
    pub if_not_exists: bool,
    pub columns: CreateColumns,
}

#[derive(PartialEq, Debug)]
pub struct InsertStatement {
    pub table: String,
    pub columns: Vec<String>,
    pub values: Vec<DbValue>,
    pub conflict_clause: Option<ConflictClause>,
}

#[derive(PartialEq, Debug)]
pub struct DestroyStatement {
    pub table: String,
}

#[derive(PartialEq, Debug, Clone)]
pub enum WhereMember {
    Value(DbValue),
    Column(String),
}

#[derive(PartialEq, Debug, Clone, Copy)]
pub enum WhereCmp {
    Eq,
    LessThan,
    GreaterThan,
    LessThanEquals,
    GreaterThanEquals,
}
impl WhereCmp {
    pub fn inverted(&self) -> Self {
        match self {
            Self::Eq => Self::Eq,
            Self::LessThan => Self::GreaterThan,
            Self::GreaterThan => Self::LessThan,
            Self::GreaterThanEquals => Self::LessThanEquals,
            Self::LessThanEquals => Self::GreaterThanEquals,
        }
    }
}

#[derive(PartialEq, Debug)]
pub struct WhereClause {
    pub left: WhereMember,
    pub cmp: WhereCmp,
    pub right: WhereMember,
}

#[derive(PartialEq, Debug)]
pub struct OrderByClause {
    sort_column: String,
    desc: bool,
}
impl OrderByClause {
    pub fn sort_column(&self) -> &str {
        &self.sort_column
    }

    pub fn desc(&self) -> bool {
        self.desc
    }
}

#[derive(PartialEq, Debug)]
pub enum ConflictAction {
    Nothing,
}
impl ConflictAction {
    pub fn as_storage_conflict_action(&self) -> storage::ConflictAction {
        match self {
            Self::Nothing => storage::ConflictAction::Nothing,
        }
    }
}

#[derive(PartialEq, Debug)]
pub struct ConflictClause {
    target_columns: Vec<String>,
    action: ConflictAction,
}
impl ConflictClause {
    pub fn as_conflict_rule(&self) -> ConflictRule {
        // TODO: Eventually make this possible handle more than one column
        let col = self.target_columns.first().unwrap().clone();
        ConflictRule {
            column: col,
            action: self.action.as_storage_conflict_action(),
        }
    }
}

#[derive(PartialEq, Debug)]
pub struct DeleteStatement {
    table: String,
    where_clause: WhereClause,
}

#[cfg(test)]
mod parser_tests {
    use super::*;

    #[test]
    fn consume() {
        let stmt = "'that' this";
        let tokens = Tokenizer::new(stmt);
        let mut parser = Parser::build(tokens).unwrap();

        assert_eq!(
            parser.consume(TokenKind::String).unwrap(),
            Token::new("that", TokenKind::String)
        );

        let res = parser.consume(TokenKind::String);
        assert!(res.is_err());
    }

    #[test]
    fn basic_select() {
        let stmt = "select foo, bar from the_data;";

        let tokens = Tokenizer::new(stmt);
        let actual = Parser::build(tokens).unwrap().parse().unwrap();
        let expected = vec![Statement::Select(SelectStatement {
            columns: SelectColumns::Only(vec![
                ColumnProjection::no_projection(String::from("foo")),
                ColumnProjection::no_projection(String::from("bar")),
            ]),
            source: Box::new(SelectSource::Table(String::from("the_data"))),
            where_clause: None,
            order_by_clause: None,
            limit: None,
        })];

        assert_eq!(actual, expected);
    }

    #[test]
    fn select_with_as() {
        let stmt = "select a as b, bar, c as d from the_data;";

        let tokens = Tokenizer::new(stmt);
        let actual = Parser::build(tokens).unwrap().parse().unwrap();
        let expected = vec![Statement::Select(SelectStatement {
            columns: SelectColumns::Only(vec![
                ColumnProjection::new(String::from("a"), String::from("b")),
                ColumnProjection::no_projection(String::from("bar")),
                ColumnProjection::new(String::from("c"), String::from("d")),
            ]),
            source: Box::new(SelectSource::Table(String::from("the_data"))),
            where_clause: None,
            order_by_clause: None,
            limit: None,
        })];

        assert_eq!(actual, expected);
    }

    #[test]
    fn basic_select_star() {
        let stmt = "select * from the_data;";

        let tokens = Tokenizer::new(stmt);
        let actual = Parser::build(tokens).unwrap().parse().unwrap();
        let expected = vec![Statement::Select(SelectStatement {
            columns: SelectColumns::All,
            source: Box::new(SelectSource::Table(String::from("the_data"))),
            where_clause: None,
            order_by_clause: None,
            limit: None,
        })];

        assert_eq!(actual, expected);
    }

    #[test]
    fn select_with_where_only() {
        let stmt = "select foo, bar from the_data where that = 'this';";

        let tokens = Tokenizer::new(stmt);
        let actual = Parser::build(tokens).unwrap().parse().unwrap();
        let expected = vec![Statement::Select(SelectStatement {
            columns: SelectColumns::Only(vec![
                ColumnProjection::no_projection(String::from("foo")),
                ColumnProjection::no_projection(String::from("bar")),
            ]),
            source: Box::new(SelectSource::Table(String::from("the_data"))),
            where_clause: Some(WhereClause {
                left: WhereMember::Column(String::from("that")),
                cmp: WhereCmp::Eq,
                right: WhereMember::Value(DbValue::String(String::from("this"))),
            }),
            order_by_clause: None,
            limit: None,
        })];

        assert_eq!(actual, expected);
    }

    #[test]
    fn select_with_where_lt_only() {
        let stmt = "select foo, bar from the_data where 1 < 2;";

        let tokens = Tokenizer::new(stmt);
        let actual = Parser::build(tokens).unwrap().parse().unwrap();
        let expected = vec![Statement::Select(SelectStatement {
            columns: SelectColumns::Only(vec![
                ColumnProjection::no_projection(String::from("foo")),
                ColumnProjection::no_projection(String::from("bar")),
            ]),
            source: Box::new(SelectSource::Table(String::from("the_data"))),
            where_clause: Some(WhereClause {
                left: WhereMember::Value(DbValue::Integer(1)),
                cmp: WhereCmp::LessThan,
                right: WhereMember::Value(DbValue::Integer(2)),
            }),
            order_by_clause: None,
            limit: None,
        })];

        assert_eq!(actual, expected);
    }

    #[test]
    fn select_with_where_gt_only() {
        let stmt = "select foo, bar from the_data where 1 > 2;";

        let tokens = Tokenizer::new(stmt);
        let actual = Parser::build(tokens).unwrap().parse().unwrap();
        let expected = vec![Statement::Select(SelectStatement {
            columns: SelectColumns::Only(vec![
                ColumnProjection::no_projection(String::from("foo")),
                ColumnProjection::no_projection(String::from("bar")),
            ]),
            source: Box::new(SelectSource::Table(String::from("the_data"))),
            where_clause: Some(WhereClause {
                left: WhereMember::Value(DbValue::Integer(1)),
                cmp: WhereCmp::GreaterThan,
                right: WhereMember::Value(DbValue::Integer(2)),
            }),
            order_by_clause: None,
            limit: None,
        })];

        assert_eq!(actual, expected);
    }

    #[test]
    fn select_with_sort_only() {
        let stmt = "select foo, bar from the_data order by baz;";

        let tokens = Tokenizer::new(stmt);
        let actual = Parser::build(tokens).unwrap().parse().unwrap();
        let expected = vec![Statement::Select(SelectStatement {
            columns: SelectColumns::Only(vec![
                ColumnProjection::no_projection(String::from("foo")),
                ColumnProjection::no_projection(String::from("bar")),
            ]),
            source: Box::new(SelectSource::Table(String::from("the_data"))),
            where_clause: None,
            order_by_clause: Some(OrderByClause {
                sort_column: String::from("baz"),
                desc: false,
            }),
            limit: None,
        })];

        assert_eq!(actual, expected);
    }

    #[test]
    fn select_with_sort_desc() {
        let stmt = "select foo, bar from the_data order by baz desc;";

        let tokens = Tokenizer::new(stmt);
        let actual = Parser::build(tokens).unwrap().parse().unwrap();
        let expected = vec![Statement::Select(SelectStatement {
            columns: SelectColumns::Only(vec![
                ColumnProjection::no_projection(String::from("foo")),
                ColumnProjection::no_projection(String::from("bar")),
            ]),
            source: Box::new(SelectSource::Table(String::from("the_data"))),
            where_clause: None,
            order_by_clause: Some(OrderByClause {
                sort_column: String::from("baz"),
                desc: true,
            }),
            limit: None,
        })];

        assert_eq!(actual, expected);
    }

    #[test]
    fn select_with_limit() {
        let stmt = "select * from the_data limit 42;";

        let tokens = Tokenizer::new(stmt);
        let actual = Parser::build(tokens).unwrap().parse().unwrap();
        let expected = vec![Statement::Select(SelectStatement {
            columns: SelectColumns::All,
            source: Box::new(SelectSource::Table(String::from("the_data"))),
            where_clause: None,
            order_by_clause: None,
            limit: Some(42),
        })];

        assert_eq!(actual, expected);
    }

    #[test]
    fn select_with_all_clauses() {
        let stmt = "select foo, bar from the_data where 'this' = that order by baz desc limit 5;";

        let tokens = Tokenizer::new(stmt);
        let actual = Parser::build(tokens).unwrap().parse().unwrap();
        let expected = vec![Statement::Select(SelectStatement {
            columns: SelectColumns::Only(vec![
                ColumnProjection::no_projection(String::from("foo")),
                ColumnProjection::no_projection(String::from("bar")),
            ]),
            source: Box::new(SelectSource::Table(String::from("the_data"))),
            where_clause: Some(WhereClause {
                left: WhereMember::Value(DbValue::String(String::from("this"))),
                cmp: WhereCmp::Eq,
                right: WhereMember::Column(String::from("that")),
            }),
            order_by_clause: Some(OrderByClause {
                sort_column: String::from("baz"),
                desc: true,
            }),
            limit: Some(5),
        })];

        assert_eq!(actual, expected);
    }

    #[test]
    fn select_with_row_id() {
        let stmt = "select foo, rowid from the_data;";

        let tokens = Tokenizer::new(stmt);
        let actual = Parser::build(tokens).unwrap().parse().unwrap();
        let expected = vec![Statement::Select(SelectStatement {
            columns: SelectColumns::Only(vec![
                ColumnProjection::no_projection(String::from("foo")),
                ColumnProjection::no_projection(String::from("rowid")),
            ]),
            source: Box::new(SelectSource::Table(String::from("the_data"))),
            where_clause: None,
            order_by_clause: None,
            limit: None,
        })];

        assert_eq!(actual, expected);
    }

    #[test]
    fn select_with_row_id_projection() {
        let stmt = "select foo, rowid as bar from the_data;";

        let tokens = Tokenizer::new(stmt);
        let actual = Parser::build(tokens).unwrap().parse().unwrap();
        let expected = vec![Statement::Select(SelectStatement {
            columns: SelectColumns::Only(vec![
                ColumnProjection::no_projection(String::from("foo")),
                ColumnProjection::new(String::from("rowid"), String::from("bar")),
            ]),
            source: Box::new(SelectSource::Table(String::from("the_data"))),
            where_clause: None,
            order_by_clause: None,
            limit: None,
        })];

        assert_eq!(actual, expected);
    }

    #[test]
    fn basic_create() {
        let stmt = "create table the_data (foo string);";
        let tokens = Tokenizer::new(stmt);
        let actual = Parser::build(tokens).unwrap().parse().unwrap();
        let expected = vec![Statement::Create(CreateStatement {
            table: String::from("the_data"),
            if_not_exists: false,
            columns: CreateColumns {
                names: vec![String::from("foo")],
                types: vec![DbType::String],
                primary_key_col: KeyColumn::Rowid,
            },
        })];

        assert_eq!(actual, expected);
    }

    #[test]
    fn create_if_not_exists() {
        let stmt = "create table if not exists the_data (foo string);";
        let tokens = Tokenizer::new(stmt);
        let actual = Parser::build(tokens).unwrap().parse().unwrap();
        let expected = vec![Statement::Create(CreateStatement {
            table: String::from("the_data"),
            if_not_exists: true,
            columns: CreateColumns {
                names: vec![String::from("foo")],
                types: vec![DbType::String],
                primary_key_col: KeyColumn::Rowid,
            },
        })];

        assert_eq!(actual, expected);
    }

    #[test]
    fn create_with_primary_key() {
        let stmt = "create table the_data (foo string primary key, bar integer);";
        let tokens = Tokenizer::new(stmt);
        let actual = Parser::build(tokens).unwrap().parse().unwrap();
        let expected = vec![Statement::Create(CreateStatement {
            table: String::from("the_data"),
            if_not_exists: false,
            columns: CreateColumns {
                names: vec![String::from("foo"), String::from("bar")],
                types: vec![DbType::String, DbType::Integer],
                primary_key_col: KeyColumn::Column(String::from("foo")),
            },
        })];

        assert_eq!(actual, expected);
    }

    #[test]
    fn create_with_multiple_primary_keys() {
        let stmt = "create table the_data (foo string primary key, bar integer primary key);";
        let tokens = Tokenizer::new(stmt);
        let actual = Parser::build(tokens).unwrap().parse();
        assert!(matches!(
            actual.unwrap_err(),
            ParsingError::MultiplePrimaryKeys
        ));
    }

    #[test]
    fn create_table_all_types() {
        let stmt = "create table the_data (foo string, bar integer, baz float);";
        let tokens = Tokenizer::new(stmt);
        let actual = Parser::build(tokens).unwrap().parse().unwrap();
        let expected = vec![Statement::Create(CreateStatement {
            table: String::from("the_data"),
            if_not_exists: false,
            columns: CreateColumns {
                names: vec![
                    String::from("foo"),
                    String::from("bar"),
                    String::from("baz"),
                ],
                types: vec![DbType::String, DbType::Integer, DbType::Float],
                primary_key_col: KeyColumn::Rowid,
            },
        })];

        assert_eq!(actual, expected);
    }

    #[test]
    fn insert_into() {
        let stmt = "insert into the_data (foo, bar, baz) values ('thing', 42, 5.25);";
        let tokens = Tokenizer::new(stmt);
        let actual = Parser::build(tokens).unwrap().parse().unwrap();
        let expected = vec![Statement::Insert(InsertStatement {
            table: String::from("the_data"),
            columns: vec![
                String::from("foo"),
                String::from("bar"),
                String::from("baz"),
            ],
            values: vec![
                DbValue::String(String::from("thing")),
                DbValue::Integer(42),
                DbValue::Float(DbFloat::new(5.25)),
            ],
            conflict_clause: None,
        })];

        assert_eq!(actual, expected);
    }

    #[test]
    fn insert_with_conflict_clause() {
        let stmt = "insert into the_data (foo, bar, baz) values ('thing', 42, 5.25) on conflict (foo, bar) DO NOTHING;";
        let tokens = Tokenizer::new(stmt);
        let actual = Parser::build(tokens).unwrap().parse().unwrap();
        let expected = vec![Statement::Insert(InsertStatement {
            table: String::from("the_data"),
            columns: vec![
                String::from("foo"),
                String::from("bar"),
                String::from("baz"),
            ],
            values: vec![
                DbValue::String(String::from("thing")),
                DbValue::Integer(42),
                DbValue::Float(DbFloat::new(5.25)),
            ],
            conflict_clause: Some(ConflictClause {
                target_columns: vec![String::from("foo"), String::from("bar")],
                action: ConflictAction::Nothing,
            }),
        })];

        assert_eq!(actual, expected);
    }

    #[test]
    fn destroy() {
        let stmt = "destroy table the_data;";
        let tokens = Tokenizer::new(stmt);
        let actual = Parser::build(tokens).unwrap().parse().unwrap();
        let expected = vec![Statement::Destroy(DestroyStatement {
            table: String::from("the_data"),
        })];

        assert_eq!(actual, expected);
    }

    #[test]
    fn multiple_statements() {
        let input = "create table if not exists the_data (foo string, bar integer); select * from the_data;";
        let tokens = Tokenizer::new(input);
        let actual = Parser::build(tokens).unwrap().parse().unwrap();
        let expected = vec![
            Statement::Create(CreateStatement {
                table: String::from("the_data"),
                if_not_exists: true,
                columns: CreateColumns {
                    names: vec![String::from("foo"), String::from("bar")],
                    types: vec![DbType::String, DbType::Integer],
                    primary_key_col: KeyColumn::Rowid,
                },
            }),
            Statement::Select(SelectStatement {
                columns: SelectColumns::All,
                source: Box::new(SelectSource::Table(String::from("the_data"))),
                where_clause: None,
                order_by_clause: None,
                limit: None,
            }),
        ];

        assert_eq!(actual, expected);
    }

    #[test]
    fn delete() {
        let input = "delete from the_data where a = 'thing';";
        let tokens = Tokenizer::new(input);
        let actual = Parser::build(tokens).unwrap().parse().unwrap();
        let expected = vec![Statement::Delete(DeleteStatement {
            table: String::from("the_data"),
            where_clause: WhereClause {
                left: WhereMember::Column(String::from("a")),
                cmp: WhereCmp::Eq,
                right: WhereMember::Value(DbValue::String(String::from("thing"))),
            },
        })];

        assert_eq!(actual, expected);
    }

    // TODO:
    // - versions of missing parts returning errors
}
