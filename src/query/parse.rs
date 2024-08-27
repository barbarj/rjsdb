use std::num::{ParseFloatError, ParseIntError};

use crate::{DbType, DbValue};

use super::tokenize::{Token, TokenKind, Tokenizer, Tokens};

#[derive(Debug)]
pub enum ParsingError {
    UnexpectedEndOfStatement,
    UnexpectedTokenType,
    ParseFloatError(ParseFloatError),
    ParseIntError(ParseIntError),
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

type Result<T> = std::result::Result<T, ParsingError>;

pub struct Parser<'a> {
    tokens: Tokens<'a>,
    lookahead: Option<Token<'a>>,
}
impl<'a> Parser<'a> {
    pub fn new(tokenizer: Tokenizer<'a>) -> Self {
        let mut tokens = tokenizer.iter();
        let lookahead = tokens.next();
        Parser { tokens, lookahead }
    }

    fn done_parsing(&self) -> bool {
        self.lookahead.is_none()
    }

    fn consume(&mut self, tk: TokenKind) -> Result<Token<'a>> {
        let token = self.lookahead.take();
        match token {
            Some(t) if t.kind() == tk => {
                self.lookahead = self.tokens.next();
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
            TokenKind::TypeString | TokenKind::TypeInteger | TokenKind::TypeFloat
        ) {
            self.lookahead = self.tokens.next();
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
            self.lookahead = self.tokens.next();
            return Ok(token);
        }
        Err(ParsingError::UnexpectedTokenType)
    }

    fn peek_kind(&self) -> Option<TokenKind> {
        self.lookahead.as_ref().map(|t| t.kind())
    }

    pub fn parse(&mut self) -> Result<Vec<Expression>> {
        self.expression_list()
    }

    fn expression_list(&mut self) -> Result<Vec<Expression>> {
        let mut expressions = Vec::new();

        while !self.done_parsing() {
            expressions.push(self.expression()?);
        }

        Ok(expressions)
    }

    fn expression(&mut self) -> Result<Expression> {
        let expr = match self.peek_kind() {
            None => return Err(ParsingError::UnexpectedEndOfStatement),
            Some(TokenKind::Select) => self.select_expression()?,
            Some(TokenKind::Create) => self.create_expression()?,
            Some(TokenKind::Insert) => self.insert_expression()?,
            Some(TokenKind::Destroy) => self.destroy_expression()?,
            Some(_) => return Err(ParsingError::UnexpectedTokenType),
        };
        self.end_of_statement()?;
        Ok(expr)
    }

    fn end_of_statement(&mut self) -> Result<()> {
        _ = self.consume(TokenKind::Semicolon)?;
        Ok(())
    }

    fn column_projection(&mut self) -> Result<ColumnProjection> {
        let in_name = self.consume(TokenKind::Identifier)?.contents().to_string();
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

    fn select_expression(&mut self) -> Result<Expression> {
        _ = self.consume(TokenKind::Select)?;

        let columns = self.select_columns()?;

        _ = self.consume(TokenKind::From)?;
        let table = self.consume(TokenKind::Identifier)?.contents().to_string();

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

        Ok(Expression::Select(SelectExpression {
            columns,
            table,
            where_clause,
            order_by_clause,
            limit,
        }))
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
                token.contents().parse::<i32>()?,
            ))),
            TokenKind::Float => Ok(WhereMember::Value(DbValue::Float(
                token.contents().parse::<f32>()?,
            ))),
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
        let sort_column = self.consume(TokenKind::Identifier)?.contents().to_string();
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

    fn create_expression(&mut self) -> Result<Expression> {
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

        Ok(Expression::Create(CreateExpression {
            table,
            if_not_exists,
            columns,
        }))
    }

    fn create_columns(&mut self) -> Result<CreateColumns> {
        _ = self.consume(TokenKind::LeftParen)?;
        let mut names = Vec::new();
        let mut types = Vec::new();
        while self.peek_kind().is_some() && self.peek_kind() != Some(TokenKind::RightParen) {
            let name = self.consume(TokenKind::Identifier)?.contents().to_string();
            let this_type = match self.consume_type_token()?.kind() {
                TokenKind::TypeString => DbType::String,
                TokenKind::TypeInteger => DbType::Integer,
                TokenKind::TypeFloat => DbType::Float,
                _ => panic!("Got a non-type token!"),
            };

            names.push(name);
            types.push(this_type);

            if self.peek_kind() != Some(TokenKind::RightParen) {
                _ = self.consume(TokenKind::Comma)?;
            }
        }
        _ = self.consume(TokenKind::RightParen)?;
        Ok(CreateColumns { names, types })
    }

    fn insert_expression(&mut self) -> Result<Expression> {
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
                TokenKind::Float => DbValue::Float(token.contents().parse::<f32>()?),
                TokenKind::Integer => DbValue::Integer(token.contents().parse::<i32>()?),
                _ => panic!("Should not happen!"),
            };

            values.push(val);
            if self.peek_kind() != Some(TokenKind::RightParen) {
                _ = self.consume(TokenKind::Comma)?;
            }
        }
        _ = self.consume(TokenKind::RightParen)?;

        Ok(Expression::Insert(InsertExpression {
            table,
            columns,
            values,
        }))
    }

    fn destroy_expression(&mut self) -> Result<Expression> {
        _ = self.consume(TokenKind::Destroy)?;
        _ = self.consume(TokenKind::Table)?;
        let table = self.consume(TokenKind::Identifier)?.contents().to_string();
        Ok(Expression::Destroy(DestroyExpression { table }))
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
pub struct CreateColumns {
    pub names: Vec<String>,
    pub types: Vec<DbType>,
}

#[derive(PartialEq, Debug)]
pub enum Expression {
    Select(SelectExpression),
    Create(CreateExpression),
    Insert(InsertExpression),
    Destroy(DestroyExpression),
}

#[derive(PartialEq, Debug)]
pub struct SelectExpression {
    pub columns: SelectColumns,
    pub table: String,
    pub where_clause: Option<WhereClause>,
    pub order_by_clause: Option<OrderByClause>,
    pub limit: Option<usize>,
}

#[derive(PartialEq, Debug)]
pub struct CreateExpression {
    pub table: String,
    pub if_not_exists: bool,
    pub columns: CreateColumns,
}

#[derive(PartialEq, Debug)]
pub struct InsertExpression {
    pub table: String,
    pub columns: Vec<String>,
    pub values: Vec<DbValue>,
}

#[derive(PartialEq, Debug)]
pub struct DestroyExpression {
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

#[cfg(test)]
mod parser_tests {
    use super::*;

    #[test]
    fn consume() {
        let stmt = "'that' this";
        let tokens = Tokenizer::new(stmt);
        let mut parser = Parser::new(tokens);

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
        let actual = Parser::new(tokens).parse().unwrap();
        let expected = vec![Expression::Select(SelectExpression {
            columns: SelectColumns::Only(vec![
                ColumnProjection::no_projection(String::from("foo")),
                ColumnProjection::no_projection(String::from("bar")),
            ]),
            table: String::from("the_data"),
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
        let actual = Parser::new(tokens).parse().unwrap();
        let expected = vec![Expression::Select(SelectExpression {
            columns: SelectColumns::Only(vec![
                ColumnProjection::new(String::from("a"), String::from("b")),
                ColumnProjection::no_projection(String::from("bar")),
                ColumnProjection::new(String::from("c"), String::from("d")),
            ]),
            table: String::from("the_data"),
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
        let actual = Parser::new(tokens).parse().unwrap();
        let expected = vec![Expression::Select(SelectExpression {
            columns: SelectColumns::All,
            table: String::from("the_data"),
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
        let actual = Parser::new(tokens).parse().unwrap();
        let expected = vec![Expression::Select(SelectExpression {
            columns: SelectColumns::Only(vec![
                ColumnProjection::no_projection(String::from("foo")),
                ColumnProjection::no_projection(String::from("bar")),
            ]),
            table: String::from("the_data"),
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
        let actual = Parser::new(tokens).parse().unwrap();
        let expected = vec![Expression::Select(SelectExpression {
            columns: SelectColumns::Only(vec![
                ColumnProjection::no_projection(String::from("foo")),
                ColumnProjection::no_projection(String::from("bar")),
            ]),
            table: String::from("the_data"),
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
        let actual = Parser::new(tokens).parse().unwrap();
        let expected = vec![Expression::Select(SelectExpression {
            columns: SelectColumns::Only(vec![
                ColumnProjection::no_projection(String::from("foo")),
                ColumnProjection::no_projection(String::from("bar")),
            ]),
            table: String::from("the_data"),
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
        let actual = Parser::new(tokens).parse().unwrap();
        let expected = vec![Expression::Select(SelectExpression {
            columns: SelectColumns::Only(vec![
                ColumnProjection::no_projection(String::from("foo")),
                ColumnProjection::no_projection(String::from("bar")),
            ]),
            table: String::from("the_data"),
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
        let actual = Parser::new(tokens).parse().unwrap();
        let expected = vec![Expression::Select(SelectExpression {
            columns: SelectColumns::Only(vec![
                ColumnProjection::no_projection(String::from("foo")),
                ColumnProjection::no_projection(String::from("bar")),
            ]),
            table: String::from("the_data"),
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
        let actual = Parser::new(tokens).parse().unwrap();
        let expected = vec![Expression::Select(SelectExpression {
            columns: SelectColumns::All,
            table: String::from("the_data"),
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
        let actual = Parser::new(tokens).parse().unwrap();
        let expected = vec![Expression::Select(SelectExpression {
            columns: SelectColumns::Only(vec![
                ColumnProjection::no_projection(String::from("foo")),
                ColumnProjection::no_projection(String::from("bar")),
            ]),
            table: String::from("the_data"),
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
    fn basic_create() {
        let stmt = "create table the_data (foo string);";
        let tokens = Tokenizer::new(stmt);
        let actual = Parser::new(tokens).parse().unwrap();
        let expected = vec![Expression::Create(CreateExpression {
            table: String::from("the_data"),
            if_not_exists: false,
            columns: CreateColumns {
                names: vec![String::from("foo")],
                types: vec![DbType::String],
            },
        })];

        assert_eq!(actual, expected);
    }

    #[test]
    fn create_if_not_exists() {
        let stmt = "create table if not exists the_data (foo string);";
        let tokens = Tokenizer::new(stmt);
        let actual = Parser::new(tokens).parse().unwrap();
        let expected = vec![Expression::Create(CreateExpression {
            table: String::from("the_data"),
            if_not_exists: true,
            columns: CreateColumns {
                names: vec![String::from("foo")],
                types: vec![DbType::String],
            },
        })];

        assert_eq!(actual, expected);
    }

    #[test]
    fn create_table_all_types() {
        let stmt = "create table the_data (foo string, bar integer, baz float);";
        let tokens = Tokenizer::new(stmt);
        let actual = Parser::new(tokens).parse().unwrap();
        let expected = vec![Expression::Create(CreateExpression {
            table: String::from("the_data"),
            if_not_exists: false,
            columns: CreateColumns {
                names: vec![
                    String::from("foo"),
                    String::from("bar"),
                    String::from("baz"),
                ],
                types: vec![DbType::String, DbType::Integer, DbType::Float],
            },
        })];

        assert_eq!(actual, expected);
    }

    #[test]
    fn insert_into() {
        let stmt = "insert into the_data (foo, bar, baz) values ('thing', 42, 5.25);";
        let tokens = Tokenizer::new(stmt);
        let actual = Parser::new(tokens).parse().unwrap();
        let expected = vec![Expression::Insert(InsertExpression {
            table: String::from("the_data"),
            columns: vec![
                String::from("foo"),
                String::from("bar"),
                String::from("baz"),
            ],
            values: vec![
                DbValue::String(String::from("thing")),
                DbValue::Integer(42),
                DbValue::Float(5.25),
            ],
        })];

        assert_eq!(actual, expected);
    }

    #[test]
    fn destroy() {
        let stmt = "destroy table the_data;";
        let tokens = Tokenizer::new(stmt);
        let actual = Parser::new(tokens).parse().unwrap();
        let expected = vec![Expression::Destroy(DestroyExpression {
            table: String::from("the_data"),
        })];

        assert_eq!(actual, expected);
    }

    #[test]
    fn multiple_statements() {
        let input = "create table if not exists the_data (foo string, bar integer); select * from the_data;";
        let tokens = Tokenizer::new(input);
        let actual = Parser::new(tokens).parse().unwrap();
        let expected = vec![
            Expression::Create(CreateExpression {
                table: String::from("the_data"),
                if_not_exists: true,
                columns: CreateColumns {
                    names: vec![String::from("foo"), String::from("bar")],
                    types: vec![DbType::String, DbType::Integer],
                },
            }),
            Expression::Select(SelectExpression {
                columns: SelectColumns::All,
                table: String::from("the_data"),
                where_clause: None,
                order_by_clause: None,
                limit: None,
            }),
        ];

        assert_eq!(actual, expected);
    }

    // TODO:
    // - versions of missing parts returning errors
}
