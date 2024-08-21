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

    pub fn parse(&mut self) -> Result<Vec<Expression<'a>>> {
        self.expression_list()
    }

    fn expression_list(&mut self) -> Result<Vec<Expression<'a>>> {
        let mut expressions = Vec::new();

        while !self.done_parsing() {
            expressions.push(self.expression()?);
        }

        Ok(expressions)
    }

    fn expression(&mut self) -> Result<Expression<'a>> {
        let expr = match self.peek_kind() {
            None => return Err(ParsingError::UnexpectedEndOfStatement),
            Some(TokenKind::Select) => self.select_expression()?,
            Some(TokenKind::Create) => self.create_expression()?,
            Some(TokenKind::Insert) => self.insert_expression()?,
            Some(TokenKind::Destroy) => self.destroy_expression()?,
            Some(_) => panic!("unimplemented!"),
            // TODO: Other expression types
        };
        self.end_of_statement()?;
        Ok(expr)
    }

    fn end_of_statement(&mut self) -> Result<()> {
        _ = self.consume(TokenKind::Semicolon)?;
        Ok(())
    }

    fn select_columns(&mut self) -> Result<SelectColumns<'a>> {
        if self.peek_kind() == Some(TokenKind::Star) {
            _ = self.consume(TokenKind::Star)?;
            return Ok(SelectColumns::All);
        }
        let first = self.consume(TokenKind::Identifier)?;
        let mut cols = vec![first.contents()];

        while self.peek_kind() == Some(TokenKind::Comma) {
            _ = self.consume(TokenKind::Comma)?;
            let token = self.consume(TokenKind::Identifier)?;
            cols.push(token.contents());
        }

        Ok(SelectColumns::Only(cols))
    }

    fn select_expression(&mut self) -> Result<Expression<'a>> {
        _ = self.consume(TokenKind::Select)?;

        let columns = self.select_columns()?;

        _ = self.consume(TokenKind::From)?;
        let table = self.consume(TokenKind::Identifier)?.contents();

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

        Ok(Expression::Select {
            columns,
            table,
            where_clause,
            order_by_clause,
        })
    }

    fn is_where_clause_member_kind(tk: TokenKind) -> bool {
        matches!(
            tk,
            TokenKind::Identifier | TokenKind::String | TokenKind::Integer | TokenKind::Float
        )
    }

    fn where_clause(&mut self) -> Result<WhereClause<'a>> {
        _ = self.consume(TokenKind::Where)?;
        let left = match self.peek_kind() {
            Some(k) if Parser::is_where_clause_member_kind(k) => self.consume(k)?,
            Some(_) => return Err(ParsingError::UnexpectedTokenType),
            None => return Err(ParsingError::UnexpectedEndOfStatement),
        };
        let cmp = match self.peek_kind() {
            Some(k) if matches!(k, TokenKind::EqualsSign) => self.consume(k)?,
            Some(_) => return Err(ParsingError::UnexpectedTokenType),
            None => return Err(ParsingError::UnexpectedEndOfStatement),
        };
        let right = match self.peek_kind() {
            Some(k) if Parser::is_where_clause_member_kind(k) => self.consume(k)?,
            Some(_) => return Err(ParsingError::UnexpectedTokenType),
            None => return Err(ParsingError::UnexpectedEndOfStatement),
        };
        Ok(WhereClause { left, cmp, right })
    }

    fn order_by_clause(&mut self) -> Result<OrderByClause<'a>> {
        _ = self.consume(TokenKind::Order)?;
        _ = self.consume(TokenKind::By)?;
        let sort_column = self.consume(TokenKind::Identifier)?.contents();
        let desc = self.peek_kind().filter(|k| *k == TokenKind::Desc).is_some();
        if desc {
            _ = self.consume(TokenKind::Desc)?;
        }
        Ok(OrderByClause { sort_column, desc })
    }

    fn create_expression(&mut self) -> Result<Expression<'a>> {
        _ = self.consume(TokenKind::Create)?;
        _ = self.consume(TokenKind::Table)?;
        let if_not_exists = self.peek_kind().filter(|k| *k == TokenKind::If).is_some();
        if if_not_exists {
            _ = self.consume(TokenKind::If)?;
            _ = self.consume(TokenKind::Not)?;
            _ = self.consume(TokenKind::Exists)?;
        }
        let table = self.consume(TokenKind::Identifier)?.contents();
        let columns = self.create_columns()?;

        Ok(Expression::Create {
            table,
            if_not_exists,
            columns,
        })
    }

    fn create_columns(&mut self) -> Result<CreateColumns<'a>> {
        _ = self.consume(TokenKind::LeftParen)?;
        let mut names = Vec::new();
        let mut types = Vec::new();
        while self.peek_kind().is_some() && self.peek_kind() != Some(TokenKind::RightParen) {
            let name = self.consume(TokenKind::Identifier)?.contents();
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

    fn insert_expression(&mut self) -> Result<Expression<'a>> {
        _ = self.consume(TokenKind::Insert)?;
        _ = self.consume(TokenKind::Into)?;

        let table = self.consume(TokenKind::Identifier)?.contents();

        let mut columns = Vec::new();
        _ = self.consume(TokenKind::LeftParen)?;
        while self.peek_kind().is_some() && self.peek_kind() != Some(TokenKind::RightParen) {
            let name = self.consume(TokenKind::Identifier)?.contents();
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

        Ok(Expression::Insert {
            table,
            columns,
            values,
        })
    }

    fn destroy_expression(&mut self) -> Result<Expression<'a>> {
        _ = self.consume(TokenKind::Destroy)?;
        _ = self.consume(TokenKind::Table)?;
        let table = self.consume(TokenKind::Identifier)?.contents();
        Ok(Expression::Destroy { table })
    }
}

#[derive(PartialEq, Debug)]
pub enum SelectColumns<'a> {
    All,
    Only(Vec<&'a str>),
}

#[derive(PartialEq, Debug)]
pub struct CreateColumns<'a> {
    names: Vec<&'a str>,
    types: Vec<DbType>,
}

#[derive(PartialEq, Debug)]
pub enum Expression<'a> {
    Select {
        columns: SelectColumns<'a>,
        table: &'a str,
        where_clause: Option<WhereClause<'a>>,
        order_by_clause: Option<OrderByClause<'a>>,
    },
    Create {
        table: &'a str,
        if_not_exists: bool,
        columns: CreateColumns<'a>,
    },
    Insert {
        table: &'a str,
        columns: Vec<&'a str>,
        // TODO: Figure out how to properly handle values.
        // should I convert number types during tokenization??
        values: Vec<DbValue>,
    },
    Destroy {
        table: &'a str,
    },
}

#[derive(PartialEq, Debug)]
pub struct WhereClause<'a> {
    left: Token<'a>,
    cmp: Token<'a>,
    right: Token<'a>,
}

#[derive(PartialEq, Debug)]
pub struct OrderByClause<'a> {
    sort_column: &'a str,
    desc: bool,
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
        let expected = vec![Expression::Select {
            columns: SelectColumns::Only(vec!["foo", "bar"]),
            table: "the_data",
            where_clause: None,
            order_by_clause: None,
        }];

        assert_eq!(actual, expected);
    }

    #[test]
    fn basic_select_star() {
        let stmt = "select * from the_data;";

        let tokens = Tokenizer::new(stmt);
        let actual = Parser::new(tokens).parse().unwrap();
        let expected = vec![Expression::Select {
            columns: SelectColumns::All,
            table: "the_data",
            where_clause: None,
            order_by_clause: None,
        }];

        assert_eq!(actual, expected);
    }

    #[test]
    fn select_with_where_only() {
        let stmt = "select foo, bar from the_data where that = 'this';";

        let tokens = Tokenizer::new(stmt);
        let actual = Parser::new(tokens).parse().unwrap();
        let expected = vec![Expression::Select {
            columns: SelectColumns::Only(vec!["foo", "bar"]),
            table: "the_data",
            where_clause: Some(WhereClause {
                left: Token::new("that", TokenKind::Identifier),
                cmp: Token::new("=", TokenKind::EqualsSign),
                right: Token::new("this", TokenKind::String),
            }),
            order_by_clause: None,
        }];

        assert_eq!(actual, expected);
    }

    #[test]
    fn select_with_sort_only() {
        let stmt = "select foo, bar from the_data order by baz;";

        let tokens = Tokenizer::new(stmt);
        let actual = Parser::new(tokens).parse().unwrap();
        let expected = vec![Expression::Select {
            columns: SelectColumns::Only(vec!["foo", "bar"]),
            table: "the_data",
            where_clause: None,
            order_by_clause: Some(OrderByClause {
                sort_column: "baz",
                desc: false,
            }),
        }];

        assert_eq!(actual, expected);
    }

    #[test]
    fn select_with_sort_desc() {
        let stmt = "select foo, bar from the_data order by baz desc;";

        let tokens = Tokenizer::new(stmt);
        let actual = Parser::new(tokens).parse().unwrap();
        let expected = vec![Expression::Select {
            columns: SelectColumns::Only(vec!["foo", "bar"]),
            table: "the_data",
            where_clause: None,
            order_by_clause: Some(OrderByClause {
                sort_column: "baz",
                desc: true,
            }),
        }];

        assert_eq!(actual, expected);
    }

    #[test]
    fn select_with_where_and_sort() {
        let stmt = "select foo, bar from the_data where 'this' = that order by baz desc;";

        let tokens = Tokenizer::new(stmt);
        let actual = Parser::new(tokens).parse().unwrap();
        let expected = vec![Expression::Select {
            columns: SelectColumns::Only(vec!["foo", "bar"]),
            table: "the_data",
            where_clause: Some(WhereClause {
                left: Token::new("this", TokenKind::String),
                cmp: Token::new("=", TokenKind::EqualsSign),
                right: Token::new("that", TokenKind::Identifier),
            }),
            order_by_clause: Some(OrderByClause {
                sort_column: "baz",
                desc: true,
            }),
        }];

        assert_eq!(actual, expected);
    }

    #[test]
    fn basic_create() {
        let stmt = "create table the_data (foo string);";
        let tokens = Tokenizer::new(stmt);
        let actual = Parser::new(tokens).parse().unwrap();
        let expected = vec![Expression::Create {
            table: "the_data",
            if_not_exists: false,
            columns: CreateColumns {
                names: vec!["foo"],
                types: vec![DbType::String],
            },
        }];

        assert_eq!(actual, expected);
    }

    #[test]
    fn create_if_not_exists() {
        let stmt = "create table if not exists the_data (foo string);";
        let tokens = Tokenizer::new(stmt);
        let actual = Parser::new(tokens).parse().unwrap();
        let expected = vec![Expression::Create {
            table: "the_data",
            if_not_exists: true,
            columns: CreateColumns {
                names: vec!["foo"],
                types: vec![DbType::String],
            },
        }];

        assert_eq!(actual, expected);
    }

    #[test]
    fn create_table_all_types() {
        let stmt = "create table the_data (foo string, bar integer, baz float);";
        let tokens = Tokenizer::new(stmt);
        let actual = Parser::new(tokens).parse().unwrap();
        let expected = vec![Expression::Create {
            table: "the_data",
            if_not_exists: false,
            columns: CreateColumns {
                names: vec!["foo", "bar", "baz"],
                types: vec![DbType::String, DbType::Integer, DbType::Float],
            },
        }];

        assert_eq!(actual, expected);
    }

    #[test]
    fn insert_into() {
        let stmt = "insert into the_data (foo, bar, baz) values ('thing', 42, 5.25);";
        let tokens = Tokenizer::new(stmt);
        let actual = Parser::new(tokens).parse().unwrap();
        let expected = vec![Expression::Insert {
            table: "the_data",
            columns: vec!["foo", "bar", "baz"],
            values: vec![
                DbValue::String(String::from("thing")),
                DbValue::Integer(42),
                DbValue::Float(5.25),
            ],
        }];

        assert_eq!(actual, expected);
    }

    #[test]
    fn destroy() {
        let stmt = "destroy table the_data;";
        let tokens = Tokenizer::new(stmt);
        let actual = Parser::new(tokens).parse().unwrap();
        let expected = vec![Expression::Destroy { table: "the_data" }];

        assert_eq!(actual, expected);
    }

    #[test]
    fn multiple_statements() {
        let input = "create table if not exists the_data (foo string, bar integer); select * from the_data;";
        let tokens = Tokenizer::new(input);
        let actual = Parser::new(tokens).parse().unwrap();
        let expected = vec![
            Expression::Create {
                table: "the_data",
                if_not_exists: true,
                columns: CreateColumns {
                    names: vec!["foo", "bar"],
                    types: vec![DbType::String, DbType::Integer],
                },
            },
            Expression::Select {
                columns: SelectColumns::All,
                table: "the_data",
                where_clause: None,
                order_by_clause: None,
            },
        ];

        assert_eq!(actual, expected);
    }

    // TODO:
    // - versions of missing parts returning errors
}
