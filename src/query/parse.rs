use super::tokenize::{Token, TokenKind, Tokenizer, Tokens};

pub enum ParsingError {
    UnexpectedEndOfStatement,
    UnexpectedTokenType,
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

    fn consume(&mut self, _tk: TokenKind) -> Result<Token<'a>> {
        let token = self.lookahead.take();
        match token {
            Some(t) if matches!(t.kind(), _tk) => {
                self.lookahead = self.tokens.next();
                Ok(t)
            }
            Some(_) => Err(ParsingError::UnexpectedTokenType),
            None => Err(ParsingError::UnexpectedEndOfStatement),
        }
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

    fn select_columns(&mut self) -> Result<Vec<&'a str>> {
        let first = self.consume(TokenKind::Identifier)?;
        let mut cols = vec![first.contents()];

        while self.peek_kind() == Some(TokenKind::Comma) {
            _ = self.consume(TokenKind::Comma)?;
            let token = self.consume(TokenKind::Identifier)?;
            cols.push(token.contents());
        }

        Ok(cols)
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

        Ok(Expression::SelectExpression {
            columns,
            table,
            where_clause,
            order_by_clause,
        })
    }

    // TODO: Visit again after adding int and float tokens
    fn where_clause(&mut self) -> Result<WhereClause<'a>> {
        let left = match self.peek_kind() {
            Some(k) if matches!(k, TokenKind::Identifier | TokenKind::String) => self.consume(k)?,
            Some(_) => return Err(ParsingError::UnexpectedTokenType),
            None => return Err(ParsingError::UnexpectedEndOfStatement),
        };
        let cmp = match self.peek_kind() {
            Some(k) if matches!(k, TokenKind::EqualsSign) => self.consume(k)?,
            Some(_) => return Err(ParsingError::UnexpectedTokenType),
            None => return Err(ParsingError::UnexpectedEndOfStatement),
        };
        let right = match self.peek_kind() {
            Some(k) if matches!(k, TokenKind::Identifier | TokenKind::String) => self.consume(k)?,
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
}

pub enum Expression<'a> {
    SelectExpression {
        columns: Vec<&'a str>,
        table: &'a str,
        where_clause: Option<WhereClause<'a>>,
        order_by_clause: Option<OrderByClause<'a>>,
    },
}

pub struct WhereClause<'a> {
    left: Token<'a>,
    cmp: Token<'a>,
    right: Token<'a>,
}

pub struct OrderByClause<'a> {
    sort_column: &'a str,
    desc: bool,
}
/*
Select foo, bar from test_table where foo = 'a' order by bar desc;

---
- access test_table
- order by bar desc
- where foo = 'a'
- Select foo, bar
---

*/

// Select
// - columns
// - from table
// - filter
// - sort

// Create
// - table
// - if not exists?
// - columns

// Insert (into)
// - table
// - columns
// - values

// Destroy
// - table
