use std::{
    collections::VecDeque,
    ops::{Range, RangeFrom, RangeTo},
    str::CharIndices,
};

#[derive(PartialEq, Debug, Clone, Copy)]
pub enum TokenKind {
    // composite kinds
    String,
    Whitespace,

    // reserved words
    Select,
    Where,
    From,
    Order,
    By,
    Desc,

    // known symbols
    Star,
    Comma,
    Semicolon,
    EqualsSign,
    SingleQuote,
}
impl TokenKind {
    /// Converts from a single char to the correct TokenKind.
    /// If this single char does not have a corresponding TokenKind
    /// that is also a single char (i.e. it's part of a string or whitespace),
    /// return None.
    fn from_known_symbol(char: char) -> Option<Self> {
        match char {
            '*' => Some(Self::Star),
            ',' => Some(Self::Comma),
            ';' => Some(Self::Semicolon),
            '=' => Some(Self::EqualsSign),
            '\'' => Some(Self::SingleQuote),
            _ => None,
        }
    }

    /// Converts from the provided string to the correct TokenKind.
    /// If this string does not match a known symbol, return None.
    fn from_reserved_word(str: &str) -> Option<Self> {
        match str.to_ascii_lowercase().as_ref() {
            "select" => Some(Self::Select),
            "where" => Some(Self::Where),
            "from" => Some(Self::From),
            "order" => Some(Self::Order),
            "by" => Some(Self::By),
            "desc" => Some(Self::Desc),
            _ => None,
        }
    }
}

#[derive(PartialEq, Debug)]
pub struct Token<'a> {
    contents: &'a str,
    kind: TokenKind,
}
impl<'a> Token<'a> {
    pub fn new(contents: &'a str, kind: TokenKind) -> Self {
        Token { contents, kind }
    }
}
enum TokenStateKind {
    String,
    Whitespace,
    KnownSymbol(TokenKind),
    None,
}
impl TokenStateKind {
    fn from_char(c: char) -> Self {
        if let Some(kind) = TokenKind::from_known_symbol(c) {
            Self::KnownSymbol(kind)
        } else if c.is_whitespace() {
            Self::Whitespace
        } else {
            Self::String
        }
    }
}

struct TokenState {
    first: usize,
    last: usize,
    kind: TokenStateKind,
}
impl TokenState {
    fn new(start: usize, kind: TokenStateKind) -> Self {
        TokenState {
            first: start,
            last: start,
            kind,
        }
    }

    fn slice<'a>(&self, reference_str: &'a str) -> &'a str {
        &reference_str[self.first..self.last + 1]
    }

    fn to_token<'a>(&self, reference_str: &'a str) -> Token<'a> {
        let kind = match self.kind {
            TokenStateKind::KnownSymbol(kind) => kind,
            TokenStateKind::String => TokenKind::from_reserved_word(self.slice(reference_str))
                .unwrap_or(TokenKind::String),
            TokenStateKind::Whitespace => TokenKind::Whitespace,
            TokenStateKind::None => panic!("TokenStateKind::None -> TokenKind is not supported"),
        };

        Token::new(self.slice(reference_str), kind)
    }
}
impl Default for TokenState {
    fn default() -> Self {
        TokenState {
            first: 0,
            last: 0,
            kind: TokenStateKind::None,
        }
    }
}

pub struct Tokenizer<'a> {
    input: &'a str,
    iter: CharIndices<'a>,
    unused_pair: Option<(usize, char)>,
}
impl<'a> Tokenizer<'a> {
    pub fn new(input: &'a str) -> Self {
        Tokenizer {
            input,
            iter: input.char_indices(),
            unused_pair: None,
        }
    }

    fn next_token(&mut self) -> Option<Token<'a>> {
        let mut token_state = TokenState::default();
        if let Some((idx, char)) = self.unused_pair {
            self.unused_pair = None;
            if let Some(kind) = TokenKind::from_known_symbol(char) {
                return Some(Token::new(&self.input[idx..idx + 1], kind));
            } else {
                let kind = if char.is_whitespace() {
                    TokenStateKind::Whitespace
                } else {
                    TokenStateKind::String
                };
                token_state = TokenState::new(idx, kind)
            }
        }

        for (idx, char) in &mut self.iter {
            match TokenStateKind::from_char(char) {
                TokenStateKind::KnownSymbol(kind) => {
                    if matches!(token_state.kind, TokenStateKind::None) {
                        return Some(Token::new(&self.input[idx..idx + 1], kind));
                    } else {
                        self.unused_pair = Some((idx, char));
                        return Some(token_state.to_token(self.input));
                    }
                }
                TokenStateKind::String if matches!(token_state.kind, TokenStateKind::None) => {
                    token_state = TokenState::new(idx, TokenStateKind::String);
                }
                TokenStateKind::String if !matches!(token_state.kind, TokenStateKind::String) => {
                    self.unused_pair = Some((idx, char));
                    return Some(token_state.to_token(self.input));
                }
                TokenStateKind::Whitespace if matches!(token_state.kind, TokenStateKind::None) => {
                    token_state = TokenState::new(idx, TokenStateKind::Whitespace);
                }
                TokenStateKind::Whitespace
                    if !matches!(token_state.kind, TokenStateKind::Whitespace) =>
                {
                    self.unused_pair = Some((idx, char));
                    return Some(token_state.to_token(self.input));
                }
                TokenStateKind::String | TokenStateKind::Whitespace => {
                    token_state.last = idx;
                }
                TokenStateKind::None => panic!("This should never happen"),
            }
        }
        if !matches!(token_state.kind, TokenStateKind::None) {
            Some(token_state.to_token(self.input))
        } else {
            None
        }
    }

    pub fn tokenize(&mut self) -> Vec<Token<'a>> {
        // scan through string slice
        // when a special, known char is encountered:
        // - end token being constructed, and call it string
        // - add token for special char of type

        let mut tokens = Vec::new();
        while let Some(token) = self.next_token() {
            println!("token: '{}'", token.contents);
            tokens.push(token);
        }
        // trim token list of whitespace
        let len = tokens.len();
        match (
            tokens.first().map(|x| x.kind),
            tokens.last().map(|x| x.kind),
        ) {
            (Some(TokenKind::Whitespace), Some(TokenKind::Whitespace)) => tokens
                .drain(Range {
                    start: 1,
                    end: len - 1,
                })
                .collect(),
            (Some(TokenKind::Whitespace), _) => tokens.drain(RangeFrom { start: 1 }).collect(),
            (_, Some(TokenKind::Whitespace)) => tokens.drain(RangeTo { end: len - 1 }).collect(),
            _ => tokens,
        }
    }
}

#[cfg(test)]
mod tokenizer_tests {

    use super::*;

    #[test]
    fn whitespace_splitting() {
        let input = "a * b";
        let res = Tokenizer::new(input).tokenize();
        let expected = vec![
            Token::new("a", TokenKind::String),
            Token::new(" ", TokenKind::Whitespace),
            Token::new("*", TokenKind::Star),
            Token::new(" ", TokenKind::Whitespace),
            Token::new("b", TokenKind::String),
        ];
        assert_eq!(res, expected);
    }

    #[test]
    fn basic_select() {
        let input = "select * from test_table;";
        let res = Tokenizer::new(input).tokenize();
        let expected = vec![
            Token::new("select", TokenKind::Select),
            Token::new(" ", TokenKind::Whitespace),
            Token::new("*", TokenKind::Star),
            Token::new(" ", TokenKind::Whitespace),
            Token::new("from", TokenKind::From),
            Token::new(" ", TokenKind::Whitespace),
            Token::new("test_table", TokenKind::String),
            Token::new(";", TokenKind::Semicolon),
        ];

        assert_eq!(res, expected);
    }

    // merges_whitespace
    #[test]
    fn merges_whitespace() {
        let input = "a  * \t\n b";
        let res = Tokenizer::new(input).tokenize();
        let expected = vec![
            Token::new("a", TokenKind::String),
            Token::new("  ", TokenKind::Whitespace),
            Token::new("*", TokenKind::Star),
            Token::new(" \t\n ", TokenKind::Whitespace),
            Token::new("b", TokenKind::String),
        ];
        assert_eq!(res, expected);
    }

    // merges_whitespace
    #[test]
    fn trims_whitespace() {
        let input = "  a*b  ";
        let res = Tokenizer::new(input).tokenize();
        let expected = vec![
            Token::new("a", TokenKind::String),
            Token::new("*", TokenKind::Star),
            Token::new("b", TokenKind::String),
        ];
        assert_eq!(res, expected);

        let input = "  a*b";
        let res = Tokenizer::new(input).tokenize();
        assert_eq!(res, expected);

        let input = "a*b  ";
        let res = Tokenizer::new(input).tokenize();
        assert_eq!(res, expected);

        let input = "a*b";
        let res = Tokenizer::new(input).tokenize();
        assert_eq!(res, expected);
    }

    #[test]
    fn case_insensitive_on_reserved_words() {
        let input = "sElEcT * FrOm test_table;";
        let res = Tokenizer::new(input).tokenize();
        let expected = vec![
            Token::new("sElEcT", TokenKind::Select),
            Token::new(" ", TokenKind::Whitespace),
            Token::new("*", TokenKind::Star),
            Token::new(" ", TokenKind::Whitespace),
            Token::new("FrOm", TokenKind::From),
            Token::new(" ", TokenKind::Whitespace),
            Token::new("test_table", TokenKind::String),
            Token::new(";", TokenKind::Semicolon),
        ];

        assert_eq!(res, expected);
    }

    #[test]
    fn complicated_query() {
        let input = "select foo, bar, baz from test_table order by foo desc;";
        let res = Tokenizer::new(input).tokenize();
        let expected = vec![
            Token::new("select", TokenKind::Select),
            Token::new(" ", TokenKind::Whitespace),
            Token::new("foo", TokenKind::String),
            Token::new(",", TokenKind::Comma),
            Token::new(" ", TokenKind::Whitespace),
            Token::new("bar", TokenKind::String),
            Token::new(",", TokenKind::Comma),
            Token::new(" ", TokenKind::Whitespace),
            Token::new("baz", TokenKind::String),
            Token::new(" ", TokenKind::Whitespace),
            Token::new("from", TokenKind::From),
            Token::new(" ", TokenKind::Whitespace),
            Token::new("test_table", TokenKind::String),
            Token::new(" ", TokenKind::Whitespace),
            Token::new("order", TokenKind::Order),
            Token::new(" ", TokenKind::Whitespace),
            Token::new("by", TokenKind::By),
            Token::new(" ", TokenKind::Whitespace),
            Token::new("foo", TokenKind::String),
            Token::new(" ", TokenKind::Whitespace),
            Token::new("desc", TokenKind::Desc),
            Token::new(";", TokenKind::Semicolon),
        ];

        assert_eq!(res, expected);
    }
}
