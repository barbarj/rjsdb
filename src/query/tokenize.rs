use std::str::Chars;

#[derive(PartialEq, Debug, Clone, Copy)]
pub enum TokenKind {
    // composite kinds
    Identifier,
    String,

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
            _ => None,
        }
    }
}

#[derive(PartialEq, Debug)]
pub struct Token {
    contents: String,
    kind: TokenKind,
}
impl Token {
    pub fn new(contents: String, kind: TokenKind) -> Self {
        Token { contents, kind }
    }

    pub fn with_kind(self, kind: TokenKind) -> Self {
        Token {
            contents: self.contents,
            kind,
        }
    }

    pub fn maybe_convert_to_keyword(self) -> Self {
        match self.contents.to_ascii_lowercase().as_str() {
            "select" => self.with_kind(TokenKind::Select),
            "where" => self.with_kind(TokenKind::Where),
            "from" => self.with_kind(TokenKind::From),
            "order" => self.with_kind(TokenKind::Order),
            "by" => self.with_kind(TokenKind::By),
            "desc" => self.with_kind(TokenKind::Desc),
            _ => self,
        }
    }
}

pub struct Tokenizer<'a> {
    chars: Chars<'a>,
    lookahead: Option<char>,
}
impl<'a> Tokenizer<'a> {
    pub fn new(input: &'a str) -> Self {
        let mut chars = input.trim().chars();
        let lookahead = chars.next();
        Tokenizer { chars, lookahead }
    }

    fn skip_whitespace(&mut self) {
        loop {
            match self.lookahead {
                None => break,
                Some(c) if !c.is_whitespace() => {
                    break;
                }
                _ => {
                    self.lookahead = self.chars.next();
                }
            }
        }
    }

    // TODO: Make this break/return an error if there is no closing quote
    /// This should only be called when we have verified
    /// that the current lookahead is a single quote.
    fn string_token(&mut self) -> Token {
        assert_eq!(self.lookahead, Some('\''));
        let mut contents = Vec::new();

        for c in &mut self.chars {
            if c == '\'' {
                break;
            }
            contents.push(c);
        }
        self.lookahead = self.chars.next();
        Token::new(contents.iter().collect(), TokenKind::String)
    }

    fn identifier_token(&mut self) -> Token {
        // Should have already proved that lookahead is not empty
        assert!(self.lookahead.is_some());
        let lookahead = self.lookahead.unwrap();
        let mut contents = vec![lookahead];

        for c in &mut self.chars {
            if c.is_whitespace() || TokenKind::from_known_symbol(c).is_some() {
                self.lookahead = Some(c);
                return Token::new(contents.iter().collect(), TokenKind::Identifier);
            }
            contents.push(c);
        }
        self.lookahead = self.chars.next();
        Token::new(contents.iter().collect(), TokenKind::Identifier)
    }

    fn next_token(&mut self) -> Option<Token> {
        self.skip_whitespace();
        let lookahead = match self.lookahead {
            None => return None,
            Some(c) => c,
        };

        if let Some(kind) = TokenKind::from_known_symbol(lookahead) {
            self.lookahead = self.chars.next();
            return Some(Token::new(lookahead.to_string(), kind));
        }
        if lookahead == '\'' {
            return Some(self.string_token());
        }
        // else construct identifier (and possibly convert to known keyword)
        let token = self.identifier_token().maybe_convert_to_keyword();
        Some(token)
    }

    pub fn iter(self) -> Tokens<'a> {
        Tokens { tokenizer: self }
    }
}

pub struct Tokens<'a> {
    tokenizer: Tokenizer<'a>,
}
impl<'a> Iterator for Tokens<'a> {
    type Item = Token;

    fn next(&mut self) -> Option<Self::Item> {
        self.tokenizer.next_token()
    }
}

#[cfg(test)]
mod tokenizer_tests {

    use super::*;

    #[test]
    fn whitespace_splitting() {
        let input = "a * b";
        let res: Vec<Token> = Tokenizer::new(input).iter().collect();
        let expected = vec![
            Token::new("a".to_string(), TokenKind::Identifier),
            Token::new("*".to_string(), TokenKind::Star),
            Token::new("b".to_string(), TokenKind::Identifier),
        ];
        assert_eq!(res, expected);
    }

    #[test]
    fn basic_select() {
        let input = "select * from test_table;";
        let res: Vec<Token> = Tokenizer::new(input).iter().collect();
        let expected = vec![
            Token::new("select".to_string(), TokenKind::Select),
            Token::new("*".to_string(), TokenKind::Star),
            Token::new("from".to_string(), TokenKind::From),
            Token::new("test_table".to_string(), TokenKind::Identifier),
            Token::new(";".to_string(), TokenKind::Semicolon),
        ];

        assert_eq!(res, expected);
    }

    // merges_whitespace
    #[test]
    fn merges_whitespace() {
        let input = "a  * \t\n b";
        let res: Vec<Token> = Tokenizer::new(input).iter().collect();
        let expected = vec![
            Token::new("a".to_string(), TokenKind::Identifier),
            Token::new("*".to_string(), TokenKind::Star),
            Token::new("b".to_string(), TokenKind::Identifier),
        ];
        assert_eq!(res, expected);
    }

    // merges_whitespace
    #[test]
    fn trims_whitespace() {
        let input = "  a*b  ";
        let res: Vec<Token> = Tokenizer::new(input).iter().collect();
        let expected = vec![
            Token::new("a".to_string(), TokenKind::Identifier),
            Token::new("*".to_string(), TokenKind::Star),
            Token::new("b".to_string(), TokenKind::Identifier),
        ];
        assert_eq!(res, expected);

        let input = "  a*b";
        let res: Vec<Token> = Tokenizer::new(input).iter().collect();
        assert_eq!(res, expected);

        let input = "a*b  ";
        let res: Vec<Token> = Tokenizer::new(input).iter().collect();
        assert_eq!(res, expected);

        let input = "a*b";
        let res: Vec<Token> = Tokenizer::new(input).iter().collect();
        assert_eq!(res, expected);
    }

    #[test]
    fn case_insensitive_on_reserved_words() {
        let input = "sElEcT * FrOm test_table;";
        let res: Vec<Token> = Tokenizer::new(input).iter().collect();
        let expected = vec![
            Token::new("sElEcT".to_string(), TokenKind::Select),
            Token::new("*".to_string(), TokenKind::Star),
            Token::new("FrOm".to_string(), TokenKind::From),
            Token::new("test_table".to_string(), TokenKind::Identifier),
            Token::new(";".to_string(), TokenKind::Semicolon),
        ];

        assert_eq!(res, expected);
    }

    #[test]
    fn complicated_query() {
        let input =
            "select foo, bar, baz from test_table where bar='that thing' order by foo desc;";
        let res: Vec<Token> = Tokenizer::new(input).iter().collect();
        let expected = vec![
            Token::new("select".to_string(), TokenKind::Select),
            Token::new("foo".to_string(), TokenKind::Identifier),
            Token::new(",".to_string(), TokenKind::Comma),
            Token::new("bar".to_string(), TokenKind::Identifier),
            Token::new(",".to_string(), TokenKind::Comma),
            Token::new("baz".to_string(), TokenKind::Identifier),
            Token::new("from".to_string(), TokenKind::From),
            Token::new("test_table".to_string(), TokenKind::Identifier),
            Token::new("where".to_string(), TokenKind::Where),
            Token::new("bar".to_string(), TokenKind::Identifier),
            Token::new("=".to_string(), TokenKind::EqualsSign),
            Token::new("that thing".to_string(), TokenKind::String),
            Token::new("order".to_string(), TokenKind::Order),
            Token::new("by".to_string(), TokenKind::By),
            Token::new("foo".to_string(), TokenKind::Identifier),
            Token::new("desc".to_string(), TokenKind::Desc),
            Token::new(";".to_string(), TokenKind::Semicolon),
        ];

        assert_eq!(res, expected);
    }
}
