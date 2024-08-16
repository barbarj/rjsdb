use regex::Regex;

#[derive(PartialEq, Debug, Clone, Copy)]
pub enum TokenKind {
    // for things like whitespace, etc.
    None,

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

struct SpecItem(TokenKind, Regex);

const TOKEN_SPEC_LEN: usize = 13;
pub struct Tokenizer<'a> {
    input: &'a str,
    cursor: usize,
    spec: [SpecItem; TOKEN_SPEC_LEN],
}
impl<'a> Tokenizer<'a> {
    pub fn new(input: &'a str) -> Self {
        Tokenizer {
            input,
            cursor: 0,
            spec: Tokenizer::spec(),
        }
    }

    fn spec() -> [SpecItem; TOKEN_SPEC_LEN] {
        [
            // skip whitespace
            SpecItem(TokenKind::None, Regex::new(r"^\s+").unwrap()),
            // single chars
            SpecItem(TokenKind::Star, Regex::new(r"^\*").unwrap()),
            SpecItem(TokenKind::Comma, Regex::new(r"^,").unwrap()),
            SpecItem(TokenKind::Semicolon, Regex::new(r"^;").unwrap()),
            SpecItem(TokenKind::EqualsSign, Regex::new(r"^=").unwrap()),
            // keywords
            SpecItem(TokenKind::Select, Regex::new(r"^(?i)select\b").unwrap()),
            SpecItem(TokenKind::Where, Regex::new(r"^(?i)where\b").unwrap()),
            SpecItem(TokenKind::From, Regex::new(r"^(?i)from\b").unwrap()),
            SpecItem(TokenKind::Order, Regex::new(r"^(?i)order\b").unwrap()),
            SpecItem(TokenKind::By, Regex::new(r"^(?i)by\b").unwrap()),
            SpecItem(TokenKind::Desc, Regex::new(r"^(?i)desc\b").unwrap()),
            // composites
            SpecItem(TokenKind::String, Regex::new(r"^'(.*)'").unwrap()),
            SpecItem(TokenKind::Identifier, Regex::new(r"^[^\s*,;=]+").unwrap()),
        ]
    }

    fn next_token(&mut self) -> Option<Token<'a>> {
        if self.cursor >= self.input.len() {
            return None;
        }

        let input = &self.input[self.cursor..];

        for SpecItem(kind, regex) in &self.spec {
            if let Some(m) = regex.find(input) {
                println!("matches: '{}'({})", m.as_str(), m.len());
                self.cursor += m.len();
                // TODO: Make this happen iteratively instead of recursively
                if matches!(kind, TokenKind::None) {
                    return self.next_token();
                }
                if matches!(kind, TokenKind::String) {
                    let s = &m.as_str()[1..m.len() - 1];
                    return Some(Token::new(s, *kind));
                }
                return Some(Token::new(m.as_str(), *kind));
            }
        }
        // TODO: Change this to return an error
        panic!("Unknown token type!");
    }

    pub fn iter(self) -> Tokens<'a> {
        Tokens { tokenizer: self }
    }
}

pub struct Tokens<'a> {
    tokenizer: Tokenizer<'a>,
}
impl<'a> Iterator for Tokens<'a> {
    type Item = Token<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let token = self.tokenizer.next_token();
        println!("{token:?}");
        token
    }
}

#[cfg(test)]
mod tokenizer_tests {
    use super::*;

    #[test]
    fn regex_test() {
        let r = Regex::new(r"^(?i)(select)(\s|$)").unwrap();
        let m = r.find("SELECT").unwrap();
        println!("{}", m.as_str());
    }

    #[test]
    fn whitespace_splitting() {
        let input = "a * b";
        let res: Vec<Token> = Tokenizer::new(input).iter().collect();
        let expected = vec![
            Token::new("a", TokenKind::Identifier),
            Token::new("*", TokenKind::Star),
            Token::new("b", TokenKind::Identifier),
        ];
        assert_eq!(res, expected);
    }

    #[test]
    fn basic_select() {
        let input = "select * from test_table;";
        let res: Vec<Token> = Tokenizer::new(input).iter().collect();
        let expected = vec![
            Token::new("select", TokenKind::Select),
            Token::new("*", TokenKind::Star),
            Token::new("from", TokenKind::From),
            Token::new("test_table", TokenKind::Identifier),
            Token::new(";", TokenKind::Semicolon),
        ];

        assert_eq!(res, expected);
    }

    // merges_whitespace
    #[test]
    fn merges_whitespace() {
        let input = "a  * \t\n b";
        let res: Vec<Token> = Tokenizer::new(input).iter().collect();
        let expected = vec![
            Token::new("a", TokenKind::Identifier),
            Token::new("*", TokenKind::Star),
            Token::new("b", TokenKind::Identifier),
        ];
        assert_eq!(res, expected);
    }

    // merges_whitespace
    #[test]
    fn trims_whitespace() {
        let input = "  a*b  ";
        let res: Vec<Token> = Tokenizer::new(input).iter().collect();
        let expected = vec![
            Token::new("a", TokenKind::Identifier),
            Token::new("*", TokenKind::Star),
            Token::new("b", TokenKind::Identifier),
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
            Token::new("sElEcT", TokenKind::Select),
            Token::new("*", TokenKind::Star),
            Token::new("FrOm", TokenKind::From),
            Token::new("test_table", TokenKind::Identifier),
            Token::new(";", TokenKind::Semicolon),
        ];

        assert_eq!(res, expected);
    }

    #[test]
    fn complicated_query() {
        let input =
            "select foo, bar, baz from test_table where bar='that thing' order by foo desc;";
        let res: Vec<Token> = Tokenizer::new(input).iter().collect();
        let expected = vec![
            Token::new("select", TokenKind::Select),
            Token::new("foo", TokenKind::Identifier),
            Token::new(",", TokenKind::Comma),
            Token::new("bar", TokenKind::Identifier),
            Token::new(",", TokenKind::Comma),
            Token::new("baz", TokenKind::Identifier),
            Token::new("from", TokenKind::From),
            Token::new("test_table", TokenKind::Identifier),
            Token::new("where", TokenKind::Where),
            Token::new("bar", TokenKind::Identifier),
            Token::new("=", TokenKind::EqualsSign),
            Token::new("that thing", TokenKind::String),
            Token::new("order", TokenKind::Order),
            Token::new("by", TokenKind::By),
            Token::new("foo", TokenKind::Identifier),
            Token::new("desc", TokenKind::Desc),
            Token::new(";", TokenKind::Semicolon),
        ];

        assert_eq!(res, expected);
    }
}
