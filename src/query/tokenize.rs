use regex::Regex;

#[derive(Debug)]
pub enum TokenizerError {
    UntokenizableInput,
}

type Result<T> = std::result::Result<T, TokenizerError>;

#[derive(PartialEq, Debug, Clone, Copy)]
pub enum TokenKind {
    // for things like whitespace, etc.
    None,

    // composite kinds
    Identifier,
    String,
    Integer,
    Float,
    UnsignedInt,

    // reserved words
    Select,
    Where,
    From,
    Order,
    By,
    Desc,
    Create,
    Table,
    If,
    Not,
    Exists,
    Insert,
    Into,
    Values,
    Destroy,
    Limit,
    As,
    On,
    Conflict,
    Do,
    Nothing,
    Primary,
    Key,
    Delete,
    TypeString,
    TypeInteger,
    TypeFloat,
    TypeUnsignedInt,

    // known symbols
    Star,
    Comma,
    Semicolon,
    EqualsSign,
    LeftParen,
    RightParen,
    LeftAngleBracket,
    RightAngleBracket,
    LessThanEquals,
    GreaterThanEquals,
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

    pub fn kind(&self) -> TokenKind {
        self.kind
    }

    pub fn contents(&self) -> &'a str {
        self.contents
    }
}

struct SpecItem(TokenKind, Regex);

const TOKEN_SPEC_LEN: usize = 43;
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
            SpecItem(TokenKind::LeftParen, Regex::new(r"^\(").unwrap()),
            SpecItem(TokenKind::RightParen, Regex::new(r"^\)").unwrap()),
            SpecItem(TokenKind::LessThanEquals, Regex::new(r"^<=").unwrap()),
            SpecItem(TokenKind::GreaterThanEquals, Regex::new(r"^>=").unwrap()),
            SpecItem(TokenKind::LeftAngleBracket, Regex::new(r"^<").unwrap()),
            SpecItem(TokenKind::RightAngleBracket, Regex::new(r"^>").unwrap()),
            // keywords
            SpecItem(TokenKind::Select, Regex::new(r"^(?i)select\b").unwrap()),
            SpecItem(TokenKind::Where, Regex::new(r"^(?i)where\b").unwrap()),
            SpecItem(TokenKind::From, Regex::new(r"^(?i)from\b").unwrap()),
            SpecItem(TokenKind::Order, Regex::new(r"^(?i)order\b").unwrap()),
            SpecItem(TokenKind::By, Regex::new(r"^(?i)by\b").unwrap()),
            SpecItem(TokenKind::Desc, Regex::new(r"^(?i)desc\b").unwrap()),
            SpecItem(TokenKind::Create, Regex::new(r"^(?i)create\b").unwrap()),
            SpecItem(TokenKind::Table, Regex::new(r"^(?i)table\b").unwrap()),
            SpecItem(TokenKind::If, Regex::new(r"^(?i)if\b").unwrap()),
            SpecItem(TokenKind::Not, Regex::new(r"^(?i)not\b").unwrap()),
            SpecItem(TokenKind::Exists, Regex::new(r"^(?i)exists\b").unwrap()),
            SpecItem(TokenKind::Insert, Regex::new(r"^(?i)insert\b").unwrap()),
            SpecItem(TokenKind::Into, Regex::new(r"^(?i)into\b").unwrap()),
            SpecItem(TokenKind::Values, Regex::new(r"^(?i)values\b").unwrap()),
            SpecItem(TokenKind::Destroy, Regex::new(r"^(?i)destroy\b").unwrap()),
            SpecItem(TokenKind::Limit, Regex::new(r"^(?i)limit\b").unwrap()),
            SpecItem(TokenKind::As, Regex::new(r"^(?i)as\b").unwrap()),
            SpecItem(TokenKind::On, Regex::new(r"^(?i)on\b").unwrap()),
            SpecItem(TokenKind::Conflict, Regex::new(r"^(?i)conflict\b").unwrap()),
            SpecItem(TokenKind::Do, Regex::new(r"^(?i)do\b").unwrap()),
            SpecItem(TokenKind::Nothing, Regex::new(r"^(?i)nothing\b").unwrap()),
            SpecItem(TokenKind::Primary, Regex::new(r"^(?i)primary\b").unwrap()),
            SpecItem(TokenKind::Key, Regex::new(r"^(?i)key\b").unwrap()),
            SpecItem(TokenKind::Delete, Regex::new(r"^(?i)delete\b").unwrap()),
            SpecItem(TokenKind::TypeString, Regex::new(r"^(?i)string\b").unwrap()),
            SpecItem(TokenKind::TypeFloat, Regex::new(r"^(?i)float\b").unwrap()),
            SpecItem(
                TokenKind::TypeInteger,
                Regex::new(r"^(?i)integer\b").unwrap(),
            ),
            SpecItem(
                TokenKind::TypeUnsignedInt,
                Regex::new(r"^(?i)unsigned int\b").unwrap(),
            ),
            // composites
            SpecItem(TokenKind::String, Regex::new("^\"(.*?)\"").unwrap()),
            SpecItem(
                TokenKind::Float,
                Regex::new(r"^-?\d+\.\d+(e-*\d+)*").unwrap(),
            ),
            SpecItem(TokenKind::Integer, Regex::new(r"^-?\d+").unwrap()),
            SpecItem(
                TokenKind::Identifier,
                Regex::new(r"^[^\s*,;=\(\)<>]+").unwrap(),
            ),
        ]
    }

    fn next_token(&mut self) -> Result<Option<Token<'a>>> {
        if self.cursor >= self.input.len() {
            return Ok(None);
        }

        let input = &self.input[self.cursor..];

        for SpecItem(kind, regex) in &self.spec {
            if let Some(m) = regex.find(input) {
                self.cursor += m.len();
                // TODO: Make this happen iteratively instead of recursively
                if matches!(kind, TokenKind::None) {
                    return self.next_token();
                }
                if matches!(kind, TokenKind::String) {
                    let s = &m.as_str()[1..m.len() - 1];
                    return Ok(Some(Token::new(s, *kind)));
                }
                return Ok(Some(Token::new(m.as_str(), *kind)));
            }
        }
        // This should never happen. Everything should at least match against a known
        // symbol or the Identifier TokenKind.
        Err(TokenizerError::UntokenizableInput)
    }

    pub fn tokens(self) -> Tokens<'a> {
        Tokens { tokenizer: self }
    }
}

pub struct Tokens<'a> {
    tokenizer: Tokenizer<'a>,
}
impl<'a> Tokens<'a> {
    pub fn to_vec(&mut self) -> Result<Vec<Token<'a>>> {
        let mut tokens = Vec::new();
        while let Some(token) = self.next_token()? {
            tokens.push(token);
        }
        Ok(tokens)
    }
    pub fn next_token(&mut self) -> Result<Option<Token<'a>>> {
        let res = self.tokenizer.next_token()?;
        Ok(res)
    }
}

#[cfg(test)]
mod tokenizer_tests {
    use super::*;

    #[test]
    fn whitespace_splitting() {
        let input = "a * b";
        let res: Vec<Token> = Tokenizer::new(input).tokens().to_vec().unwrap();
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
        let res: Vec<Token> = Tokenizer::new(input).tokens().to_vec().unwrap();
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
        let res: Vec<Token> = Tokenizer::new(input).tokens().to_vec().unwrap();
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
        let res: Vec<Token> = Tokenizer::new(input).tokens().to_vec().unwrap();
        let expected = vec![
            Token::new("a", TokenKind::Identifier),
            Token::new("*", TokenKind::Star),
            Token::new("b", TokenKind::Identifier),
        ];
        assert_eq!(res, expected);

        let input = "  a*b";
        let res: Vec<Token> = Tokenizer::new(input).tokens().to_vec().unwrap();
        assert_eq!(res, expected);

        let input = "a*b  ";
        let res: Vec<Token> = Tokenizer::new(input).tokens().to_vec().unwrap();
        assert_eq!(res, expected);

        let input = "a*b";
        let res: Vec<Token> = Tokenizer::new(input).tokens().to_vec().unwrap();
        assert_eq!(res, expected);
    }

    #[test]
    fn case_insensitive_on_reserved_words() {
        let input = "sElEcT * FrOm test_table;";
        let res: Vec<Token> = Tokenizer::new(input).tokens().to_vec().unwrap();
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
    fn greedy_with_strings() {
        let input = "\"string1\" then \"string2\"";
        let res: Vec<Token> = Tokenizer::new(input).tokens().to_vec().unwrap();
        let expected = vec![
            Token::new("string1", TokenKind::String),
            Token::new("then", TokenKind::Identifier),
            Token::new("string2", TokenKind::String),
        ];

        assert_eq!(res, expected);
    }

    #[test]
    fn all_tokens_in_a_string() {
        let input =
            "select foo, bar, baz from test_table where bar=\"that thing\" order by foo) desc; -12, -12.3 create table if not ( exists string integer float insert into values destroy -5.134e11 4.122e-38 limit <> <= >= as on conflict do nothing primary key rowid delete unsigned int;";
        let res: Vec<Token> = Tokenizer::new(input).tokens().to_vec().unwrap();
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
            Token::new(")", TokenKind::RightParen),
            Token::new("desc", TokenKind::Desc),
            Token::new(";", TokenKind::Semicolon),
            Token::new("-12", TokenKind::Integer),
            Token::new(",", TokenKind::Comma),
            Token::new("-12.3", TokenKind::Float),
            Token::new("create", TokenKind::Create),
            Token::new("table", TokenKind::Table),
            Token::new("if", TokenKind::If),
            Token::new("not", TokenKind::Not),
            Token::new("(", TokenKind::LeftParen),
            Token::new("exists", TokenKind::Exists),
            Token::new("string", TokenKind::TypeString),
            Token::new("integer", TokenKind::TypeInteger),
            Token::new("float", TokenKind::TypeFloat),
            Token::new("insert", TokenKind::Insert),
            Token::new("into", TokenKind::Into),
            Token::new("values", TokenKind::Values),
            Token::new("destroy", TokenKind::Destroy),
            Token::new("-5.134e11", TokenKind::Float),
            Token::new("4.122e-38", TokenKind::Float),
            Token::new("limit", TokenKind::Limit),
            Token::new("<", TokenKind::LeftAngleBracket),
            Token::new(">", TokenKind::RightAngleBracket),
            Token::new("<=", TokenKind::LessThanEquals),
            Token::new(">=", TokenKind::GreaterThanEquals),
            Token::new("as", TokenKind::As),
            Token::new("on", TokenKind::On),
            Token::new("conflict", TokenKind::Conflict),
            Token::new("do", TokenKind::Do),
            Token::new("nothing", TokenKind::Nothing),
            Token::new("primary", TokenKind::Primary),
            Token::new("key", TokenKind::Key),
            Token::new("rowid", TokenKind::Identifier),
            Token::new("delete", TokenKind::Delete),
            Token::new("unsigned int", TokenKind::TypeUnsignedInt),
            Token::new(";", TokenKind::Semicolon),
        ];

        assert_eq!(res, expected);
    }
}
