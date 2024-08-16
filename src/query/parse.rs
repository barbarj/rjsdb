use super::tokenize::{Token, Tokenizer, Tokens};

struct Parser<'a> {
    tokens: Tokens<'a>,
}
impl<'a> Parser<'a> {
    fn new(tokenizer: Tokenizer<'a>) -> Self {
        Parser {
            tokens: tokenizer.iter(),
        }
    }
}

enum DagNode {
    Select {
        columns: Vec<String>,
        from_table: String,
        clauses: Vec<Clause>,
    },
}

struct Clause {}

// TODO:
// - construct SQL grammar in ~BNF (Backus-Naur Form)
trait Parse {
    fn parse(parser: &mut Parser) -> Option<DagNode>;
}

// TODO: Switch to using petgraph instead
// this is not a great way to do this probably.
// that lifetime setup is ehhhhh.

struct LogicalQueryPlan {
    // command: CommandType,
}

// pub fn parse(tokens: &[Token<'_>]) -> LogicalQueryPlan {
//     LogicalQueryPlan {}
// }

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
