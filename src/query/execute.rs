use std::{borrow::Cow, cmp::Ordering, iter::zip};

use crate::{
    storage::{Column, ColumnWithIndex, Row, Rows, Schema, StorageError, StorageLayer},
    DbValue,
};

use super::parse::{
    CreateStatement, DestroyStatement, InsertStatement, OrderByClause, SelectColumns,
    SelectStatement, Statement, WhereClause, WhereCmp, WhereMember,
};

#[derive(Debug)]
pub enum ExecutionError {
    StorageError(StorageError),
    UnknownColumnNameProvided,
    MismatchedTypeComparision,
}
impl From<StorageError> for ExecutionError {
    fn from(value: StorageError) -> Self {
        Self::StorageError(value)
    }
}

type Result<T> = std::result::Result<T, ExecutionError>;

pub enum QueryResult<'a> {
    Ok,
    NothingToDo,
    Rows(ResultRows<'a>),
}

pub struct ResultRows<'a> {
    source: RowsSource<'a>,
}
impl<'a> ResultRows<'a> {
    fn new(source: RowsSource<'a>) -> Self {
        ResultRows { source }
    }

    pub fn schema(&self) -> Cow<'a, Schema> {
        self.source.schema()
    }
}
impl<'a> Iterator for ResultRows<'a> {
    type Item = Cow<'a, Row>;

    fn next(&mut self) -> Option<Self::Item> {
        self.source.next()
    }
}

// TODO: Rework this at some point to actually do plan optimization
pub struct ExecutablePlan {
    plan: Vec<Statement>,
}
impl ExecutablePlan {
    pub fn new(plan: Vec<Statement>) -> Self {
        ExecutablePlan { plan }
    }

    fn select<'strg>(
        &self,
        select_expr: &SelectStatement,
        storage: &'strg mut StorageLayer,
    ) -> Result<QueryResult<'strg>> {
        let rows = storage.table_scan(&select_expr.table)?;
        let source = RowsSource::Table(TableRowsIter::new(rows));
        let source = if let Some(where_clause) = &select_expr.where_clause {
            let filter = FilterRowsIter::build(source, where_clause)?;
            println!("{:?}", filter.predicate);
            RowsSource::Filter(filter)
        } else {
            source
        };
        let source = if let Some(order_by_clause) = &select_expr.order_by_clause {
            RowsSource::Sort(SortRowsIter::build(source, order_by_clause)?)
        } else {
            source
        };
        let source = RowsSource::Select(SelectRowsIter::new(source, &select_expr.columns));
        let source = if let Some(limit) = &select_expr.limit {
            RowsSource::Limit(LimitRowsIter::new(source, limit))
        } else {
            source
        };

        Ok(QueryResult::Rows(ResultRows::new(source)))
    }

    fn create<'strg>(
        &self,
        create_stmt: &CreateStatement,
        storage: &'strg mut StorageLayer,
    ) -> Result<QueryResult<'strg>> {
        if create_stmt.if_not_exists && storage.table_exists(&create_stmt.table) {
            return Ok(QueryResult::Ok);
        }
        let pairs = zip(
            create_stmt.columns.names.iter(),
            create_stmt.columns.types.iter(),
        );
        let cols = pairs
            .map(|(name, _type)| Column::new(name.to_string(), *_type))
            .collect();
        let primary_key_col = create_stmt.columns.primary_key_col.as_storage_key_column();

        storage.create_table(&create_stmt.table, &Schema::new(cols), primary_key_col)?;
        Ok(QueryResult::Ok)
    }

    fn insert<'strg>(
        &self,
        insert_stmt: &InsertStatement,
        storage: &'strg mut StorageLayer,
    ) -> Result<QueryResult<'strg>> {
        let schema = storage.table_schema(&insert_stmt.table)?;
        let order: Result<Vec<usize>> = insert_stmt
            .columns
            .iter()
            .map(|name| match schema.column_position(name) {
                Some(pos) => Ok(pos),
                None => Err(ExecutionError::UnknownColumnNameProvided),
            })
            .collect();
        let order = order?;

        let mut unordered_vals: Vec<(usize, &DbValue)> =
            zip(order, insert_stmt.values.iter()).collect();
        unordered_vals.sort_by_key(|p| p.0);

        let vals = unordered_vals.iter().map(|r| r.1.clone()).collect();
        let rows = vec![Row::new(vals)];

        storage.insert_rows(&insert_stmt.table, rows)?;
        Ok(QueryResult::Ok)
    }

    fn destroy<'strg>(
        &self,
        destroy_stmt: &DestroyStatement,
        storage: &'strg mut StorageLayer,
    ) -> Result<QueryResult<'strg>> {
        storage.destroy_table(&destroy_stmt.table)?;
        Ok(QueryResult::Ok)
    }

    fn execute_stmt<'strg>(
        &self,
        stmt: &Statement,
        storage: &'strg mut StorageLayer,
    ) -> Result<QueryResult<'strg>> {
        match stmt {
            Statement::Select(s) => self.select(s, storage),
            Statement::Create(c) => self.create(c, storage),
            Statement::Insert(i) => self.insert(i, storage),
            Statement::Destroy(d) => self.destroy(d, storage),
        }
    }

    pub fn execute<'strg>(&self, storage: &'strg mut StorageLayer) -> Result<QueryResult<'strg>> {
        if self.plan.is_empty() {
            return Ok(QueryResult::NothingToDo);
        }
        let last_idx = self.plan.len() - 1;
        let last_expr = self
            .plan
            .get(last_idx)
            .expect("There should be an expression here");
        for expr in self.plan[0..last_idx].iter() {
            _ = self.execute_stmt(expr, storage)?;
        }
        self.execute_stmt(last_expr, storage)
    }
}

enum RowsSource<'a> {
    Table(TableRowsIter<'a>),
    Select(SelectRowsIter<'a>),
    Filter(FilterRowsIter<'a>),
    Sort(SortRowsIter<'a>),
    Limit(LimitRowsIter<'a>),
}
impl<'a> RowsSource<'a> {
    fn schema(&self) -> Cow<'a, Schema> {
        match self {
            Self::Table(t) => Cow::Owned(t.rows.schema.clone()),
            Self::Select(s) => s.schema.clone(),
            Self::Filter(f) => f.schema.clone(),
            Self::Sort(s) => s.schema.clone(),
            Self::Limit(l) => l.schema.clone(),
        }
    }
}
impl<'a> Iterator for RowsSource<'a> {
    type Item = Cow<'a, Row>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Table(t) => t.next(),
            Self::Select(s) => s.next(),
            Self::Filter(f) => f.next(),
            Self::Sort(s) => s.next(),
            Self::Limit(l) => l.next(),
        }
    }
}

struct TableRowsIter<'a> {
    rows: Rows<'a>,
    cursor: usize,
}
impl<'a> TableRowsIter<'a> {
    fn new(rows: Rows<'a>) -> Self {
        TableRowsIter { rows, cursor: 0 }
    }
}
impl<'a> Iterator for TableRowsIter<'a> {
    type Item = Cow<'a, Row>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.cursor >= self.rows.rows.len() {
            return None;
        }
        let row = self.rows.rows.get(self.cursor);
        self.cursor += 1;
        row.map(Cow::Borrowed)
    }
}

struct SelectRowsIter<'a> {
    source: Box<RowsSource<'a>>,
    schema: Cow<'a, Schema>,
    column_project: Box<dyn Fn(Cow<'a, Row>) -> Cow<'a, Row>>,
}
impl<'a> SelectRowsIter<'a> {
    fn new(source: RowsSource<'a>, columns: &SelectColumns) -> Self {
        let source_schema = source.schema();
        match columns {
            SelectColumns::All => SelectRowsIter {
                source: Box::new(source),
                schema: source_schema,
                column_project: Box::new(|r| r.clone()),
            },
            SelectColumns::Only(cols) => {
                // TODO: Handle situations where column name that doesn't exist in schema is provided
                let columns_with_indexes: Vec<(&ColumnWithIndex, &str)> = cols
                    .iter()
                    .filter_map(|col| {
                        source_schema
                            .get(&col.in_name)
                            .map(|c| (c, col.out_name.as_str()))
                    })
                    .collect();
                let indices: Vec<usize> =
                    columns_with_indexes.iter().map(|ci| ci.0.index).collect();

                let columns = columns_with_indexes
                    .iter()
                    .map(|ci| ci.0.column.with_name(ci.1.to_string()))
                    .collect();

                let new_schema = Cow::Owned(Schema::new(columns));

                let projection = move |r: Cow<'a, Row>| {
                    // TODO: Handle situations where column name that doesn't exist in schema is provided
                    let data = indices
                        .iter()
                        .filter_map(|idx| r.data.get(*idx))
                        .cloned()
                        .collect();
                    Cow::Owned(Row::new(data))
                };

                SelectRowsIter {
                    source: Box::new(source),
                    schema: new_schema,
                    column_project: Box::new(projection),
                }
            }
        }
    }
}
impl<'a> Iterator for SelectRowsIter<'a> {
    type Item = Cow<'a, Row>;

    fn next(&mut self) -> Option<Self::Item> {
        self.source.next().map(|r| (self.column_project)(r.clone()))
    }
}

struct LimitRowsIter<'a> {
    source: Box<RowsSource<'a>>,
    schema: Cow<'a, Schema>,
    rows_left: usize,
}
impl<'a> LimitRowsIter<'a> {
    fn new(source: RowsSource<'a>, limit: &usize) -> Self {
        let schema = source.schema();
        LimitRowsIter {
            source: Box::new(source),
            schema,
            rows_left: *limit,
        }
    }
}
impl<'a> Iterator for LimitRowsIter<'a> {
    type Item = Cow<'a, Row>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(row) = self.source.next().filter(|_| self.rows_left > 0) {
            self.rows_left -= 1;
            Some(row)
        } else {
            None
        }
    }
}

#[derive(Debug)]
enum FilterType {
    ValueValue {
        left: DbValue,
        right: DbValue,
        cmp: WhereCmp,
    },
    ColumnValue {
        col_pos: usize,
        val: DbValue,
        cmp: WhereCmp,
    },
    ColumnColumn {
        col1_pos: usize,
        col2_pos: usize,
        cmp: WhereCmp,
    },
}
impl FilterType {
    fn build(where_clause: &WhereClause, schema: &Schema) -> Result<Self> {
        match (&where_clause.left, &where_clause.right) {
            (WhereMember::Value(val), WhereMember::Column(name)) => match schema.column(name) {
                Some(col) if col._type == val.db_type() => {
                    let col_pos = match schema.column_position(&col.name) {
                        Some(pos) => pos,
                        None => return Err(ExecutionError::UnknownColumnNameProvided),
                    };
                    Ok(Self::ColumnValue {
                        col_pos,
                        val: val.clone(),
                        cmp: where_clause.cmp,
                    })
                }
                Some(_) => Err(ExecutionError::MismatchedTypeComparision),
                None => Err(ExecutionError::UnknownColumnNameProvided),
            },
            (WhereMember::Column(name), WhereMember::Value(val)) => match schema.column(name) {
                Some(col) if col._type == val.db_type() => {
                    let col_pos = match schema.column_position(&col.name) {
                        Some(pos) => pos,
                        None => return Err(ExecutionError::UnknownColumnNameProvided),
                    };
                    Ok(FilterType::ColumnValue {
                        col_pos,
                        val: val.clone(),
                        cmp: where_clause.cmp,
                    })
                }
                Some(_) => Err(ExecutionError::MismatchedTypeComparision),
                None => Err(ExecutionError::UnknownColumnNameProvided),
            },
            (WhereMember::Value(val1), WhereMember::Value(val2)) => {
                if val1.db_type() != val2.db_type() {
                    Err(ExecutionError::MismatchedTypeComparision)
                } else {
                    Ok(FilterType::ValueValue {
                        left: val1.clone(),
                        right: val2.clone(),
                        cmp: where_clause.cmp,
                    })
                }
            }
            (WhereMember::Column(name1), WhereMember::Column(name2)) => {
                match (schema.column(name1), schema.column(name2)) {
                    (None, _) => Err(ExecutionError::UnknownColumnNameProvided),
                    (_, None) => Err(ExecutionError::UnknownColumnNameProvided),
                    (Some(col1), Some(col2)) if col1._type != col2._type => {
                        Err(ExecutionError::MismatchedTypeComparision)
                    }
                    _ => {
                        let left_pos = match schema.column_position(name1) {
                            Some(pos) => pos,
                            None => return Err(ExecutionError::UnknownColumnNameProvided),
                        };
                        let right_pos = match schema.column_position(name2) {
                            Some(pos) => pos,
                            None => return Err(ExecutionError::UnknownColumnNameProvided),
                        };
                        Ok(FilterType::ColumnColumn {
                            col1_pos: left_pos,
                            col2_pos: right_pos,
                            cmp: where_clause.cmp,
                        })
                    }
                }
            }
        }
    }

    fn row_predicate(&self, row: &Row) -> bool {
        let (left, right, cmp) = match self {
            Self::ColumnColumn {
                col1_pos,
                col2_pos,
                cmp,
            } => {
                let left = row
                    .data
                    .get(*col1_pos)
                    .expect("Should always have a value here");
                let right = row
                    .data
                    .get(*col2_pos)
                    .expect("Should always have a value here");
                (left, right, cmp)
            }
            Self::ColumnValue { col_pos, val, cmp } => {
                let left = row
                    .data
                    .get(*col_pos)
                    .expect("Should always have a value here");
                (left, val, cmp)
            }
            Self::ValueValue { left, right, cmp } => (left, right, cmp),
        };
        match cmp {
            WhereCmp::Eq => left == right,
            WhereCmp::LessThan => left < right,
            WhereCmp::GreaterThan => left > right,
            WhereCmp::LessThanEquals => left <= right,
            WhereCmp::GreaterThanEquals => left >= right,
        }
    }
}

// TODO: Construct predicate in a more intentional way, probably during physical plan phase
// when I get that set up
struct FilterRowsIter<'a> {
    source: Box<RowsSource<'a>>,
    predicate: FilterType,
    schema: Cow<'a, Schema>,
}
impl<'a> FilterRowsIter<'a> {
    pub fn build(source: RowsSource<'a>, where_clause: &WhereClause) -> Result<Self> {
        let schema = source.schema();
        let predicate = FilterType::build(where_clause, &schema)?;

        Ok(FilterRowsIter {
            source: Box::new(source),
            predicate,
            schema,
        })
    }
}
impl<'a> Iterator for FilterRowsIter<'a> {
    type Item = Cow<'a, Row>;

    fn next(&mut self) -> Option<Self::Item> {
        self.source.find(|row| self.predicate.row_predicate(row))
    }
}

fn sort_key_fn(clause: &OrderByClause, schema: &Schema) -> Result<impl Fn(&Row) -> Vec<DbValue>> {
    let sort_col = clause.sort_column().to_string();
    let pos = match schema.column_position(&sort_col) {
        Some(pos) => pos,
        None => return Err(ExecutionError::UnknownColumnNameProvided),
    };
    let key_fn = move |r: &Row| {
        let mut key = Vec::new();
        let v = r
            .data
            .get(pos)
            .expect("We've already verified this will exist");
        key.push(v.clone());
        key
    };
    Ok(key_fn)
}

struct SortRowsIter<'a> {
    schema: Cow<'a, Schema>,
    sorted_rows: Vec<Cow<'a, Row>>,
    cursor: usize,
}
impl<'a> SortRowsIter<'a> {
    pub fn build(source: RowsSource<'a>, sort_clause: &OrderByClause) -> Result<Self> {
        let schema = source.schema();
        let mut rows = Vec::new();
        for row in source {
            rows.push(row);
        }

        let key_fn = sort_key_fn(sort_clause, &schema)?;
        rows.sort_by(|a, b| {
            let a_key = key_fn(a);
            let b_key = key_fn(b);
            for (a, b) in zip(a_key, b_key) {
                let res = match (a, b) {
                    (DbValue::String(a), DbValue::String(b)) => a.cmp(&b),
                    (DbValue::Float(a), DbValue::Float(b)) => {
                        if a > b {
                            Ordering::Greater
                        } else if a < b {
                            Ordering::Less
                        } else {
                            Ordering::Equal
                        }
                    }
                    (DbValue::Integer(a), DbValue::Integer(b)) => a.cmp(&b),
                    // we're comparing the same columns, so the types of two values in the same position should always match
                    _ => panic!("Mis-matched type comparisons should be impossible"),
                };
                match res {
                    Ordering::Equal => (),
                    Ordering::Greater => return Ordering::Greater,
                    Ordering::Less => return Ordering::Less,
                }
            }
            Ordering::Equal
        });
        if sort_clause.desc() {
            rows.reverse();
        }

        Ok(SortRowsIter {
            schema,
            sorted_rows: rows,
            cursor: 0,
        })
    }
}
impl<'a> Iterator for SortRowsIter<'a> {
    type Item = Cow<'a, Row>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.cursor >= self.sorted_rows.len() {
            return None;
        }
        let row = self.sorted_rows.get(self.cursor);
        self.cursor += 1;
        row.cloned()
    }
}
