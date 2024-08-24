use std::{borrow::Cow, iter::zip};

use crate::{
    storage::{Column, Row, Rows, Schema, StorageError, StorageLayer},
    DbValue,
};

use super::parse::{
    CreateExpression, DestroyExpression, Expression, InsertExpression, OrderByClause,
    SelectColumns, SelectExpression, WhereClause,
};

#[derive(Debug)]
pub enum ExecutionError {
    StorageError(StorageError),
    UnknownColumnNameProvided,
}
impl From<StorageError> for ExecutionError {
    fn from(value: StorageError) -> Self {
        Self::StorageError(value)
    }
}

type Result<T> = std::result::Result<T, ExecutionError>;

pub enum QueryResult<'a> {
    Ok,
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
    plan: Vec<Expression>,
}
impl ExecutablePlan {
    pub fn new(plan: Vec<Expression>) -> Self {
        ExecutablePlan { plan }
    }

    fn select<'strg>(
        &self,
        select_expr: &SelectExpression,
        storage: &'strg mut StorageLayer,
    ) -> Result<QueryResult<'strg>> {
        let rows = storage.table_scan(&select_expr.table)?;
        let source = RowsSource::Table(TableRowsIter::new(rows));
        let source = if let Some(where_clause) = &select_expr.where_clause {
            RowsSource::Filter(FilterRowsIter::new(source, where_clause))
        } else {
            source
        };
        let source = if let Some(order_by_clause) = &select_expr.order_by_clause {
            RowsSource::Sort(SortRowsIter::new(source, order_by_clause))
        } else {
            source
        };
        let source = RowsSource::Select(SelectRowsIter::new(source, &select_expr.columns));
        Ok(QueryResult::Rows(ResultRows::new(source)))
    }

    fn create<'strg>(
        &self,
        create_expr: &CreateExpression,
        storage: &'strg mut StorageLayer,
    ) -> Result<QueryResult<'strg>> {
        if create_expr.if_not_exists && storage.table_exists(&create_expr.table) {
            return Ok(QueryResult::Ok);
        }
        let pairs = zip(
            create_expr.columns.names.iter(),
            create_expr.columns.types.iter(),
        );
        let cols = pairs
            .map(|(name, _type)| Column::new(name.to_string(), *_type))
            .collect();

        storage.create_table(&create_expr.table, &Schema::new(cols))?;
        Ok(QueryResult::Ok)
    }

    fn insert<'strg>(
        &self,
        insert_expr: &InsertExpression,
        storage: &'strg mut StorageLayer,
    ) -> Result<QueryResult<'strg>> {
        let schema = storage.table_schema(&insert_expr.table)?;
        let mut order = Vec::new();
        for col in insert_expr.columns.iter() {
            let index = match schema.column_position(col) {
                Some(i) => i,
                None => return Err(ExecutionError::UnknownColumnNameProvided),
            };
            order.push(index);
        }
        let values: Vec<DbValue> = order
            .iter()
            .filter_map(|i| insert_expr.values.get(*i).cloned())
            .collect();
        let rows: Vec<Row> = vec![Row::new(values)];
        storage.insert_rows(&insert_expr.table, rows)?;
        Ok(QueryResult::Ok)
    }

    fn destroy<'strg>(
        &self,
        destroy_expr: &DestroyExpression,
        storage: &'strg mut StorageLayer,
    ) -> Result<QueryResult<'strg>> {
        storage.destroy_table(&destroy_expr.table)?;
        Ok(QueryResult::Ok)
    }

    fn execute_expression<'strg>(
        &self,
        expr: &Expression,
        storage: &'strg mut StorageLayer,
    ) -> Result<QueryResult<'strg>> {
        match expr {
            Expression::Select(s) => self.select(s, storage),
            Expression::Create(c) => self.create(c, storage),
            Expression::Insert(i) => self.insert(i, storage),
            Expression::Destroy(d) => self.destroy(d, storage),
        }
    }

    pub fn execute<'strg>(&self, storage: &'strg mut StorageLayer) -> Result<QueryResult<'strg>> {
        let last_idx = self.plan.len() - 1;
        let last_expr = self
            .plan
            .get(last_idx)
            .expect("There should be an expression here");
        for expr in self.plan[0..last_idx].iter() {
            _ = self.execute_expression(expr, storage)?;
        }
        self.execute_expression(last_expr, storage)
    }
}

enum RowsSource<'a> {
    Table(TableRowsIter<'a>),
    Select(SelectRowsIter<'a>),
    Filter(FilterRowsIter<'a>),
    Sort(SortRowsIter<'a>),
}
impl<'a> RowsSource<'a> {
    fn schema(&self) -> Cow<'a, Schema> {
        match self {
            Self::Table(t) => Cow::Owned(t.rows.schema.clone()),
            Self::Select(s) => s.schema.clone(),
            Self::Filter(f) => f.schema.clone(),
            Self::Sort(s) => s.schema.clone(),
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
        let schema = source.schema();
        match columns {
            SelectColumns::All => SelectRowsIter {
                source: Box::new(source),
                schema,
                column_project: Box::new(|r| r.clone()),
            },
            // TODO: Probably refactor this. It's a bit of a mess
            SelectColumns::Only(cols) => {
                // TODO: Handle situations where column name that doesn't exist in schema is provided
                let columns: Vec<Column> = cols
                    .iter()
                    .filter_map(|name| schema.column(name))
                    .cloned()
                    .collect();
                let schema: Cow<'a, Schema> = Cow::Owned(Schema::new(columns));
                let indices: Vec<usize> = cols
                    .iter()
                    .filter_map(|name| schema.column_position(name))
                    .collect();

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
                    schema,
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

// TODO: Make actually filter
struct FilterRowsIter<'a> {
    source: Box<RowsSource<'a>>,
    predicate: Box<dyn Fn(&Row) -> bool>,
    schema: Cow<'a, Schema>,
}
impl<'a> FilterRowsIter<'a> {
    pub fn new(source: RowsSource<'a>, where_clause: &WhereClause) -> Self {
        let schema = source.schema();
        FilterRowsIter {
            source: Box::new(source),
            predicate: Box::new(where_clause.predicate()),
            schema,
        }
    }
}
impl<'a> Iterator for FilterRowsIter<'a> {
    type Item = Cow<'a, Row>;

    fn next(&mut self) -> Option<Self::Item> {
        self.source.next().filter(|row| (self.predicate)(row))
    }
}

struct SortRowsIter<'a> {
    schema: Cow<'a, Schema>,
    sorted_rows: Vec<Cow<'a, Row>>,
    cursor: usize,
}
impl<'a> SortRowsIter<'a> {
    pub fn new(source: RowsSource<'a>, sort_clause: &OrderByClause) -> Self {
        let schema = source.schema();
        let mut rows = Vec::new();
        for row in source {
            rows.push(row);
        }

        let key_fn = sort_clause.sort_key();
        rows.sort_by_cached_key(|r: &Cow<'a, Row>| key_fn(r, &schema));
        if sort_clause.desc() {
            rows.reverse();
        }

        SortRowsIter {
            schema,
            sorted_rows: rows,
            cursor: 0,
        }
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
