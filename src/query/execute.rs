use std::{borrow::Cow, iter::zip};

use crate::{
    storage::{Column, Row, Rows, Schema, StorageError, StorageLayer},
    DbValue,
};

use super::parse::{
    CreateExpression, DestroyExpression, Expression, InsertExpression, OrderByClause,
    SelectColumns, SelectExpression, WhereClause,
};

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

pub enum QueryResult {
    Ok,
    Rows(QueryRows),
}
pub struct QueryRows {
    schema: Schema,
    rows: Vec<Row>,
}

// TODO: Rework this at some point to actually do plan optimization
pub struct ExecutablePlan<'plan> {
    storage: &'plan mut StorageLayer,
    plan: &'plan Vec<Expression<'plan>>,
}
impl<'plan> ExecutablePlan<'plan> {
    pub fn new(plan: &'plan Vec<Expression<'plan>>, storage: &'plan mut StorageLayer) -> Self {
        ExecutablePlan { storage, plan }
    }

    fn select(&mut self, select_expr: &SelectExpression) -> Result<QueryResult> {
        let rows = self.storage.table_scan(select_expr.table)?;

        // where
        // if let Some(where_clause) = select_expr.where_clause {
        //     rows = rows.filter(where_clause.predicate);
        // }
        // // order by
        // // if let Some(order_by_clause) = select_expr.order_by_clause {
        // //     let mut all: Vec<Row> = rows.collect();
        // //     all.sort_by(|a, b|
        // // }

        // // select cols
        // if select_expr.columns != SelectColumns::All {
        //     rows = rows.map(|row| {
        //         let data = row.data.iter().filter(|)
        //     })
        // }
        Ok(QueryResult::Ok)
    }

    fn create(&mut self, create_expr: &CreateExpression) -> Result<QueryResult> {
        let pairs = zip(
            create_expr.columns.names.iter(),
            create_expr.columns.types.iter(),
        );
        let cols = pairs
            .map(|(name, _type)| Column::new(name.to_string(), *_type))
            .collect();

        self.storage
            .create_table(create_expr.table, &Schema::new(cols))?;
        Ok(QueryResult::Ok)
    }

    fn insert(&mut self, insert_expr: &InsertExpression) -> Result<QueryResult> {
        let schema = self.storage.table_schema(insert_expr.table)?;
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
        self.storage.insert_rows(insert_expr.table, rows)?;
        Ok(QueryResult::Ok)
    }

    fn destroy(&mut self, destroy_expr: &DestroyExpression) -> Result<QueryResult> {
        self.storage.destroy_table(destroy_expr.table)?;
        Ok(QueryResult::Ok)
    }

    fn should_flush(&self) -> bool {
        self.plan
            .iter()
            .filter(|e| !matches!(e, Expression::Select(_)))
            .count()
            > 0
    }

    pub fn execute(&mut self) -> Result<QueryResult> {
        let mut res = QueryResult::Ok;
        for expr in self.plan.iter() {
            res = match expr {
                Expression::Select(s) => self.select(s)?,
                Expression::Create(c) => self.create(c)?,
                Expression::Insert(i) => self.insert(i)?,
                Expression::Destroy(d) => self.destroy(d)?,
            }
        }
        if self.should_flush() {
            self.storage.flush()?;
        }
        Ok(res)
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
            Self::Table(t) => t.schema.clone(),
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
    schema: Cow<'a, Schema>,
    cursor: usize,
}
impl<'a> TableRowsIter<'a> {
    fn new(rows: Rows<'a>, schema: Cow<'a, Schema>) -> Self {
        TableRowsIter {
            rows,
            schema,
            cursor: 0,
        }
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
    fn new(source: RowsSource<'a>, columns: SelectColumns) -> Self {
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
    pub fn new(source: RowsSource<'a>, sort_clause: &OrderByClause<'a>) -> Self {
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
