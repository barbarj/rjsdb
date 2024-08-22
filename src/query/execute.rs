use std::{
    borrow::Cow,
    iter::{zip, FromFn},
};

use crate::{
    storage::{Column, Row, Rows, Schema, StorageError, StorageLayer, Table},
    DbValue,
};

use super::parse::{
    CreateExpression, DestroyExpression, Expression, InsertExpression, SelectColumns,
    SelectExpression,
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
// impl<Iter> From<Iter> for QueryRows
// where
//     Iter: Iterator<Item = Row>,
// {
//     fn from(value: Iter) -> Self {
//         QueryRows {
//             rows: value.collect(),
//         }
//     }
// }

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
            .map(|i| insert_expr.values.get(*i).cloned())
            .flatten()
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

// trait RowsIterator<'a>: Iterator<Item = &'a Row> {
//     fn schema(&self) -> &Schema;
// }

// struct RowsIterator<'a, F>
// where
//     F: FnMut() -> Option<Cow<'a, Row>>,
// {
//     schema: Cow<'a, Schema>,
//     rows: FromFn<F>,
// }
// impl<'a, F> RowsIterator<'a, F>
// where
//     F: FnMut() -> Option<Cow<'a, Row>>,
// {
//     fn select<G>(rows_iter: RowsIterator<'a, G>, columns: SelectColumns) -> Self
//     where
//         G: FnMut() -> Option<Cow<'a, Row>>,
//     {
//         let (schema, indices_to_keep) = match columns {
//             SelectColumns::All => (
//                 rows_iter.schema,
//                 (0..rows_iter.schema.schema().len()).collect(),
//             ),
//             SelectColumns::Only(cols) => {
//                 let new_cols: Vec<(usize, &Column)> = rows_iter
//                     .schema
//                     .schema()
//                     .iter()
//                     .enumerate()
//                     .filter(|(idx, schema_col)| {
//                         cols.iter().any(|select_col| schema_col.name == *select_col)
//                     })
//                     .collect();
//                 let schema_cols = new_cols.iter().map(|p| p.1.clone()).collect();
//                 let schema = Cow::Owned(Schema::new(schema_cols));
//                 let indices_to_keep: Vec<usize> = new_cols.iter().map(|p| p.0).collect();
//                 (schema, indices_to_keep)
//             }
//         };

//         let iter = std::iter::from_fn(move || {
//             rows_iter.rows.next().map(|r| {
//                 Cow::Owned(Row::new(
//                     r.data
//                         .iter()
//                         .enumerate()
//                         .filter(|(idx, v)| indices_to_keep.contains(idx))
//                         .map(|p| p.1.to_owned())
//                         .collect(),
//                 ))
//             })
//         });

//         RowsIterator { schema, rows: iter }

//         // match columns {
//         //     SelectColumns::All => RowsIterator {
//         //         schema: rows_iter.schema,
//         //         rows: std::iter::from_fn(move || rows_iter.rows.next()),
//         //     }, // TODO: Apply the type-mapping equivalent of identity to this
//         //     SelectColumns::Only(cols) => {
//         //         let new_cols: Vec<(usize, &Column)> = rows_iter
//         //             .schema
//         //             .schema()
//         //             .iter()
//         //             .enumerate()
//         //             .filter(|(idx, schema_col)| {
//         //                 cols.iter().any(|select_col| schema_col.name == *select_col)
//         //             })
//         //             .collect();
//         //         let schema_cols = new_cols.iter().map(|p| p.1.clone()).collect();
//         //         let schema = Cow::Owned(Schema::new(schema_cols));
//         //         let indices_to_keep: Vec<usize> = new_cols.iter().map(|p| p.0).collect();

//         //         let iter = std::iter::from_fn(move || {
//         //             rows_iter.rows.next().map(|r| {
//         //                 Row::new(
//         //                     r.data
//         //                         .iter()
//         //                         .enumerate()
//         //                         .filter(|(idx, v)| indices_to_keep.contains(idx))
//         //                         .map(|p| p.1.to_owned())
//         //                         .collect(),
//         //                 )
//         //             })
//         //         });

//         //         RowsIterator { schema, rows: iter }
//         //     }
//         // };
//     }
// }

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
        row.map(|r| Cow::Borrowed(r))
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
                    .map(|name| schema.column(name))
                    .flatten()
                    .map(|c| c.clone())
                    .collect();
                let schema: Cow<'a, Schema> = Cow::Owned(Schema::new(columns));
                let indices: Vec<usize> = cols
                    .iter()
                    .map(|name| schema.column_position(name))
                    .flatten()
                    .collect();

                let projection = move |r: Cow<'a, Row>| {
                    // TODO: Handle situations where column name that doesn't exist in schema is provided
                    let data = indices
                        .iter()
                        .map(|idx| r.data.get(*idx))
                        .flatten()
                        .map(|x| x.clone())
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
    schema: Cow<'a, Schema>,
}
impl<'a> FilterRowsIter<'a> {
    pub fn new(source: RowsSource<'a>) -> Self {
        let schema = source.schema();
        FilterRowsIter {
            source: Box::new(source),
            schema,
        }
    }
}
// impl<'a> RowsIterator for FilterRowsIter<'a> {
//     fn iter(&self) -> impl Iterator<Item = &Row> {
//         self.input.iter()
//     }

//     fn schema(&self) -> &Schema {
//         self.input.schema()
//     }
// }
struct SortRowsIter<'a> {
    source: Box<RowsSource<'a>>,
    schema: Cow<'a, Schema>,
    //TODO:  sorted rows and cursor
}
impl<'a> SortRowsIter<'a> {
    // TODO: Should take sort columns
    pub fn new(source: RowsSource<'a>) -> Self {
        let schema = source.schema();
        SortRowsIter {
            source: Box::new(source),
            schema,
        }
    }
}
// impl<'a> RowsIterator for SortRowsIter<'a> {
//     fn iter(&self) -> impl Iterator<Item = &Row> {
//         self.input.iter()
//     }

//     fn schema(&self) -> &Schema {
//         self.input.schema()
//     }
// }

// struct Select<Iter>
// where
//     Iter: Iterator<Item = Row>,
// {
//     input: Iter,
// }
// impl<Iter> Iterator for Select<Iter>
// where
//     Iter: Iterator<Item = Row>,
// {
//     type Item = Row;

//     fn next(&mut self) -> Option<Self::Item> {
//         self.input.next()
//     }
// }

// struct Filter<Iter>
// where
//     Iter: Iterator<Item = Row>,
// {
//     input: Iter,
//     predicate: fn(&Row) -> bool,
// }
// impl<Iter> Iterator for Filter<Iter>
// where
//     Iter: Iterator<Item = Row>,
// {
//     type Item = Row;

//     fn next(&mut self) -> Option<Self::Item> {
//         self.input.find(self.predicate)
//     }
// }

// /// Stores rows in reverse order, so first item is at end
// /// of the vec, giving O(1) access with pop()
// struct Sort<Iter, K>
// where
//     Iter: Iterator<Item = Row>,
//     K: Ord,
// {
//     input: Iter,
//     rows: Vec<Row>,
//     is_sorted: bool,
//     key_fn: fn(&Row) -> K,
//     desc: bool,
// }
// impl<Iter, K> Sort<Iter, K>
// where
//     Iter: Iterator<Item = Row>,
//     K: Ord,
// {
//     fn new(input: Iter, key_fn: fn(&Row) -> K, desc: bool) -> Self {
//         Sort {
//             input,
//             rows: Vec::new(),
//             is_sorted: false,
//             key_fn,
//             desc,
//         }
//     }
// }
// impl<Iter, K> Iterator for Sort<Iter, K>
// where
//     Iter: Iterator<Item = Row>,
//     K: Ord,
// {
//     type Item = Row;

//     fn next(&mut self) -> Option<Self::Item> {
//         if !self.is_sorted {
//             for row in &mut self.input {
//                 self.rows.push(row);
//             }
//             self.rows.sort_by_key(self.key_fn);
//             if !self.desc {
//                 self.rows.reverse();
//             }
//             self.is_sorted = true;
//         }

//         self.rows.pop()
//     }
// }
