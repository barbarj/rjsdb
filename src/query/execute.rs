use std::{borrow::Cow, iter::zip};

use crate::{
    storage::{Column, ColumnWithIndex, Row, Rows, Schema, StorageError, StorageLayer},
    DbType, DbValue,
};

use super::parse::{
    CreateStatement, DeleteStatement, DestroyStatement, InsertStatement, OrderByClause,
    ParsingError, SelectColumns, SelectSource, SelectStatement, Statement, WhereClause, WhereCmp,
    WhereMember,
};

#[derive(Debug)]
pub enum ExecutionError {
    ParsingError(ParsingError),
    StorageError(StorageError),
    UnknownColumnNameProvided,
    MismatchedTypeComparision,
    UncoercableValueProvided,
}
impl From<StorageError> for ExecutionError {
    fn from(value: StorageError) -> Self {
        Self::StorageError(value)
    }
}
impl From<ParsingError> for ExecutionError {
    fn from(value: ParsingError) -> Self {
        Self::ParsingError(value)
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

    fn build_select_source_rows<'strg>(
        &self,
        select_source: &SelectSource,
        storage: &'strg mut StorageLayer,
        uses_rowid: bool,
    ) -> Result<RowsSource<'strg>> {
        let source = match select_source {
            SelectSource::Table(name) => {
                let rows = storage.table_scan(name, uses_rowid)?;
                RowsSource::Table(rows)
            }
            SelectSource::Expression(inner_stmt) => self.compose_select(inner_stmt, storage)?,
        };
        Ok(source)
    }

    fn compose_select<'strg>(
        &self,
        select_stmt: &SelectStatement,
        storage: &'strg mut StorageLayer,
    ) -> Result<RowsSource<'strg>> {
        let source =
            self.build_select_source_rows(&select_stmt.source, storage, select_stmt.uses_row_id())?;
        let source = if let Some(where_clause) = &select_stmt.where_clause {
            let filter = FilterRowsIter::build(source, where_clause)?;
            RowsSource::Filter(filter)
        } else {
            source
        };
        let source = if let Some(order_by_clause) = &select_stmt.order_by_clause {
            RowsSource::Sort(SortRowsIter::build(source, order_by_clause)?)
        } else {
            source
        };
        let source = RowsSource::Select(SelectRowsIter::new(source, &select_stmt.columns));
        let source = if let Some(limit) = &select_stmt.limit {
            RowsSource::Limit(LimitRowsIter::new(source, limit))
        } else {
            source
        };
        Ok(source)
    }

    fn select<'strg>(
        &self,
        select_stmt: &SelectStatement,
        storage: &'strg mut StorageLayer,
    ) -> Result<QueryResult<'strg>> {
        let source = self.compose_select(select_stmt, storage)?;

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
        let schema = Schema::new(cols);
        let primary_key_col = create_stmt
            .columns
            .primary_key_col
            .as_storage_key_column(&schema)?;

        storage.create_table(create_stmt.table.clone(), schema, primary_key_col)?;
        Ok(QueryResult::Ok)
    }

    fn insert<'strg>(
        &self,
        insert_stmt: &InsertStatement,
        storage: &'strg mut StorageLayer,
    ) -> Result<QueryResult<'strg>> {
        let schema = storage.table_schema(&insert_stmt.table)?;

        let indexed_vals: Result<Vec<(usize, DbType, &DbValue)>> =
            zip(insert_stmt.columns.iter(), insert_stmt.values.iter())
                .map(|(name, val)| match schema.get(name) {
                    Some(ci) if val.db_type().coerceable_to(&ci.column._type) => {
                        Ok((ci.index, ci.column._type, val))
                    }
                    Some(_) => Err(ExecutionError::UncoercableValueProvided),
                    None => Err(ExecutionError::UnknownColumnNameProvided),
                })
                .collect();
        let mut indexed_vals = indexed_vals?;
        indexed_vals.sort_by_key(|x| x.0);
        let vals: Vec<DbValue> = indexed_vals
            .into_iter()
            .filter_map(|(_, _type, val)| val.coerced_to(_type))
            .collect();

        let rows = vec![Row::new(vals)];

        let conflict_rule = insert_stmt
            .conflict_clause
            .as_ref()
            .map(|c| c.as_conflict_rule());
        storage.insert_rows(&insert_stmt.table, &rows, conflict_rule)?;
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

    fn delete<'strg>(
        &self,
        delete_stmt: &DeleteStatement,
        storage: &'strg mut StorageLayer,
    ) -> Result<QueryResult<'strg>> {
        //compose select with where clause,
        let select_stmt = delete_stmt.generated_select_statement();
        let ids: Vec<usize> = if let QueryResult::Rows(rows) = self.select(&select_stmt, storage)? {
            rows.map(|r| {
                let v = r.data.first().expect("Should always have a row id here");
                match v {
                    DbValue::UnsignedInt(id) => *id as usize,
                    _ => panic!("Should never have a row id of another kind"),
                }
            })
            .collect()
        } else {
            panic!("this should never happen");
        };
        storage.delete_rows(&delete_stmt.table, &ids)?;
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
            Statement::Delete(d) => self.delete(d, storage),
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
        for stmt in self.plan[0..last_idx].iter() {
            _ = self.execute_stmt(stmt, storage)?;
        }
        self.execute_stmt(last_expr, storage)
    }
}

enum RowsSource<'a> {
    Table(Rows<'a>),
    Select(SelectRowsIter<'a>),
    Filter(FilterRowsIter<'a>),
    Sort(SortRowsIter<'a>),
    Limit(LimitRowsIter<'a>),
}
impl<'a> RowsSource<'a> {
    fn schema(&self) -> Cow<'a, Schema> {
        match self {
            Self::Table(t) => t.schema.clone(),
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

struct SelectRowsIter<'a> {
    source: Box<RowsSource<'a>>,
    schema: Cow<'a, Schema>,
    column_project: Box<dyn Fn(Cow<'a, Row>) -> Cow<'a, Row>>,
}
impl<'a> SelectRowsIter<'a> {
    fn new(source: RowsSource<'a>, columns: &SelectColumns) -> Self {
        let source_schema = source.schema();
        match columns {
            SelectColumns::All => {
                let mut schema = source_schema.into_owned();
                if let Some(removed_pos) = schema.column_position("rowid") {
                    schema.remove("rowid");
                    SelectRowsIter {
                        source: Box::new(source),
                        schema: Cow::Owned(schema),
                        column_project: Box::new(move |r| {
                            let mut r = r.into_owned();
                            r.data.remove(removed_pos);
                            Cow::Owned(r)
                        }),
                    }
                } else {
                    SelectRowsIter {
                        source: Box::new(source),
                        schema: Cow::Owned(schema),
                        column_project: Box::new(|r| r.clone()),
                    }
                }
            }
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
        col: String,
        val: DbValue,
        cmp: WhereCmp,
        schema: Schema,
    },
    ColumnColumn {
        col1: String,
        col2: String,
        _type: DbType,
        cmp: WhereCmp,
        schema: Schema,
    },
}
impl FilterType {
    fn validated_column_against(col: &str, schema: &Schema, against: DbType) -> Result<String> {
        match schema.column(col) {
            Some(c) if c._type.coerceable_to(&against) => Ok(col.to_string()),
            Some(_) => Err(ExecutionError::MismatchedTypeComparision),
            None => Err(ExecutionError::UnknownColumnNameProvided),
        }
    }

    fn validated_column_column(
        col1: &str,
        col2: &str,
        schema: &Schema,
    ) -> Result<(String, String, DbType)> {
        match (schema.column(col1), schema.column(col2)) {
            (Some(c1), Some(c2)) if c1._type.coerceable_to(&c2._type) => {
                Ok((col1.to_string(), col2.to_string(), c1._type))
            }
            (Some(_), Some(_)) => Err(ExecutionError::MismatchedTypeComparision),
            _ => Err(ExecutionError::UnknownColumnNameProvided),
        }
    }

    fn val_to_col_type(val: &DbValue, col: &str, schema: &Schema) -> Result<DbValue> {
        let _type = match schema.column(col) {
            Some(c) => c._type,
            None => return Err(ExecutionError::UnknownColumnNameProvided),
        };
        match val.coerced_to(_type) {
            Some(v) => Ok(v),
            None => Err(ExecutionError::MismatchedTypeComparision),
        }
    }

    fn build(where_clause: &WhereClause, schema: &Schema) -> Result<Self> {
        match (&where_clause.left, &where_clause.right) {
            (WhereMember::Value(val), WhereMember::Column(col)) => Ok(Self::ColumnValue {
                col: FilterType::validated_column_against(col, schema, val.db_type())?,
                val: FilterType::val_to_col_type(val, col, schema)?,
                cmp: where_clause.cmp.inverted(), // predicates assume value was always on the right, so we need to invert the comparison type
                schema: schema.clone(),
            }),
            (WhereMember::Column(col), WhereMember::Value(val)) => Ok(Self::ColumnValue {
                col: FilterType::validated_column_against(col, schema, val.db_type())?,
                val: FilterType::val_to_col_type(val, col, schema)?,
                cmp: where_clause.cmp,
                schema: schema.clone(),
            }),
            (WhereMember::Value(val1), WhereMember::Value(val2)) => {
                let val2 = val2.coerced_to(val1.db_type());
                match val2 {
                    Some(val2) => Ok(FilterType::ValueValue {
                        left: val1.clone(),
                        right: val2,
                        cmp: where_clause.cmp,
                    }),
                    None => Err(ExecutionError::MismatchedTypeComparision),
                }
            }
            (WhereMember::Column(col1), WhereMember::Column(col2)) => {
                let (col1, col2, _type) = FilterType::validated_column_column(col1, col2, schema)?;
                Ok(Self::ColumnColumn {
                    col1,
                    col2,
                    _type,
                    cmp: where_clause.cmp,
                    schema: schema.clone(),
                })
            }
        }
    }

    fn row_predicate(&self, row: &Row) -> bool {
        let (left, right, cmp) = match self {
            Self::ColumnColumn {
                col1,
                col2,
                _type,
                cmp,
                schema,
            } => {
                let left = schema
                    .column_value(col1, row)
                    .expect("Should always have a value")
                    .coerced_to(*_type)
                    .expect("Already validated this conversion works");
                let right = schema
                    .column_value(col2, row)
                    .expect("Should always have a value")
                    .coerced_to(*_type)
                    .expect("Already validated this conversion works");
                (left, right, cmp)
            }
            Self::ColumnValue {
                col,
                val,
                cmp,
                schema,
            } => {
                let left = schema
                    .column_value(col, row)
                    .expect("Should always have a value")
                    .clone();
                (left, val.clone(), cmp)
            }
            Self::ValueValue { left, right, cmp } => (left.clone(), right.clone(), cmp),
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
    let pos = match schema.column_position(clause.sort_column()) {
        Some(pos) => pos,
        None => return Err(ExecutionError::UnknownColumnNameProvided),
    };
    let key_fn = move |r: &Row| {
        let v = r
            .data
            .get(pos)
            .expect("We've already verified this will exist")
            .clone();
        let key = vec![v];
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
        rows.sort_by_cached_key(|row| key_fn(row));
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
