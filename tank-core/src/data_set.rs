use crate::{
    Driver, Executor, Expression, Query, RawQuery, Result, RowLabeled, TableRef,
    stream::Stream,
    writer::{Context, SqlWriter},
};

/// Queryable data source (table or join tree).
///
/// Implementors know how to render themselves inside a FROM clause and whether
/// column references should be qualified with schema and table.
pub trait DataSet {
    /// Should columns be qualified (`schema.table.col`)?
    fn qualified_columns() -> bool
    where
        Self: Sized;
    /// Render the sql bits representing this data set into `out`.
    fn write_query(&self, writer: &dyn SqlWriter, context: &mut Context, out: &mut RawQuery);
    /// TableRef representing this data set
    fn table_ref(&self) -> TableRef;
    /// Fetch a SELECT query and stream labeled rows.
    fn select<'s, Exec, Item>(
        &'s self,
        executor: &'s mut Exec,
        columns: impl IntoIterator<Item = Item> + Clone,
        condition: impl Expression,
        limit: Option<u32>,
    ) -> impl Stream<Item = Result<RowLabeled>> + 's
    where
        Self: Sized,
        Exec: Executor,
        Item: Expression,
    {
        let mut query = RawQuery::with_capacity(1024);
        executor
            .driver()
            .sql_writer()
            .write_select(&mut query, columns, self, condition, limit);
        executor.fetch(query)
    }
    /// Prepare a SELECT query.
    fn prepare<Exec, Item>(
        &self,
        executor: &mut Exec,
        columns: impl IntoIterator<Item = Item> + Clone,
        condition: impl Expression,
        limit: Option<u32>,
    ) -> impl Future<Output = Result<Query<Exec::Driver>>>
    where
        Self: Sized,
        Item: Expression,
        Exec: Executor,
    {
        let mut query = RawQuery::with_capacity(1024);
        executor
            .driver()
            .sql_writer()
            .write_select(&mut query, columns, self, condition, limit);
        executor.prepare(query)
    }
}

impl DataSet for &dyn DataSet {
    fn qualified_columns() -> bool
    where
        Self: Sized,
    {
        unreachable!("Cannot call static qualified_columns on a dyn object directly");
    }
    fn write_query(&self, writer: &dyn SqlWriter, context: &mut Context, out: &mut RawQuery) {
        (*self).write_query(writer, context, out)
    }
    fn table_ref(&self) -> TableRef {
        (*self).table_ref()
    }
}
