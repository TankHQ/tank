use crate::{
    RawQuery, TableRef,
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
