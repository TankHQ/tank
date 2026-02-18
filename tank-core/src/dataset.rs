use crate::{
    DynQuery, TableRef,
    writer::{Context, SqlWriter},
};

/// Queryable data source (table or join tree).
///
/// Implementors know how to render themselves inside a FROM clause.
pub trait Dataset {
    /// Should columns be qualified (`schema.table.col`)?
    fn qualified_columns() -> bool
    where
        Self: Sized;
    /// Render this dataset into SQL.
    fn write_query(&self, writer: &dyn SqlWriter, context: &mut Context, out: &mut DynQuery);
    /// Table reference for this dataset.
    fn table_ref(&self) -> TableRef;
}

impl Dataset for &dyn Dataset {
    fn qualified_columns() -> bool
    where
        Self: Sized,
    {
        unreachable!("Cannot call static qualified_columns on a dyn object directly");
    }
    fn write_query(&self, writer: &dyn SqlWriter, context: &mut Context, out: &mut DynQuery) {
        (*self).write_query(writer, context, out)
    }
    fn table_ref(&self) -> TableRef {
        (*self).table_ref()
    }
}
