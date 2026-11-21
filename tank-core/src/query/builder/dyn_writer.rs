use super::view::{InsertView, SelectView, UpdateView};
use crate::{
    AnyQuery, ColumnDef, Context, Dataset, Expression, InsertIntoQuery, SelectQuery, SqlCoreWriter,
    SqlExpressionWriter, SqlFragmentWriter, SqlValueWriter, SqlWriter, TableRef, UpdateQuery,
};

/// The statement entry points of [`SqlWriter`], callable through `dyn` with concrete arguments.
pub(crate) trait DynWriter: Send {
    fn write_transaction_begin(&self, out: &mut AnyQuery);
    fn write_transaction_commit(&self, out: &mut AnyQuery);
    fn write_transaction_rollback(&self, out: &mut AnyQuery);
    fn write_create_schema(&self, out: &mut AnyQuery, table: &TableRef, if_not_exists: bool);
    fn write_drop_schema(&self, out: &mut AnyQuery, table: &TableRef, if_exists: bool);
    fn write_create_table(&self, out: &mut AnyQuery, table: &TableRef, if_not_exists: bool);
    fn write_column_comment(&self, context: &mut Context, out: &mut AnyQuery, column: &ColumnDef);
    fn write_drop_table(&self, out: &mut AnyQuery, table: &TableRef, if_exists: bool);
    fn write_select(&self, out: &mut AnyQuery, query: SelectView<'_>);
    fn write_insert(&self, out: &mut AnyQuery, query: InsertView<'_>);
    fn write_update(&self, out: &mut AnyQuery, query: UpdateView<'_>);
    fn write_delete(&self, out: &mut AnyQuery, from: &dyn Dataset, condition: &dyn Expression);
}

/// Writer of a [`BoxQueryBuilder`](crate::BoxQueryBuilder).
/// Forwards each statement to the writer of the executing driver.
pub(crate) struct AnyWriter {
    writer: Box<dyn DynWriter>,
}

impl AnyWriter {
    pub(crate) fn new(writer: Box<dyn DynWriter>) -> Self {
        Self { writer }
    }
}

impl SqlCoreWriter for AnyWriter {
    fn as_dyn(&self) -> &dyn SqlWriter {
        self
    }
}
impl SqlValueWriter for AnyWriter {}
impl SqlExpressionWriter for AnyWriter {}
impl SqlFragmentWriter for AnyWriter {}
impl SqlWriter for AnyWriter {
    fn write_transaction_begin(&self, out: &mut AnyQuery) {
        self.writer.write_transaction_begin(out);
    }

    fn write_transaction_commit(&self, out: &mut AnyQuery) {
        self.writer.write_transaction_commit(out);
    }

    fn write_transaction_rollback(&self, out: &mut AnyQuery) {
        self.writer.write_transaction_rollback(out);
    }

    fn write_create_schema(&self, out: &mut AnyQuery, table: &TableRef, if_not_exists: bool) {
        self.writer.write_create_schema(out, table, if_not_exists);
    }

    fn write_drop_schema(&self, out: &mut AnyQuery, table: &TableRef, if_exists: bool) {
        self.writer.write_drop_schema(out, table, if_exists);
    }

    fn write_create_table(&self, out: &mut AnyQuery, table: &TableRef, if_not_exists: bool) {
        self.writer.write_create_table(out, table, if_not_exists);
    }

    fn write_column_comment(&self, context: &mut Context, out: &mut AnyQuery, column: &ColumnDef) {
        self.writer.write_column_comment(context, out, column);
    }

    fn write_drop_table(&self, out: &mut AnyQuery, table: &TableRef, if_exists: bool) {
        self.writer.write_drop_table(out, table, if_exists);
    }

    fn write_select<'a, Data>(&self, out: &mut AnyQuery, query: impl SelectQuery<Data>)
    where
        Data: Dataset + 'a,
    {
        SelectView::with(&query, |query| self.writer.write_select(out, query));
    }

    fn write_insert(&self, out: &mut AnyQuery, query: impl InsertIntoQuery) {
        InsertView::with(&query, |query| self.writer.write_insert(out, query));
    }

    fn write_update<Data>(&self, out: &mut AnyQuery, query: impl UpdateQuery<Data>)
    where
        Data: Dataset,
    {
        UpdateView::with(&query, |query| self.writer.write_update(out, query));
    }

    fn write_delete(&self, out: &mut AnyQuery, from: &dyn Dataset, condition: impl Expression) {
        self.writer.write_delete(out, from, &condition);
    }
}

impl<W: SqlWriter> DynWriter for W {
    fn write_transaction_begin(&self, out: &mut AnyQuery) {
        SqlWriter::write_transaction_begin(self, out);
    }
    fn write_transaction_commit(&self, out: &mut AnyQuery) {
        SqlWriter::write_transaction_commit(self, out);
    }
    fn write_transaction_rollback(&self, out: &mut AnyQuery) {
        SqlWriter::write_transaction_rollback(self, out);
    }
    fn write_create_schema(&self, out: &mut AnyQuery, table: &TableRef, if_not_exists: bool) {
        SqlWriter::write_create_schema(self, out, table, if_not_exists);
    }
    fn write_drop_schema(&self, out: &mut AnyQuery, table: &TableRef, if_exists: bool) {
        SqlWriter::write_drop_schema(self, out, table, if_exists);
    }
    fn write_create_table(&self, out: &mut AnyQuery, table: &TableRef, if_not_exists: bool) {
        SqlWriter::write_create_table(self, out, table, if_not_exists);
    }
    fn write_column_comment(&self, context: &mut Context, out: &mut AnyQuery, column: &ColumnDef) {
        SqlWriter::write_column_comment(self, context, out, column);
    }
    fn write_drop_table(&self, out: &mut AnyQuery, table: &TableRef, if_exists: bool) {
        SqlWriter::write_drop_table(self, out, table, if_exists);
    }
    fn write_select(&self, out: &mut AnyQuery, query: SelectView<'_>) {
        SqlWriter::write_select(self, out, query);
    }
    fn write_insert(&self, out: &mut AnyQuery, query: InsertView<'_>) {
        SqlWriter::write_insert(self, out, query);
    }
    fn write_update(&self, out: &mut AnyQuery, query: UpdateView<'_>) {
        SqlWriter::write_update(self, out, query);
    }
    fn write_delete(&self, out: &mut AnyQuery, from: &dyn Dataset, condition: &dyn Expression) {
        SqlWriter::write_delete(self, out, from, condition);
    }
}
