use crate::{
    ColumnDef, Dataset, DynQuery, Expression, Fragment, InsertIntoQuery, IsTrue, SelectQuery,
    TableRef, UpdateQuery,
    future::Either,
    separated_by,
    writer::{Context, SqlCoreWriter, SqlExpressionWriter, SqlFragmentWriter, SqlValueWriter},
};

/// SQL dialect printer for complete statements, combining [`SqlCoreWriter`],
/// [`SqlValueWriter`], [`SqlExpressionWriter`], and [`SqlFragmentWriter`].
pub trait SqlWriter:
    SqlCoreWriter + SqlValueWriter + SqlExpressionWriter + SqlFragmentWriter
{
    /// Emit BEGIN statement.
    fn write_transaction_begin(&self, out: &mut DynQuery) {
        if !out.is_empty() {
            out.push('\n');
        }
        out.push_str("BEGIN;");
    }

    /// Emit COMMIT statement.
    fn write_transaction_commit(&self, out: &mut DynQuery) {
        if !out.is_empty() {
            out.push('\n');
        }
        out.push_str("COMMIT;");
    }

    /// Emit ROLLBACK statement.
    fn write_transaction_rollback(&self, out: &mut DynQuery) {
        if !out.is_empty() {
            out.push('\n');
        }
        out.push_str("ROLLBACK;");
    }

    /// Emit CREATE SCHEMA.
    fn write_create_schema(&self, out: &mut DynQuery, table: &TableRef, if_not_exists: bool)
    where
        Self: Sized,
    {
        out.buffer().reserve(32 + table.schema.len());
        if !out.is_empty() {
            out.push('\n');
        }
        out.push_str("CREATE SCHEMA ");
        let mut context = Context::new(Fragment::SqlCreateSchema, table.qualified_columns());
        if if_not_exists {
            out.push_str("IF NOT EXISTS ");
        }
        self.write_identifier(&mut context, out, &table.schema, true);
        out.push(';');
    }

    /// Emit DROP SCHEMA.
    fn write_drop_schema(&self, out: &mut DynQuery, table: &TableRef, if_exists: bool)
    where
        Self: Sized,
    {
        let mut context = Context::new(Fragment::SqlDropSchema, table.qualified_columns());
        out.buffer().reserve(32 + table.schema.len());
        if !out.is_empty() {
            out.push('\n');
        }
        out.push_str("DROP SCHEMA ");
        if if_exists {
            out.push_str("IF EXISTS ");
        }
        self.write_identifier(&mut context, out, &table.schema, true);
        out.push(';');
    }

    /// Emit CREATE TABLE with columns and constraints.
    fn write_create_table(&self, out: &mut DynQuery, table: &TableRef, if_not_exists: bool)
    where
        Self: Sized,
    {
        let mut context = Context::new(Fragment::SqlCreateTable, table.qualified_columns());
        let estimated = 128 + table.columns.len() * 64 + table.primary_key.len() * 24;
        out.buffer().reserve(estimated);
        if !out.is_empty() {
            out.push('\n');
        }
        out.push_str("CREATE TABLE ");
        if if_not_exists {
            out.push_str("IF NOT EXISTS ");
        }
        self.write_table_ref(&mut context, out, table);
        out.push_str(" (\n");
        separated_by(
            out,
            table.columns,
            |out, col| {
                self.write_create_table_column_fragment(&mut context, out, col);
            },
            ",\n",
        );
        let pk = table.primary_key;
        if pk.len() > 1 {
            self.write_create_table_primary_key_fragment(&mut context, out, pk.iter().map(|v| *v));
        }
        for unique in table.unique {
            if unique.len() > 0 {
                out.push_str(",\nUNIQUE (");
                separated_by(
                    out,
                    unique.iter(),
                    |out, col| {
                        self.write_identifier(
                            &mut context
                                .switch_fragment(Fragment::SqlCreateTableUnique)
                                .current,
                            out,
                            col.name(),
                            true,
                        );
                    },
                    ", ",
                );
                out.push(')');
            }
        }
        let foreign_keys = table.columns.iter().filter(|c| c.references.is_some());
        separated_by(
            out,
            foreign_keys,
            |out, column| {
                let references = column.references.as_ref().unwrap();
                out.push_str(",\nFOREIGN KEY (");
                self.write_identifier(&mut context, out, &column.name(), true);
                out.push_str(") REFERENCES ");
                self.write_table_ref(&mut context, out, &references.table());
                out.push('(');
                self.write_column_ref(&mut context, out, references);
                out.push(')');
                if let Some(on_delete) = &column.on_delete {
                    out.push_str(" ON DELETE ");
                    self.write_create_table_references_action(&mut context, out, on_delete);
                }
                if let Some(on_update) = &column.on_update {
                    out.push_str(" ON UPDATE ");
                    self.write_create_table_references_action(&mut context, out, on_update);
                }
            },
            "",
        );
        out.push_str(");");
    }

    /// Write the `COMMENT ON COLUMN` statement of a single column.
    fn write_column_comment(&self, context: &mut Context, out: &mut DynQuery, column: &ColumnDef) {
        let mut context = context.switch_fragment(Fragment::SqlCommentOnColumn);
        context.current.qualify_columns = true;
        if !out.is_empty() {
            out.push('\n');
        }
        out.push_str("COMMENT ON COLUMN ");
        self.write_column_ref(&mut context.current, out, column.into());
        out.push_str(" IS ");
        self.write_string(&mut context.current, out, column.comment);
        out.push(';');
    }

    /// Write DROP TABLE statement.
    fn write_drop_table(&self, out: &mut DynQuery, table: &TableRef, if_exists: bool)
    where
        Self: Sized,
    {
        out.buffer()
            .reserve(32 + table.schema.len() + table.name.len());
        if !out.is_empty() {
            out.push('\n');
        }
        out.push_str("DROP TABLE ");
        let mut context = Context::new(Fragment::SqlDropTable, table.qualified_columns());
        if if_exists {
            out.push_str("IF EXISTS ");
        }
        self.write_table_ref(&mut context, out, table);
        out.push(';');
    }

    /// Write SELECT statement.
    fn write_select<'a, Data>(&self, out: &mut DynQuery, query: impl SelectQuery<Data>)
    where
        Self: Sized,
        Data: Dataset + 'a,
    {
        let columns = query.get_select();
        let columns_count = columns.clone().into_iter().count();
        out.buffer().reserve(256 + columns_count * 32);
        if !out.is_empty() {
            out.push('\n');
        }
        out.push_str("SELECT ");
        if query.is_distinct() {
            out.push_str("DISTINCT ");
        }
        let qualified = query
            .get_from()
            .as_ref()
            .is_some_and(Dataset::qualified_columns);
        let mut context = Context::new(Fragment::SqlSelect, qualified);
        if columns_count != 0 {
            separated_by(
                out,
                columns.clone(),
                |out, col| {
                    col.write_query(self, &mut context, out);
                },
                ", ",
            );
        } else {
            out.push('*');
        }
        if let Some(from) = query.get_from() {
            out.push_str("\nFROM ");
            from.write_table_name(
                self,
                &mut context.switch_fragment(Fragment::SqlSelectFrom).current,
                out,
            );
        }
        if let Some(condition) = query.get_where()
            && !condition.accept_visitor(&mut IsTrue, self, &mut context, out)
        {
            out.push_str("\nWHERE ");
            condition.write_query(
                self,
                &mut context.switch_fragment(Fragment::SqlSelectWhere).current,
                out,
            );
        }
        let mut group_by = query.get_group_by().peekable();
        if group_by.peek().is_some() {
            out.push_str("\nGROUP BY ");
            let mut context = context.switch_fragment(Fragment::SqlSelectGroupBy);
            separated_by(
                out,
                group_by,
                |out, col| {
                    col.write_query(self, &mut context.current, out);
                },
                ", ",
            );
        }
        if let Some(having) = query.get_having() {
            out.push_str("\nHAVING ");
            having.write_query(
                self,
                &mut context.switch_fragment(Fragment::SqlSelectHaving).current,
                out,
            );
        }
        let mut order_by = query.get_order_by().peekable();
        if order_by.peek().is_some() {
            out.push_str("\nORDER BY ");
            let mut context = context.switch_fragment(Fragment::SqlSelectOrderBy);
            separated_by(
                out,
                order_by,
                |out, col| {
                    col.write_query(self, &mut context.current, out);
                },
                ", ",
            );
        }
        self.write_limit_offset(&mut context, out, query.get_limit(), query.get_offset());
        out.push(';');
    }

    /// Write INSERT statement printing rows of values or a SELECT.
    fn write_insert(&self, out: &mut DynQuery, query: impl InsertIntoQuery)
    where
        Self: Sized,
    {
        let table = query.get_table();
        let columns = query.get_columns();
        // A SELECT without named columns fills all the table columns, rows always name theirs.
        let write_head = |out: &mut DynQuery, context: &mut Context, all: bool| {
            if !out.is_empty() {
                out.push('\n');
            }
            out.push_str("INSERT INTO ");
            self.write_table_ref(context, out, table);
            if !all || !columns.is_empty() {
                out.push_str(" (");
                separated_by(
                    out,
                    columns,
                    |out, column| self.write_identifier(context, out, column.name(), true),
                    ", ",
                );
                out.push(')');
            }
        };
        let mut context = Context::new(Fragment::SqlInsertInto, table.qualified_columns());
        let rows = match query.get_values() {
            Either::Left(rows) => rows,
            Either::Right(select) => {
                write_head(out, &mut context, true);
                return self.write_select(out, select);
            }
        };
        let mut rows = rows.peekable();
        if rows.peek().is_none() {
            return;
        }
        out.buffer()
            .reserve(128 + columns.len() * (32 + rows.size_hint().0 * 16));
        write_head(out, &mut context, false);
        out.push_str(" VALUES");
        let mut context = context.switch_fragment(Fragment::SqlInsertIntoValues);
        separated_by(
            out,
            rows,
            |out, row| {
                out.push_str("\n(");
                separated_by(
                    out,
                    row,
                    |out, value| self.write_value(&mut context.current, out, &value),
                    ", ",
                );
                out.push(')');
            },
            ",",
        );
        if query.get_update() {
            self.write_insert_update_fragment(
                &mut context.current,
                out,
                table.primary_key.iter().copied(),
                columns.iter(),
            );
        }
        out.push(';');
    }

    /// Write UPDATE statement.
    fn write_update<Data>(&self, out: &mut DynQuery, query: impl UpdateQuery<Data>)
    where
        Self: Sized,
        Data: Dataset,
    {
        let table = query.get_from();
        let assignments = query.get_set();
        let assignments_count = assignments.clone().count();
        if assignments_count == 0 {
            return;
        }
        out.buffer().reserve(128 + assignments_count * 32);
        if !out.is_empty() {
            out.push('\n');
        }
        out.push_str("UPDATE ");
        let mut context = Context::new(Fragment::SqlUpdate, table.qualified_columns());
        table.write_table_name(self, &mut context, out);
        out.push_str("\nSET ");
        {
            let mut set = context.switch_fragment(Fragment::SqlUpdateSet);
            // A SET target is a bare column name, never a qualified one.
            set.current.qualify_columns = false;
            separated_by(
                out,
                assignments,
                |out, assignment| assignment.write_query(self, &mut set.current, out),
                ",\n",
            );
        }
        if let Some(condition) = query.get_where()
            && !condition.accept_visitor(&mut IsTrue, self, &mut context, out)
        {
            out.push_str("\nWHERE ");
            condition.write_query(
                self,
                &mut context.switch_fragment(Fragment::SqlUpdateWhere).current,
                out,
            );
        }
        out.push(';');
    }

    /// Write DELETE statement.
    fn write_delete(&self, out: &mut DynQuery, from: &dyn Dataset, condition: impl Expression)
    where
        Self: Sized,
    {
        out.buffer().reserve(128);
        if !out.is_empty() {
            out.push('\n');
        }
        out.push_str("DELETE FROM ");
        let mut context = Context::new(Fragment::SqlDeleteFrom, from.qualified_columns());
        from.write_table_name(self, &mut context, out);
        out.push_str("\nWHERE ");
        condition.write_query(
            self,
            &mut context
                .switch_fragment(Fragment::SqlDeleteFromWhere)
                .current,
            out,
        );
        out.push(';');
    }
}

/// Generic SQL writer.
pub struct GenericSqlWriter;
impl GenericSqlWriter {
    /// New generic writer.
    pub fn new() -> Self {
        Self {}
    }
}
impl SqlCoreWriter for GenericSqlWriter {
    fn as_dyn(&self) -> &dyn SqlWriter {
        self
    }
}
impl SqlValueWriter for GenericSqlWriter {}
impl SqlExpressionWriter for GenericSqlWriter {}
impl SqlFragmentWriter for GenericSqlWriter {}
impl SqlWriter for GenericSqlWriter {}
