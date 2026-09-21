use crate::*;
use std::fmt::Write;

/// SQL dialect printer: the user-facing API.
///
/// Everything a caller needs lives here. [`SqlWriter`] produces complete
/// statements (queries and definitions), and is the only trait a user has to
/// bring into scope. The overridable pieces those statements are built from are
/// exposed, split by concern, through the fragment traits ([`SqlCoreWriter`],
/// [`SqlValueWriter`], [`SqlExpressionWriter`], [`SqlFragmentWriter`]) which
/// driver authors implement.
///
/// [`SqlCoreWriter::as_dyn`] upcasts to this view so a fragment can reach any
/// other fragment.
pub trait SqlWriter:
    SqlCoreWriter + SqlValueWriter + SqlExpressionWriter + SqlFragmentWriter
{
    /// Write SELECT statement.
    fn write_select<'a, Data>(&self, out: &mut DynQuery, query: &impl SelectQuery<Data>)
    where
        Self: Sized,
        Data: Dataset + 'a,
    {
        let Some(from) = query.get_from() else {
            log::error!("The query does not have the FROM clause");
            return;
        };
        let columns = query.get_select();
        let columns_count = columns.clone().into_iter().count();
        out.buffer().reserve(128 + columns_count * 32);
        if !out.is_empty() {
            out.push('\n');
        }
        out.push_str("SELECT ");
        let mut context = Context::new(Fragment::SqlSelect, Data::qualified_columns());
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
        out.push_str("\nFROM ");
        from.write_table_name(
            self,
            &mut context.switch_fragment(Fragment::SqlSelectFrom).current,
            out,
        );
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
        if let Some(limit) = query.get_limit() {
            let _ = write!(out, "\nLIMIT {limit}");
        }
        out.push(';');
    }

    /// Write INSERT statement.
    fn write_insert<It>(&self, out: &mut DynQuery, entities: It, update: bool)
    where
        Self: Sized,
        It: IntoIterator,
        It::Item: AsEntity,
    {
        type E<It> = <<It as IntoIterator>::Item as AsEntity>::Entity;
        let table = E::<It>::table();
        let mut entities = entities.into_iter().peekable();
        if entities.peek().is_none() {
            return;
        };
        let cols = E::<It>::columns().len();
        out.buffer().reserve(128 + cols * 32);
        if !out.is_empty() {
            out.push('\n');
        }
        out.push_str("INSERT INTO ");
        let mut context = Context::new(Fragment::SqlInsertInto, E::<It>::qualified_columns());
        self.write_table_ref(&mut context, out, table);
        out.push_str(" (");
        separated_by(
            out,
            E::<It>::columns().iter(),
            |out, col| {
                self.write_identifier(&mut context, out, col.name(), true);
            },
            ", ",
        );
        out.push_str(") VALUES");
        let mut context = context.switch_fragment(Fragment::SqlInsertIntoValues);
        separated_by(
            out,
            entities,
            |out, entity| {
                out.push_str("\n(");
                entity
                    .as_entity()
                    .write_query(self.as_dyn(), &mut context.current, out);
                out.push(')');
            },
            ",",
        );
        if update {
            self.write_insert_update_fragment::<E<It>>(
                &mut context.current,
                out,
                E::<It>::columns().iter(),
            );
        }
        out.push(';');
    }

    /// Write DELETE statement.
    fn write_delete<E>(&self, out: &mut DynQuery, condition: impl Expression)
    where
        Self: Sized,
        E: Entity,
    {
        let table = E::table();
        out.buffer().reserve(128);
        if !out.is_empty() {
            out.push('\n');
        }
        out.push_str("DELETE FROM ");
        let mut context = Context::new(Fragment::SqlDeleteFrom, E::qualified_columns());
        self.write_table_ref(&mut context, out, table);
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

    /// Emit CREATE SCHEMA.
    fn write_create_schema<E>(&self, out: &mut DynQuery, if_not_exists: bool)
    where
        Self: Sized,
        E: Entity,
    {
        let table = E::table();
        out.buffer().reserve(32 + table.schema.len());
        if !out.is_empty() {
            out.push('\n');
        }
        out.push_str("CREATE SCHEMA ");
        let mut context = Context::new(Fragment::SqlCreateSchema, E::qualified_columns());
        if if_not_exists {
            out.push_str("IF NOT EXISTS ");
        }
        self.write_identifier(&mut context, out, &table.schema, true);
        out.push(';');
    }

    /// Emit DROP SCHEMA.
    fn write_drop_schema<E>(&self, out: &mut DynQuery, if_exists: bool)
    where
        Self: Sized,
        E: Entity,
    {
        let mut context = Context::new(Fragment::SqlDropSchema, E::qualified_columns());
        let table = E::table();
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

    /// Emit CREATE TABLE with columns, constraints & comments.
    fn write_create_table<E>(&self, out: &mut DynQuery, if_not_exists: bool)
    where
        Self: Sized,
        E: Entity,
    {
        let mut context = Context::new(Fragment::SqlCreateTable, E::qualified_columns());
        let table = E::table();
        let estimated = 128 + E::columns().len() * 64 + E::primary_key_def().len() * 24;
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
            E::columns(),
            |out, col| {
                self.write_create_table_column_fragment(&mut context, out, col);
            },
            ",\n",
        );
        let pk = E::primary_key_def();
        if pk.len() > 1 {
            self.write_create_table_primary_key_fragment(&mut context, out, pk.iter().map(|v| *v));
        }
        for unique in E::unique_defs() {
            if unique.len() > 1 {
                out.push_str(",\nUNIQUE (");
                separated_by(
                    out,
                    unique,
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
        let foreign_keys = E::columns().iter().filter(|c| c.references.is_some());
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
                    self.write_create_table_references_action_fragment(
                        &mut context,
                        out,
                        on_delete,
                    );
                }
                if let Some(on_update) = &column.on_update {
                    out.push_str(" ON UPDATE ");
                    self.write_create_table_references_action_fragment(
                        &mut context,
                        out,
                        on_update,
                    );
                }
            },
            "",
        );
        out.push_str(");");
        self.write_column_comments_statements_fragment::<E>(&mut context, out);
    }

    /// Write DROP TABLE statement.
    fn write_drop_table<E>(&self, out: &mut DynQuery, if_exists: bool)
    where
        Self: Sized,
        E: Entity,
    {
        let table = E::table();
        out.buffer()
            .reserve(24 + table.schema.len() + table.name.len());
        if !out.is_empty() {
            out.push('\n');
        }
        out.push_str("DROP TABLE ");
        let mut context = Context::new(Fragment::SqlDropTable, E::qualified_columns());
        if if_exists {
            out.push_str("IF EXISTS ");
        }
        self.write_table_ref(&mut context, out, table);
        out.push(';');
    }

    /// Emit BEGIN statement.
    fn write_transaction_begin(&self, out: &mut DynQuery) {
        out.push_str("BEGIN;");
    }

    /// Emit COMMIT statement.
    fn write_transaction_commit(&self, out: &mut DynQuery) {
        out.push_str("COMMIT;");
    }

    /// Emit ROLLBACK statement.
    fn write_transaction_rollback(&self, out: &mut DynQuery) {
        out.push_str("ROLLBACK;");
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
