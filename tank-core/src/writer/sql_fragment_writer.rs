use crate::*;

/// Overridable SQL fragments.
///
/// The user-facing statements themselves ([`SqlWriter::write_create_table`],
/// [`SqlWriter::write_select`], etc.) live in [`SqlWriter`]; this trait exposes
/// the pieces those statements are assembled from so driver authors can
/// override them. Every method is suffixed with `_fragment` to make it clear it
/// is a building block rather than a complete statement.
pub trait SqlFragmentWriter: SqlValueWriter {
    /// Render join keyword(s) for the given join type.
    fn write_join_type_fragment(
        &self,
        _context: &mut Context,
        out: &mut DynQuery,
        join_type: &JoinType,
    ) {
        out.push_str(match &join_type {
            JoinType::Default => "JOIN",
            JoinType::Inner => "INNER JOIN",
            JoinType::Outer => "OUTER JOIN",
            JoinType::Left => "LEFT JOIN",
            JoinType::Right => "RIGHT JOIN",
            JoinType::Cross => "CROSS JOIN",
            JoinType::Natural => "NATURAL JOIN",
        });
    }

    /// Render a JOIN clause fragment.
    fn write_join_fragment(
        &self,
        context: &mut Context,
        out: &mut DynQuery,
        join: &Join<&dyn Dataset, &dyn Dataset, &dyn Expression>,
    ) {
        let mut context = context.switch_fragment(Fragment::SqlJoin);
        context.current.qualify_columns = true;
        join.lhs
            .write_table_name(self.as_dyn(), &mut context.current, out);
        out.push(' ');
        self.write_join_type_fragment(&mut context.current, out, &join.join);
        out.push(' ');
        join.rhs
            .write_table_name(self.as_dyn(), &mut context.current, out);
        if let Some(on) = &join.on {
            out.push_str(" ON ");
            on.write_query(self.as_dyn(), &mut context.current, out);
        }
    }

    /// Emit single column definition fragment.
    fn write_create_table_column_fragment(
        &self,
        context: &mut Context,
        out: &mut DynQuery,
        column: &ColumnDef,
    ) where
        Self: Sized,
    {
        self.write_identifier(context, out, &column.name(), true);
        out.push(' ');
        let len = out.len();
        self.write_column_overridden_type(context, out, column, &column.column_type);
        let didnt_write_type = out.len() == len;
        if didnt_write_type {
            self.write_column_type(context, out, &column.value);
        }
        if !column.nullable && column.primary_key == PrimaryKeyType::None {
            out.push_str(" NOT NULL");
        }
        if column.default.is_set() {
            out.push_str(" DEFAULT ");
            column.default.write_query(self.as_dyn(), context, out);
        }
        if column.primary_key == PrimaryKeyType::PrimaryKey {
            // Composite primary key will be printed elsewhere
            out.push_str(" PRIMARY KEY");
        }
        if column.unique && column.primary_key != PrimaryKeyType::PrimaryKey {
            out.push_str(" UNIQUE");
        }
        if !column.comment.is_empty() {
            self.write_column_comment_inline_fragment(context, out, column);
        }
    }

    /// Write PRIMARY KEY constraint fragment.
    fn write_create_table_primary_key_fragment<'a, It>(
        &self,
        context: &mut Context,
        out: &mut DynQuery,
        primary_key: It,
    ) where
        Self: Sized,
        It: IntoIterator<Item = &'a ColumnDef>,
        It::IntoIter: Clone,
    {
        out.push_str(",\nPRIMARY KEY (");
        separated_by(
            out,
            primary_key,
            |out, col| {
                self.write_identifier(
                    &mut context
                        .switch_fragment(Fragment::SqlCreateTablePrimaryKey)
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

    /// Write referential action fragment.
    fn write_create_table_references_action_fragment(
        &self,
        _context: &mut Context,
        out: &mut DynQuery,
        action: &Action,
    ) {
        out.push_str(match action {
            Action::NoAction => "NO ACTION",
            Action::Restrict => "RESTRICT",
            Action::Cascade => "CASCADE",
            Action::SetNull => "SET NULL",
            Action::SetDefault => "SET DEFAULT",
        });
    }

    fn write_column_comment_inline_fragment(
        &self,
        _context: &mut Context,
        _out: &mut DynQuery,
        _column: &ColumnDef,
    ) where
        Self: Sized,
    {
    }

    /// Write column comments fragment.
    fn write_column_comments_statements_fragment<E>(
        &self,
        context: &mut Context,
        out: &mut DynQuery,
    ) where
        Self: Sized,
        E: Entity,
    {
        let mut context = context.switch_fragment(Fragment::SqlCommentOnColumn);
        context.current.qualify_columns = true;
        for c in E::columns().iter().filter(|c| !c.comment.is_empty()) {
            out.push_str("\nCOMMENT ON COLUMN ");
            self.write_column_ref(&mut context.current, out, c.into());
            out.push_str(" IS ");
            self.write_string(&mut context.current, out, c.comment);
            out.push(';');
        }
    }

    /// Write ON CONFLICT DO UPDATE fragment for upsert.
    fn write_insert_update_fragment<'a, E>(
        &self,
        context: &mut Context,
        out: &mut DynQuery,
        columns: impl Iterator<Item = &'a ColumnDef> + Clone,
    ) where
        Self: Sized,
        E: Entity,
    {
        let pk = E::primary_key_def();
        if pk.is_empty() {
            return;
        }
        out.push_str("\nON CONFLICT");
        context.fragment = Fragment::SqlInsertIntoOnConflict;
        out.push_str(" (");
        separated_by(
            out,
            pk,
            |out, col| {
                self.write_identifier(context, out, col.name(), true);
            },
            ", ",
        );
        out.push(')');
        let mut update_cols = columns
            .filter(|c| c.primary_key == PrimaryKeyType::None)
            .peekable();
        if update_cols.peek().is_some() {
            out.push_str(" DO UPDATE SET\n");
            separated_by(
                out,
                update_cols,
                |out, col| {
                    self.write_identifier(context, out, col.name(), true);
                    out.push_str(" = EXCLUDED.");
                    self.write_identifier(context, out, col.name(), true);
                },
                ",\n",
            );
        } else {
            out.push_str(" DO NOTHING");
        }
    }
}
