use crate::{
    ColumnDef, ColumnRef, DynQuery, Fragment, TableRef, Value, write_escaped,
    writer::{Context, SqlWriter},
};
use std::{collections::BTreeMap, mem};

/// Core SQL writer for identifiers, table/column references, and type names.
pub trait SqlCoreWriter: Send {
    /// Upcasts `self` to a [`SqlWriter`] trait object.
    fn as_dyn(&self) -> &dyn SqlWriter;

    /// Separator used for qualified names (e.g., schema.table.column).
    fn separator(&self) -> &str {
        "."
    }

    /// Determines if the current SQL context supports alias declarations.
    fn is_alias_declaration(&self, context: &mut Context) -> bool {
        match context.fragment {
            Fragment::SqlSelectFrom | Fragment::SqlJoin => true,
            _ => false,
        }
    }

    /// Writes an identifier (like a table or column name) to the query buffer, optionally quoting it.
    fn write_identifier(
        &self,
        _context: &mut Context,
        out: &mut DynQuery,
        value: &str,
        quoted: bool,
    ) {
        if quoted {
            out.push('"');
            write_escaped(out, value, '"', "\"\"");
            out.push('"');
        } else {
            out.push_str(value);
        }
    }

    /// Write table reference.
    fn write_table_ref(&self, context: &mut Context, out: &mut DynQuery, value: &TableRef) {
        let alias_declaration = self.is_alias_declaration(context);
        if alias_declaration || value.alias.is_empty() {
            if !value.schema.is_empty() {
                self.write_identifier(context, out, &value.schema, context.quote_identifiers);
                out.push_str(self.separator());
            }
            self.write_identifier(context, out, &value.name, context.quote_identifiers);
        }
        if !value.alias.is_empty() {
            if alias_declaration {
                out.push(' ');
            }
            out.push_str(&value.alias);
        }
    }

    /// Write column reference.
    fn write_column_ref(&self, context: &mut Context, out: &mut DynQuery, value: &ColumnRef) {
        if context.qualify_columns {
            let table_ref = mem::take(&mut context.table_ref);
            let mut schema = &table_ref.schema;
            if schema.is_empty() {
                schema = &value.schema;
            }
            let mut table = &table_ref.alias;
            if table.is_empty() {
                table = &table_ref.name;
            }
            if table.is_empty() {
                table = &value.table;
            }
            if !table.is_empty() {
                if !schema.is_empty() {
                    self.write_identifier(context, out, schema, context.quote_identifiers);
                    out.push_str(self.separator());
                }
                self.write_identifier(context, out, table, context.quote_identifiers);
                out.push_str(self.separator());
            }
            context.table_ref = table_ref
        }
        self.write_identifier(context, out, &value.name, context.quote_identifiers);
    }

    /// Write overridden column type.
    fn write_column_overridden_type(
        &self,
        _context: &mut Context,
        out: &mut DynQuery,
        _column: &ColumnDef,
        types: &BTreeMap<&'static str, &'static str>,
    ) {
        if let Some(t) = types.get("") {
            out.push_str(t);
        }
    }

    /// Write SQL type name.
    fn write_column_type(&self, context: &mut Context, out: &mut DynQuery, value: &Value<'_>) {
        match value {
            Value::Boolean(..) => out.push_str("BOOLEAN"),
            Value::Int8(..) => out.push_str("TINYINT"),
            Value::Int16(..) => out.push_str("SMALLINT"),
            Value::Int32(..) => out.push_str("INTEGER"),
            Value::Int64(..) => out.push_str("BIGINT"),
            Value::Int128(..) => out.push_str("HUGEINT"),
            Value::UInt8(..) => out.push_str("UTINYINT"),
            Value::UInt16(..) => out.push_str("USMALLINT"),
            Value::UInt32(..) => out.push_str("UINTEGER"),
            Value::UInt64(..) => out.push_str("UBIGINT"),
            Value::UInt128(..) => out.push_str("UHUGEINT"),
            Value::Float32(..) => out.push_str("FLOAT"),
            Value::Float64(..) => out.push_str("DOUBLE"),
            Value::Decimal(_, precision, scale, ..) => {
                out.push_str("DECIMAL");
                if (*precision, *scale) != (0, 0) {
                    out.push('(');
                    out.push_str(itoa::Buffer::new().format(*precision));
                    out.push(',');
                    out.push_str(itoa::Buffer::new().format(*scale));
                    out.push(')');
                }
            }
            Value::Char(..) => out.push_str("CHAR(1)"),
            Value::Varchar(..) => out.push_str("TEXT"),
            Value::Blob(..) => out.push_str("BLOB"),
            Value::Date(..) => out.push_str("DATE"),
            Value::Time(..) => out.push_str("TIME"),
            Value::Timestamp(..) => out.push_str("TIMESTAMP"),
            Value::TimestampWithTimezone(..) => out.push_str("TIMESTAMPTZ"),
            Value::Interval(..) => out.push_str("INTERVAL"),
            Value::Uuid(..) => out.push_str("UUID"),
            Value::Array(_, t, ..) => {
                self.write_column_type(context, out, &t.element);
                out.push('[');
                out.push_str(itoa::Buffer::new().format(t.len));
                out.push(']');
            }
            Value::List(_, t, ..) => {
                self.write_column_type(context, out, &t.element);
                out.push_str("[]");
            }
            Value::Map(_, t, ..) => {
                out.push_str("MAP(");
                self.write_column_type(context, out, &t.key);
                out.push(',');
                self.write_column_type(context, out, &t.value);
                out.push(')');
            }
            Value::Json(..) => out.push_str("JSON"),
            _ => log::error!("Unexpected tank::Value, variant {value:?} is not supported"),
        };
    }
}
