use crate::*;
use std::{collections::BTreeMap, fmt::Write, mem};

/// Core SQL writer operations: identifiers, table/column references and type names.
///
/// This is the root of the `SqlWriter` trait hierarchy. It declares `as_dyn`,
/// the upcast to the most-derived [`SqlWriter`] bundle, which every other writer
/// trait uses to dispatch calls across trait boundaries.
pub trait SqlCoreWriter: Send {
    /// Upcasts self to the full [`SqlWriter`] view.
    ///
    /// This is declared once, in the root trait, and returns the most-derived
    /// bundle so that methods on any layer can reach methods on any other layer
    /// through a single trait object.
    fn as_dyn(&self) -> &dyn SqlWriter;

    /// Separator used for qualified names (e.g. schema.table.column)
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

    /// Writes an identifier (like a table or column name) to the query builder, optionally quoting it.
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
                let _ = write!(out, " {}", value.alias);
            } else {
                out.push_str(&value.alias);
            }
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

    /// Write overridden type.
    fn write_column_overridden_type(
        &self,
        _context: &mut Context,
        out: &mut DynQuery,
        _column: &ColumnDef,
        types: &BTreeMap<&'static str, &'static str>,
    ) {
        if let Some(t) = types
            .iter()
            .find_map(|(k, v)| if *k == "" { Some(v) } else { None })
        {
            out.push_str(t);
        }
    }

    /// Write SQL type name.
    fn write_column_type(&self, context: &mut Context, out: &mut DynQuery, value: &Value) {
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
            Value::Decimal(.., precision, scale) => {
                out.push_str("DECIMAL");
                if (precision, scale) != (&0, &0) {
                    let _ = write!(out, "({precision},{scale})");
                }
            }
            Value::Char(..) => out.push_str("CHAR(1)"),
            Value::Varchar(..) => out.push_str("VARCHAR"),
            Value::Blob(..) => out.push_str("BLOB"),
            Value::Date(..) => out.push_str("DATE"),
            Value::Time(..) => out.push_str("TIME"),
            Value::Timestamp(..) => out.push_str("TIMESTAMP"),
            Value::TimestampWithTimezone(..) => out.push_str("TIMESTAMPTZ"),
            Value::Interval(..) => out.push_str("INTERVAL"),
            Value::Uuid(..) => out.push_str("UUID"),
            Value::Array(.., inner, size) => {
                self.write_column_type(context, out, inner);
                let _ = write!(out, "[{size}]");
            }
            Value::List(.., inner) => {
                self.write_column_type(context, out, inner);
                out.push_str("[]");
            }
            Value::Map(.., key, value) => {
                out.push_str("MAP(");
                self.write_column_type(context, out, key);
                out.push(',');
                self.write_column_type(context, out, value);
                out.push(')');
            }
            Value::Json(..) => out.push_str("JSON"),
            _ => log::error!("Unexpected tank::Value, variant {value:?} is not supported"),
        };
    }
}
