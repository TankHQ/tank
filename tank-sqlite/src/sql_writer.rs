use std::{collections::BTreeMap, fmt::Write};
use tank_core::{ColumnDef, ColumnRef, Context, Entity, SqlWriter, TableRef, Value};

/// SQL writer for SQLite dialect.
///
/// Emits SQLite specific SQL syntax to mantain compatibility with tank operations.
pub struct SQLiteSqlWriter {}

impl SqlWriter for SQLiteSqlWriter {
    fn as_dyn(&self) -> &dyn SqlWriter {
        self
    }

    fn write_column_overridden_type(
        &self,
        _context: &mut Context,
        out: &mut String,
        _column: &ColumnDef,
        types: &BTreeMap<&'static str, &'static str>,
    ) {
        if let Some(t) = types
            .iter()
            .find_map(|(k, v)| if *k == "sqlite" { Some(v) } else { None })
        {
            out.push_str(t);
        }
    }

    fn write_column_ref(&self, context: &mut Context, out: &mut String, value: &ColumnRef) {
        if context.qualify_columns && !value.table.is_empty() {
            out.push('"');
            if !value.schema.is_empty() {
                self.write_escaped(context, out, &value.schema, '"', "\"\"");
                out.push('.');
            }
            self.write_escaped(context, out, &value.table, '"', "\"\"");
            out.push_str("\".");
        }
        self.write_identifier_quoted(context, out, &value.name);
    }

    fn write_table_ref(&self, context: &mut Context, out: &mut String, value: &TableRef) {
        if self.alias_declaration(context) || value.alias.is_empty() {
            out.push('"');
            if !value.schema.is_empty() {
                self.write_escaped(context, out, &value.schema, '"', "\"\"");
                out.push('.');
            }
            self.write_escaped(context, out, &value.name, '"', "\"\"");
            out.push('"');
        }
        if !value.alias.is_empty() {
            let _ = write!(out, " {}", value.alias);
        }
    }

    fn write_column_type(&self, _context: &mut Context, out: &mut String, value: &Value) {
        match value {
            Value::Boolean(..) => out.push_str("INTEGER"),
            Value::Int8(..) => out.push_str("INTEGER"),
            Value::Int16(..) => out.push_str("INTEGER"),
            Value::Int32(..) => out.push_str("INTEGER"),
            Value::Int64(..) => out.push_str("INTEGER"),
            Value::UInt8(..) => out.push_str("INTEGER"),
            Value::UInt16(..) => out.push_str("INTEGER"),
            Value::UInt32(..) => out.push_str("INTEGER"),
            Value::UInt64(..) => out.push_str("INTEGER"),
            Value::Float32(..) => out.push_str("REAL"),
            Value::Float64(..) => out.push_str("REAL"),
            Value::Decimal(.., precision, scale) => {
                out.push_str("REAL");
                if (precision, scale) != (&0, &0) {
                    let _ = write!(out, "({precision},{scale})");
                }
            }
            Value::Char(..) => out.push_str("TEXT"),
            Value::Varchar(..) => out.push_str("TEXT"),
            Value::Blob(..) => out.push_str("BLOB"),
            Value::Date(..) => out.push_str("TEXT"),
            Value::Time(..) => out.push_str("TEXT"),
            Value::Timestamp(..) => out.push_str("TEXT"),
            Value::TimestampWithTimezone(..) => out.push_str("TEXT"),
            Value::Uuid(..) => out.push_str("TEXT"),
            _ => log::error!(
                "Unexpected tank::Value, SQLite does not support {:?}",
                value
            ),
        };
    }

    fn write_value_infinity(&self, _context: &mut Context, out: &mut String, negative: bool) {
        if negative {
            out.push('-');
        }
        out.push_str("1.0e+10000");
    }

    fn write_value_nan(&self, context: &mut Context, out: &mut String) {
        log::warn!("SQLite does not support float NaN values, will write NULL instead");
        self.write_value_none(context, out);
    }

    fn write_value_blob(&self, _context: &mut Context, out: &mut String, value: &[u8]) {
        out.push_str("X'");
        for b in value {
            let _ = write!(out, "{:02X}", b);
        }
        out.push('\'');
    }

    fn write_create_schema<E>(&self, _buff: &mut String, _if_not_exists: bool)
    where
        Self: Sized,
        E: Entity,
    {
        // SQLite does not support schema
    }

    fn write_drop_schema<E>(&self, _buff: &mut String, _if_exists: bool)
    where
        Self: Sized,
        E: Entity,
    {
        // SQLite does not support schema
    }

    fn write_column_comments_statements<E>(&self, _context: &mut Context, _buff: &mut String)
    where
        Self: Sized,
        E: Entity,
    {
    }
}
