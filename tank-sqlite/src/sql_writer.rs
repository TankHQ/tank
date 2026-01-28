use std::{collections::BTreeMap, fmt::Write};
use tank_core::{
    ColumnDef, ColumnRef, Context, DynQuery, Entity, GenericSqlWriter, SqlWriter, TableRef, Value,
    write_escaped,
};

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
        out: &mut DynQuery,
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

    fn write_column_ref(&self, context: &mut Context, out: &mut DynQuery, value: &ColumnRef) {
        if context.qualify_columns && !value.table.is_empty() {
            out.push('"');
            if !value.schema.is_empty() {
                write_escaped(out, &value.schema, '"', "\"\"");
                out.push('.');
            }
            write_escaped(out, &value.table, '"', "\"\"");
            out.push_str("\".");
        }
        self.write_identifier(context, out, &value.name, true);
    }

    fn write_table_ref(&self, context: &mut Context, out: &mut DynQuery, value: &TableRef) {
        if self.alias_declaration(context) || value.alias.is_empty() {
            out.push('"');
            if !value.schema.is_empty() {
                write_escaped(out, &value.schema, '"', "\"\"");
                out.push('.');
            }
            write_escaped(out, &value.name, '"', "\"\"");
            out.push('"');
        }
        if !value.alias.is_empty() {
            let _ = write!(out, " {}", value.alias);
        }
    }

    fn write_column_type(&self, _context: &mut Context, out: &mut DynQuery, value: &Value) {
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
            _ => log::error!("Unexpected tank::Value, SQLite does not support {value:?}"),
        };
    }

    fn write_value_f32(&self, context: &mut Context, out: &mut DynQuery, value: f32) {
        if value.is_infinite() {
            if value.is_sign_negative() {
                out.push('-');
            }
            out.push_str("1.0e+10000");
            return;
        }
        if value.is_nan() {
            log::warn!("SQLite does not support float NaN values, will write NULL instead");
            self.write_value_none(context, out);
            return;
        }
        GenericSqlWriter::new().write_value_f32(context, out, value);
    }

    fn write_value_f64(&self, context: &mut Context, out: &mut DynQuery, value: f64) {
        if value.is_infinite() {
            if value.is_sign_negative() {
                out.push('-');
            }
            out.push_str("1.0e+10000");
            return;
        }
        if value.is_nan() {
            log::warn!("SQLite does not support float NaN values, will write NULL instead");
            self.write_value_none(context, out);
            return;
        }
        GenericSqlWriter::new().write_value_f64(context, out, value);
    }

    fn write_value_blob(&self, _context: &mut Context, out: &mut DynQuery, value: &[u8]) {
        out.push_str("X'");
        for b in value {
            let _ = write!(out, "{:02X}", b);
        }
        out.push('\'');
    }

    fn write_create_schema<E>(&self, _out: &mut DynQuery, _if_not_exists: bool)
    where
        Self: Sized,
        E: Entity,
    {
        // SQLite does not support schema
    }

    fn write_drop_schema<E>(&self, _out: &mut DynQuery, _if_exists: bool)
    where
        Self: Sized,
        E: Entity,
    {
        // SQLite does not support schema
    }

    fn write_column_comments_statements<E>(&self, _context: &mut Context, _out: &mut DynQuery)
    where
        Self: Sized,
        E: Entity,
    {
    }
}
