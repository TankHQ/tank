use std::{
    collections::{BTreeMap, HashMap},
    fmt::Write,
    mem,
};
use tank_core::{
    AnyQuery, ColumnDef, ColumnRef, Context, Expression, GenericSqlWriter, SqlCoreWriter,
    SqlExpressionWriter, SqlFragmentWriter, SqlValueWriter, SqlWriter, TableRef, Value,
    write_escaped,
};

/// SQL writer for SQLite dialect.
///
/// Emits SQLite specific SQL syntax to mantain compatibility with tank operations.
pub struct SQLiteSqlWriter {}

impl SqlCoreWriter for SQLiteSqlWriter {
    fn as_dyn(&self) -> &dyn SqlWriter {
        self
    }

    fn write_column_overridden_type(
        &self,
        _context: &mut Context,
        out: &mut AnyQuery,
        _column: &ColumnDef,
        types: &BTreeMap<&'static str, &'static str>,
    ) {
        if let Some(t) = types.get("sqlite") {
            out.push_str(t);
        }
    }

    fn write_column_ref(&self, context: &mut Context, out: &mut AnyQuery, value: &ColumnRef) {
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
                out.push('"');
                if !schema.is_empty() {
                    write_escaped(out, schema, '"', "\"\"");
                    out.push_str(self.separator());
                }
                write_escaped(out, table, '"', "\"\"");
                out.push('"');
                out.push_str(self.separator());
            }
            context.table_ref = table_ref;
        }
        self.write_identifier(context, out, &value.name, true);
    }

    fn write_table_ref(&self, context: &mut Context, out: &mut AnyQuery, value: &TableRef) {
        if self.is_alias_declaration(context) || value.alias.is_empty() {
            out.push('"');
            if !value.schema.is_empty() {
                write_escaped(out, &value.schema, '"', "\"\"");
                out.push_str(self.separator());
            }
            write_escaped(out, &value.name, '"', "\"\"");
            out.push('"');
        }
        if !value.alias.is_empty() {
            let _ = write!(out, " {}", value.alias);
        }
    }

    fn write_column_type(&self, _context: &mut Context, out: &mut AnyQuery, value: &Value<'_>) {
        match value {
            Value::Boolean(..)
            | Value::Int8(..)
            | Value::Int16(..)
            | Value::Int32(..)
            | Value::Int64(..)
            | Value::UInt8(..)
            | Value::UInt16(..)
            | Value::UInt32(..)
            | Value::UInt64(..) => out.push_str("INTEGER"),
            Value::Float32(..) | Value::Float64(..) | Value::Decimal(..) => out.push_str("REAL"),
            Value::Char(..)
            | Value::Varchar(..)
            | Value::Date(..)
            | Value::Time(..)
            | Value::Timestamp(..)
            | Value::TimestampWithTimezone(..)
            | Value::Uuid(..)
            | Value::IpAddr(..)
            | Value::Json(..) => out.push_str("TEXT"),
            Value::Blob(..) => out.push_str("BLOB"),
            _ => log::error!("Unexpected tank::Value, SQLite does not support {value:?}"),
        };
    }
}

impl SqlValueWriter for SQLiteSqlWriter {
    fn write_value_f32(&self, context: &mut Context, out: &mut AnyQuery, value: f32) {
        if value.is_infinite() {
            if value.is_sign_negative() {
                out.push('-');
            }
            out.push_str("1.0e+10000");
            return;
        }
        if value.is_nan() {
            log::warn!("SQLite does not support float NaN values, will write NULL instead");
            self.write_null(context, out);
            return;
        }
        GenericSqlWriter::new().write_value_f32(context, out, value);
    }

    fn write_value_f64(&self, context: &mut Context, out: &mut AnyQuery, value: f64) {
        if value.is_infinite() {
            if value.is_sign_negative() {
                out.push('-');
            }
            out.push_str("1.0e+10000");
            return;
        }
        if value.is_nan() {
            log::warn!("SQLite does not support float NaN values, will write NULL instead");
            self.write_null(context, out);
            return;
        }
        GenericSqlWriter::new().write_value_f64(context, out, value);
    }

    fn write_blob(&self, _context: &mut Context, out: &mut AnyQuery, value: &[u8]) {
        const HEX: &[u8; 16] = b"0123456789ABCDEF";
        out.buffer().reserve(3 * value.len() + 3);
        out.push_str("X'");
        for &b in value {
            out.push(HEX[(b >> 4) as usize] as char);
            out.push(HEX[(b & 0xF) as usize] as char);
        }
        out.push('\'');
    }

    fn write_list(
        &self,
        _context: &mut Context,
        _out: &mut AnyQuery,
        _value: &mut dyn Iterator<Item = &dyn Expression>,
        _t: Option<&Value<'_>>,
        _is_array: bool,
    ) {
        log::error!("SQLite does not support arrays or lists");
    }

    fn write_map(
        &self,
        _context: &mut Context,
        _out: &mut AnyQuery,
        _value: &HashMap<Value<'_>, Value<'_>>,
    ) {
        log::error!("SQLite does not support maps");
    }
}

impl SqlExpressionWriter for SQLiteSqlWriter {
    fn write_current_timestamp_ms(&self, _context: &mut Context, out: &mut AnyQuery) {
        out.push_str("(unixepoch('subsec') * 1000)");
    }
}

impl SqlFragmentWriter for SQLiteSqlWriter {
    fn write_limit_offset(
        &self,
        context: &mut Context,
        out: &mut AnyQuery,
        limit: Option<u32>,
        offset: Option<u32>,
    ) {
        // SQLite only accepts OFFSET after a LIMIT; -1 means unlimited.
        if limit.is_none() && offset.is_some() {
            out.push_str("\nLIMIT -1");
        }
        GenericSqlWriter.write_limit_offset(context, out, limit, offset);
    }
}

impl SqlWriter for SQLiteSqlWriter {
    fn write_create_schema(&self, _out: &mut AnyQuery, _table: &TableRef, _if_not_exists: bool)
    where
        Self: Sized,
    {
        // SQLite does not support schema
    }

    fn write_drop_schema(&self, _out: &mut AnyQuery, _table: &TableRef, _if_exists: bool)
    where
        Self: Sized,
    {
        // SQLite does not support schema
    }

    fn write_column_comment(
        &self,
        _context: &mut Context,
        _out: &mut AnyQuery,
        _column: &ColumnDef,
    ) {
    }
}
