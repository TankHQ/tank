use std::collections::BTreeMap;
use std::fmt::Write;
use tank_core::{ColumnDef, Context, DataSet, Entity, Expression, Fragment, SqlWriter, Value};

#[derive(Default)]
pub struct ScyllaDBSqlWriter {}

impl SqlWriter for ScyllaDBSqlWriter {
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
            .find_map(|(k, v)| if *k == "scylladb" { Some(v) } else { None })
        {
            out.push_str(t);
        }
    }

    fn write_column_type(&self, context: &mut Context, out: &mut String, value: &Value) {
        match value {
            Value::Boolean(..) => out.push_str("BOOLEAN"),
            Value::Int8(..) => out.push_str("TINYINT"),
            Value::Int16(..) => out.push_str("SMALLINT"),
            Value::Int32(..) => out.push_str("INT"),
            Value::Int64(..) => out.push_str("BIGINT"),
            Value::Int128(..) => out.push_str("VARINT"),
            Value::UInt8(..) => out.push_str("SMALLINT"),
            Value::UInt16(..) => out.push_str("INT"),
            Value::UInt32(..) => out.push_str("BIGINT"),
            Value::UInt64(..) => out.push_str("VARINT"),
            Value::UInt128(..) => out.push_str("VARINT"),
            Value::Float32(..) => out.push_str("FLOAT"),
            Value::Float64(..) => out.push_str("DOUBLE"),
            Value::Decimal(..) => out.push_str("DECIMAL"),
            Value::Char(..) => out.push_str("ASCII"),
            Value::Varchar(..) => out.push_str("TEXT"),
            Value::Blob(..) => out.push_str("BLOB"),
            Value::Date(..) => out.push_str("DATE"),
            Value::Time(..) => out.push_str("TIME"),
            Value::Timestamp(..) => out.push_str("TIMESTAMP"),
            Value::TimestampWithTimezone(..) => out.push_str("TIMESTAMP"),
            Value::Interval(..) => out.push_str("DURATION"),
            Value::Uuid(..) => out.push_str("UUID"),
            Value::Array(.., inner, size) => {
                out.push_str("VECTOR<");
                self.write_column_type(context, out, inner);
                let _ = write!(out, ",{size}>");
            }
            Value::List(.., inner) => {
                out.push_str("LIST<");
                self.write_column_type(context, out, inner);
                out.push('>');
            }
            Value::Map(.., key, value) => {
                out.push_str("MAP<");
                self.write_column_type(context, out, key);
                out.push(',');
                self.write_column_type(context, out, value);
                out.push('>');
            }
            Value::Json(..) => out.push_str("TEXT"),
            _ => log::error!(
                "Unexpected tank::Value, variant {:?} is not supported",
                value
            ),
        };
    }

    fn write_insert_update_fragment<'a, E>(
        &self,
        _context: &mut Context,
        _out: &mut String,
        _columns: impl Iterator<Item = &'a ColumnDef>,
    ) where
        Self: Sized,
        E: Entity,
    {
        // CQL does not need separate update logic, a INSERT is already a UPSERT
    }

    fn write_delete<E>(&self, out: &mut String, condition: &impl Expression)
    where
        Self: Sized,
        E: Entity,
    {
        let is_true = condition.is_true();
        if is_true {
            out.push_str("TRUNCATE ");
        } else {
            out.reserve(128 + E::table().schema().len() + E::table().name().len());
            if !out.is_empty() {
                out.push('\n');
            }
            out.push_str("DELETE FROM ");
        }
        let mut context = Context::new(Fragment::SqlDeleteFrom, E::qualified_columns());
        self.write_table_ref(&mut context, out, E::table());
        if !is_true {
            out.push_str("\nWHERE ");
            condition.write_query(
                self,
                &mut context
                    .switch_fragment(Fragment::SqlDeleteFromWhere)
                    .current,
                out,
            );
        }
        out.push(';');
    }
}
