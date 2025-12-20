use std::collections::BTreeMap;
use std::fmt::Write;
use tank_core::{
    ColumnDef, Context, DataSet, Entity, Error, Expression, Fragment, PrimaryKeyType, Result,
    SqlWriter, Value, future::Either, indoc::indoc, separated_by,
};
use uuid::Uuid;

#[derive(Default)]
pub struct ScyllaDBSqlWriter {}

impl SqlWriter for ScyllaDBSqlWriter {
    fn as_dyn(&self) -> &dyn SqlWriter {
        self
    }

    fn executes_multiple_statements(&self) -> bool {
        false
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
                if matches!(inner.as_ref(), Value::Char(..)) {
                    out.push_str("ASCII");
                } else {
                    out.push_str("VECTOR<");
                    self.write_column_type(context, out, inner);
                    let _ = write!(out, ",{size}>");
                }
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

    fn write_value_blob(&self, context: &mut Context, out: &mut String, value: &[u8]) {
        let delimiter = if context.fragment == Fragment::Json {
            "\""
        } else {
            ""
        };
        let _ = write!(out, "{delimiter}0x");
        for v in value {
            let _ = write!(out, "{:X}", v);
        }
        out.push_str(delimiter);
    }

    fn write_value_uuid(&self, context: &mut Context, out: &mut String, value: &Uuid) {
        if context.is_inside_json() {
            let _ = write!(out, "\"{value}\"");
        } else {
            let _ = write!(out, "{value}");
        };
    }

    fn write_value_list(
        &self,
        context: &mut Context,
        out: &mut String,
        value: Either<&Box<[Value]>, &Vec<Value>>,
        ty: &Value,
        elem_ty: &Value,
    ) {
        if matches!(ty, Value::Array(..)) && matches!(elem_ty, Value::Char(..)) {
            // Array of characters are stored as ASCII
            let value = match value {
                Either::Left(v) => v
                    .iter()
                    .map(|v| {
                        if let Value::Char(Some(v)) = v {
                            Ok(v)
                        } else {
                            return Err(Error::msg(""));
                        }
                    })
                    .collect::<Result<String>>(),
                Either::Right(v) => v
                    .iter()
                    .map(|v| {
                        if let Value::Char(Some(v)) = v {
                            Ok(v)
                        } else {
                            return Err(Error::msg(""));
                        }
                    })
                    .collect::<Result<String>>(),
            }
            .unwrap();
            self.write_value_string(context, out, &value);
            return;
        }
        out.push('[');
        separated_by(
            out,
            match value {
                Either::Left(v) => v.iter(),
                Either::Right(v) => v.iter(),
            },
            |out, v| {
                self.write_value(context, out, v);
            },
            ",",
        );
        out.push(']');
    }

    fn write_create_schema<E>(&self, out: &mut String, if_not_exists: bool)
    where
        Self: Sized,
        E: Entity,
    {
        out.reserve(32 + E::table().schema.len());
        if !out.is_empty() {
            out.push('\n');
        }
        out.push_str("CREATE KEYSPACE ");
        let mut context = Context::new(Fragment::SqlCreateSchema, E::qualified_columns());
        if if_not_exists {
            out.push_str("IF NOT EXISTS ");
        }
        self.write_identifier_quoted(&mut context, out, E::table().schema());
        out.push('\n');
        out.push_str(indoc! {r#"
            WITH replication = {
                'class': 'SimpleStrategy',
                'replication_factor': 1
            };
        "#});
    }

    fn write_drop_schema<E>(&self, out: &mut String, if_exists: bool)
    where
        Self: Sized,
        E: Entity,
    {
        out.reserve(24 + E::table().schema.len());
        if !out.is_empty() {
            out.push('\n');
        }
        out.push_str("DROP KEYSPACE ");
        let mut context = Context::new(Fragment::SqlDropSchema, E::qualified_columns());
        if if_exists {
            out.push_str("IF EXISTS ");
        }
        self.write_identifier_quoted(&mut context, out, E::table().schema());
        out.push(';');
    }

    fn write_create_table_column_fragment(
        &self,
        context: &mut Context,
        out: &mut String,
        column: &ColumnDef,
    ) where
        Self: Sized,
    {
        self.write_identifier_quoted(context, out, &column.name());
        out.push(' ');
        let len = out.len();
        self.write_column_overridden_type(context, out, column, &column.column_type);
        let didnt_write_type = out.len() == len;
        if didnt_write_type {
            SqlWriter::write_column_type(self, context, out, &column.value);
        }
        if column.primary_key == PrimaryKeyType::PrimaryKey {
            // Composite primary key will be printed elsewhere
            out.push_str(" PRIMARY KEY");
        }
    }

    fn write_column_comments_statements<E>(&self, context: &mut Context, out: &mut String)
    where
        Self: Sized,
        E: Entity,
    {
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
