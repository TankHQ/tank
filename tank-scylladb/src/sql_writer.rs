use std::collections::BTreeMap;
use std::fmt::Write;
use tank_core::{
    ColumnDef, Context, DataSet, Entity, Error, Expression, Fragment, Interval, PrimaryKeyType,
    Result, SqlWriter, Value, future::Either, indoc::indoc, print_timer, separated_by,
};
use time::Time;
use uuid::Uuid;

/// SQL writer for ScyllaDB / Cassandra dialect.
///
/// Emits ScyllaDB / Cassandra specific SQL syntax to mantain compatibility with tank operations.
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

    fn write_value_infinity(&self, _context: &mut Context, out: &mut String, negative: bool) {
        if negative {
            out.push('-');
        }
        out.push_str("Infinity");
    }
    fn write_value_time(
        &self,
        context: &mut Context,
        out: &mut String,
        value: &Time,
        timestamp: bool,
    ) {
        let nanos = value.nanosecond();
        print_timer(
            out,
            match context.fragment {
                Fragment::Json if !timestamp => "\"",
                _ if !timestamp => "'",
                _ => "",
            },
            value.hour() as _,
            value.minute(),
            value.second(),
            nanos - nanos % 1_000_000,
        );
    }

    fn write_value_blob(&self, context: &mut Context, out: &mut String, value: &[u8]) {
        let delimiter = if context.fragment == Fragment::Json {
            "\""
        } else {
            ""
        };
        let _ = write!(out, "{delimiter}0x");
        for v in value {
            let _ = write!(out, "{:02X}", v);
        }
        out.push_str(delimiter);
    }

    fn value_interval_units(&self) -> &[(&str, i128)] {
        static UNITS: &[(&str, i128)] = &[
            ("d", Interval::NANOS_IN_DAY),
            ("h", Interval::NANOS_IN_SEC * 3600),
            ("m", Interval::NANOS_IN_SEC * 60),
            ("s", Interval::NANOS_IN_SEC),
            ("us", 1_000),
            ("ns", 1),
        ];
        UNITS
    }

    fn write_value_interval(&self, _context: &mut Context, out: &mut String, value: &Interval) {
        if value.is_zero() {
            out.push_str("0s");
        }
        let mut months = value.months;
        let mut nanos = value.nanos + value.days as i128 * Interval::NANOS_IN_DAY;
        if months != 0 {
            if months > 48 || months % 12 == 0 {
                let _ = write!(out, "{}y", months / 12);
                months = months % 12;
            }
            if months != 0 {
                let _ = write!(out, "{months}mo");
            }
        }
        for &(name, factor) in self.value_interval_units() {
            let rem = nanos % factor;
            if rem == 0 || factor / rem > 1_000_000 {
                let value = nanos / factor;
                if value != 0 {
                    let _ = write!(out, "{value}{name}");
                    nanos = rem;
                    if nanos == 0 {
                        break;
                    }
                }
            }
        }
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

    fn write_create_table_primary_key_fragment<'a, It>(
        &self,
        context: &mut Context,
        out: &mut String,
        primary_key: It,
    ) where
        Self: Sized,
        It: IntoIterator<Item = &'a ColumnDef>,
        It::IntoIter: Clone,
    {
        let primary_key = primary_key.into_iter();
        let mut parentheses_closed = false;
        out.push_str(",\nPRIMARY KEY (");
        let has_clustering = primary_key
            .clone()
            .find(|v: &&'a ColumnDef| v.clustering_key)
            .is_some();
        if has_clustering {
            out.push('(');
        }
        let mut primary_key = primary_key.into_iter().peekable();
        while let Some(col) = primary_key.next() {
            self.write_identifier_quoted(
                &mut context
                    .switch_fragment(Fragment::SqlCreateTablePrimaryKey)
                    .current,
                out,
                col.name(),
            );
            if let Some(next) = primary_key.peek() {
                if next.clustering_key && !parentheses_closed {
                    out.push(')');
                    parentheses_closed = true;
                }
                out.push(',');
            }
        }
        out.push(')');
    }

    fn write_column_comments_statements<E>(&self, _context: &mut Context, _out: &mut String)
    where
        Self: Sized,
        E: Entity,
    {
    }

    fn write_insert<'b, E>(
        &self,
        out: &mut String,
        entities: impl IntoIterator<Item = &'b E>,
        _update: bool,
    ) where
        Self: Sized,
        E: Entity + 'b,
    {
        let mut it = entities.into_iter().map(Entity::row_filtered).peekable();
        let mut row = it.next();
        let multiple = row.is_some() && it.peek().is_some();
        if multiple {
            out.push_str("BEGIN BATCH\n");
        }
        while let Some(current) = row {
            if !out.is_empty() {
                out.push('\n');
            }
            out.push_str("INSERT INTO ");
            let mut context = Context::new(Fragment::SqlInsertInto, E::qualified_columns());
            self.write_table_ref(&mut context, out, E::table());
            out.push_str(" (");
            separated_by(
                out,
                current.iter(),
                |out, (name, ..)| {
                    self.write_identifier_quoted(&mut context, out, name);
                },
                ", ",
            );
            out.push_str(")\nVALUES (");
            let mut context = context.switch_fragment(Fragment::SqlInsertIntoValues);
            separated_by(
                out,
                current.iter(),
                |out, (_, value)| {
                    self.write_value(&mut context.current, out, value);
                },
                ", ",
            );
            out.push_str(");");
            row = it.next();
        }
        if multiple {
            out.push_str("\nAPPLY BATCH;");
        }
    }

    fn write_delete<E>(&self, out: &mut String, condition: impl Expression)
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
