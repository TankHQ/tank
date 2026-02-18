use std::{
    collections::{BTreeMap, HashMap},
    fmt::Write,
};
use tank_core::{
    ColumnDef, Context, DynQuery, EitherIterator, Entity, Fragment, GenericSqlWriter, Interval,
    PrimaryKeyType, SqlWriter, Value, print_timer, separated_by, write_escaped,
};

/// SQL writer for MySQL / MariaDB dialect.
///
/// Emits MySQL / MariaDB specific SQL syntax to mantain compatibility with tank operations.
#[derive(Default)]
pub struct MySQLSqlWriter {}

pub type MariaDBWriter = MySQLSqlWriter;

impl MySQLSqlWriter {
    const DEFAULT_PK_VARCHAR_TYPE: &'static str = "VARCHAR(60)";
}

impl SqlWriter for MySQLSqlWriter {
    fn as_dyn(&self) -> &dyn SqlWriter {
        self
    }

    fn write_identifier(
        &self,
        _context: &mut Context,
        out: &mut DynQuery,
        value: &str,
        quoted: bool,
    ) {
        if quoted {
            out.push('`');
            write_escaped(out, value, '"', "``");
            out.push('`');
        } else {
            out.push_str(value);
        }
    }

    fn write_column_overridden_type(
        &self,
        _context: &mut Context,
        out: &mut DynQuery,
        column: &ColumnDef,
        types: &BTreeMap<&'static str, &'static str>,
    ) {
        if let Some(t) = types
            .iter()
            .find_map(|(k, v)| {
                if *k == "mysql" || *k == "mariadb" {
                    Some(*v)
                } else {
                    None
                }
            })
            .or_else(|| {
                if matches!(column.value, Value::Varchar(..))
                    && column.primary_key != PrimaryKeyType::None
                {
                    Some(Self::DEFAULT_PK_VARCHAR_TYPE)
                } else {
                    None
                }
            })
        {
            out.push_str(t);
        }
    }

    fn write_column_type(&self, context: &mut Context, out: &mut DynQuery, value: &Value) {
        if context.fragment == Fragment::Casting {
            match value {
                Value::Int8(..)
                | Value::Int16(..)
                | Value::Int32(..)
                | Value::Int64(..)
                | Value::Int128(..) => {
                    out.push_str("SIGNED");
                    return;
                }
                Value::UInt8(..)
                | Value::UInt16(..)
                | Value::UInt32(..)
                | Value::UInt64(..)
                | Value::UInt128(..) => {
                    out.push_str("UNSIGNED");
                    return;
                }
                _ => {}
            }
        }
        match value {
            Value::Boolean(..) => out.push_str("BOOLEAN"),
            Value::Int8(..) => out.push_str("TINYINT"),
            Value::Int16(..) => out.push_str("SMALLINT"),
            Value::Int32(..) => out.push_str("INTEGER"),
            Value::Int64(..) => out.push_str("BIGINT"),
            Value::Int128(..) => out.push_str("NUMERIC(39)"),
            Value::UInt8(..) => out.push_str("TINYINT UNSIGNED"),
            Value::UInt16(..) => out.push_str("SMALLINT UNSIGNED"),
            Value::UInt32(..) => out.push_str("INTEGER UNSIGNED"),
            Value::UInt64(..) => out.push_str("BIGINT UNSIGNED"),
            Value::UInt128(..) => out.push_str("NUMERIC(39) UNSIGNED"),
            Value::Float32(..) => out.push_str("FLOAT"),
            Value::Float64(..) => out.push_str("DOUBLE"),
            Value::Decimal(.., precision, scale) => {
                out.push_str("DECIMAL");
                if (precision, scale) != (&0, &0) {
                    let _ = write!(out, "({},{})", precision, scale);
                }
            }
            Value::Char(..) => out.push_str("CHAR(1)"),
            Value::Varchar(..) => out.push_str("TEXT"),
            Value::Blob(..) => out.push_str("BLOB"),
            Value::Date(..) => out.push_str("DATE"),
            Value::Time(..) => out.push_str("TIME(6)"),
            Value::Timestamp(..) => out.push_str("DATETIME"),
            Value::TimestampWithTimezone(..) => out.push_str("DATETIME"),
            Value::Interval(..) => out.push_str("TIME(6)"),
            Value::Uuid(..) => out.push_str("CHAR(36)"),
            Value::Array(..) => out.push_str("JSON"),
            Value::List(..) => out.push_str("JSON"),
            Value::Map(..) => out.push_str("JSON"),
            Value::Json(..) => out.push_str("JSON"),
            _ => log::error!("Unexpected tank::Value, MySQL does not support {value:?}"),
        };
    }

    fn write_value_f32(&self, context: &mut Context, out: &mut DynQuery, value: f32) {
        if value.is_infinite() || value.is_nan() {
            if value.is_infinite() {
                log::error!(
                    "MySQL does not support float infinite values, will write NULL instead"
                );
            } else {
                log::warn!("MySQL does not support float NaN values, will write NULL instead");
            }
            self.write_value_none(context, out);
            return;
        }
        GenericSqlWriter::new().write_value_f32(context, out, value);
    }

    fn write_value_f64(&self, context: &mut Context, out: &mut DynQuery, value: f64) {
        if value.is_infinite() || value.is_nan() {
            if value.is_infinite() {
                log::error!(
                    "MySQL does not support float infinite values, will write NULL instead"
                );
            } else {
                log::warn!("MySQL does not support float NaN values, will write NULL instead");
            }
            self.write_value_none(context, out);
            return;
        }
        GenericSqlWriter::new().write_value_f64(context, out, value);
    }

    fn write_value_interval(&self, context: &mut Context, out: &mut DynQuery, value: &Interval) {
        let delimiter = if context.is_inside_json() { "\"" } else { "\'" };
        let (h, m, s, ns) = value.as_hmsns();
        print_timer(out, delimiter, h as _, m, s, ns);
    }

    fn write_value_list(
        &self,
        context: &mut Context,
        out: &mut DynQuery,
        value: &mut dyn Iterator<Item = &Value>,
        _ty: &Value,
        _elem_ty: &Value,
    ) {
        let is_json = context.is_inside_json();
        let mut context = context.switch_fragment(Fragment::Json);
        if !is_json {
            out.push('\'');
        }
        out.push('[');
        separated_by(
            out,
            value,
            |out, v| {
                self.write_value(&mut context.current, out, v);
            },
            ",",
        );
        out.push(']');
        if !is_json {
            out.push('\'');
        }
    }
    fn write_value_map(
        &self,
        context: &mut Context,
        out: &mut DynQuery,
        value: &HashMap<Value, Value>,
    ) {
        let inside_string = context.fragment == Fragment::Json;
        let mut context = context.switch_fragment(Fragment::Json);
        if !inside_string {
            out.push('\'');
        }
        out.push('{');
        separated_by(
            out,
            value,
            |out, (k, v)| {
                {
                    let mut context = context.current.switch_fragment(Fragment::JsonKey);
                    self.write_value(&mut context.current, out, k);
                }
                out.push(':');
                self.write_value(&mut context.current, out, v);
            },
            ",",
        );
        out.push('}');
        if !inside_string {
            out.push('\'');
        }
    }

    fn write_expression_operand_current_timestamp_ms(
        &self,
        _context: &mut Context,
        out: &mut DynQuery,
    ) {
        out.push_str("CAST(UNIX_TIMESTAMP(NOW(3)) * 1000 AS UNSIGNED)");
    }

    fn write_column_comment_inline(
        &self,
        mut context: &mut Context,
        out: &mut DynQuery,
        column: &ColumnDef,
    ) where
        Self: Sized,
    {
        out.push_str(" COMMENT ");
        self.write_value_string(&mut context, out, column.comment);
    }

    fn write_column_comments_statements<E>(&self, _context: &mut Context, _out: &mut DynQuery)
    where
        Self: Sized,
        E: Entity,
    {
    }

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
        if pk.len() == 0 {
            return;
        }
        out.push_str("\nON DUPLICATE KEY UPDATE");
        let mut update_cols = columns
            .clone()
            .filter(|c| c.primary_key == PrimaryKeyType::None)
            .peekable();
        let update_cols = if update_cols.peek().is_some() {
            EitherIterator::Left(update_cols)
        } else {
            EitherIterator::Right(columns.filter(|c| c.primary_key != PrimaryKeyType::None))
        };
        separated_by(
            out,
            update_cols,
            |out, v| {
                self.write_identifier(context, out, v.name(), true);
                out.push_str(" = VALUES(");
                self.write_identifier(context, out, v.name(), true);
                out.push(')');
            },
            ",\n",
        );
    }
}
