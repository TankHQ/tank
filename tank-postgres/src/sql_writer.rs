use std::{collections::BTreeMap, fmt::Write};
use tank_core::{ColumnDef, Context, Dataset, DynQuery, Entity, SqlWriter, Value, separated_by};
use time::{Date, OffsetDateTime, PrimitiveDateTime, Time};

/// Postgres SQL writer.
pub struct PostgresSqlWriter {}

impl PostgresSqlWriter {
    /// Write COPY FROM STDIN BINARY.
    pub fn write_copy<'b, E>(&self, out: &mut DynQuery)
    where
        Self: Sized,
        E: Entity + 'b,
    {
        out.buffer().reserve(128);
        out.push_str("COPY ");
        let mut context = Context::new(Default::default(), E::qualified_columns());
        self.write_table_ref(&mut context, out, E::table());
        out.push_str(" (");
        separated_by(
            out,
            E::columns().iter(),
            |out, col| {
                self.write_identifier(&mut context, out, col.name(), true);
            },
            ", ",
        );
        out.push_str(") FROM STDIN BINARY;");
    }
}

impl SqlWriter for PostgresSqlWriter {
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
        if let Some(t) = types.iter().find_map(|(k, v)| {
            if *k == "postgres" || *k == "postgresql" {
                Some(v)
            } else {
                None
            }
        }) {
            out.push_str(t);
        }
    }

    fn write_column_type(&self, context: &mut Context, out: &mut DynQuery, value: &Value) {
        match value {
            Value::Boolean(..) => out.push_str("BOOLEAN"),
            Value::Int8(..) => out.push_str("SMALLINT"),
            Value::Int16(..) => out.push_str("SMALLINT"),
            Value::Int32(..) => out.push_str("INTEGER"),
            Value::Int64(..) => out.push_str("BIGINT"),
            Value::Int128(..) => out.push_str("NUMERIC(39)"),
            Value::UInt8(..) => out.push_str("SMALLINT"),
            Value::UInt16(..) => out.push_str("INTEGER"),
            Value::UInt32(..) => out.push_str("BIGINT"),
            Value::UInt64(..) => out.push_str("NUMERIC(19)"),
            Value::UInt128(..) => out.push_str("NUMERIC(39)"),
            Value::Float32(..) => out.push_str("FLOAT4"),
            Value::Float64(..) => out.push_str("FLOAT8"),
            Value::Decimal(.., precision, scale) => {
                out.push_str("NUMERIC");
                if (precision, scale) != (&0, &0) {
                    let _ = write!(out, "({},{})", precision, scale);
                }
            }
            Value::Char(..) => out.push_str("CHAR(1)"),
            Value::Varchar(..) => out.push_str("TEXT"),
            Value::Blob(..) => out.push_str("BYTEA"),
            Value::Date(..) => out.push_str("DATE"),
            Value::Time(..) => out.push_str("TIME"),
            Value::Timestamp(..) => out.push_str("TIMESTAMP"),
            Value::TimestampWithTimezone(..) => out.push_str("TIMESTAMP WITH TIME ZONE"),
            Value::Interval(..) => out.push_str("INTERVAL"),
            Value::Uuid(..) => out.push_str("UUID"),
            Value::Array(.., inner, size) => {
                self.write_column_type(context, out, inner);
                let _ = write!(out, "[{}]", size);
            }
            Value::List(.., inner) => {
                self.write_column_type(context, out, inner);
                out.push_str("[]");
            }
            Value::Map(..) | Value::Json(..) | Value::Struct(..) => out.push_str("JSON"),
            _ => log::error!("Unexpected tank::Value, Postgres does not support {value:?}"),
        };
    }

    fn write_value_blob(&self, _context: &mut Context, out: &mut DynQuery, value: &[u8]) {
        out.push_str("'\\x");
        for b in value {
            let _ = write!(out, "{:02X}", b);
        }
        out.push('\'');
    }

    fn write_value_date(
        &self,
        _context: &mut Context,
        out: &mut DynQuery,
        value: &Date,
        timestamp: bool,
    ) {
        let (l, r) = if timestamp {
            ("", "")
        } else {
            ("'", "'::DATE")
        };
        let (year, suffix) = if !timestamp && value.year() <= 0 {
            // Year 0 in Postgres is 1 BC
            (value.year().abs() + 1, " BC")
        } else {
            (value.year(), "")
        };
        let _ = write!(
            out,
            "{l}{:04}-{:02}-{:02}{suffix}{r}",
            year,
            value.month() as u8,
            value.day()
        );
    }

    fn write_value_time(
        &self,
        _context: &mut Context,
        out: &mut DynQuery,
        value: &Time,
        timestamp: bool,
    ) {
        let mut subsecond = value.nanosecond();
        let mut width = 9;
        while width > 1 && subsecond % 10 == 0 {
            subsecond /= 10;
            width -= 1;
        }
        let (l, r) = if timestamp {
            ("", "")
        } else {
            ("'", "'::TIME")
        };
        let _ = write!(
            out,
            "{l}{:02}:{:02}:{:02}.{:0width$}{r}",
            value.hour(),
            value.minute(),
            value.second(),
            subsecond
        );
    }

    fn write_value_timestamp(
        &self,
        context: &mut Context,
        out: &mut DynQuery,
        value: &PrimitiveDateTime,
    ) {
        out.push('\'');
        self.write_value_date(context, out, &value.date(), true);
        out.push('T');
        self.write_value_time(context, out, &value.time(), true);
        if value.date().year() <= 0 {
            out.push_str(" BC");
        }
        out.push_str("'::TIMESTAMP");
    }

    fn write_value_timestamptz(
        &self,
        context: &mut Context,
        out: &mut DynQuery,
        value: &OffsetDateTime,
    ) {
        out.push('\'');
        self.write_value_date(context, out, &value.date(), true);
        out.push('T');
        self.write_value_time(context, out, &value.time(), true);
        let _ = write!(
            out,
            "{:+03}:{:02}",
            value.offset().whole_hours(),
            value.offset().whole_minutes() % 60
        );
        if value.date().year() <= 0 {
            out.push_str(" BC");
        }
        out.push_str("'::TIMESTAMPTZ");
    }

    fn write_value_list(
        &self,
        context: &mut Context,
        out: &mut DynQuery,
        value: &mut dyn Iterator<Item = &Value>,
        ty: &Value,
        _elem_ty: &Value,
    ) {
        out.push_str("ARRAY[");
        separated_by(
            out,
            value,
            |out, v| {
                self.write_value(context, out, v);
            },
            ",",
        );
        out.push_str("]::");
        self.write_column_type(context, out, ty);
    }

    fn write_expression_operand_question_mark(&self, context: &mut Context, out: &mut DynQuery) {
        context.counter += 1;
        let _ = write!(out, "${}", context.counter);
    }
}
