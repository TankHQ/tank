use std::{collections::BTreeMap, fmt::Write};
use tank_core::{
    ColumnDef, Context, Dataset, DynQuery, Entity, Expression, Fragment, SqlWriter, Value,
    separated_by,
};
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

    fn write_blob(&self, _context: &mut Context, out: &mut DynQuery, value: &[u8]) {
        out.push_str("'\\x");
        for b in value {
            let _ = write!(out, "{:02X}", b);
        }
        out.push('\'');
    }

    fn write_date(&self, context: &mut Context, out: &mut DynQuery, value: &Date) {
        let (l, r) = match context.fragment {
            Fragment::None | Fragment::ParameterBinding | Fragment::Timestamp => ("", ""),
            Fragment::Json | Fragment::JsonKey => ("\"", "\""),
            _ => ("'", "'::DATE"),
        };
        let (year, suffix) = if value.year() <= 0 {
            // Year 0 in Postgres is 1 BC
            let suffix = if context.fragment == Fragment::Timestamp {
                ""
            } else {
                " BC"
            };
            (value.year().abs() + 1, suffix)
        } else {
            (value.year(), "")
        };
        let month = value.month() as u8;
        let day = value.day();
        let _ = write!(out, "{l}{year:04}-{month:02}-{day:02}{suffix}{r}");
    }

    fn write_time(&self, context: &mut Context, out: &mut DynQuery, value: &Time) {
        let (l, r) = match context.fragment {
            Fragment::None | Fragment::ParameterBinding | Fragment::Timestamp => ("", ""),
            Fragment::Json | Fragment::JsonKey => ("\"", "\""),
            _ => ("'", "'::TIME"),
        };
        let (h, m, s, ns) = value.as_hms_nano();
        let mut subsecond = ns;
        let mut width = 9;
        while width > 1 && subsecond % 10 == 0 {
            subsecond /= 10;
            width -= 1;
        }
        let _ = write!(out, "{l}{h:02}:{m:02}:{s:02}.{subsecond:0width$}{r}",);
    }

    fn write_timestamp(
        &self,
        context: &mut Context,
        out: &mut DynQuery,
        value: &PrimitiveDateTime,
    ) {
        let is_timestamp = context.fragment == Fragment::Timestamp;
        let (l, r) = match context.fragment {
            Fragment::None | Fragment::ParameterBinding | Fragment::Timestamp => ("", ""),
            Fragment::Json | Fragment::JsonKey => ("\"", "\""),
            _ => ("'", "'::TIMESTAMP"),
        };
        let mut context = context.switch_fragment(Fragment::Timestamp);
        out.push_str(l);
        self.write_date(&mut context.current, out, &value.date());
        out.push('T');
        self.write_time(&mut context.current, out, &value.time());
        if !is_timestamp && value.date().year() <= 0 {
            out.push_str(" BC");
        }
        out.push_str(r);
    }

    fn write_timestamptz(&self, context: &mut Context, out: &mut DynQuery, value: &OffsetDateTime) {
        let (l, r) = match context.fragment {
            Fragment::None | Fragment::ParameterBinding | Fragment::Timestamp => ("", ""),
            Fragment::Json | Fragment::JsonKey => ("\"", "\""),
            _ => ("'", "'::TIMESTAMPTZ"),
        };
        let mut context = context.switch_fragment(Fragment::Timestamp);
        out.push_str(l);
        self.write_timestamp(
            &mut context.current,
            out,
            &PrimitiveDateTime::new(value.date(), value.time()),
        );
        let total_minutes = value.offset().whole_minutes();
        let sign = if total_minutes >= 0 { '+' } else { '-' };
        let _ = write!(
            out,
            "{}{:02}:{:02}",
            sign,
            (total_minutes.abs() / 60) as u8,
            (total_minutes.abs() % 60) as u8
        );
        if value.date().year() <= 0 {
            out.push_str(" BC");
        }
        out.push_str(r);
    }

    fn write_list(
        &self,
        context: &mut Context,
        out: &mut DynQuery,
        value: &mut dyn Iterator<Item = &dyn Expression>,
        ty: Option<&Value>,
        _is_array: bool,
    ) {
        out.push_str("ARRAY[");
        separated_by(
            out,
            value,
            |out, v| {
                v.write_query(self, context, out);
            },
            ",",
        );
        out.push(']');
        if let Some(ty) = ty {
            out.push_str("::");
            self.write_column_type(context, out, ty);
            out.push_str("[]");
        }
    }

    fn write_question_mark(&self, context: &mut Context, out: &mut DynQuery) {
        context.counter += 1;
        let _ = write!(out, "${}", context.counter);
    }

    fn write_current_timestamp_ms(&self, _context: &mut Context, out: &mut DynQuery) {
        out.push_str("CAST(EXTRACT(EPOCH FROM NOW()) * 1000 AS BIGINT)");
    }
}
