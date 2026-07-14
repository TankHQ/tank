use std::{
    collections::{BTreeMap, HashMap},
    fmt::Write,
};
use tank_core::{
    BinaryOpType, ColumnDef, Context, Dataset, DynQuery, Entity, Fragment, Interval,
    PrimaryKeyType, SqlWriter, TableRef, Value, separated_by, write_escaped,
};
use time::{OffsetDateTime, PrimitiveDateTime};

/// ClickHouse SQL writer.
#[derive(Default, Clone, Copy, Debug)]
pub struct ClickHouseSqlWriter {
    replacing_merge_tree: bool,
}

impl ClickHouseSqlWriter {
    pub const fn new() -> Self {
        Self {
            replacing_merge_tree: true,
        }
    }

    pub const fn chdb() -> Self {
        Self {
            replacing_merge_tree: false,
        }
    }
}

fn write_datetime_literal(
    out: &mut DynQuery,
    quote: &str,
    year: i32,
    month: u8,
    day: u8,
    hour: u8,
    minute: u8,
    second: u8,
    nanos: u32,
) {
    out.push_str(quote);
    let _ = write!(
        out,
        "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
        year, month, day, hour, minute, second,
    );
    if nanos != 0 {
        let mut frac = format!("{nanos:09}");
        while frac.ends_with('0') {
            frac.pop();
        }
        out.push('.');
        out.push_str(&frac);
    }
    out.push_str(quote);
}

impl SqlWriter for ClickHouseSqlWriter {
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
            write_escaped(out, value, '`', "``");
            out.push('`');
        } else {
            out.push_str(value);
        }
    }

    fn write_null(&self, context: &mut Context, out: &mut DynQuery) {
        match context.fragment {
            Fragment::Json | Fragment::JsonKey => out.push_str("null"),
            Fragment::SqlSelect => out.push_str("CAST(NULL AS Nullable(String))"),
            _ => out.push_str("NULL"),
        }
    }

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

    fn write_column_overridden_type(
        &self,
        _context: &mut Context,
        out: &mut DynQuery,
        _column: &ColumnDef,
        types: &BTreeMap<&'static str, &'static str>,
    ) {
        if let Some(t) = types.get("clickhouse").copied() {
            out.push_str(t);
        }
    }

    fn write_column_type(&self, context: &mut Context, out: &mut DynQuery, value: &Value) {
        match value {
            Value::Boolean(..) => out.push_str("Bool"),
            Value::Int8(..) => out.push_str("Int8"),
            Value::Int16(..) => out.push_str("Int16"),
            Value::Int32(..) => out.push_str("Int32"),
            Value::Int64(..) => out.push_str("Int64"),
            Value::Int128(..) => out.push_str("Int128"),
            Value::UInt8(..) => out.push_str("UInt8"),
            Value::UInt16(..) => out.push_str("UInt16"),
            Value::UInt32(..) => out.push_str("UInt32"),
            Value::UInt64(..) => out.push_str("UInt64"),
            Value::UInt128(..) => out.push_str("UInt128"),
            Value::Float32(..) => out.push_str("Float32"),
            Value::Float64(..) => out.push_str("Float64"),
            Value::Decimal(.., precision, scale) => {
                if (*precision, *scale) == (0, 0) {
                    out.push_str("Decimal(18, 6)");
                } else {
                    let _ = write!(out, "Decimal({precision}, {scale})");
                }
            }
            Value::Char(..) | Value::Varchar(..) => out.push_str("String"),
            Value::Blob(..) => out.push_str("String"),
            Value::Date(..) => out.push_str("Date"),
            Value::Time(..) => out.push_str("String"),
            Value::Interval(..) => out.push_str("String"),
            Value::Timestamp(..) => out.push_str("DateTime64(9, 'UTC')"),
            Value::TimestampWithTimezone(..) => out.push_str("DateTime64(9, 'UTC')"),
            Value::Uuid(..) => out.push_str("UUID"),
            Value::Array(_, inner, _) => {
                out.push_str("Array(");
                self.write_column_type(context, out, inner);
                out.push(')');
            }
            Value::List(_, inner) => {
                out.push_str("Array(");
                self.write_column_type(context, out, inner);
                out.push(')');
            }
            Value::Map(_, key, val) => {
                out.push_str("Map(");
                self.write_column_type(context, out, key);
                out.push_str(", ");
                self.write_column_type(context, out, val);
                out.push(')');
            }
            Value::Json(..) => out.push_str("String"),
            _ => log::error!("Unexpected tank::Value, ClickHouse does not support {value:?}"),
        }
    }

    fn write_string(&self, context: &mut Context, out: &mut DynQuery, value: &str) {
        if matches!(
            context.fragment,
            Fragment::None | Fragment::ParameterBinding
        ) {
            out.push_str(value);
            return;
        }
        if matches!(context.fragment, Fragment::Json | Fragment::JsonKey) {
            out.push('"');
            for c in value.chars() {
                match c {
                    '"' => out.push_str("\\\""),
                    '\\' => out.push_str("\\\\"),
                    '\n' => out.push_str("\\n"),
                    '\r' => out.push_str("\\r"),
                    '\t' => out.push_str("\\t"),
                    c => out.push(c),
                }
            }
            out.push('"');
            return;
        }
        out.push('\'');
        for c in value.chars() {
            match c {
                '\'' => out.push_str("\\'"),
                '\\' => out.push_str("\\\\"),
                '\n' => out.push_str("\\n"),
                '\r' => out.push_str("\\r"),
                '\t' => out.push_str("\\t"),
                c => out.push(c),
            }
        }
        out.push('\'');
    }

    fn write_blob(&self, _context: &mut Context, out: &mut DynQuery, value: &[u8]) {
        out.push('\'');
        for b in value {
            let _ = write!(out, "{:02X}", b);
        }
        out.push('\'');
    }

    fn expression_binary_op_fragments(
        &self,
        context: &mut Context,
        op_type: BinaryOpType,
    ) -> (&str, &str, &str, bool, bool) {
        match op_type {
            BinaryOpType::BitwiseAnd => ("bitAnd(", ", ", ")", true, true),
            BinaryOpType::BitwiseOr => ("bitOr(", ", ", ")", true, true),
            BinaryOpType::ShiftLeft => ("bitShiftLeft(", ", ", ")", true, true),
            BinaryOpType::ShiftRight => ("bitShiftRight(", ", ", ")", true, true),
            BinaryOpType::Like => ("like(materialize(", "), ", ")", true, true),
            BinaryOpType::NotLike => ("NOT like(materialize(", "), ", ")", true, true),
            other => {
                let base: &dyn SqlWriter = &tank_core::GenericSqlWriter {};
                base.expression_binary_op_fragments(context, other)
            }
        }
    }

    fn write_date(&self, context: &mut Context, out: &mut DynQuery, value: &time::Date) {
        let year = value.year();
        let formatted = format!(
            "{}{:04}-{:02}-{:02}",
            if year < 0 { "-" } else { "" },
            year.unsigned_abs(),
            value.month() as u8,
            value.day(),
        );
        self.write_string(context, out, &formatted);
    }

    fn write_interval(&self, context: &mut Context, out: &mut DynQuery, value: &Interval) {
        let total_nanos: i128 = value.months as i128 * 30 * Interval::NANOS_IN_DAY
            + value.days as i128 * Interval::NANOS_IN_DAY
            + value.nanos;
        self.write_string(context, out, &format!("{total_nanos}ns"));
    }

    fn write_timestamp(
        &self,
        context: &mut Context,
        out: &mut DynQuery,
        value: &PrimitiveDateTime,
    ) {
        let quote = match context.fragment {
            Fragment::None | Fragment::ParameterBinding => "",
            Fragment::Json | Fragment::JsonKey => "\"",
            _ => "'",
        };
        write_datetime_literal(
            out,
            quote,
            value.year(),
            value.month() as u8,
            value.day(),
            value.hour(),
            value.minute(),
            value.second(),
            value.nanosecond(),
        );
    }

    fn write_timestamptz(&self, context: &mut Context, out: &mut DynQuery, value: &OffsetDateTime) {
        let utc = value.to_offset(time::UtcOffset::UTC);
        let quote = match context.fragment {
            Fragment::None | Fragment::ParameterBinding => "",
            Fragment::Json | Fragment::JsonKey => "\"",
            _ => "'",
        };
        write_datetime_literal(
            out,
            quote,
            utc.year(),
            utc.month() as u8,
            utc.day(),
            utc.hour(),
            utc.minute(),
            utc.second(),
            utc.nanosecond(),
        );
    }

    fn write_current_timestamp_ms(&self, _context: &mut Context, out: &mut DynQuery) {
        out.push_str("toUnixTimestamp64Milli(now64())");
    }

    fn write_map(&self, context: &mut Context, out: &mut DynQuery, value: &HashMap<Value, Value>) {
        out.push_str("map(");
        separated_by(
            out,
            value,
            |out, (k, v)| {
                self.write_value(context, out, k);
                out.push(',');
                self.write_value(context, out, v);
            },
            ",",
        );
        out.push(')');
    }

    fn write_create_table_column_fragment(
        &self,
        context: &mut Context,
        out: &mut DynQuery,
        column: &ColumnDef,
    ) where
        Self: Sized,
    {
        self.write_identifier(context, out, column.name(), true);
        out.push(' ');

        let len = out.len();
        self.write_column_overridden_type(context, out, column, &column.column_type);
        let overridden = out.len() > len;

        if !overridden {
            let can_be_nullable = !matches!(
                &column.value,
                Value::Array(..) | Value::List(..) | Value::Map(..)
            );
            if column.nullable && column.primary_key == PrimaryKeyType::None && can_be_nullable {
                out.push_str("Nullable(");
                SqlWriter::write_column_type(self, context, out, &column.value);
                out.push(')');
            } else {
                SqlWriter::write_column_type(self, context, out, &column.value);
            }
        }
    }

    fn write_create_table<E>(&self, out: &mut DynQuery, if_not_exists: bool)
    where
        Self: Sized,
        E: Entity,
    {
        let mut context = Context::new(Fragment::SqlCreateTable, E::qualified_columns());
        let table = E::table();
        let estimated = 128 + E::columns().len() * 64 + E::primary_key_def().len() * 24;
        out.buffer().reserve(estimated);
        if !out.is_empty() {
            out.push('\n');
        }
        out.push_str("CREATE TABLE ");
        if if_not_exists {
            out.push_str("IF NOT EXISTS ");
        }
        self.write_table_ref(&mut context, out, table);
        out.push_str(" (\n");
        separated_by(
            out,
            E::columns(),
            |out, col| {
                self.write_create_table_column_fragment(&mut context, out, col);
            },
            ",\n",
        );
        out.push_str("\n)");

        let pk = E::primary_key_def();
        if self.replacing_merge_tree || !pk.is_empty() {
            out.push_str("\nENGINE = ReplacingMergeTree()");
        } else {
            out.push_str("\nENGINE = MergeTree()");
        }
        if pk.is_empty() {
            let order_cols: Vec<&ColumnDef> = E::columns()
                .iter()
                .filter(|c| {
                    !c.nullable
                        && !matches!(c.value, Value::Array(..) | Value::List(..) | Value::Map(..))
                })
                .collect();
            if order_cols.is_empty() {
                out.push_str("\nORDER BY tuple()");
            } else {
                out.push_str("\nORDER BY (");
                separated_by(
                    out,
                    order_cols,
                    |out, col| {
                        self.write_identifier(&mut context, out, col.name(), true);
                    },
                    ", ",
                );
                out.push(')');
            }
        } else {
            out.push_str("\nORDER BY (");
            separated_by(
                out,
                pk.iter(),
                |out, col| {
                    self.write_identifier(&mut context, out, col.name(), true);
                },
                ", ",
            );
            out.push(')');
        }
        out.push(';');
    }

    fn write_create_schema<E>(&self, out: &mut DynQuery, if_not_exists: bool)
    where
        Self: Sized,
        E: Entity,
    {
        let table = E::table();
        if table.schema.is_empty() {
            return;
        }
        let mut context = Context::new(Fragment::SqlCreateSchema, E::qualified_columns());
        out.buffer().reserve(32 + table.schema.len());
        if !out.is_empty() {
            out.push('\n');
        }
        out.push_str("CREATE DATABASE ");
        if if_not_exists {
            out.push_str("IF NOT EXISTS ");
        }
        self.write_identifier(&mut context, out, &table.schema, true);
        out.push(';');
    }

    fn write_drop_schema<E>(&self, out: &mut DynQuery, if_exists: bool)
    where
        Self: Sized,
        E: Entity,
    {
        let table = E::table();
        if table.schema.is_empty() {
            return;
        }
        let mut context = Context::new(Fragment::SqlDropSchema, E::qualified_columns());
        out.buffer().reserve(32 + table.schema.len());
        if !out.is_empty() {
            out.push('\n');
        }
        out.push_str("DROP DATABASE ");
        if if_exists {
            out.push_str("IF EXISTS ");
        }
        self.write_identifier(&mut context, out, &table.schema, true);
        out.push(';');
    }

    fn write_insert_update_fragment<'a, E>(
        &self,
        _context: &mut Context,
        _out: &mut DynQuery,
        _columns: impl Iterator<Item = &'a ColumnDef> + Clone,
    ) where
        Self: Sized,
        E: Entity,
    {
    }

    fn write_column_comments_statements<E>(&self, _context: &mut Context, _out: &mut DynQuery)
    where
        Self: Sized,
        E: Entity,
    {
    }
}
