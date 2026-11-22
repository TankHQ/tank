use crate::ClickHousePrepared;
use anyhow::anyhow;
use std::{
    collections::{BTreeMap, HashMap},
    fmt::Write,
};
use tank_core::{
    AnyQuery, BinaryOpType, Case, ColumnDef, Context, Dataset, Expression, Fragment,
    GenericSqlWriter, Interval, IsTrue, PrimaryKeyType, Result, SqlCoreWriter,
    SqlExpressionWriter, SqlFragmentWriter, SqlValueWriter, SqlWriter, TableRef, UpdateQuery,
    Value, separated_by, write_escaped,
};
use time::{OffsetDateTime, PrimitiveDateTime};

/// ClickHouse SQL writer.
#[derive(Clone, Copy, Debug)]
pub struct ClickHouseSqlWriter {
    replacing_merge_tree: bool,
}

impl Default for ClickHouseSqlWriter {
    fn default() -> Self {
        Self::new()
    }
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

    /// Renders a prepared statement into the SQL to execute.
    ///
    /// ClickHouse has no wire-level parameter binding.
    /// The writer emits provisional `{pN:Nullable(String)}` markers and records their offsets.
    /// See: [`SqlWriter::write_question_mark`].
    /// This turns those markers into the final query with `SET param_pN = ...` directives.
    pub fn build_parameterized_sql(
        &self,
        prepared: &ClickHousePrepared,
    ) -> Result<(Option<String>, String)> {
        let markers = prepared.markers();
        let params = prepared.params();
        if markers.len() < params.len() {
            let error = anyhow!("Not enough parameter markers for the bound values");
            log::error!("{error:#}");
            return Err(error);
        }
        let mut directive: Option<String> = None;
        for (i, value) in params.iter().enumerate() {
            let line = directive.get_or_insert_with(String::new);
            if i == 0 {
                line.push_str("SET ");
            } else {
                line.push_str(", ");
            }
            let _ = write!(line, "param_p{i} = ");
            if value.is_null() {
                // ClickHouse represents a NULL query parameter as `'\\N'`.
                line.push_str("'\\\\N'");
            } else {
                let mut out = AnyQuery::with_capacity(16);
                self.write_value(&mut Context::fragment(Fragment::SqlSelect), &mut out, value);
                line.push_str(out.as_str().as_ref());
            }
        }
        let sql = prepared.sql();
        let mut out = String::with_capacity(sql.len() + 16);
        let mut cursor = 0;
        for (i, marker) in markers.iter().enumerate() {
            if marker.begin > marker.end
                || marker.end > sql.len()
                || marker.begin < cursor
                || !sql.is_char_boundary(marker.begin)
                || !sql.is_char_boundary(marker.end)
            {
                continue;
            }
            out.push_str(&sql[cursor..marker.begin]);
            let _ = write!(out, "{{p{i}:");
            match params.get(i) {
                Some(value) if !value.is_null() => {
                    let mut ty = AnyQuery::with_capacity(16);
                    self.write_column_type(
                        &mut Context::fragment(Fragment::SqlCreateTable),
                        &mut ty,
                        value,
                    );
                    out.push_str("Nullable(");
                    out.push_str(ty.as_str().as_ref());
                    out.push(')');
                }
                // `Nullable(Nothing)` accepts a NULL parameter against any column type.
                _ => out.push_str("Nullable(Nothing)"),
            }
            out.push('}');
            cursor = marker.end;
        }
        out.push_str(&sql[cursor..]);
        Ok((directive, out))
    }

    fn write_datetime_literal(out: &mut AnyQuery, quote: &str, value: &PrimitiveDateTime) {
        out.push_str(quote);
        let _ = write!(
            out,
            "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
            value.year(),
            value.month() as u8,
            value.day(),
            value.hour(),
            value.minute(),
            value.second(),
        );
        let nanos = value.nanosecond();
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
}

impl SqlCoreWriter for ClickHouseSqlWriter {
    fn as_dyn(&self) -> &dyn SqlWriter {
        self
    }

    fn write_identifier(
        &self,
        _context: &mut Context,
        out: &mut AnyQuery,
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

    fn write_column_overridden_type(
        &self,
        _context: &mut Context,
        out: &mut AnyQuery,
        _column: &ColumnDef,
        types: &BTreeMap<&'static str, &'static str>,
    ) {
        if let Some(t) = types.get("clickhouse").copied() {
            out.push_str(t);
        }
    }

    fn write_column_type(&self, context: &mut Context, out: &mut AnyQuery, value: &Value) {
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
                    out.push_str("Decimal(38,9)");
                } else {
                    let _ = write!(out, "Decimal({precision},{scale})");
                }
            }
            Value::Char(..)
            | Value::Varchar(..)
            | Value::Blob(..)
            | Value::Time(..)
            | Value::Interval(..)
            | Value::Json(..) => out.push_str("String"),
            Value::Date(..) => out.push_str("Date"),
            Value::Timestamp(..) | Value::TimestampWithTimezone(..) => {
                out.push_str("DateTime64(9,'UTC')")
            }
            Value::Uuid(..) => out.push_str("UUID"),
            Value::IpAddr(..) => out.push_str("IPv6"),
            Value::Array(_, t) | Value::List(_, t) => {
                out.push_str("Array(");
                self.write_column_type(context, out, &t.element);
                out.push(')');
            }
            Value::Map(_, t) => {
                out.push_str("Map(");
                self.write_column_type(context, out, &t.key);
                out.push(',');
                self.write_column_type(context, out, &t.value);
                out.push(')');
            }
            _ => log::error!("Unexpected tank::Value, ClickHouse does not support {value:?}"),
        }
    }
}

impl SqlValueWriter for ClickHouseSqlWriter {
    fn write_null(&self, context: &mut Context, out: &mut AnyQuery) {
        match context.fragment {
            Fragment::Json | Fragment::JsonKey => out.push_str("null"),
            Fragment::SqlSelect => out.push_str("CAST(NULL AS Nullable(String))"),
            _ => out.push_str("NULL"),
        }
    }

    fn write_string(&self, context: &mut Context, out: &mut AnyQuery, value: &str) {
        let quote = match context.fragment {
            Fragment::None | Fragment::ParameterBinding => {
                out.push_str(value);
                return;
            }
            Fragment::Json | Fragment::JsonKey => '"',
            _ => '\'',
        };
        out.push(quote);
        for c in value.chars() {
            match c {
                '\\' => out.push_str("\\\\"),
                '\n' => out.push_str("\\n"),
                '\r' => out.push_str("\\r"),
                '\t' => out.push_str("\\t"),
                c if c == quote => {
                    out.push('\\');
                    out.push(c);
                }
                c => out.push(c),
            }
        }
        out.push(quote);
    }

    fn write_blob(&self, _context: &mut Context, out: &mut AnyQuery, value: &[u8]) {
        out.push('\'');
        for b in value {
            let _ = write!(out, "{:02X}", b);
        }
        out.push('\'');
    }

    fn write_date(&self, context: &mut Context, out: &mut AnyQuery, value: &time::Date) {
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

    fn write_interval(&self, context: &mut Context, out: &mut AnyQuery, value: &Interval) {
        self.write_string(context, out, &format!("{}ns", value.as_ns()));
    }

    fn write_timestamp(
        &self,
        context: &mut Context,
        out: &mut AnyQuery,
        value: &PrimitiveDateTime,
    ) {
        let quote = match context.fragment {
            Fragment::None | Fragment::ParameterBinding => "",
            Fragment::Json | Fragment::JsonKey => "\"",
            _ => "'",
        };
        Self::write_datetime_literal(out, quote, value);
    }

    fn write_timestamptz(&self, context: &mut Context, out: &mut AnyQuery, value: &OffsetDateTime) {
        let utc = value.to_offset(time::UtcOffset::UTC);
        let quote = match context.fragment {
            Fragment::None | Fragment::ParameterBinding => "",
            Fragment::Json | Fragment::JsonKey => "\"",
            _ => "'",
        };
        Self::write_datetime_literal(out, quote, &PrimitiveDateTime::new(utc.date(), utc.time()));
    }

    fn write_map(&self, context: &mut Context, out: &mut AnyQuery, value: &HashMap<Value, Value>) {
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
}

impl SqlExpressionWriter for ClickHouseSqlWriter {
    fn write_question_mark(&self, context: &mut Context, out: &mut AnyQuery) {
        let id = context.counter;
        context.counter += 1;
        out.push_str("{p");
        let _ = write!(out, "{id}");
        out.push(':');
        out.push_str("Nullable(String)}");
    }

    fn expression_binary_op_fragments(
        &self,
        context: &Context,
        op_type: BinaryOpType,
    ) -> (&str, &str, &str, bool, bool) {
        match op_type {
            BinaryOpType::BitwiseAnd => ("bitAnd(", ", ", ")", true, true),
            BinaryOpType::BitwiseOr => ("bitOr(", ", ", ")", true, true),
            BinaryOpType::ShiftLeft => ("bitShiftLeft(", ", ", ")", true, true),
            BinaryOpType::ShiftRight => ("bitShiftRight(", ", ", ")", true, true),
            BinaryOpType::Like => ("like(materialize(", "), ", ")", true, true),
            BinaryOpType::NotLike => ("NOT like(materialize(", "), ", ")", true, true),
            other => GenericSqlWriter.expression_binary_op_fragments(context, other),
        }
    }

    fn write_current_timestamp_ms(&self, _context: &mut Context, out: &mut AnyQuery) {
        out.push_str("toUnixTimestamp64Milli(now64())");
    }

    fn write_case(&self, context: &mut Context, out: &mut AnyQuery, value: &Case) {
        // ClickHouse's `caseWithExpression` (simple CASE) mis-evaluates NULL defaults in older
        // versions. Searched CASE compiles to `multiIf` and preserves SQL NULL semantics.
        out.push_str("CASE");
        for (when, then) in value.when {
            out.push_str(" WHEN ");
            if let Some(subject) = value.value {
                out.push('(');
                subject.write_query(self, context, out);
                out.push_str(") = (");
                when.write_query(self, context, out);
                out.push(')');
            } else {
                when.write_query(self, context, out);
            }
            out.push_str(" THEN ");
            then.write_query(self, context, out);
        }
        if let Some(otherwise) = value.otherwise {
            out.push_str(" ELSE ");
            otherwise.write_query(self, context, out);
        }
        out.push_str(" END");
    }
}

impl SqlFragmentWriter for ClickHouseSqlWriter {
    fn write_create_table_column_fragment(
        &self,
        context: &mut Context,
        out: &mut AnyQuery,
        column: &ColumnDef,
    ) {
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
                self.write_column_type(context, out, &column.value);
                out.push(')');
            } else {
                self.write_column_type(context, out, &column.value);
            }
        }
    }

    fn write_insert_update_fragment<'a>(
        &self,
        _context: &mut Context,
        _out: &mut AnyQuery,
        _primary_key: impl Iterator<Item = &'a ColumnDef> + Clone,
        _columns: impl Iterator<Item = &'a ColumnDef> + Clone,
    ) where
        Self: Sized,
    {
    }
}

impl SqlWriter for ClickHouseSqlWriter {
    fn write_column_comment(
        &self,
        _context: &mut Context,
        _out: &mut AnyQuery,
        _column: &ColumnDef,
    ) {
    }

    fn write_create_table(&self, out: &mut AnyQuery, table: &TableRef, if_not_exists: bool)
    where
        Self: Sized,
    {
        let mut context = Context::new(Fragment::SqlCreateTable, table.qualified_columns());
        let pk: Vec<&ColumnDef> = table.primary_key().collect();
        let estimated = 128 + table.columns.len() * 64 + pk.len() * 24;
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
            table.columns.iter(),
            |out, col| {
                self.write_create_table_column_fragment(&mut context, out, col);
            },
            ",\n",
        );
        out.push_str("\n)");

        out.push_str(if self.replacing_merge_tree || !pk.is_empty() {
            "\nENGINE = ReplacingMergeTree()"
        } else {
            "\nENGINE = MergeTree()"
        });
        let order_cols: Vec<&ColumnDef> = if pk.is_empty() {
            table
                .columns
                .iter()
                .filter(|c| {
                    !c.nullable
                        && !matches!(c.value, Value::Array(..) | Value::List(..) | Value::Map(..))
                })
                .collect()
        } else {
            pk
        };
        if order_cols.is_empty() {
            out.push_str("\nORDER BY tuple()");
        } else {
            out.push_str("\nORDER BY (");
            separated_by(
                out,
                order_cols.iter(),
                |out, col| {
                    self.write_identifier(&mut context, out, col.name(), true);
                },
                ", ",
            );
            out.push(')');
        }
        out.push(';');
    }

    fn write_create_schema(&self, out: &mut AnyQuery, table: &TableRef, if_not_exists: bool)
    where
        Self: Sized,
    {
        if table.schema.is_empty() {
            return;
        }
        let mut context = Context::new(Fragment::SqlCreateSchema, table.qualified_columns());
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

    fn write_drop_schema(&self, out: &mut AnyQuery, table: &TableRef, if_exists: bool)
    where
        Self: Sized,
    {
        if table.schema.is_empty() {
            return;
        }
        let mut context = Context::new(Fragment::SqlDropSchema, table.qualified_columns());
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

    fn write_update<Data>(&self, out: &mut AnyQuery, query: impl UpdateQuery<Data>)
    where
        Self: Sized,
        Data: Dataset,
    {
        let table = query.get_from();
        let assignments = query.get_set();
        let assignments_count = assignments.clone().count();
        if assignments_count == 0 {
            return;
        }
        out.buffer().reserve(160 + assignments_count * 32);
        if !out.is_empty() {
            out.push('\n');
        }
        out.push_str("ALTER TABLE ");
        let mut context = Context::new(Fragment::SqlUpdate, table.qualified_columns());
        table.write_table_name(self, &mut context, out);
        out.push_str("\nUPDATE ");
        {
            let mut set = context.switch_fragment(Fragment::SqlUpdateSet);
            set.current.qualify_columns = false;
            separated_by(
                out,
                assignments,
                |out, assignment| assignment.write_query(self, &mut set.current, out),
                ",\n",
            );
        }
        out.push_str("\nWHERE ");
        if let Some(condition) = query.get_where()
            && !condition.accept_visitor(&mut IsTrue, self, &mut context, out)
        {
            condition.write_query(
                self,
                &mut context.switch_fragment(Fragment::SqlUpdateWhere).current,
                out,
            );
        } else {
            out.push('1');
        }
        out.push_str("\nSETTINGS mutations_sync = 1;");
    }
}
