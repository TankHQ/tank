use std::{
    collections::{BTreeMap, HashMap},
    fmt::Write,
};
use tank_core::{
    AsEntity, BinaryOp, BinaryOpType, ColumnDef, Context, Dataset, DynQuery, Entity, Error,
    Expression, ExpressionVisitor, Fragment, GenericSqlWriter, Interval, IsFalse, IsTrue, Join,
    Operand, PrimaryKeyType, SelectQuery, SqlWriter, UnaryOp, UnaryOpType, Value, WriteInList,
    possibly_parenthesized, separated_by, write_escaped,
};
use time::{OffsetDateTime, PrimitiveDateTime, Time};

#[derive(Default)]
struct IsPredicate;

impl ExpressionVisitor for IsPredicate {
    fn visit_unary_op(
        &mut self,
        _writer: &dyn SqlWriter,
        _context: &mut Context,
        _out: &mut DynQuery,
        value: &UnaryOp<&dyn Expression>,
    ) -> bool {
        matches!(value.op, UnaryOpType::Not)
    }

    fn visit_binary_op(
        &mut self,
        _writer: &dyn SqlWriter,
        _context: &mut Context,
        _out: &mut DynQuery,
        value: &BinaryOp<&dyn Expression, &dyn Expression>,
    ) -> bool {
        matches!(
            value.op,
            BinaryOpType::Or
                | BinaryOpType::And
                | BinaryOpType::Equal
                | BinaryOpType::NotEqual
                | BinaryOpType::Less
                | BinaryOpType::Greater
                | BinaryOpType::LessEqual
                | BinaryOpType::GreaterEqual
                | BinaryOpType::In
                | BinaryOpType::NotIn
                | BinaryOpType::Is
                | BinaryOpType::IsNot
                | BinaryOpType::Like
                | BinaryOpType::NotLike
                | BinaryOpType::Regexp
                | BinaryOpType::NotRegexp
                | BinaryOpType::Glob
                | BinaryOpType::NotGlob
        )
    }
}

/// SQL writer for Microsoft SQL Server (T-SQL) dialect.
#[derive(Default, Clone, Copy, Debug)]
pub struct MSSQLSqlWriter;

impl MSSQLSqlWriter {
    const DEFAULT_KEY_VARCHAR_TYPE: &'static str = "NVARCHAR(128)";
    const MAX_INSERT_VALUES_ROWS: usize = 1000;

    pub const fn new() -> Self {
        Self
    }

    fn quote_json_literal(out: &mut DynQuery, start: usize) {
        let body = out.buffer().split_off(start);
        out.push_str("N'");
        write_escaped(out, &body, '\'', "''");
        out.push('\'');
    }

    fn write_predicate(&self, context: &mut Context, out: &mut DynQuery, expr: &dyn Expression) {
        if expr.accept_visitor(&mut IsTrue, self, context, out) {
            out.push_str("1 = 1");
        } else if expr.accept_visitor(&mut IsFalse, self, context, out) {
            out.push_str("1 = 0");
        } else if expr.accept_visitor(&mut IsPredicate, self, context, out) {
            expr.write_query(self, context, out);
        } else {
            expr.write_query(self, context, out);
            out.push_str(" = 1");
        }
    }
}

impl SqlWriter for MSSQLSqlWriter {
    fn as_dyn(&self) -> &dyn SqlWriter {
        self
    }

    fn write_question_mark(&self, context: &mut Context, out: &mut DynQuery) {
        context.counter += 1;
        let _ = write!(out, "@P{}", context.counter);
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
            .find_map(|(k, v)| (*k == "mssql" || *k == "sqlserver").then_some(*v))
            .or_else(|| {
                if matches!(column.value, Value::Varchar(..))
                    && (column.primary_key != PrimaryKeyType::None
                        || column.unique
                        || column.references.is_some())
                {
                    Some(Self::DEFAULT_KEY_VARCHAR_TYPE)
                } else {
                    None
                }
            })
        {
            out.push_str(t);
        }
    }

    fn write_column_type(&self, _context: &mut Context, out: &mut DynQuery, value: &Value<'_>) {
        match value {
            Value::Boolean(..) => out.push_str("BIT"),
            Value::Int8(..) => out.push_str("SMALLINT"),
            Value::Int16(..) => out.push_str("SMALLINT"),
            Value::Int32(..) => out.push_str("INT"),
            Value::Int64(..) => out.push_str("BIGINT"),
            Value::Int128(..) => out.push_str("DECIMAL(38)"),
            Value::UInt8(..) => out.push_str("TINYINT"),
            Value::UInt16(..) => out.push_str("INT"),
            Value::UInt32(..) => out.push_str("BIGINT"),
            Value::UInt64(..) => out.push_str("DECIMAL(20)"),
            Value::UInt128(..) => out.push_str("DECIMAL(38)"),
            Value::Float32(..) => out.push_str("REAL"),
            Value::Float64(..) => out.push_str("FLOAT"),
            Value::Decimal(_, precision, scale, ..) => {
                if (*precision, *scale) == (0, 0) {
                    out.push_str("DECIMAL(38,10)");
                } else {
                    let _ = write!(out, "DECIMAL({precision},{scale})");
                }
            }
            Value::Char(..) => out.push_str("NCHAR(1)"),
            Value::Varchar(..) => out.push_str("NVARCHAR(MAX)"),
            Value::Blob(..) => out.push_str("VARBINARY(MAX)"),
            Value::Date(..) => out.push_str("DATE"),
            Value::Time(..) => out.push_str("TIME"),
            Value::Timestamp(..) => out.push_str("DATETIME2"),
            Value::TimestampWithTimezone(..) => out.push_str("DATETIMEOFFSET"),
            Value::Interval(..) => out.push_str("NVARCHAR(MAX)"),
            Value::Uuid(..) => out.push_str("UNIQUEIDENTIFIER"),
            Value::Array(_, t, ..)
                if matches!(t.element, Value::Float32(..)) && (1..=1998).contains(&t.len) =>
            {
                let _ = write!(out, "VECTOR({})", t.len);
            }
            Value::Array(..)
            | Value::List(..)
            | Value::Map(..)
            | Value::Json(..)
            | Value::Struct(..) => out.push_str("JSON"),
            _ => log::error!("Unexpected tank::Value, SQL Server does not support {value:?}"),
        }
    }

    fn write_bool(&self, context: &mut Context, out: &mut DynQuery, value: bool) {
        match context.fragment {
            Fragment::JsonKey => {
                out.push('"');
                out.push_str(["false", "true"][value as usize]);
                out.push('"');
            }
            Fragment::Json => {
                out.push_str(["false", "true"][value as usize]);
            }
            _ => {
                out.push_str(["0", "1"][value as usize]);
            }
        }
    }

    fn write_value_f32(&self, context: &mut Context, out: &mut DynQuery, value: f32) {
        if value.is_infinite() || value.is_nan() {
            if value.is_infinite() {
                log::error!(
                    "SQL Server does not support float infinite values, will write NULL instead"
                );
            } else {
                log::warn!("SQL Server does not support float NaN values, will write NULL instead");
            }
            self.write_null(context, out);
            return;
        }
        // SQL Server's native JSON type limits numeric literals to 38 digits (1e38).
        let quote_in_json = context.fragment == Fragment::Json && value.abs() >= 1e38;
        if quote_in_json {
            out.push('"');
        }
        GenericSqlWriter::new().write_value_f32(context, out, value);
        if quote_in_json {
            out.push('"');
        }
    }

    fn write_value_f64(&self, context: &mut Context, out: &mut DynQuery, value: f64) {
        if value.is_infinite() || value.is_nan() {
            if value.is_infinite() {
                log::error!(
                    "SQL Server does not support float infinite values, will write NULL instead"
                );
            } else {
                log::warn!("SQL Server does not support float NaN values, will write NULL instead");
            }
            self.write_null(context, out);
            return;
        }
        // SQL Server's native JSON type limits numeric literals to 38 digits (1e38).
        let quote_in_json = context.fragment == Fragment::Json && value.abs() >= 1e38;
        if quote_in_json {
            out.push('"');
        }
        GenericSqlWriter::new().write_value_f64(context, out, value);
        if quote_in_json {
            out.push('"');
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
            match serde_json::to_string(value) {
                Ok(s) => out.push_str(&s),
                Err(e) => {
                    let error = Error::new(e).context("Failed to serialize string as JSON");
                    log::error!("{error:#}");
                }
            }
            return;
        }
        out.push_str("N'");
        write_escaped(out, value, '\'', "''");
        out.push('\'');
    }

    fn write_blob(&self, context: &mut Context, out: &mut DynQuery, value: &[u8]) {
        let delimiter = if matches!(context.fragment, Fragment::Json | Fragment::JsonKey) {
            "\""
        } else {
            ""
        };
        let _ = write!(out, "{delimiter}0x");
        for b in value {
            let _ = write!(out, "{b:02X}");
        }
        out.push_str(delimiter);
    }

    fn write_time(&self, context: &mut Context, out: &mut DynQuery, value: &Time) {
        let d = match context.fragment {
            Fragment::None | Fragment::ParameterBinding | Fragment::Timestamp => "",
            Fragment::Json | Fragment::JsonKey => "\"",
            _ => "'",
        };
        let (h, m, s, ns) = value.as_hms_nano();
        // SQL Server TIME(7)/DATETIME2(7)/DATETIMEOFFSET(7) accept at most 7 fractional digits.
        let mut subsecond = ns / 100;
        let mut width = 7;
        while width > 1 && subsecond % 10 == 0 {
            subsecond /= 10;
            width -= 1;
        }
        let _ = write!(out, "{d}{h:02}:{m:02}:{s:02}.{subsecond:0width$}{d}");
    }

    fn write_timestamp(
        &self,
        context: &mut Context,
        out: &mut DynQuery,
        value: &PrimitiveDateTime,
    ) {
        let d = match context.fragment {
            Fragment::None | Fragment::ParameterBinding | Fragment::Timestamp => "",
            Fragment::Json | Fragment::JsonKey => "\"",
            _ => "'",
        };
        let mut context = context.switch_fragment(Fragment::Timestamp);
        out.push_str(d);
        self.write_date(&mut context.current, out, &value.date());
        out.push('T');
        self.write_time(&mut context.current, out, &value.time());
        out.push_str(d);
    }

    fn write_timestamptz(&self, context: &mut Context, out: &mut DynQuery, value: &OffsetDateTime) {
        let d = match context.fragment {
            Fragment::None | Fragment::ParameterBinding | Fragment::Timestamp => "",
            Fragment::Json | Fragment::JsonKey => "\"",
            _ => "'",
        };
        let mut context = context.switch_fragment(Fragment::Timestamp);
        out.push_str(d);
        self.write_timestamp(
            &mut context.current,
            out,
            &PrimitiveDateTime::new(value.date(), value.time()),
        );
        let total_minutes = value.offset().whole_minutes();
        let sign = if total_minutes >= 0 { '+' } else { '-' };
        let _ = write!(
            out,
            "{sign}{:02}:{:02}",
            (total_minutes.abs() / 60) as u8,
            (total_minutes.abs() % 60) as u8,
        );
        out.push_str(d);
    }

    fn write_interval(&self, context: &mut Context, out: &mut DynQuery, value: &Interval) {
        let d = match context.fragment {
            Fragment::None | Fragment::ParameterBinding => "",
            Fragment::Json | Fragment::JsonKey => "\"",
            _ => "'",
        };
        out.push_str(d);
        if value.is_zero() {
            out.push_str("0 SECONDS");
        }
        macro_rules! write_unit {
            ($out:ident, $len:ident, $val:expr, $unit:expr) => {
                if $out.len() > $len {
                    $out.push(' ');
                    $len = $out.len();
                }
                let _ = write!(
                    $out,
                    "{} {}{}",
                    $val,
                    $unit,
                    if $val != 1 && $val != -1 { "S" } else { "" }
                );
            };
        }
        let mut months = value.months;
        let mut nanos = value.nanos as i128 + value.days as i128 * Interval::NANOS_IN_DAY;
        let mut len = out.len();
        if months != 0 {
            if months.unsigned_abs() > 48 || months % 12 == 0 {
                write_unit!(out, len, months / 12, "YEAR");
                months %= 12;
            }
            if months != 0 {
                write_unit!(out, len, months, "MONTH");
            }
        }
        for &(name, factor) in self.value_interval_units() {
            let rem = nanos % factor;
            if rem == 0 || (rem != 0 && factor / rem.abs() > 1_000_000) {
                let unit_val = nanos / factor;
                if unit_val != 0 {
                    write_unit!(out, len, unit_val, name);
                    nanos = rem;
                    if nanos == 0 {
                        break;
                    }
                }
            }
        }
        out.push_str(d);
    }

    fn write_list(
        &self,
        context: &mut Context,
        out: &mut DynQuery,
        value: &mut dyn Iterator<Item = &dyn Expression>,
        _t: Option<&Value<'_>>,
        _is_array: bool,
    ) {
        let is_json = matches!(
            context.fragment,
            Fragment::Json | Fragment::JsonKey | Fragment::None | Fragment::ParameterBinding
        );
        let mut context = context.switch_fragment(Fragment::Json);
        let start = out.len();
        out.push('[');
        separated_by(
            out,
            value,
            |out, v| {
                v.write_query(self, &mut context.current, out);
            },
            ",",
        );
        out.push(']');
        if !is_json {
            Self::quote_json_literal(out, start);
        }
    }

    fn write_map(
        &self,
        context: &mut Context,
        out: &mut DynQuery,
        value: &HashMap<Value<'_>, Value<'_>>,
    ) {
        let is_json = matches!(
            context.fragment,
            Fragment::Json | Fragment::JsonKey | Fragment::None | Fragment::ParameterBinding
        );
        let mut context = context.switch_fragment(Fragment::Json);
        let start = out.len();
        out.push('{');
        separated_by(
            out,
            value,
            |out, (k, v)| {
                {
                    let mut key_ctx = context.current.switch_fragment(Fragment::JsonKey);
                    self.write_value(&mut key_ctx.current, out, k);
                }
                out.push(':');
                self.write_value(&mut context.current, out, v);
            },
            ",",
        );
        out.push('}');
        if !is_json {
            Self::quote_json_literal(out, start);
        }
    }

    fn write_current_timestamp_ms(&self, _context: &mut Context, out: &mut DynQuery) {
        // SQL Server on Linux (SQLPAL) caches its user-space clock tick (~1-2ms behind host CLOCK_REALTIME).
        out.push_str(
            "((DATEDIFF_BIG(MICROSECOND, '1970-01-01T00:00:00Z', SYSDATETIMEOFFSET()) + 1500) / 1000)",
        );
    }

    fn write_operand(&self, context: &mut Context, out: &mut DynQuery, value: &Operand) {
        match value {
            Operand::Null => self.write_null(context, out),
            Operand::LitBool(v) => self.write_bool(context, out, *v),
            Operand::LitInt(v) => self.write_value_i128(context, out, *v),
            Operand::LitFloat(v) => self.write_value_f64(context, out, *v),
            Operand::LitStr(v) => self.write_string(context, out, v),
            Operand::LitIdent(v) => {
                self.write_identifier(context, out, v, context.quote_identifiers)
            }
            Operand::LitField(v) => {
                let separator = self.separator();
                for (i, part) in v.iter().enumerate() {
                    if i > 0 {
                        out.push_str(separator);
                    }
                    out.push_str(part);
                }
            }
            Operand::LitList(v) => self.write_list(
                context,
                out,
                &mut v.iter().map(|v| v as &dyn Expression),
                None,
                false,
            ),
            Operand::LitTuple(v) => {
                self.write_tuple(context, out, &mut v.iter().map(|v| v as &dyn Expression))
            }
            Operand::Type(v) => self.write_column_type(context, out, v),
            Operand::Variable(v) => self.write_value(context, out, v),
            Operand::Value(v) => self.write_value(context, out, v),
            Operand::Call(f, args) => self.write_function(context, out, f, args),
            Operand::Asterisk => out.push('*'),
            Operand::QuestionMark => self.write_question_mark(context, out),
            Operand::CurrentTimestampMs => self.write_current_timestamp_ms(context, out),
        }
    }

    fn write_unary_op(
        &self,
        context: &mut Context,
        out: &mut DynQuery,
        value: &UnaryOp<&dyn Expression>,
    ) {
        match value.op {
            UnaryOpType::Negative => {
                out.push('-');
                possibly_parenthesized!(
                    out,
                    value.arg.precedence(self.as_dyn())
                        <= self.expression_unary_op_precedence(&value.op),
                    value.arg.write_query(self.as_dyn(), context, out)
                );
            }
            UnaryOpType::Not => {
                out.push_str("NOT (");
                self.write_predicate(context, out, value.arg);
                out.push(')');
            }
        }
    }

    fn write_binary_op(
        &self,
        context: &mut Context,
        out: &mut DynQuery,
        value: &BinaryOp<&dyn Expression, &dyn Expression>,
    ) {
        if value.op == BinaryOpType::Alias
            && matches!(
                context.fragment,
                Fragment::SqlSelectOrderBy | Fragment::SqlSelectGroupBy
            )
        {
            return value.lhs.write_query(self.as_dyn(), context, out);
        }
        if value.op == BinaryOpType::Alias
            && context.fragment == Fragment::SqlSelect
            && value
                .lhs
                .accept_visitor(&mut IsPredicate, self, context, out)
        {
            out.push_str("CASE WHEN ");
            self.write_predicate(context, out, value.lhs);
            out.push_str(" THEN CAST(1 AS BIT) ELSE CAST(0 AS BIT) END AS ");
            let mut alias_ctx = context.switch_fragment(Fragment::Aliasing);
            value
                .rhs
                .write_query(self.as_dyn(), &mut alias_ctx.current, out);
            return;
        }
        if matches!(value.op, BinaryOpType::And | BinaryOpType::Or) {
            let (_, infix, _, _, _) = self.expression_binary_op_fragments(context, value.op);
            let precedence = self.expression_binary_op_precedence(&value.op);
            possibly_parenthesized!(
                out,
                value.lhs.precedence(self.as_dyn()) < precedence,
                self.write_predicate(context, out, value.lhs)
            );
            out.push_str(infix);
            possibly_parenthesized!(
                out,
                value.rhs.precedence(self.as_dyn()) <= precedence,
                self.write_predicate(context, out, value.rhs)
            );
            return;
        }
        let (prefix, infix, suffix, lhs_parenthesized, rhs_parenthesized) =
            self.expression_binary_op_fragments(context, value.op);
        let precedence = self.expression_binary_op_precedence(&value.op);
        out.push_str(prefix);
        possibly_parenthesized!(
            out,
            !lhs_parenthesized && value.lhs.precedence(self.as_dyn()) < precedence,
            value.lhs.write_query(self.as_dyn(), context, out)
        );
        out.push_str(infix);
        let mut context = context.switch_fragment(match value.op {
            BinaryOpType::Cast => Fragment::Casting,
            BinaryOpType::Alias => Fragment::Aliasing,
            _ => context.fragment,
        });
        if matches!(value.op, BinaryOpType::In | BinaryOpType::NotIn)
            && value
                .rhs
                .accept_visitor(&mut WriteInList, self.as_dyn(), &mut context.current, out)
        {
            out.push_str(suffix);
        } else {
            possibly_parenthesized!(
                out,
                !rhs_parenthesized && value.rhs.precedence(self.as_dyn()) <= precedence,
                value
                    .rhs
                    .write_query(self.as_dyn(), &mut context.current, out)
            );
            out.push_str(suffix);
        }
    }

    fn write_join(
        &self,
        context: &mut Context,
        out: &mut DynQuery,
        join: &Join<&dyn Dataset, &dyn Dataset, &dyn Expression>,
    ) {
        let mut context = context.switch_fragment(Fragment::SqlJoin);
        context.current.qualify_columns = true;
        join.lhs
            .write_table_name(self.as_dyn(), &mut context.current, out);
        out.push(' ');
        self.write_join_type(&mut context.current, out, &join.join);
        out.push(' ');
        join.rhs
            .write_table_name(self.as_dyn(), &mut context.current, out);
        if let Some(on) = &join.on {
            out.push_str(" ON ");
            self.write_predicate(&mut context.current, out, on);
        }
    }

    fn write_transaction_begin(&self, out: &mut DynQuery) {
        if !out.is_empty() {
            out.push('\n');
        }
        out.push_str("BEGIN TRANSACTION;");
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
        if !out.is_empty() {
            out.push('\n');
        }
        let mut schema_lit = DynQuery::default();
        write_escaped(&mut schema_lit, &table.schema, '\'', "''");
        let mut quoted = DynQuery::default();
        let mut context = Context::new(Fragment::SqlCreateSchema, E::qualified_columns());
        self.write_identifier(&mut context, &mut quoted, &table.schema, true);
        let mut quoted_esc = DynQuery::default();
        write_escaped(&mut quoted_esc, &quoted.as_str(), '\'', "''");
        if if_not_exists {
            let _ = write!(
                out,
                "IF Schema_ID(N'{}') IS NULL EXEC(N'CREATE SCHEMA {}');",
                schema_lit.as_str(),
                quoted_esc.as_str(),
            );
        } else {
            let _ = write!(out, "EXEC(N'CREATE SCHEMA {}');", quoted_esc.as_str());
        }
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
        if !out.is_empty() {
            out.push('\n');
        }
        out.push_str("DROP SCHEMA ");
        if if_exists {
            out.push_str("IF EXISTS ");
        }
        self.write_identifier(&mut context, out, &table.schema, true);
        out.push(';');
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
        if if_not_exists {
            let mut target = DynQuery::default();
            self.write_table_ref(&mut context, &mut target, table);
            let mut target_esc = DynQuery::default();
            write_escaped(&mut target_esc, &target.as_str(), '\'', "''");
            let _ = write!(
                out,
                "IF OBJECT_ID(N'{}', N'U') IS NULL\n",
                target_esc.as_str()
            );
        }
        out.push_str("CREATE TABLE ");
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
        let pk = E::primary_key_def();
        if pk.len() > 1 {
            self.write_create_table_primary_key_fragment(&mut context, out, pk.iter().copied());
        }
        for unique in E::unique_defs() {
            if unique.len() > 0 {
                out.push_str(",\nUNIQUE (");
                separated_by(
                    out,
                    unique,
                    |out, col| {
                        self.write_identifier(
                            &mut context
                                .switch_fragment(Fragment::SqlCreateTableUnique)
                                .current,
                            out,
                            col.name(),
                            true,
                        );
                    },
                    ", ",
                );
                out.push(')');
            }
        }
        let foreign_keys = E::columns().iter().filter(|c| c.references.is_some());
        separated_by(
            out,
            foreign_keys,
            |out, column| {
                let references = column.references.as_ref().unwrap();
                out.push_str(",\nFOREIGN KEY (");
                self.write_identifier(&mut context, out, column.name(), true);
                out.push_str(") REFERENCES ");
                self.write_table_ref(&mut context, out, &references.table());
                out.push('(');
                self.write_column_ref(&mut context, out, references);
                out.push(')');
                if let Some(on_delete) = &column.on_delete {
                    out.push_str(" ON DELETE ");
                    self.write_create_table_references_action(&mut context, out, on_delete);
                }
                if let Some(on_update) = &column.on_update {
                    out.push_str(" ON UPDATE ");
                    self.write_create_table_references_action(&mut context, out, on_update);
                }
            },
            "",
        );
        out.push_str(");");
    }

    fn write_column_comment(&self, _context: &mut Context, out: &mut DynQuery, column: &ColumnDef) {
        if column.comment.is_empty() {
            return;
        }
        if !out.is_empty() {
            out.push('\n');
        }
        let schema = if column.schema().is_empty() {
            "dbo"
        } else {
            column.schema()
        };
        let mut schema_esc = DynQuery::default();
        write_escaped(&mut schema_esc, schema, '\'', "''");
        let mut table_esc = DynQuery::default();
        write_escaped(&mut table_esc, column.table(), '\'', "''");
        let mut col_esc = DynQuery::default();
        write_escaped(&mut col_esc, column.name(), '\'', "''");
        let mut comment_esc = DynQuery::default();
        write_escaped(&mut comment_esc, column.comment, '\'', "''");
        let _ = write!(
            out,
            "IF EXISTS (\
                SELECT 1 FROM sys.extended_properties ep \
                JOIN sys.tables t ON ep.major_id = t.object_id \
                JOIN sys.schemas s ON t.schema_id = s.schema_id \
                JOIN sys.columns c ON ep.major_id = c.object_id AND ep.minor_id = c.column_id \
                WHERE ep.name = N'MS_Description' AND s.name = N'{s}' AND t.name = N'{t}' AND c.name = N'{c}'\
            ) \
            EXEC sp_updateextendedproperty @name = N'MS_Description', @value = N'{v}', @level0type = N'SCHEMA', @level0name = N'{s}', @level1type = N'TABLE', @level1name = N'{t}', @level2type = N'COLUMN', @level2name = N'{c}'; \
            ELSE \
            EXEC sp_addextendedproperty @name = N'MS_Description', @value = N'{v}', @level0type = N'SCHEMA', @level0name = N'{s}', @level1type = N'TABLE', @level1name = N'{t}', @level2type = N'COLUMN', @level2name = N'{c}';",
            s = schema_esc.as_str(),
            t = table_esc.as_str(),
            c = col_esc.as_str(),
            v = comment_esc.as_str(),
        );
    }

    fn write_select<'a, Data>(&self, out: &mut DynQuery, query: &impl SelectQuery<Data>)
    where
        Self: Sized,
        Data: Dataset + 'a,
    {
        let Some(from) = query.get_from() else {
            log::error!("The query does not have the FROM clause");
            return;
        };
        let columns = query.get_select();
        let columns_count = columns.clone().into_iter().count();
        out.buffer().reserve(128 + columns_count * 32);
        if !out.is_empty() {
            out.push('\n');
        }
        out.push_str("SELECT ");
        if let Some(limit) = query.get_limit() {
            let _ = write!(out, "TOP ({limit}) ");
        }
        let mut context = Context::new(Fragment::SqlSelect, Data::qualified_columns());
        if columns_count != 0 {
            separated_by(
                out,
                columns.clone(),
                |out, col| {
                    if col.accept_visitor(&mut IsPredicate, self, &mut context, out) {
                        out.push_str("CASE WHEN ");
                        self.write_predicate(&mut context, out, &col);
                        out.push_str(" THEN CAST(1 AS BIT) ELSE CAST(0 AS BIT) END");
                    } else {
                        col.write_query(self, &mut context, out);
                    }
                },
                ", ",
            );
        } else {
            out.push('*');
        }
        out.push_str("\nFROM ");
        from.write_table_name(
            self,
            &mut context.switch_fragment(Fragment::SqlSelectFrom).current,
            out,
        );
        if let Some(condition) = query.get_where()
            && !condition.accept_visitor(&mut IsTrue, self, &mut context, out)
        {
            out.push_str("\nWHERE ");
            self.write_predicate(
                &mut context.switch_fragment(Fragment::SqlSelectWhere).current,
                out,
                condition,
            );
        }
        let mut group_by = query.get_group_by().peekable();
        if group_by.peek().is_some() {
            out.push_str("\nGROUP BY ");
            let mut context = context.switch_fragment(Fragment::SqlSelectGroupBy);
            separated_by(
                out,
                group_by,
                |out, col| {
                    col.write_query(self, &mut context.current, out);
                },
                ", ",
            );
        }
        if let Some(having) = query.get_having() {
            out.push_str("\nHAVING ");
            self.write_predicate(
                &mut context.switch_fragment(Fragment::SqlSelectHaving).current,
                out,
                having,
            );
        }
        let mut order_by = query.get_order_by().peekable();
        if order_by.peek().is_some() {
            out.push_str("\nORDER BY ");
            let mut context = context.switch_fragment(Fragment::SqlSelectOrderBy);
            separated_by(
                out,
                order_by,
                |out, col| {
                    col.write_query(self, &mut context.current, out);
                },
                ", ",
            );
        }
        out.push(';');
    }

    fn write_insert<It>(&self, out: &mut DynQuery, entities: It, update: bool)
    where
        Self: Sized,
        It: IntoIterator,
        It::Item: AsEntity,
    {
        type E<It> = <<It as IntoIterator>::Item as AsEntity>::Entity;
        let table = E::<It>::table();
        let mut entities = entities.into_iter().peekable();
        if entities.peek().is_none() {
            return;
        }
        let pk = E::<It>::primary_key_def();
        if update && !pk.is_empty() {
            if !out.is_empty() {
                out.push('\n');
            }
            let mut context = Context::new(Fragment::SqlInsertInto, E::<It>::qualified_columns());
            out.push_str("MERGE INTO ");
            self.write_table_ref(&mut context, out, table);
            out.push_str(" AS \"target\"\nUSING (VALUES");
            {
                let mut val_ctx = context.switch_fragment(Fragment::SqlInsertIntoValues);
                separated_by(
                    out,
                    entities,
                    |out, entity| {
                        out.push_str("\n(");
                        entity
                            .as_entity()
                            .write_query(self.as_dyn(), &mut val_ctx.current, out);
                        out.push(')');
                    },
                    ",",
                );
            }
            out.push_str("\n) AS \"source\" (");
            separated_by(
                out,
                E::<It>::columns().iter(),
                |out, col| {
                    self.write_identifier(&mut context, out, col.name(), true);
                },
                ", ",
            );
            out.push_str(")\nON ");
            separated_by(
                out,
                pk.iter(),
                |out, col| {
                    out.push_str("\"target\".");
                    self.write_identifier(&mut context, out, col.name(), true);
                    out.push_str(" = \"source\".");
                    self.write_identifier(&mut context, out, col.name(), true);
                },
                " AND ",
            );
            let mut update_cols = E::<It>::columns()
                .iter()
                .filter(|c| c.primary_key == PrimaryKeyType::None)
                .peekable();
            if update_cols.peek().is_some() {
                out.push_str("\nWHEN MATCHED THEN UPDATE SET\n");
                separated_by(
                    out,
                    update_cols,
                    |out, col| {
                        out.push_str("\"target\".");
                        self.write_identifier(&mut context, out, col.name(), true);
                        out.push_str(" = \"source\".");
                        self.write_identifier(&mut context, out, col.name(), true);
                    },
                    ",\n",
                );
            }
            out.push_str("\nWHEN NOT MATCHED THEN INSERT (");
            separated_by(
                out,
                E::<It>::columns().iter(),
                |out, col| {
                    self.write_identifier(&mut context, out, col.name(), true);
                },
                ", ",
            );
            out.push_str(")\nVALUES (");
            separated_by(
                out,
                E::<It>::columns().iter(),
                |out, col| {
                    out.push_str("\"source\".");
                    self.write_identifier(&mut context, out, col.name(), true);
                },
                ", ",
            );
            out.push_str(");");
            return;
        }

        let cols = E::<It>::columns().len();
        let rows = entities.size_hint().0.max(1);
        out.buffer().reserve(128 + (cols + rows * cols) * 16);
        let mut context = Context::new(Fragment::SqlInsertInto, E::<It>::qualified_columns());
        while entities.peek().is_some() {
            if !out.is_empty() {
                out.push('\n');
            }
            out.push_str("INSERT INTO ");
            self.write_table_ref(&mut context, out, table);
            out.push_str(" (");
            separated_by(
                out,
                E::<It>::columns().iter(),
                |out, col| {
                    self.write_identifier(&mut context, out, col.name(), true);
                },
                ", ",
            );
            out.push_str(") VALUES");
            let mut val_ctx = context.switch_fragment(Fragment::SqlInsertIntoValues);
            separated_by(
                out,
                entities.by_ref().take(Self::MAX_INSERT_VALUES_ROWS),
                |out, entity| {
                    out.push_str("\n(");
                    entity
                        .as_entity()
                        .write_query(self.as_dyn(), &mut val_ctx.current, out);
                    out.push(')');
                },
                ",",
            );
            out.push(';');
        }
    }

    fn write_delete<E>(&self, out: &mut DynQuery, condition: impl Expression)
    where
        Self: Sized,
        E: Entity,
    {
        let table = E::table();
        out.buffer().reserve(128);
        if !out.is_empty() {
            out.push('\n');
        }
        out.push_str("DELETE FROM ");
        let mut context = Context::new(Fragment::SqlDeleteFrom, E::qualified_columns());
        self.write_table_ref(&mut context, out, table);
        out.push_str("\nWHERE ");
        self.write_predicate(
            &mut context
                .switch_fragment(Fragment::SqlDeleteFromWhere)
                .current,
            out,
            &condition,
        );
        out.push(';');
    }
}
