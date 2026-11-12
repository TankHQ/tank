use std::collections::{BTreeMap, HashMap};
use tank_core::{
    BinaryOpType, ColumnDef, Context, DynQuery, GenericSqlWriter, Interval, SqlWriter, Value,
    separated_by,
};

/// SQL writer for the DuckDB dialect.
///
/// Emits DuckDB specific SQL syntax to mantain compatibility with tank operations.
#[derive(Default)]
pub struct DuckDBSqlWriter {}

impl SqlWriter for DuckDBSqlWriter {
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
        if let Some(t) = types.get("duckdb") {
            out.push_str(t);
        }
    }

    fn value_interval_units(&self) -> &[(&str, i128)] {
        static UNITS: &[(&str, i128)] = &[
            ("DAY", Interval::NANOS_IN_DAY),
            ("HOUR", Interval::NANOS_IN_SEC * 3600),
            ("MINUTE", Interval::NANOS_IN_SEC * 60),
            ("SECOND", Interval::NANOS_IN_SEC),
            ("MICROSECOND", 1_000),
        ];
        UNITS
    }

    fn write_map(
        &self,
        context: &mut Context,
        out: &mut DynQuery,
        value: &HashMap<Value<'_>, Value<'_>>,
    ) {
        out.push_str("MAP{");
        separated_by(
            out,
            value,
            |out, (k, v)| {
                self.write_value(context, out, k);
                out.push(':');
                self.write_value(context, out, v);
            },
            ",",
        );
        out.push('}');
    }

    fn expression_binary_op_fragments(
        &self,
        context: &Context,
        op_type: BinaryOpType,
    ) -> (&str, &str, &str, bool, bool) {
        match op_type {
            BinaryOpType::NotGlob => ("NOT (", " GLOB ", ")", false, false),
            _ => GenericSqlWriter.expression_binary_op_fragments(context, op_type),
        }
    }

    fn write_current_timestamp_ms(&self, _context: &mut Context, out: &mut DynQuery) {
        out.push_str("epoch_ms(current_timestamp)");
    }
}
