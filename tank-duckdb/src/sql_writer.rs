use std::{
    collections::{BTreeMap, HashMap},
    fmt::Write,
};
use tank_core::{ColumnDef, Context, Interval, SqlWriter, Value, separated_by};

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
        out: &mut String,
        _column: &ColumnDef,
        types: &BTreeMap<&'static str, &'static str>,
    ) {
        if let Some(t) = types
            .iter()
            .find_map(|(k, v)| if *k == "duckdb" { Some(v) } else { None })
        {
            out.push_str(t);
        }
    }

    fn write_value_blob(&self, _context: &mut Context, out: &mut String, value: &[u8]) {
        out.push('\'');
        for b in value {
            let _ = write!(out, "\\x{:X}", b);
        }
        out.push('\'');
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

    fn write_value_map(
        &self,
        context: &mut Context,
        out: &mut String,
        value: &HashMap<Value, Value>,
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
}
