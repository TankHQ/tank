use std::collections::BTreeMap;
use tank_core::{ColumnDef, Context, SqlWriter};

#[derive(Default)]
pub struct YourDBSqlWriter {}

impl SqlWriter for YourDBSqlWriter {
    fn as_dyn(&self) -> &dyn SqlWriter {
        self
    }
    fn write_column_overridden_type(
        &self,
        _context: &mut Context,
        out: &mut RawQuery,
        _column: &ColumnDef,
        types: &BTreeMap<&'static str, &'static str>,
    ) {
        if let Some(t) = types
            .iter()
            .find_map(|(k, v)| if *k == "yourdb" { Some(v) } else { None })
        {
            out.push_str(t);
        }
    }
}
