use std::collections::BTreeMap;
use tank_core::{
    ColumnDef, Context, DynQuery, SqlCoreWriter, SqlExpressionWriter, SqlFragmentWriter,
    SqlValueWriter, SqlWriter,
};

#[derive(Default)]
pub struct YourDBSqlWriter {}

impl SqlCoreWriter for YourDBSqlWriter {
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
        if let Some(t) = types.get("yourdb") {
            out.push_str(t);
        }
    }
}
impl SqlValueWriter for YourDBSqlWriter {}
impl SqlExpressionWriter for YourDBSqlWriter {}
impl SqlFragmentWriter for YourDBSqlWriter {}
impl SqlWriter for YourDBSqlWriter {}
