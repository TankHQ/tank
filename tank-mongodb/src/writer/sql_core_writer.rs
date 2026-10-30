use crate::{MongoDBDriver, MongoDBPrepared, MongoDBSqlWriter};
use mongodb::bson::Bson;
use tank_core::{ColumnRef, Context, DynQuery, SqlCoreWriter, SqlWriter};

impl SqlCoreWriter for MongoDBSqlWriter {
    fn as_dyn(&self) -> &dyn SqlWriter {
        self
    }

    fn write_identifier(
        &self,
        _context: &mut Context,
        out: &mut DynQuery,
        value: &str,
        _quoted: bool,
    ) {
        let out = if let Some(target) = out
            .as_prepared::<MongoDBDriver>()
            .and_then(MongoDBPrepared::current_bson)
        {
            *target = Bson::String(String::new());
            let Bson::String(value) = target else {
                unreachable!("It must be a string here");
            };
            value
        } else {
            out.buffer()
        };
        out.push('$');
        out.push_str(value);
    }

    fn write_column_ref(&self, context: &mut Context, out: &mut DynQuery, value: &ColumnRef) {
        self.write_identifier(context, out, &value.name, false);
    }
}
