use crate::prepared::{SelectPayload, ValkeyPrepared, Payload};
use crate::visitor::KeyValueVisitor;
use std::mem;
use tank_core::{
    dataset::Dataset,
    expression::{Expression, ExpressionVisitor},
    query::SelectQuery,
    writer::{Context, SqlWriter},
    DynQuery, TableRef, Value,
    column::PrimaryKeyType,
    visitor::{Visitor, VisitorMut},
};

pub struct ValkeySqlWriter {}

impl SqlWriter for ValkeySqlWriter {
    fn as_dyn(&self) -> &dyn SqlWriter {
        self
    }

    fn write_select<'a, Data>(&self, out: &mut DynQuery, query: &impl SelectQuery<Data>)
    where
        Self: Sized,
        Data: Dataset + 'a,
    {
        let (Some(table), where_expr) = (query.get_from(), query.get_where()) else {
            log::error!("The query does not have the FROM clause");
            return;
        };
        let table = table.table_ref();
        if table.name.is_empty() {
            log::error!("The table is not specified for Valkey");
            return;
        }

        let mut columns = Vec::new();
        for column in query.get_select() {
             let mut ctx = Context::default();
             let name = column.as_identifier(&mut ctx);
             columns.push(name);
        }

        // Extract Primary Keys
        let pk_columns: Vec<_> = table.columns.iter()
            .filter(|c| matches!(c.primary_key, PrimaryKeyType::PrimaryKey | PrimaryKeyType::PartOfPrimaryKey))
            .collect();

        let mut key_visitor = KeyValueVisitor::default();
        if let Some(expr) = where_expr {
            let mut ctx = Context::default();
            let mut dummy_out = DynQuery::String(String::new());
            expr.accept_visitor(&mut key_visitor, self, &mut ctx, &mut dummy_out);
        }

        let mut exact_key = true;
        let mut built_key = format!("{}:{}", table.schema, table.name);

        // Check if we have all PK parts
        if pk_columns.is_empty() {
            // No PK defined on table? Cannot use key lookup.
            exact_key = false;
        } else {
            for pk in pk_columns {
                if let Some(val) = key_visitor.values.get(pk.name()) {
                    let val_str = value_to_key_component(val);
                    built_key.push_str(":");
                    built_key.push_str(&val_str);
                } else {
                    // Start wildcard matching?
                    // User requirement: "Only supports simple field: value patterns. And only for primary key".
                    // Implies strict equality support.
                    exact_key = false;
                    break;
                }
            }
        }

        let select_payload = SelectPayload {
            table,
            columns,
            key_prefix: built_key,
            key_suffix: None,
            exact_key,
        };

        if let Some(prepared) = out.as_prepared::<crate::ValkeyDriver>() {
             prepared.payload = Payload::Select(Box::new(select_payload));
        } else {
             log::error!("ValkeySqlWriter: Output query is not a ValkeyPrepared");
        }
    }

    fn write_value(&self, _context: &mut Context, _out: &mut DynQuery, _value: &Value) {}
    fn write_column_ref(&self, _context: &mut Context, _out: &mut DynQuery, _value: &tank_core::column::ColumnRef) {}
    fn write_identifier(&self, _context: &mut Context, _out: &mut DynQuery, _name: &str, _quoted: bool) {}
    fn write_value_none(&self, _context: &mut Context, _out: &mut DynQuery) {}
}

fn value_to_key_component(v: &Value) -> String {
    match v {
        Value::Boolean(Some(b)) => b.to_string(),
        Value::Int8(Some(i)) => i.to_string(),
        Value::Int16(Some(i)) => i.to_string(),
        Value::Int32(Some(i)) => i.to_string(),
        Value::Int64(Some(i)) => i.to_string(),
        Value::UInt8(Some(i)) => i.to_string(),
        Value::UInt16(Some(i)) => i.to_string(),
        Value::UInt32(Some(i)) => i.to_string(),
        Value::UInt64(Some(i)) => i.to_string(),
        Value::Float32(Some(f)) => f.to_string(),
        Value::Float64(Some(f)) => f.to_string(),
        Value::Varchar(Some(s)) => s.to_string(),
        Value::Char(Some(c)) => c.to_string(),
        Value::Uuid(Some(u)) => u.to_string(),
        // Fallback for others or None
        _ => "".to_string(),
    }
}
