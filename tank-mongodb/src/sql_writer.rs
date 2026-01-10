use std::collections::BTreeMap;
use tank_core::{ColumnDef, Context, SqlWriter, Entity, Value as TankValue};
use serde_json::{Map as JsonMap, Value as JsonValue};

#[derive(Default)]
pub struct MongoDBSqlWriter {}

fn tank_value_to_json(v: &TankValue) -> JsonValue {
    match v {
        TankValue::Null => JsonValue::Null,
        TankValue::Boolean(Some(b)) => JsonValue::Bool(*b),
        TankValue::Boolean(None) => JsonValue::Null,
        TankValue::Int32(Some(i)) => JsonValue::Number((*i).into()),
        TankValue::Int64(Some(i)) => JsonValue::Number((*i).into()),
        TankValue::Int8(Some(i)) => JsonValue::Number((*i as i32).into()),
        TankValue::Int16(Some(i)) => JsonValue::Number((*i as i32).into()),
        TankValue::Varchar(Some(s)) => JsonValue::String(s.to_string()),
        TankValue::Char(Some(c)) => JsonValue::String(c.to_string()),
        TankValue::Float32(Some(f)) => JsonValue::Number(serde_json::Number::from_f64(*f as f64).unwrap_or_default()),
        TankValue::Float64(Some(f)) => JsonValue::Number(serde_json::Number::from_f64(*f).unwrap_or_default()),
        TankValue::Json(Some(j)) => j.clone(),
        other => JsonValue::String(format!("{:?}", other)),
    }
}

impl SqlWriter for MongoDBSqlWriter {
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
            .find_map(|(k, v)| if *k == "mongodb" { Some(v) } else { None })
        {
            out.push_str(t);
        }
    }

    fn write_create_table<E>(&self, out: &mut RawQuery, _if_not_exists: bool)
    where
        Self: Sized,
        E: Entity,
    {
        let name = E::table().full_name();
        if !out.is_empty() {
            out.push('\n');
        }
        out.push_str(&format!("MONGO:CREATE_COLLECTION {name};"));
    }

    fn write_drop_table<E>(&self, out: &mut RawQuery, _if_exists: bool)
    where
        Self: Sized,
        E: Entity,
    {
        let name = E::table().full_name();
        if !out.is_empty() {
            out.push('\n');
        }
        out.push_str(&format!("MONGO:DROP_COLLECTION {name};"));
    }

    fn write_create_schema<E>(&self, out: &mut RawQuery, _if_not_exists: bool)
    where
        Self: Sized,
        E: Entity,
    {
        let schema = E::table().schema().to_string();
        if !schema.is_empty() {
            if !out.is_empty() {
                out.push('\n');
            }
            out.push_str(&format!("MONGO:CREATE_DATABASE {schema};"));
        }
    }

    fn write_drop_schema<E>(&self, out: &mut RawQuery, _if_exists: bool)
    where
        Self: Sized,
        E: Entity,
    {
        let schema = E::table().schema().to_string();
        if !schema.is_empty() {
            if !out.is_empty() {
                out.push('\n');
            }
            out.push_str(&format!("MONGO:DROP_DATABASE {schema};"));
        }
    }

    fn write_insert<'b, E>(&self, out: &mut RawQuery, entities: impl IntoIterator<Item = &'b E>, _update: bool)
    where
        Self: Sized,
        E: Entity + 'b,
    {
        let mut docs = Vec::<JsonValue>::new();
        for ent in entities.into_iter() {
            let row = ent.row_filtered();
            let mut map = JsonMap::new();
            for (k, v) in row.into_iter() {
                map.insert(k.to_string(), tank_value_to_json(&v));
            }
            docs.push(JsonValue::Object(map));
        }
        if docs.is_empty() {
            return;
        }
        let name = E::table().full_name();
        let payload = if docs.len() == 1 { docs.into_iter().next().unwrap() } else { JsonValue::Array(docs) };
        if !out.is_empty() {
            out.push('\n');
        }
        out.push_str(&format!("MONGO:INSERT {} {};", name, payload.to_string()));
    }
}
