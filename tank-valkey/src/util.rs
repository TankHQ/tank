use std::borrow::Cow;
use tank_core::TableRef;

pub fn table_to_key(table: &TableRef) -> Cow<'static, str> {
    if !table.alias.is_empty() {
        return table.alias.clone();
    }
    let mut key = table.name.clone();
    if !table.schema.is_empty() {
        key = format!("{}:{}", table.schema, key).into();
    }
    key
}
