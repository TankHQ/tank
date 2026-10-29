use anyhow::{anyhow, ensure};
use serde_json::Value as JsonValue;
use std::borrow::Cow;
use tank_core::{
    ErrorContext, Result, RowLabels, RowOwned, RowsAffected, Value, ValueOwned,
};

pub(crate) enum ParsedChunk {
    Rows(std::vec::IntoIter<RowOwned>),
    Affected(RowsAffected),
}

fn json_to_tank(value: JsonValue) -> ValueOwned {
    match value {
        JsonValue::Null => Value::Null,
        JsonValue::String(v) => Value::Varchar(Some(Cow::Owned(v))),
        other => Value::Json(Some(other)),
    }
}

pub(crate) fn parse_result(data: &[u8]) -> Result<ParsedChunk> {
    if data.iter().all(u8::is_ascii_whitespace) {
        return Ok(ParsedChunk::Affected(Default::default()));
    }
    let mut response: JsonValue = serde_json::from_slice(data)?;
    let meta = response["meta"]
        .as_array()
        .context("Missing chDB JSON meta schema")?;
    let keys = meta
        .iter()
        .map(|column| {
            column["type"]
                .as_str()
                .context("Missing chDB column type")?;
            column["name"]
                .as_str()
                .map(str::to_owned)
                .context("Missing chDB column name")
        })
        .collect::<Result<Vec<_>>>()?;
    let labels: RowLabels = keys
        .iter()
        .map(|key| {
            key.rsplit('.')
                .next()
                .unwrap_or(key)
                .trim_matches('`')
                .trim_matches('"')
                .to_owned()
        })
        .collect();
    let rows = response["data"]
        .as_array_mut()
        .context("Missing chDB JSON data")?
        .iter_mut()
        .map(|row| {
            let values = match row {
                JsonValue::Array(values) => {
                    ensure!(
                        values.len() == keys.len(),
                        "ChDB JSON row width differs from schema"
                    );
                    std::mem::take(values)
                        .into_iter()
                        .map(json_to_tank)
                        .collect()
                }
                JsonValue::Object(row) => keys
                    .iter()
                    .map(|key| {
                        row.remove(key)
                            .map(json_to_tank)
                            .with_context(|| format!("Missing chDB column {key}"))
                    })
                    .collect::<Result<_>>()?,
                _ => return Err(anyhow!("Expected chDB JSON row array or object")),
            };
            Ok(RowOwned::new(labels.clone(), values))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(ParsedChunk::Rows(rows.into_iter()))
}
