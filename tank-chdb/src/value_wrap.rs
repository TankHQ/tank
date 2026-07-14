use serde_json::Value as JsonValue;
use std::{borrow::Cow, sync::Arc};
use tank_core::{QueryResult, Result, Row, Value};

fn json_to_tank(value: JsonValue) -> Value {
    match value {
        JsonValue::Null => Value::Null,
        JsonValue::String(v) => Value::Varchar(Some(Cow::Owned(v))),
        other => Value::Json(Some(other)),
    }
}

pub(crate) fn json_compact_to_results(data: &[u8]) -> Result<Vec<QueryResult>> {
    if data.is_empty() {
        return Ok(vec![]);
    }

    let mut lines = data.split(|b| *b == b'\n').filter(|l| !l.is_empty());
    let Some(names_line) = lines.next() else {
        return Ok(vec![]);
    };
    let names_json: JsonValue = serde_json::from_slice(names_line)?;
    let JsonValue::Array(names_arr) = names_json else {
        return Ok(vec![]);
    };
    let labels: Arc<[String]> = names_arr
        .into_iter()
        .map(|v| match v {
            JsonValue::String(s) => s
                .rsplit('.')
                .next()
                .unwrap_or(&s)
                .trim_matches('`')
                .trim_matches('"')
                .to_owned(),
            other => other.to_string(),
        })
        .collect::<Vec<_>>()
        .into();

    let _ = lines.next();

    let mut rows = Vec::new();

    for line in lines {
        let json: JsonValue = serde_json::from_slice(line)?;
        let JsonValue::Array(items) = json else {
            continue;
        };
        let values = labels
            .iter()
            .enumerate()
            .map(|(idx, _)| {
                items
                    .get(idx)
                    .cloned()
                    .map(json_to_tank)
                    .unwrap_or(Value::Null)
            })
            .collect::<Vec<_>>();
        rows.push(QueryResult::Row(Row::new(labels.clone(), values.into())));
    }

    Ok(rows)
}

pub(crate) fn build_chdb_path(url: &url::Url) -> Option<Cow<'static, str>> {
    if let Some(path) = url
        .query_pairs()
        .find_map(|(k, v)| (k.eq_ignore_ascii_case("path") && !v.is_empty()).then_some(v))
    {
        return Some(Cow::Owned(path.to_string()));
    }

    let raw = url.path().trim();
    if raw.is_empty() || raw == "/" {
        None
    } else {
        Some(Cow::Owned(raw.trim_start_matches('/').to_string()))
    }
}
