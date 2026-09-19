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

pub(crate) struct JsonRowParser {
    pending: Vec<u8>,
    labels: Option<Arc<[String]>>,
    object_keys: Option<Arc<[String]>>,
    header_lines: u8,
    compact: bool,
}

impl JsonRowParser {
    pub(crate) fn new() -> Self {
        Self {
            pending: Vec::new(),
            labels: None,
            object_keys: None,
            header_lines: 0,
            compact: false,
        }
    }

    pub(crate) fn push<F>(&mut self, mut data: &[u8], mut send: F) -> Result<()>
    where
        F: FnMut(QueryResult),
    {
        while let Some(index) = data.iter().position(|byte| *byte == b'\n') {
            let line = &data[..index];
            if self.pending.is_empty() {
                self.parse_line(line, &mut send)?;
            } else {
                self.pending.extend_from_slice(line);
                let pending = std::mem::take(&mut self.pending);
                let result = self.parse_line(&pending, &mut send);
                self.pending = pending;
                self.pending.clear();
                result?;
            }
            data = &data[index + 1..];
        }
        if !data.is_empty() {
            self.pending.extend_from_slice(data);
        }
        Ok(())
    }

    pub(crate) fn finish<F>(&mut self, mut send: F) -> Result<()>
    where
        F: FnMut(QueryResult),
    {
        if self.pending.is_empty() {
            return Ok(());
        }
        let pending = std::mem::take(&mut self.pending);
        self.parse_line(&pending, &mut send)
    }

    fn parse_line<F>(&mut self, line: &[u8], send: &mut F) -> Result<()>
    where
        F: FnMut(QueryResult),
    {
        if line.is_empty() {
            return Ok(());
        }
        if self.header_lines == 0 {
            let json: JsonValue = serde_json::from_slice(line)?;
            match json {
                JsonValue::Array(names_arr) => {
                    self.labels = Some(
                        names_arr
                            .into_iter()
                            .map(|value| match value {
                                JsonValue::String(name) => name
                                    .rsplit('.')
                                    .next()
                                    .unwrap_or(&name)
                                    .trim_matches('`')
                                    .trim_matches('"')
                                    .to_owned(),
                                other => other.to_string(),
                            })
                            .collect::<Vec<_>>()
                            .into(),
                    );
                    self.object_keys = self.labels.clone();
                    self.header_lines = 1;
                    self.compact = true;
                    return Ok(());
                }
                JsonValue::Object(items) => {
                    let keys = items.keys().cloned().collect::<Vec<_>>();
                    self.labels = Some(
                        keys.iter()
                            .map(|key| normalize_label(key))
                            .collect::<Vec<_>>()
                            .into(),
                    );
                    self.object_keys = Some(keys.into());
                    self.header_lines = 2;
                    self.emit_object(&items, send);
                    return Ok(());
                }
                _ => {
                    self.header_lines = 2;
                    return Ok(());
                }
            }
        }
        if self.compact && self.header_lines == 1 {
            self.header_lines = 2;
            return Ok(());
        }

        let Some(labels) = &self.labels else {
            return Ok(());
        };
        let json: JsonValue = serde_json::from_slice(line)?;
        if self.compact {
            let JsonValue::Array(items) = json else {
                return Ok(());
            };
            let values = labels
                .iter()
                .enumerate()
                .map(|(index, _)| {
                    items
                        .get(index)
                        .cloned()
                        .map(json_to_tank)
                        .unwrap_or(Value::Null)
                })
                .collect::<Vec<_>>();
            send(QueryResult::Row(Row::new(labels.clone(), values.into())));
        } else if let JsonValue::Object(items) = json {
            self.emit_object(&items, send);
        }
        Ok(())
    }

    fn emit_object<F>(&mut self, items: &serde_json::Map<String, JsonValue>, send: &mut F)
    where
        F: FnMut(QueryResult),
    {
        let Some(labels) = &self.labels else {
            return;
        };
        let Some(keys) = &self.object_keys else {
            return;
        };
        let values = labels
            .iter()
            .zip(keys.iter())
            .map(|(_, key)| {
                items
                    .get(key)
                    .cloned()
                    .map(json_to_tank)
                    .unwrap_or(Value::Null)
            })
            .collect::<Vec<_>>();
        send(QueryResult::Row(Row::new(labels.clone(), values.into())));
    }
}

fn normalize_label(name: &str) -> String {
    name.rsplit('.')
        .next()
        .unwrap_or(name)
        .trim_matches('`')
        .trim_matches('"')
        .to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_rows_split_across_chunks() {
        let mut parser = JsonRowParser::new();
        let mut rows = Vec::new();
        parser
            .push(b"[\"a\",\"b\"]\n[\"UInt8\",\"String\"]\n[1,\"hel", |row| {
                rows.push(row)
            })
            .unwrap();
        parser
            .push(b"lo\"]\n[2, null]\n", |row| rows.push(row))
            .unwrap();
        parser.finish(|row| rows.push(row)).unwrap();

        assert_eq!(rows.len(), 2);
        let QueryResult::Row(row) = &rows[0] else {
            panic!("expected row");
        };
        assert_eq!(row.labels.as_ref(), ["a", "b"]);
        assert_eq!(row.values[0], Value::Json(Some(serde_json::json!(1))));
        assert_eq!(row.values[1], Value::Varchar(Some(Cow::Borrowed("hello"))));
    }
}
