use anyhow::anyhow;
use std::{
    fmt::{self, Debug, Display, Write as _},
    mem,
};
use tank_core::{
    AsValue, Context, DynQuery, Fragment, Prepared, QueryParam, Result, SqlWriter, Value,
};

/// ClickHouse prepared statement.
///
/// ClickHouse has no wire-level parameter binding: the equivalent is typed query
/// parameters (`{name:Type}`) fed through `SET param_name = ...`. The writer emits
/// provisional `{pN:Nullable(String)}` markers and records their offsets
#[derive(Debug)]
pub struct ClickHousePrepared {
    pub(crate) sql: String,
    pub(crate) markers: Vec<QueryParam>,
    pub(crate) params: Vec<Value>,
    pub(crate) index: u64,
}

impl ClickHousePrepared {
    /// Create a prepared statement for the given SQL and parameter markers.
    pub fn new(sql: String, markers: Vec<QueryParam>) -> Self {
        Self {
            sql,
            markers,
            params: Vec::new(),
            index: 0,
        }
    }

    /// Build what to execute: an optional single `SET param_pN = ...` directive
    /// binding every current value, plus the final query with each marker's type
    /// replaced by the bound value's actual type. Uses the recorded offsets, no
    /// re-scanning.
    pub fn build_sql(&self, writer: &impl SqlWriter) -> Result<(Option<String>, String)> {
        if self.markers.len() < self.params.len() {
            let error = anyhow!("Not enough parameter markers for the bound values");
            log::error!("{error:#}");
            return Err(error);
        }
        let mut directive: Option<String> = None;
        for (i, value) in self.params.iter().enumerate() {
            let line = directive.get_or_insert_with(String::new);
            if i == 0 {
                line.push_str("SET ");
            } else {
                line.push_str(", ");
            }
            let _ = write!(line, "param_p{i} = ");
            if value.is_null() {
                // ClickHouse represents a NULL query parameter as the `'\\N'`
                // sentinel; a plain `NULL` becomes the string "NULL".
                line.push_str("'\\\\N'");
            } else {
                let mut out = DynQuery::with_capacity(16);
                writer.write_value(&mut Context::fragment(Fragment::SqlSelect), &mut out, value);
                line.push_str(out.as_str().as_ref());
            }
        }
        let mut out = String::with_capacity(self.sql.len() + 16);
        let mut cursor = 0;
        for (i, marker) in self.markers.iter().enumerate() {
            if marker.begin > marker.end || marker.end > self.sql.len() {
                continue;
            }
            out.push_str(&self.sql[cursor..marker.begin]);
            let _ = write!(out, "{{p{i}:");
            match self.params.get(i) {
                Some(value) if !value.is_null() => {
                    let mut ty = DynQuery::with_capacity(16);
                    writer.write_column_type(
                        &mut Context::fragment(Fragment::SqlCreateTable),
                        &mut ty,
                        value,
                    );
                    out.push_str("Nullable(");
                    out.push_str(ty.as_str().as_ref());
                    out.push(')');
                }
                // `Nullable(Nothing)` is the type that accepts a NULL parameter
                // against any column type.
                _ => out.push_str("Nullable(Nothing)"),
            }
            out.push('}');
            cursor = marker.end;
        }
        out.push_str(&self.sql[cursor..]);
        Ok((directive, out))
    }

    /// Take the bound parameters, resetting the bind index.
    pub fn take_params(&mut self) -> Vec<Value> {
        self.index = 0;
        mem::take(&mut self.params)
    }
}

impl Prepared for ClickHousePrepared {
    fn as_any(self: Box<Self>) -> Box<dyn std::any::Any> {
        self
    }

    fn clear_bindings(&mut self) -> Result<&mut Self> {
        self.params.clear();
        self.index = 0;
        Ok(self)
    }

    fn bind(&mut self, value: impl AsValue) -> Result<&mut Self> {
        self.bind_index(value, self.index)
    }

    fn bind_index(&mut self, value: impl AsValue, index: u64) -> Result<&mut Self> {
        let count = self.markers.len() as u64;
        if self.params.is_empty() {
            self.params.resize_with(count as _, Default::default);
        }
        let target = self.params.get_mut(index as usize).ok_or_else(|| {
            let error =
                anyhow!("Index {index} cannot be bound, the query has only {count} parameters");
            log::error!("{error:#}");
            error
        })?;
        *target = value.as_value();
        self.index = index + 1;
        Ok(self)
    }
}

impl Display for ClickHousePrepared {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ClickHousePrepared: {}", self.sql)
    }
}
