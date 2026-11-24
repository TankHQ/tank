use crate::ChDBSqlWriter;
use anyhow::anyhow;
use chdb_rust::{QueryParam, QueryParams};
use std::{
    borrow::Cow,
    fmt::{self, Display, Write as _},
    hint, mem,
};
use tank_core::{
    AnyQuery, AsValue, Context, Fragment, Prepared, Result, SqlCoreWriter, SqlValueWriter, Value,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum QueryMode {
    Stream,
    Reader,
    Command,
    Unknown,
    FormatNullError,
}

/// Prepared statement for chDB using native `{pN:Type}` parameter binding.
#[derive(Debug)]
pub struct ChDBPrepared {
    template: String,
    markers: Vec<(usize, usize)>,
    // Per parameter: (name, ClickHouse type, encoded param, bound flag).
    params: Vec<(String, Cow<'static, str>, QueryParam, bool)>,
    sql: AnyQuery,
    mode: QueryMode,
    sql_dirty: bool,
    index: u64,
}

impl ChDBPrepared {
    const WRITER: ChDBSqlWriter = ChDBSqlWriter::chdb();
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    const NULL_TYPE: &'static str = "Nullable(Nothing)";

    pub(crate) fn classify_sql(sql: &str) -> QueryMode {
        let mut tokens = sql
            .trim_end_matches(|c: char| c.is_ascii_whitespace() || c == ';')
            .rsplit(|c: char| c.is_ascii_whitespace());
        if matches!(
            (tokens.next(), tokens.next()),
            (Some(fmt), Some(kw)) if fmt.eq_ignore_ascii_case("null") && kw.eq_ignore_ascii_case("format")
        ) {
            return QueryMode::FormatNullError;
        }
        let first = sql
            .trim_start()
            .split(|c: char| !c.is_ascii_alphabetic())
            .next()
            .unwrap_or_default();
        if first.eq_ignore_ascii_case("select") || first.eq_ignore_ascii_case("with") {
            QueryMode::Stream
        } else if first.eq_ignore_ascii_case("show")
            || first.eq_ignore_ascii_case("describe")
            || first.eq_ignore_ascii_case("desc")
            || first.eq_ignore_ascii_case("explain")
            || first.eq_ignore_ascii_case("exists")
            || first.eq_ignore_ascii_case("check")
        {
            QueryMode::Reader
        } else if first.eq_ignore_ascii_case("insert")
            || first.eq_ignore_ascii_case("create")
            || first.eq_ignore_ascii_case("drop")
            || first.eq_ignore_ascii_case("alter")
            || first.eq_ignore_ascii_case("delete")
            || first.eq_ignore_ascii_case("update")
            || first.eq_ignore_ascii_case("truncate")
            || first.eq_ignore_ascii_case("set")
            || first.eq_ignore_ascii_case("use")
        {
            QueryMode::Command
        } else {
            QueryMode::Unknown
        }
    }

    pub fn new(sql: String) -> Self {
        const SUFFIX: &str = ":Nullable(String)}";
        let mode = Self::classify_sql(&sql);
        let mut markers = Vec::new();
        let mut cursor = 0;
        while let Some(found) = sql[cursor..].find("{p") {
            let begin = cursor + found;
            let Some(end) = sql[begin..].find(SUFFIX) else {
                break;
            };
            let digits = &sql[begin + 2..begin + end];
            if !digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit()) {
                markers.push((begin, begin + end + SUFFIX.len()));
            }
            cursor = begin + end + SUFFIX.len();
        }
        let count = markers.len();
        let params = (0..count)
            .map(|i| {
                (
                    format!("p{i}"),
                    Cow::Borrowed(Self::NULL_TYPE),
                    QueryParam::Null,
                    false,
                )
            })
            .collect();
        Self {
            template: sql,
            markers,
            params,
            sql: AnyQuery::default(),
            mode,
            sql_dirty: count > 0,
            index: 0,
        }
    }

    #[inline(always)]
    pub(crate) const fn mode(&self) -> QueryMode {
        self.mode
    }

    fn param_type(value: &Value<'_>) -> Cow<'static, str> {
        if value.is_null() {
            return Cow::Borrowed(Self::NULL_TYPE);
        }
        match value {
            Value::Boolean(..) => Cow::Borrowed("Nullable(Bool)"),
            Value::Int8(..) => Cow::Borrowed("Nullable(Int8)"),
            Value::Int16(..) => Cow::Borrowed("Nullable(Int16)"),
            Value::Int32(..) => Cow::Borrowed("Nullable(Int32)"),
            Value::Int64(..) => Cow::Borrowed("Nullable(Int64)"),
            Value::Int128(..) => Cow::Borrowed("Nullable(Int128)"),
            Value::UInt8(..) => Cow::Borrowed("Nullable(UInt8)"),
            Value::UInt16(..) => Cow::Borrowed("Nullable(UInt16)"),
            Value::UInt32(..) => Cow::Borrowed("Nullable(UInt32)"),
            Value::UInt64(..) => Cow::Borrowed("Nullable(UInt64)"),
            Value::UInt128(..) => Cow::Borrowed("Nullable(UInt128)"),
            Value::Float32(..) => Cow::Borrowed("Nullable(Float32)"),
            Value::Float64(..) => Cow::Borrowed("Nullable(Float64)"),
            Value::Char(..)
            | Value::Varchar(..)
            | Value::Blob(..)
            | Value::Time(..)
            | Value::Interval(..)
            | Value::Json(..) => Cow::Borrowed("Nullable(String)"),
            Value::Date(..) => Cow::Borrowed("Nullable(Date)"),
            Value::Timestamp(..) | Value::TimestampWithTimezone(..) => {
                Cow::Borrowed("Nullable(DateTime64(9,'UTC'))")
            }
            Value::Uuid(..) => Cow::Borrowed("Nullable(UUID)"),
            Value::IpAddr(..) => Cow::Borrowed("Nullable(IPv6)"),
            other => {
                let mut out = AnyQuery::with_capacity(24);
                out.push_str("Nullable(");
                Self::WRITER.write_column_type(
                    &mut Context::fragment(Fragment::SqlCreateTable),
                    &mut out,
                    other,
                );
                out.push(')');
                Cow::Owned(out.into())
            }
        }
    }

    fn to_query_param(value: Value<'_>) -> QueryParam {
        if value.is_null() {
            return QueryParam::Null;
        }
        let param = 'simple_value: {
            let param = match value {
                Value::Boolean(Some(v)) => QueryParam::Bool(v),
                Value::Int8(Some(v)) => QueryParam::Int64(v as i64),
                Value::Int16(Some(v)) => QueryParam::Int64(v as i64),
                Value::Int32(Some(v)) => QueryParam::Int64(v as i64),
                Value::Int64(Some(v)) => QueryParam::Int64(v),
                Value::UInt8(Some(v)) => QueryParam::UInt64(v as u64),
                Value::UInt16(Some(v)) => QueryParam::UInt64(v as u64),
                Value::UInt32(Some(v)) => QueryParam::UInt64(v as u64),
                Value::UInt64(Some(v)) => QueryParam::UInt64(v),
                Value::Float32(Some(v)) => QueryParam::Float64(v as f64),
                Value::Float64(Some(v)) => QueryParam::Float64(v),
                Value::Varchar(Some(Cow::Owned(s))) => break 'simple_value QueryParam::Text(s),
                Value::Varchar(Some(Cow::Borrowed(s))) => QueryParam::Text(s.to_owned()),
                Value::Char(Some(c)) => QueryParam::Text(c.to_string()),
                Value::Blob(Some(ref bytes)) => {
                    let mut hex = String::with_capacity(bytes.len() * 2);
                    for &b in bytes.as_ref() {
                        hex.push(Self::HEX[(b >> 4) as usize] as char);
                        hex.push(Self::HEX[(b & 0xf) as usize] as char);
                    }
                    let param = QueryParam::Text(hex);
                    if matches!(bytes, Cow::Owned(_)) {
                        break 'simple_value param;
                    }
                    param
                }
                Value::Json(Some(ref j)) => break 'simple_value QueryParam::Text(j.to_string()),
                ref other => {
                    let mut out = AnyQuery::with_capacity(32);
                    Self::WRITER.write_value(
                        &mut Context::fragment(Fragment::None),
                        &mut out,
                        other,
                    );
                    break 'simple_value QueryParam::Raw(out.into());
                }
            };
            // Avoid an opaque Value::drop call for simple values that do not allocate memory.
            mem::forget(value);
            param
        };
        param
    }

    /// Returns the typed SQL text, rebuilding it only when parameter types changed.
    pub(crate) fn sql(&mut self) -> &str {
        if self.markers.is_empty() {
            return &self.template;
        }
        if self.sql_dirty {
            self.sql.clear();
            self.sql.buffer().reserve(self.template.len() + 16);
            let mut cursor = 0;
            for (i, &(begin, end)) in self.markers.iter().enumerate() {
                if begin > end
                    || end > self.template.len()
                    || begin < cursor
                    || !self.template.is_char_boundary(begin)
                    || !self.template.is_char_boundary(end)
                {
                    continue;
                }
                self.sql.push_str(&self.template[cursor..begin]);
                let _ = write!(self.sql, "{{p{i}:{}}}", self.params[i].1);
                cursor = end;
            }
            self.sql.push_str(&self.template[cursor..]);
            self.sql_dirty = false;
        }
        self.sql.buffer().as_str()
    }

    pub(crate) fn has_bound_params(&self) -> bool {
        self.params.iter().any(|p| p.3)
    }

    pub(crate) fn build_query_params(&self) -> QueryParams {
        let mut params = QueryParams::new();
        for (name, _, value, bound) in &self.params {
            if *bound {
                params = params.bind(name.as_str(), value.clone());
            }
        }
        params
    }
}

impl Prepared for ChDBPrepared {
    #[inline(always)]
    fn as_any(self: Box<Self>) -> Box<dyn std::any::Any> {
        self
    }

    #[inline(always)]
    fn is_empty(&self) -> bool {
        false
    }

    #[inline(always)]
    fn clear_bindings(&mut self) -> Result<&mut Self> {
        for param in &mut self.params {
            param.3 = false;
        }
        self.index = 0;
        Ok(self)
    }

    #[inline(always)]
    fn bind(&mut self, value: impl AsValue) -> Result<&mut Self> {
        self.bind_index(value, self.index)
    }

    #[inline(always)]
    fn bind_index(&mut self, value: impl AsValue, index: u64) -> Result<&mut Self> {
        let Some((_, prev_ty, slot, bound)) = self.params.get_mut(index as usize) else {
            hint::cold_path();
            let error = anyhow!(
                "Index {index} cannot be bound, the query has only {} parameters",
                self.markers.len()
            );
            log::error!("{error:#}");
            return Err(error);
        };
        let value = value.as_value();
        let ty = Self::param_type(&value);
        if *prev_ty != ty {
            *prev_ty = ty;
            self.sql_dirty = true;
        }
        *slot = Self::to_query_param(value);
        *bound = true;
        self.index = index + 1;
        Ok(self)
    }
}

impl Display for ChDBPrepared {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ChDBPrepared: {}", self.template)
    }
}
