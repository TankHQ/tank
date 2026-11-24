use crate::ChDBSqlWriter;
use anyhow::anyhow;
use std::{
    borrow::Cow,
    fmt::{self, Display, Write as _},
    hint, mem,
};
use tank_core::{
    AnyQuery, AsValue, Context, Fragment, Prepared, Result, SqlCoreWriter, SqlValueWriter, Value,
};

/// Prepared statement for chDB using native `{pN:Type}` parameter binding.
///
/// Reuses per-parameter string buffers and caches the typed SQL text across
/// executions whenever the bound parameter types stay the same.
#[derive(Debug)]
pub struct ChDBPrepared {
    template: String,
    markers: Vec<(usize, usize)>,
    names: Vec<String>,
    types: Vec<Cow<'static, str>>,
    values: Vec<String>,
    bound: Vec<bool>,
    sql: AnyQuery,
    sql_dirty: bool,
    index: u64,
}

impl ChDBPrepared {
    const WRITER: ChDBSqlWriter = ChDBSqlWriter::chdb();
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    const NULL_TYPE: &'static str = "Nullable(Nothing)";

    pub fn new(sql: String) -> Self {
        let markers = Self::scan_markers(&sql);
        let count = markers.len();
        let names = (0..count).map(|i| format!("p{i}")).collect();
        let types = vec![Cow::Borrowed(Self::NULL_TYPE); count];
        let values = (0..count).map(|_| String::new()).collect();
        let bound = vec![false; count];
        Self {
            template: sql,
            markers,
            names,
            types,
            values,
            bound,
            sql: AnyQuery::default(),
            sql_dirty: count > 0,
            index: 0,
        }
    }

    fn scan_markers(sql: &str) -> Vec<(usize, usize)> {
        const SUFFIX: &str = ":Nullable(String)}";
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
        markers
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

    fn write_escaped(dst: &mut String, value: &str) {
        let bytes = value.as_bytes();
        let mut start = 0;
        for (i, &b) in bytes.iter().enumerate() {
            let esc = match b {
                b'\\' => "\\\\",
                b'\t' => "\\t",
                b'\n' => "\\n",
                b'\r' => "\\r",
                0 => "\\0",
                _ => continue,
            };
            dst.push_str(&value[start..i]);
            dst.push_str(esc);
            start = i + 1;
        }
        dst.push_str(&value[start..]);
    }

    fn write_param(dst: &mut String, value: Value<'_>) {
        dst.clear();
        if value.is_null() {
            dst.push_str("\\N");
            return;
        }
        match value {
            Value::Boolean(Some(v)) => {
                dst.push_str(if v { "true" } else { "false" });
                mem::forget(value);
            }
            Value::Int8(Some(v)) => {
                let _ = write!(dst, "{v}");
                mem::forget(value);
            }
            Value::Int16(Some(v)) => {
                let _ = write!(dst, "{v}");
                mem::forget(value);
            }
            Value::Int32(Some(v)) => {
                let _ = write!(dst, "{v}");
                mem::forget(value);
            }
            Value::Int64(Some(v)) => {
                let _ = write!(dst, "{v}");
                mem::forget(value);
            }
            Value::UInt8(Some(v)) => {
                let _ = write!(dst, "{v}");
                mem::forget(value);
            }
            Value::UInt16(Some(v)) => {
                let _ = write!(dst, "{v}");
                mem::forget(value);
            }
            Value::UInt32(Some(v)) => {
                let _ = write!(dst, "{v}");
                mem::forget(value);
            }
            Value::UInt64(Some(v)) => {
                let _ = write!(dst, "{v}");
                mem::forget(value);
            }
            Value::Float32(Some(v)) => {
                let _ = write!(dst, "{v}");
                mem::forget(value);
            }
            Value::Float64(Some(v)) => {
                let _ = write!(dst, "{v}");
                mem::forget(value);
            }
            Value::Varchar(Some(ref s)) => {
                Self::write_escaped(dst, s);
                if matches!(s, Cow::Borrowed(_)) {
                    mem::forget(value);
                }
            }
            Value::Char(Some(c)) => {
                let mut buf = [0u8; 4];
                Self::write_escaped(dst, c.encode_utf8(&mut buf));
                mem::forget(value);
            }
            Value::Blob(Some(ref bytes)) => {
                dst.reserve(bytes.len() * 2);
                for &b in bytes.as_ref() {
                    dst.push(Self::HEX[(b >> 4) as usize] as char);
                    dst.push(Self::HEX[(b & 0xf) as usize] as char);
                }
                if matches!(bytes, Cow::Borrowed(_)) {
                    mem::forget(value);
                }
            }
            Value::Json(Some(ref j)) => {
                let mut tmp = String::new();
                let _ = write!(tmp, "{j}");
                Self::write_escaped(dst, &tmp);
            }
            other => {
                let mut out = AnyQuery::new(mem::take(dst));
                Self::WRITER.write_value(&mut Context::fragment(Fragment::None), &mut out, &other);
                *dst = out.into();
            }
        }
    }

    /// Returns the typed SQL text and bound `(name, value)` pairs for FFI execution.
    pub(crate) fn prepare_for_exec(&mut self) -> (&str, &[String], &[String], &[bool]) {
        if self.markers.is_empty() {
            return (&self.template, &[], &[], &[]);
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
                let _ = write!(self.sql, "{{p{i}:{}}}", self.types[i]);
                cursor = end;
            }
            self.sql.push_str(&self.template[cursor..]);
            self.sql_dirty = false;
        }
        (self.sql.buffer().as_str(), &self.names, &self.values, &self.bound)
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
        self.bound.fill(false);
        self.index = 0;
        Ok(self)
    }

    #[inline(always)]
    fn bind(&mut self, value: impl AsValue) -> Result<&mut Self> {
        self.bind_index(value, self.index)
    }

    #[inline(always)]
    fn bind_index(&mut self, value: impl AsValue, index: u64) -> Result<&mut Self> {
        let idx = index as usize;
        let Some(slot) = self.values.get_mut(idx) else {
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
        if let Some(prev) = self.types.get_mut(idx)
            && *prev != ty
        {
            *prev = ty;
            self.sql_dirty = true;
        }
        Self::write_param(slot, value);
        if let Some(b) = self.bound.get_mut(idx) {
            *b = true;
        }
        self.index = index + 1;
        Ok(self)
    }
}

impl Display for ChDBPrepared {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ChDBPrepared: {}", self.template)
    }
}
