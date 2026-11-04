use crate::MSSQLSqlWriter;
use anyhow::anyhow;
use std::borrow::Cow;
use tank_core::{
    Context, DynQuery, Error, Fragment, Result, RowLabels, RowOwned, SqlWriter, Value, ValueOwned,
    decimal_from_scaled,
};
use tiberius::{ColumnData, FromSql, IntoSql, Row, ToSql, numeric::Numeric};
use time::{Date, OffsetDateTime, PrimitiveDateTime, Time, UtcOffset};

pub(crate) struct ValueWrap<'a>(pub(crate) Cow<'a, Value<'a>>);

pub(crate) struct SqlParam(pub(crate) ColumnData<'static>);

impl<'a> IntoSql<'a> for SqlParam {
    fn into_sql(self) -> ColumnData<'a> {
        self.0
    }
}

impl<'a> ValueWrap<'a> {
    pub(crate) fn from_column_data(data: ColumnData<'static>) -> Result<ValueOwned> {
        Ok(match data {
            ColumnData::Bit(v) => Value::Boolean(v),
            ColumnData::U8(v) => Value::UInt8(v),
            ColumnData::I16(v) => Value::Int16(v),
            ColumnData::I32(v) => Value::Int32(v),
            ColumnData::I64(v) => Value::Int64(v),
            ColumnData::F32(v) => Value::Float32(v),
            ColumnData::F64(v) => Value::Float64(v),
            ColumnData::Guid(v) => Value::Uuid(v),
            ColumnData::Numeric(None) => Value::Decimal(None, 0, 0),
            ColumnData::Numeric(Some(n)) => {
                let dec = decimal_from_scaled(n.value(), n.scale() as u32)?;
                Value::Decimal(Some(dec), n.precision(), dec.scale() as u8)
            }
            ColumnData::Binary(v) => Value::Blob(v),
            ColumnData::Date(..) => Value::Date(Date::from_sql(&data)?),
            ColumnData::Time(..) => Value::Time(Time::from_sql(&data)?),
            ColumnData::DateTime(..)
            | ColumnData::SmallDateTime(..)
            | ColumnData::DateTime2(..) => Value::Timestamp(PrimitiveDateTime::from_sql(&data)?),
            ColumnData::DateTimeOffset(..) => {
                Value::TimestampWithTimezone(OffsetDateTime::from_sql(&data)?)
            }
            ColumnData::Xml(v) => {
                Value::Varchar(v.map(|x| Cow::Owned(x.into_owned().into_string())))
            }
            ColumnData::String(None) => Value::Varchar(None),
            ColumnData::String(Some(s)) => {
                if matches!(s.as_bytes().first(), Some(b'[' | b'{'))
                    && let Ok(json @ (serde_json::Value::Array(..) | serde_json::Value::Object(..))) =
                        serde_json::from_str::<serde_json::Value>(&s)
                {
                    Value::Json(Some(json))
                } else {
                    Value::Varchar(Some(s))
                }
            }
        })
    }

    pub(crate) fn from_row(row: Row, labels: Option<&RowLabels>) -> Result<RowOwned> {
        let names = match labels {
            Some(labels) => labels.clone(),
            None => row.columns().iter().map(|c| c.name().to_owned()).collect(),
        };
        let values = row
            .into_iter()
            .map(Self::from_column_data)
            .collect::<Result<Vec<_>>>()?;
        Ok(RowOwned::new(names, values))
    }
}

impl<'a> From<&'a Value<'a>> for ValueWrap<'a> {
    fn from(value: &'a Value<'a>) -> Self {
        Self(Cow::Borrowed(value))
    }
}

impl<'a> From<Value<'a>> for ValueWrap<'a> {
    fn from(value: Value<'a>) -> Self {
        Self(Cow::Owned(value))
    }
}

impl<'a> TryFrom<ValueWrap<'a>> for ColumnData<'static> {
    type Error = Error;

    fn try_from(value: ValueWrap<'a>) -> Result<Self> {
        Ok(match value.0.into_owned() {
            Value::Null => ColumnData::I32(None),
            Value::Boolean(v) => ColumnData::Bit(v),
            Value::Int8(v) => ColumnData::I16(v.map(i16::from)),
            Value::Int16(v) => ColumnData::I16(v),
            Value::Int32(v) => ColumnData::I32(v),
            Value::Int64(v) => ColumnData::I64(v),
            Value::Int128(None) => ColumnData::Numeric(None),
            Value::Int128(Some(v)) => ColumnData::Numeric(Some(Numeric::new_with_scale(*v, 0))),
            Value::UInt8(v) => ColumnData::U8(v),
            Value::UInt16(v) => ColumnData::I32(v.map(i32::from)),
            Value::UInt32(v) => ColumnData::I64(v.map(i64::from)),
            Value::UInt64(None) => ColumnData::Numeric(None),
            Value::UInt64(Some(v)) => {
                ColumnData::Numeric(Some(Numeric::new_with_scale(v as i128, 0)))
            }
            Value::UInt128(None) => ColumnData::Numeric(None),
            Value::UInt128(Some(v)) => {
                let v = i128::try_from(*v)
                    .map_err(|_| anyhow!("u128 value {v} is out of range for SQL Server"))?;
                ColumnData::Numeric(Some(Numeric::new_with_scale(v, 0)))
            }
            Value::Float32(v) => ColumnData::F32(v),
            Value::Float64(v) => ColumnData::F64(v),
            Value::Decimal(None, ..) => ColumnData::Numeric(None),
            Value::Decimal(Some(v), ..) => match v.to_sql() {
                ColumnData::Numeric(n) => ColumnData::Numeric(n),
                _ => unreachable!(),
            },
            Value::Char(None) => ColumnData::String(None),
            Value::Char(Some(v)) => ColumnData::String(Some(Cow::Owned(v.to_string()))),
            Value::Varchar(None) => ColumnData::String(None),
            Value::Varchar(Some(v)) => ColumnData::String(Some(Cow::Owned(v.into_owned()))),
            Value::Blob(None) => ColumnData::Binary(None),
            Value::Blob(Some(v)) => ColumnData::Binary(Some(Cow::Owned(v.into_owned()))),
            Value::Date(None) => ColumnData::Date(None),
            Value::Date(Some(v)) => {
                if v.year() < 1 {
                    return Err(anyhow!("Date {v} is out of range for SQL Server"));
                }
                match v.to_sql() {
                    ColumnData::Date(d) => ColumnData::Date(d),
                    _ => unreachable!(),
                }
            }
            Value::Time(None) => ColumnData::Time(None),
            Value::Time(Some(v)) => match v.to_sql() {
                ColumnData::Time(t) => ColumnData::Time(t),
                _ => unreachable!(),
            },
            Value::Timestamp(None) => ColumnData::DateTime2(None),
            Value::Timestamp(Some(v)) => {
                if v.year() < 1 {
                    return Err(anyhow!("Timestamp {v} is out of range for SQL Server"));
                }
                match v.to_sql() {
                    ColumnData::DateTime2(dt) => ColumnData::DateTime2(dt),
                    _ => unreachable!(),
                }
            }
            Value::TimestampWithTimezone(None) => ColumnData::DateTimeOffset(None),
            Value::TimestampWithTimezone(Some(v)) => {
                if v.to_offset(UtcOffset::UTC).year() < 1 {
                    return Err(anyhow!("Timestamp {v} is out of range for SQL Server"));
                }
                match v.to_sql() {
                    ColumnData::DateTimeOffset(dto) => ColumnData::DateTimeOffset(dto),
                    _ => unreachable!(),
                }
            }
            Value::Interval(None) => ColumnData::String(None),
            Value::Interval(Some(v)) => {
                let mut out = DynQuery::default();
                MSSQLSqlWriter::default().write_interval(
                    &mut Context::new(Fragment::ParameterBinding, false),
                    &mut out,
                    &v,
                );
                ColumnData::String(Some(Cow::Owned(out.into())))
            }
            Value::Uuid(v) => ColumnData::Guid(v),
            Value::Json(None) => ColumnData::String(None),
            Value::Json(Some(v)) => ColumnData::String(Some(Cow::Owned(v.to_string()))),
            Value::Unknown(None) => ColumnData::String(None),
            Value::Unknown(Some(v)) => ColumnData::String(Some(Cow::Owned(v.into_owned()))),
            v @ (Value::Array(..) | Value::List(..) | Value::Map(..) | Value::Struct(..)) => {
                if v.is_null() {
                    ColumnData::String(None)
                } else {
                    let mut out = DynQuery::default();
                    MSSQLSqlWriter::default().write_value(
                        &mut Context::new(Fragment::Json, false),
                        &mut out,
                        &v,
                    );
                    ColumnData::String(Some(Cow::Owned(out.into())))
                }
            }
        })
    }
}
