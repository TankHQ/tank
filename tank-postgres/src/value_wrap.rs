use crate::{
    interval_wrap::IntervalWrap,
    util::{extract_value, flatten_array},
};
use bytes::BytesMut;
use postgres_protocol::types::array_to_sql;
use postgres_types::{FromSql, IsNull, ToSql, Type, to_sql_checked};
use rust_decimal::{Decimal, prelude::FromPrimitive};
use std::{borrow::Cow, error::Error};
use tank_core::Value;

#[derive(Clone, Debug)]
pub(crate) struct ValueWrap<'a>(pub(crate) Cow<'a, Value>);

impl<'a> ValueWrap<'a> {
    pub fn take_value(self) -> Value {
        match self.0 {
            Cow::Borrowed(v) => v.clone(),
            Cow::Owned(v) => v,
        }
    }
}

impl<'a> Default for ValueWrap<'a> {
    fn default() -> Self {
        Self(Cow::Borrowed(&Value::Null))
    }
}

impl<'a> From<&'a Value> for ValueWrap<'a> {
    fn from(value: &'a Value) -> Self {
        ValueWrap(Cow::Borrowed(value))
    }
}

impl<'a> From<Value> for ValueWrap<'a> {
    fn from(value: Value) -> Self {
        ValueWrap(Cow::Owned(value))
    }
}

impl<'a> FromSql<'a> for ValueWrap<'a> {
    fn from_sql(ty: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn Error + Sync + Send>> {
        Self::from_sql_nullable(ty, Some(raw))
    }
    fn from_sql_null(ty: &Type) -> Result<Self, Box<dyn Error + Sync + Send>> {
        Self::from_sql_nullable(ty, None)
    }
    fn from_sql_nullable(
        ty: &Type,
        raw: Option<&'a [u8]>,
    ) -> Result<Self, Box<dyn Error + Sync + Send>> {
        extract_value(ty, raw).map(Into::into)
    }

    fn accepts(_ty: &Type) -> bool {
        true
    }
}

impl<'a> ToSql for ValueWrap<'a> {
    fn to_sql(&self, ty: &Type, out: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>>
    where
        Self: Sized,
    {
        match &self.0.as_ref() {
            Value::Null => None::<String>.to_sql(ty, out),
            Value::Boolean(v) => v.to_sql(ty, out),
            Value::Int8(v) => v.map(|v| v as i16).to_sql(ty, out),
            Value::Int16(v) => v.to_sql(ty, out),
            Value::Int32(v) => v.to_sql(ty, out),
            Value::Int64(v) => v.to_sql(ty, out),
            Value::Int128(v) => v.map(|v| Decimal::from_i128(v)).to_sql(ty, out),
            Value::UInt8(v) => v.map(|v| v as i16).to_sql(ty, out),
            Value::UInt16(v) => v.map(|v| v as i32).to_sql(ty, out),
            Value::UInt32(v) => v.map(|v| v as i64).to_sql(ty, out),
            Value::UInt64(v) => v.map(|v| Decimal::from_u64(v)).to_sql(ty, out),
            Value::UInt128(v) => v.map(|v| Decimal::from_u128(v)).to_sql(ty, out),
            Value::Float32(v) => v.to_sql(ty, out),
            Value::Float64(v) => v.to_sql(ty, out),
            Value::Decimal(v, _, _) => v.to_sql(ty, out),
            Value::Char(v) => v.map(|v| v.to_string()).to_sql(ty, out),
            Value::Varchar(v) => v.to_sql(ty, out),
            Value::Blob(v) => v.as_deref().to_sql(ty, out),
            Value::Date(v) => v.to_sql(ty, out),
            Value::Time(v) => v.to_sql(ty, out),
            Value::Timestamp(v) => v.to_sql(ty, out),
            Value::TimestampWithTimezone(v) => v.to_sql(ty, out),
            Value::Interval(v) => v.map(IntervalWrap).to_sql(ty, out),
            Value::Uuid(v) => v.to_sql(ty, out),
            Value::Array(v, element, ..) => match v {
                Some(v) => {
                    let (vector, dimensions, element_type) = flatten_array(&**v, element);
                    array_to_sql(
                        dimensions,
                        element_type.oid(),
                        vector.into_iter().map(|v| ValueWrap(Cow::Borrowed(v))),
                        |e, w| match e.to_sql(&element_type, w)? {
                            IsNull::No => Ok(postgres_protocol::IsNull::No),
                            IsNull::Yes => Ok(postgres_protocol::IsNull::Yes),
                        },
                        out,
                    )?;
                    Ok(IsNull::No)
                }
                None => None::<Vec<ValueWrap>>.to_sql(ty, out),
            },
            Value::List(v, element) => match v {
                Some(v) => {
                    let (vector, dimensions, element_type) = flatten_array(v.as_slice(), element);
                    array_to_sql(
                        dimensions,
                        element_type.oid(),
                        vector.into_iter().map(|v| ValueWrap(Cow::Borrowed(v))),
                        |e, w| match e.to_sql(&element_type, w)? {
                            IsNull::No => Ok(postgres_protocol::IsNull::No),
                            IsNull::Yes => Ok(postgres_protocol::IsNull::Yes),
                        },
                        out,
                    )?;
                    Ok(IsNull::No)
                }
                None => None::<Vec<ValueWrap>>.to_sql(ty, out),
            },
            _ => {
                return Err(tank_core::Error::msg(format!(
                    "tank::Value variant `{:?}` is not supported by Postgres",
                    &self.0
                ))
                .into());
            }
        }
    }

    fn accepts(_ty: &Type) -> bool
    where
        Self: Sized,
    {
        true
    }

    to_sql_checked!();
}
