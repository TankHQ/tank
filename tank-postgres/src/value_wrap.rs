use crate::{
    interval_wrap::IntervalWrap,
    util::{extract_value, flatten_array},
};
use bytes::BytesMut;
use postgres_protocol::types::array_to_sql;
use postgres_types::{FromSql, IsNull, ToSql, Type, to_sql_checked};
use rust_decimal::{Decimal, prelude::FromPrimitive};
use std::error::Error;
use tank_core::Value;

#[derive(Debug, Default, Clone)]
pub(crate) struct ValueWrap(pub(crate) Value);

impl From<Value> for ValueWrap {
    fn from(value: Value) -> Self {
        ValueWrap(value)
    }
}

impl<'a> FromSql<'a> for ValueWrap {
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

impl ToSql for ValueWrap {
    fn to_sql(&self, ty: &Type, out: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>>
    where
        Self: Sized,
    {
        match &self.0 {
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
                        vector.into_iter().map(|v| ValueWrap(v.clone())),
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
                        vector.into_iter().map(|v| ValueWrap(v.clone())),
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

pub(crate) struct VecWrap<T>(pub Vec<T>);

impl<'a, T: FromSql<'a>> FromSql<'a> for VecWrap<T> {
    fn from_sql_null(ty: &Type) -> Result<Self, Box<dyn Error + Sync + Send>> {
        Vec::<T>::from_sql_null(ty).map(VecWrap)
    }
    fn from_sql_nullable(
        ty: &Type,
        raw: Option<&'a [u8]>,
    ) -> Result<Self, Box<dyn Error + Sync + Send>> {
        Vec::<T>::from_sql_nullable(ty, raw).map(VecWrap)
    }
    fn from_sql(ty: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn Error + Sync + Send>> {
        Vec::<T>::from_sql(ty, raw).map(VecWrap)
    }
    fn accepts(ty: &Type) -> bool {
        Vec::<T>::accepts(ty)
    }
}

impl From<VecWrap<ValueWrap>> for Vec<Value> {
    fn from(value: VecWrap<ValueWrap>) -> Self {
        value.0.into_iter().map(|v| v.0).collect()
    }
}
