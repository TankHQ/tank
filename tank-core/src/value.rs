use crate::{AsValue, Error, Result, interval::Interval};
use proc_macro2::TokenStream;
use quote::{ToTokens, quote};
use rust_decimal::Decimal;
use serde_json::Value as JsonValue;
use std::{collections::HashMap, hash::Hash, mem::discriminant};
use time::{Date, OffsetDateTime, PrimitiveDateTime, Time};
use uuid::Uuid;

/// SQL value representation used across Tank.
///
/// Variants hold `Option<T>` to store the actual value (`None` means SQL NULL) and additional needed type information.
#[derive(Default, Debug, Clone)]
pub enum Value {
    /// Untyped NULL.
    #[default]
    Null,
    Boolean(Option<bool>),
    Int8(Option<i8>),
    Int16(Option<i16>),
    Int32(Option<i32>),
    Int64(Option<i64>),
    Int128(Option<i128>),
    UInt8(Option<u8>),
    UInt16(Option<u16>),
    UInt32(Option<u32>),
    UInt64(Option<u64>),
    UInt128(Option<u128>),
    Float32(Option<f32>),
    Float64(Option<f64>),
    /// Decimal with width and scale information.
    Decimal(Option<Decimal>, /* width: */ u8, /* scale: */ u8),
    Char(Option<char>),
    Varchar(Option<String>),
    Blob(Option<Box<[u8]>>),
    Date(Option<Date>),
    Time(Option<Time>),
    Timestamp(Option<PrimitiveDateTime>),
    TimestampWithTimezone(Option<OffsetDateTime>),
    Interval(Option<Interval>),
    Uuid(Option<Uuid>),
    /// Fixed-size homogeneous array.
    Array(
        Option<Box<[Value]>>,
        /* type: */ Box<Value>,
        /* len: */ u32,
    ),
    /// Variable-length homogeneous list.
    List(Option<Vec<Value>>, /* type: */ Box<Value>),
    /// Map with homogeneous key/value types.
    Map(
        Option<HashMap<Value, Value>>,
        /* key: */ Box<Value>,
        /* value: */ Box<Value>,
    ),
    Json(Option<JsonValue>),
    /// Struct with named fields and types.
    Struct(
        Option<Vec<(String, Value)>>,
        /* type: */ Vec<(String, Value)>,
    ),
    /// Unknown value type (usually the driver cannot provide type information).
    Unknown(Option<String>),
}

impl Value {
    pub fn same_type(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Decimal(.., l_width, l_scale), Self::Decimal(.., r_width, r_scale)) => {
                (*l_width == 0 || *r_width == 0 || l_width == r_width)
                    && (*l_scale == 0 || *r_scale == 0 || l_scale == r_scale)
            }
            (Self::Array(.., l_type, l_len), Self::Array(.., r_type, r_len)) => {
                l_len == r_len && l_type.same_type(&r_type)
            }
            (Self::List(.., l), Self::List(.., r)) => l.same_type(r),
            (Self::Map(.., l_key, l_value), Self::Map(.., r_key, r_value)) => {
                l_key.same_type(r_key) && l_value.same_type(&r_value)
            }
            _ => discriminant(self) == discriminant(other),
        }
    }

    pub fn is_null(&self) -> bool {
        match self {
            Value::Null
            | Value::Boolean(None, ..)
            | Value::Int8(None, ..)
            | Value::Int16(None, ..)
            | Value::Int32(None, ..)
            | Value::Int64(None, ..)
            | Value::Int128(None, ..)
            | Value::UInt8(None, ..)
            | Value::UInt16(None, ..)
            | Value::UInt32(None, ..)
            | Value::UInt64(None, ..)
            | Value::UInt128(None, ..)
            | Value::Float32(None, ..)
            | Value::Float64(None, ..)
            | Value::Decimal(None, ..)
            | Value::Char(None, ..)
            | Value::Varchar(None, ..)
            | Value::Blob(None, ..)
            | Value::Date(None, ..)
            | Value::Time(None, ..)
            | Value::Timestamp(None, ..)
            | Value::TimestampWithTimezone(None, ..)
            | Value::Interval(None, ..)
            | Value::Uuid(None, ..)
            | Value::Array(None, ..)
            | Value::List(None, ..)
            | Value::Map(None, ..)
            | Value::Json(None, ..)
            | Value::Json(Some(serde_json::Value::Null), ..)
            | Value::Struct(None, ..)
            | Value::Unknown(None, ..) => true,
            _ => false,
        }
    }

    pub fn as_null(&self) -> Value {
        match self {
            Value::Null => Value::Null,
            Value::Boolean(..) => Value::Boolean(None),
            Value::Int8(..) => Value::Int8(None),
            Value::Int16(..) => Value::Int16(None),
            Value::Int32(..) => Value::Int32(None),
            Value::Int64(..) => Value::Int64(None),
            Value::Int128(..) => Value::Int128(None),
            Value::UInt8(..) => Value::UInt8(None),
            Value::UInt16(..) => Value::UInt16(None),
            Value::UInt32(..) => Value::UInt32(None),
            Value::UInt64(..) => Value::UInt64(None),
            Value::UInt128(..) => Value::UInt128(None),
            Value::Float32(..) => Value::Float32(None),
            Value::Float64(..) => Value::Float64(None),
            Value::Decimal(.., w, s) => Value::Decimal(None, *w, *s),
            Value::Char(..) => Value::Char(None),
            Value::Varchar(..) => Value::Varchar(None),
            Value::Blob(..) => Value::Blob(None),
            Value::Date(..) => Value::Date(None),
            Value::Time(..) => Value::Time(None),
            Value::Timestamp(..) => Value::Timestamp(None),
            Value::TimestampWithTimezone(..) => Value::TimestampWithTimezone(None),
            Value::Interval(..) => Value::Interval(None),
            Value::Uuid(..) => Value::Uuid(None),
            Value::Array(.., t, len) => Value::Array(None, t.clone(), *len),
            Value::List(.., t) => Value::List(None, t.clone()),
            Value::Map(.., k, v) => Value::Map(None, k.clone(), v.clone()),
            Value::Json(..) => Value::Json(None),
            Value::Struct(.., t) => Value::Struct(None, t.clone()),
            Value::Unknown(..) => Value::Unknown(None),
        }
    }

    pub fn try_as(self, value: &Value) -> Result<Value> {
        if self.same_type(value) {
            return Ok(self);
        }
        match value {
            Value::Boolean(..) => bool::try_from_value(self).map(AsValue::as_value),
            Value::Int8(..) => i8::try_from_value(self).map(AsValue::as_value),
            Value::Int16(..) => i16::try_from_value(self).map(AsValue::as_value),
            Value::Int32(..) => i32::try_from_value(self).map(AsValue::as_value),
            Value::Int64(..) => i64::try_from_value(self).map(AsValue::as_value),
            Value::Int128(..) => i128::try_from_value(self).map(AsValue::as_value),
            Value::UInt8(..) => u8::try_from_value(self).map(AsValue::as_value),
            Value::UInt16(..) => u16::try_from_value(self).map(AsValue::as_value),
            Value::UInt32(..) => u32::try_from_value(self).map(AsValue::as_value),
            Value::UInt64(..) => u64::try_from_value(self).map(AsValue::as_value),
            Value::UInt128(..) => u128::try_from_value(self).map(AsValue::as_value),
            Value::Float32(..) => f32::try_from_value(self).map(AsValue::as_value),
            Value::Float64(..) => f64::try_from_value(self).map(AsValue::as_value),
            Value::Decimal(..) => Decimal::try_from_value(self).map(AsValue::as_value),
            Value::Char(..) => char::try_from_value(self).map(AsValue::as_value),
            Value::Varchar(..) => String::try_from_value(self).map(AsValue::as_value),
            Value::Blob(..) => Box::<[u8]>::try_from_value(self).map(AsValue::as_value),
            Value::Date(..) => Date::try_from_value(self).map(AsValue::as_value),
            Value::Time(..) => Time::try_from_value(self).map(AsValue::as_value),
            Value::Timestamp(..) => PrimitiveDateTime::try_from_value(self).map(AsValue::as_value),
            Value::TimestampWithTimezone(..) => {
                OffsetDateTime::try_from_value(self).map(AsValue::as_value)
            }
            Value::Interval(..) => Interval::try_from_value(self).map(AsValue::as_value),
            Value::Uuid(..) => Uuid::try_from_value(self).map(AsValue::as_value),
            // Value::Array(.., ty, len) => {
            //     Box::<[Value]>::try_from_value(self).map(AsValue::as_value)
            // }
            // Value::List(..) => Box::<[Value]>::try_from_value(self).map(AsValue::as_value),
            // Value::Map(..) => Date::try_from_value(self).map(AsValue::as_value),
            _ => {
                return Err(Error::msg(format!(
                    "Cannot convert value {:?} into value {:?}",
                    self, value
                )));
            }
        }
    }
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Null, Self::Null) => true,
            (Self::Boolean(l), Self::Boolean(r)) => l == r,
            (Self::Int8(l), Self::Int8(r)) => l == r,
            (Self::Int16(l), Self::Int16(r)) => l == r,
            (Self::Int32(l), Self::Int32(r)) => l == r,
            (Self::Int64(l), Self::Int64(r)) => l == r,
            (Self::Int128(l), Self::Int128(r)) => l == r,
            (Self::UInt8(l), Self::UInt8(r)) => l == r,
            (Self::UInt16(l), Self::UInt16(r)) => l == r,
            (Self::UInt32(l), Self::UInt32(r)) => l == r,
            (Self::UInt64(l), Self::UInt64(r)) => l == r,
            (Self::UInt128(l), Self::UInt128(r)) => l == r,
            (Self::Float32(l), Self::Float32(r)) => {
                l == r
                    || l.and_then(|l| r.and_then(|r| Some(l.is_nan() && r.is_nan())))
                        .unwrap_or_default()
            }
            (Self::Float64(l), Self::Float64(r)) => {
                l == r
                    || l.and_then(|l| r.and_then(|r| Some(l.is_nan() && r.is_nan())))
                        .unwrap_or_default()
            }
            (Self::Decimal(l, l_width, l_scale), Self::Decimal(r, r_width, r_scale)) => {
                l == r && l_width == r_width && l_scale == r_scale
            }
            (Self::Char(l), Self::Char(r)) => l == r,
            (Self::Varchar(l), Self::Varchar(r)) => l == r,
            (Self::Blob(l), Self::Blob(r)) => l == r,
            (Self::Date(l), Self::Date(r)) => l == r,
            (Self::Time(l), Self::Time(r)) => l == r,
            (Self::Timestamp(l), Self::Timestamp(r)) => l == r,
            (Self::TimestampWithTimezone(l), Self::TimestampWithTimezone(r)) => l == r,
            (Self::Interval(l), Self::Interval(r)) => l == r,
            (Self::Uuid(l), Self::Uuid(r)) => l == r,
            (Self::Array(l, ..), Self::Array(r, ..)) => l == r && self.same_type(other),
            (Self::List(l, ..), Self::List(r, ..)) => l == r && self.same_type(other),
            (Self::Map(None, ..), Self::Map(None, ..)) => self.same_type(other),
            (Self::Map(Some(l), ..), Self::Map(Some(r), ..)) => {
                l.is_empty() == r.is_empty() && self.same_type(other)
            }
            (Self::Map(..), Self::Map(..)) => self.same_type(other),
            (Self::Json(l), Self::Json(r)) => l == r,
            (Self::Unknown(..), Self::Unknown(..)) => false,
            _ => false,
        }
    }
}

impl Eq for Value {}

impl Hash for Value {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        use Value::*;
        discriminant(self).hash(state);
        match self {
            Null => {}
            Boolean(v) => v.hash(state),
            Int8(v) => v.hash(state),
            Int16(v) => v.hash(state),
            Int32(v) => v.hash(state),
            Int64(v) => v.hash(state),
            Int128(v) => v.hash(state),

            UInt8(v) => v.hash(state),
            UInt16(v) => v.hash(state),
            UInt32(v) => v.hash(state),
            UInt64(v) => v.hash(state),
            UInt128(v) => v.hash(state),
            Float32(Some(v)) => {
                v.to_bits().hash(state);
            }
            Float32(None) => None::<u32>.hash(state),
            Float64(Some(v)) => {
                v.to_bits().hash(state);
            }
            Float64(None) => None::<u64>.hash(state),
            Decimal(v, width, scale) => {
                v.hash(state);
                width.hash(state);
                scale.hash(state);
            }
            Char(v) => v.hash(state),
            Varchar(v) => v.hash(state),
            Blob(v) => v.hash(state),
            Date(v) => v.hash(state),
            Time(v) => v.hash(state),
            Timestamp(v) => v.hash(state),
            TimestampWithTimezone(v) => v.hash(state),
            Interval(v) => v.hash(state),
            Uuid(v) => v.hash(state),
            Array(v, typ, len) => {
                v.hash(state);
                typ.hash(state);
                len.hash(state);
            }
            List(v, typ) => {
                v.hash(state);
                typ.hash(state);
            }
            Map(v, key, val) => {
                match v {
                    Some(map) => {
                        for (key, val) in map {
                            key.hash(state);
                            val.hash(state);
                        }
                    }
                    None => {}
                }
                key.hash(state);
                val.hash(state);
            }
            Json(v) => v.hash(state),
            Struct(v, t) => {
                match v {
                    Some(v) => v.hash(state),
                    None => {}
                }
                t.hash(state);
            }
            Unknown(v) => v.hash(state),
        }
    }
}

/// Internally decoded type info for macros.
#[derive(Default)]
pub struct TypeDecoded {
    /// Representative value establishing variant & metadata.
    pub value: Value,
    /// Nullability indicator.
    pub nullable: bool,
    /// Passive flag (exclude from INSERT column/value list).
    pub passive: bool,
}

pub type CheckPassive = Box<dyn Fn(TokenStream) -> TokenStream>;

impl ToTokens for Value {
    fn to_tokens(&self, tokens: &mut proc_macro2::TokenStream) {
        let ts = match self {
            Value::Null => quote!(::tank::Value::Null),
            Value::Boolean(..) => quote!(::tank::Value::Boolean(None)),
            Value::Int8(..) => quote!(::tank::Value::Int8(None)),
            Value::Int16(..) => quote!(::tank::Value::Int16(None)),
            Value::Int32(..) => quote!(::tank::Value::Int32(None)),
            Value::Int64(..) => quote!(::tank::Value::Int64(None)),
            Value::Int128(..) => quote!(::tank::Value::Int128(None)),
            Value::UInt8(..) => quote!(::tank::Value::UInt8(None)),
            Value::UInt16(..) => quote!(::tank::Value::UInt16(None)),
            Value::UInt32(..) => quote!(::tank::Value::UInt32(None)),
            Value::UInt64(..) => quote!(::tank::Value::UInt64(None)),
            Value::UInt128(..) => quote!(::tank::Value::UInt128(None)),
            Value::Float32(..) => quote!(::tank::Value::Float32(None)),
            Value::Float64(..) => quote!(::tank::Value::Float64(None)),
            Value::Decimal(.., width, scale) => {
                quote!(::tank::Value::Decimal(None, #width, #scale))
            }
            Value::Char(..) => quote!(tank::Value::Char(None)),
            Value::Varchar(..) => quote!(::tank::Value::Varchar(None)),
            Value::Blob(..) => quote!(::tank::Value::Blob(None)),
            Value::Date(..) => quote!(::tank::Value::Date(None)),
            Value::Time(..) => quote!(::tank::Value::Time(None)),
            Value::Timestamp(..) => quote!(::tank::Value::Timestamp(None)),
            Value::TimestampWithTimezone(..) => quote!(::tank::Value::TimestampWithTimezone(None)),
            Value::Interval(..) => quote!(::tank::Value::Interval(None)),
            Value::Uuid(..) => quote!(::tank::Value::Uuid(None)),
            Value::Array(.., inner, size) => {
                quote!(::tank::Value::Array(None, Box::new(#inner), #size))
            }
            Value::List(.., inner) => {
                let inner = inner.as_ref().to_token_stream();
                quote!(::tank::Value::List(None, Box::new(#inner)))
            }
            Value::Map(.., key, value) => {
                let key = key.as_ref().to_token_stream();
                let value = value.as_ref().to_token_stream();
                quote!(::tank::Value::Map(None, Box::new(#key), Box::new(#value)))
            }
            Value::Json(..) => quote!(::tank::Value::Json(None)),
            Value::Struct(.., t) => {
                let values = t.into_iter().map(|(k, v)| quote!((#k.into(), #v)));
                quote!(::tank::Value::Struct(None, vec!(#(#values),*)))
            }
            Value::Unknown(..) => quote!(::tank::Value::Unknown(None)),
        };
        tokens.extend(ts);
    }
}
