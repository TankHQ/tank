#![allow(unused_imports)]
use crate::{
    CollectionType, Error, FixedDecimal, Interval, MapType, Result, StructType, Value, ValueOwned,
    consume_while, extract_number, truncate_long,
};
use crate::{month_to_number, number_to_month};
use anyhow::{Context, anyhow};
#[cfg(feature = "chrono")]
use chrono::{Datelike, Offset, Timelike};
use rust_decimal::{Decimal, prelude::FromPrimitive, prelude::ToPrimitive};
use std::{
    any,
    borrow::Cow,
    cell::{Cell, RefCell},
    collections::{BTreeMap, HashMap, LinkedList, VecDeque},
    hash::Hash,
    mem,
    num::{
        NonZeroI8, NonZeroI16, NonZeroI32, NonZeroI64, NonZeroI128, NonZeroIsize, NonZeroU8,
        NonZeroU16, NonZeroU32, NonZeroU64, NonZeroU128, NonZeroUsize,
    },
    rc::Rc,
    sync::{Arc, RwLock},
};
use time::{PrimitiveDateTime, UtcDateTime, format_description::parse_borrowed};
use uuid::Uuid;

/// Converts Rust inputs to values and decodes supported destination types.
pub trait AsValue {
    /// Return NULL variant.
    fn as_empty_value() -> ValueOwned;
    /// Convert to a value, retaining borrows for at most the lifetime of `Self`.
    fn as_value<'a>(self) -> Value<'a>
    where
        Self: 'a;
    /// Decode into `Self`; input-only implementations can use the default.
    fn try_from_value(_value: impl AsValue) -> Result<Self>
    where
        Self: Sized,
    {
        Err(anyhow!(
            "Decoding into {} is not implemented",
            any::type_name::<Self>()
        ))
    }
    /// Parse string.
    fn parse(input: impl AsRef<str>) -> Result<Self>
    where
        Self: Sized,
    {
        Err(anyhow!(
            "Cannot parse '{}' as {} (the parse method is not implemented)",
            truncate_long!(input.as_ref()),
            any::type_name::<Self>()
        ))
    }
}

impl AsValue for Value<'_> {
    fn as_empty_value() -> ValueOwned {
        Value::Null
    }
    #[inline(always)]
    fn as_value<'a>(self) -> Value<'a>
    where
        Self: 'a,
    {
        self
    }
    #[inline(always)]
    fn try_from_value(value: impl AsValue) -> Result<Self>
    where
        Self: Sized,
    {
        Ok(value.as_value().into_owned())
    }
}

impl AsValue for &Value<'_> {
    fn as_empty_value() -> ValueOwned {
        Value::as_empty_value()
    }
    fn as_value<'a>(self) -> Value<'a>
    where
        Self: 'a,
    {
        match self {
            Value::Null => Value::Null,
            Value::Boolean(v) => Value::Boolean(*v),
            Value::Int8(v) => Value::Int8(*v),
            Value::Int16(v) => Value::Int16(*v),
            Value::Int32(v) => Value::Int32(*v),
            Value::Int64(v) => Value::Int64(*v),
            Value::Int128(v) => Value::Int128(v.as_deref().copied().map(Box::new)),
            Value::UInt8(v) => Value::UInt8(*v),
            Value::UInt16(v) => Value::UInt16(*v),
            Value::UInt32(v) => Value::UInt32(*v),
            Value::UInt64(v) => Value::UInt64(*v),
            Value::UInt128(v) => Value::UInt128(v.as_deref().copied().map(Box::new)),
            Value::Float32(v) => Value::Float32(*v),
            Value::Float64(v) => Value::Float64(*v),
            Value::Decimal(v, w, s) => Value::Decimal(*v, *w, *s),
            Value::Char(v) => Value::Char(*v),
            Value::Varchar(v) => Value::Varchar(v.as_deref().map(Cow::Borrowed)),
            Value::Blob(v) => Value::Blob(v.as_deref().map(Cow::Borrowed)),
            Value::Date(v) => Value::Date(*v),
            Value::Time(v) => Value::Time(*v),
            Value::Timestamp(v) => Value::Timestamp(*v),
            Value::TimestampWithTimezone(v) => Value::TimestampWithTimezone(*v),
            Value::Interval(v) => Value::Interval(*v),
            Value::Uuid(v) => Value::Uuid(*v),
            Value::Array(v, t) => Value::Array(
                v.as_ref()
                    .map(|v| v.iter().map(AsValue::as_value).collect()),
                Box::new(CollectionType {
                    element: (&t.element).as_value(),
                    len: t.len,
                }),
            ),
            Value::List(v, t) => Value::List(
                v.as_ref()
                    .map(|v| v.iter().map(AsValue::as_value).collect()),
                Box::new(CollectionType {
                    element: (&t.element).as_value(),
                    len: t.len,
                }),
            ),
            Value::Map(v, t) => Value::Map(
                v.as_ref().map(|v| {
                    Box::new(
                        v.iter()
                            .map(|(k, v)| (k.as_value(), v.as_value()))
                            .collect(),
                    )
                }),
                Box::new(MapType {
                    key: (&t.key).as_value(),
                    value: (&t.value).as_value(),
                }),
            ),
            Value::Json(v) => Value::Json(v.clone()),
            Value::Struct(v, t) => Value::Struct(
                v.as_ref()
                    .map(|v| v.iter().map(|(k, v)| (k.clone(), v.as_value())).collect()),
                Box::new(StructType {
                    types: t
                        .types
                        .iter()
                        .map(|(k, v)| (k.clone(), v.as_value()))
                        .collect(),
                    name: t.name.clone(),
                }),
            ),
            Value::Unknown(v) => Value::Unknown(v.as_deref().map(Cow::Borrowed)),
        }
    }
}

impl<'a> From<&'a str> for Value<'a> {
    #[inline(always)]
    fn from(value: &'a str) -> Self {
        Value::Varchar(Some(Cow::Borrowed(value)))
    }
}

macro_rules! impl_as_value {
    (@box $source:ty, $destination:path $(, $pat_rest:pat => $expr_rest:expr)* $(,)?) => {
        impl_as_value!(@impl $source, $destination, |s| Box::new(s), ($destination(Some(v), ..)) => *v $(, $pat_rest => $expr_rest)*);
    };
    ($source:ty, $destination:path $(, $pat_rest:pat => $expr_rest:expr)* $(,)?) => {
        // Bind and `forget` `val` so `Copy` payloads skip `Value` drop glue.
        impl_as_value!(@impl $source, $destination, |s| s as _, (val @ $destination(Some(v), ..)) => { mem::forget(val); v as _ } $(, $pat_rest => $expr_rest)*);
    };
    (@impl $source:ty, $destination:path, |$s:ident| $wrap:expr, ($fast_pat:pat) => $unwrap:expr $(, $pat_rest:pat => $expr_rest:expr)* $(,)?) => {
        impl AsValue for $source {
            #[inline(always)]
            fn as_empty_value() -> ValueOwned {
                $destination(None)
            }
            #[inline(always)]
            fn as_value<'a>(self) -> Value<'a> where Self: 'a {
                let $s = self;
                $destination(Some($wrap))
            }
            #[inline(always)]
            fn try_from_value(value: impl AsValue) -> Result<Self> {
                #[cold]
                #[inline(never)]
                // Pass `&mut` + `mem::take` instead of by-value `Value` so LLVM does not
                // coalesce the match scrutinee into a `byval` stack slot on the hot path,
                // then `forget` the `Value::Null` left behind to omit inline drop glue.
                fn fallback(value: &mut Value<'_>) -> Result<$source> {
                    let value = mem::take(value);
                    match value {
                        $($pat_rest => $expr_rest,)*
                        Value::Int8(Some(v), ..) => <$source>::try_from(v).map_err(|_| {
                            anyhow!("Value {v}: i8 is out of range for {}", any::type_name::<$source>())
                        }),
                        Value::Int16(Some(v), ..) => <$source>::try_from(v).map_err(|_| {
                            anyhow!("Value {v}: i16 is out of range for {}", any::type_name::<$source>())
                        }),
                        Value::Int32(Some(v), ..) => <$source>::try_from(v).map_err(|_| {
                            anyhow!("Value {v}: i32 is out of range for {}", any::type_name::<$source>())
                        }),
                        Value::Int64(Some(v), ..) => <$source>::try_from(v).map_err(|_| {
                            anyhow!("Value {v}: i64 is out of range for {}", any::type_name::<$source>())
                        }),
                        Value::Int128(Some(v), ..) => <$source>::try_from(*v).map_err(|_| {
                            anyhow!("Value {v}: i128 is out of range for {}", any::type_name::<$source>())
                        }),
                        Value::UInt8(Some(v), ..) => <$source>::try_from(v).map_err(|_| {
                            anyhow!("Value {v}: u8 is out of range for {}", any::type_name::<$source>())
                        }),
                        Value::UInt16(Some(v), ..) => <$source>::try_from(v).map_err(|_| {
                            anyhow!("Value {v}: u16 is out of range for {}", any::type_name::<$source>())
                        }),
                        Value::UInt32(Some(v), ..) => <$source>::try_from(v).map_err(|_| {
                            anyhow!("Value {v}: u32 is out of range for {}", any::type_name::<$source>())
                        }),
                        Value::UInt64(Some(v), ..) => <$source>::try_from(v).map_err(|_| {
                            anyhow!("Value {v}: u64 is out of range for {}", any::type_name::<$source>())
                        }),
                        Value::UInt128(Some(v), ..) => <$source>::try_from(*v).map_err(|_| {
                            anyhow!("Value {v}: u128 is out of range for {}", any::type_name::<$source>())
                        }),
                        Value::Float64(Some(v), ..) => {
                            if v.is_finite() && v.fract() == 0.0 {
                                let min = <$source>::MIN as f64;
                                let max_exclusive = <$source>::MAX as f64 + 1.0;
                                if v >= min && v < max_exclusive {
                                    return Ok(v as $source);
                                }
                            }
                            Err(anyhow!(
                                "Value {v}: f64 does not fit into {}",
                                any::type_name::<$source>(),
                            ))
                        }
                        Value::Float32(Some(v), ..) => {
                            <$source>::try_from_value(Value::Float64(Some(v as f64)))
                        }
                        Value::Varchar(Some(ref v), ..) => <$source as AsValue>::parse(v).with_context(|| {
                            format!("While parsing a {} from `{}`", any::type_name::<$source>(), v)
                        }),
                        Value::Json(Some(serde_json::Value::Number(v)), ..) => {
                            let integer = v.as_i128().or_else(|| {
                                let f = v.as_f64()?;
                                let max_exclusive = i128::MAX as f64 + 1.0;
                                (f.fract() == 0.0 && f >= i128::MIN as f64 && f < max_exclusive)
                                    .then_some(f as i128)
                            });
                            if let Some(v) = integer
                                && let Ok(v) = <$source>::try_from(v)
                            {
                                return Ok(v);
                            }
                            Err(anyhow!(
                                "Value {v} from json number is out of range for {} (extracted integer is: {integer:?})",
                                any::type_name::<$source>(),
                            ))
                        }
                        Value::Json(Some(serde_json::Value::String(ref v)), ..) => <$source as AsValue>::parse(v),
                        Value::Unknown(Some(ref v), ..) => <$source>::parse(v).with_context(|| {
                            format!("While parsing a {} from `{}`", any::type_name::<$source>(), v)
                        }),
                        _ => Err(anyhow!(
                            "Cannot convert {value:?} to {}",
                            any::type_name::<$source>(),
                        )),
                    }
                }
                match value.as_value() {
                    $fast_pat => Ok($unwrap),
                    mut other => {
                        let res = fallback(&mut other);
                        mem::forget(other);
                        res
                    }
                }
            }
            fn parse(input: impl AsRef<str>) -> Result<Self> {
                input.as_ref().parse::<Self>().map_err(Into::into)
            }
        }
        impl AsValue for &$source {
            fn as_empty_value() -> ValueOwned {
                <$source>::as_empty_value()
            }
            #[inline(always)]
            fn as_value<'a>(self) -> Value<'a> where Self: 'a {
                (*self).as_value()
            }
        }
    };
}

macro_rules! impl_int_decimal {
    ($source:ty, $destination:path, $to_fn:ident $(, @box $($box:tt)?)?) => {
        impl_as_value!(
            $(@box $($box)?)? $source,
            $destination,
            Value::Decimal(Some(v), ..) => {
                let make_error = || anyhow!("Value {v}: Decimal does not fit into {}", any::type_name::<$source>());
                if !v.is_integer() {
                    return Err(make_error().context("The value is not an integer"));
                }
                v.$to_fn().ok_or_else(make_error)
            }
        );
    };
}

impl_as_value!(i8, Value::Int8);
impl_as_value!(i16, Value::Int16);
impl_int_decimal!(i32, Value::Int32, to_i32);
impl_int_decimal!(i64, Value::Int64, to_i64);
impl_int_decimal!(i128, Value::Int128, to_i128, @box);
impl_int_decimal!(isize, Value::Int64, to_isize);
impl_as_value!(u8, Value::UInt8);
impl_as_value!(u16, Value::UInt16);
impl_as_value!(u32, Value::UInt32);
impl_int_decimal!(u64, Value::UInt64, to_u64);
impl_int_decimal!(u128, Value::UInt128, to_u128, @box);
impl_int_decimal!(usize, Value::UInt64, to_usize);

macro_rules! impl_as_value {
    ($source:ty, $helper:path) => {
        impl AsValue for $source {
            #[inline(always)]
            fn as_empty_value() -> ValueOwned {
                <$helper>::as_empty_value()
            }
            fn as_value<'a>(self) -> Value<'a>
            where
                Self: 'a,
            {
                AsValue::as_value(Into::<$helper>::into(self))
            }
            fn try_from_value(value: impl AsValue) -> Result<Self>
            where
                Self: Sized,
            {
                Ok(<$helper as AsValue>::try_from_value(value)?.try_into()?)
            }
        }
        impl AsValue for &$source {
            fn as_empty_value() -> ValueOwned {
                <$source>::as_empty_value()
            }
            fn as_value<'a>(self) -> Value<'a>
            where
                Self: 'a,
            {
                (*self).as_value()
            }
        }
    };
}

impl_as_value!(NonZeroI8, i8);
impl_as_value!(NonZeroI16, i16);
impl_as_value!(NonZeroI32, i32);
impl_as_value!(NonZeroI64, i64);
impl_as_value!(NonZeroI128, i128);
impl_as_value!(NonZeroIsize, isize);
impl_as_value!(NonZeroU8, u8);
impl_as_value!(NonZeroU16, u16);
impl_as_value!(NonZeroU32, u32);
impl_as_value!(NonZeroU64, u64);
impl_as_value!(NonZeroU128, u128);
impl_as_value!(NonZeroUsize, usize);

macro_rules! impl_as_value {
    (@reference $source:ty) => {
        impl AsValue for &$source {
            fn as_empty_value() -> ValueOwned {
                <$source>::as_empty_value()
            }
            #[inline(always)]
            fn as_value<'a>(self) -> Value<'a> where Self: 'a {
                (*self).as_value()
            }
        }
    };
    (@copy $source:ty { $($body:tt)* }) => {
        impl AsValue for $source { $($body)* }
        impl_as_value!(@reference $source);
    };
    (String, $destination:path, $($rest:tt)*) => {
        impl_as_value!(
            @owned String,
            ($destination(Some(v), ..)) => match v {
                // Match `Cow` variant directly so `to_owned()` inlines instead of calling `<String as From<Cow<str>>>::from`.
                Cow::Borrowed(v) => v.to_owned(),
                Cow::Owned(v) => v,
            },
            $destination,
            $($rest)*
        );
        impl AsValue for &String {
            fn as_empty_value() -> ValueOwned {
                String::as_empty_value()
            }
            #[inline(always)]
            fn as_value<'a>(self) -> Value<'a> where Self: 'a {
                self.as_str().as_value()
            }
        }
    };
    ($source:ty, $destination:path, $($rest:tt)*) => {
        // Bind and `forget` `val` so `Copy` payloads skip `Value` drop glue.
        impl_as_value!(@owned $source, (val @ $destination(Some(v), ..)) => { mem::forget(val); v.into() }, $destination, $($rest)*);
        impl_as_value!(@reference $source);
    };
    (@owned $source:ty, ($fast_pat:pat) => $unwrap:expr, $destination:path, $extract:expr $(, $pat_rest:pat => $expr_rest:expr)* $(,)?) => {
        impl AsValue for $source {
            #[inline(always)]
            fn as_empty_value() -> ValueOwned {
                $destination(None)
            }
            #[inline(always)]
            fn as_value<'a>(self) -> Value<'a> where Self: 'a {
                $destination(Some(self.into()))
            }
            #[inline(always)]
            fn try_from_value(value: impl AsValue) -> Result<Self> {
                #[cold]
                #[inline(never)]
                // Pass `&mut` + `mem::take` instead of by-value `Value` so LLVM does not
                // coalesce the match scrutinee into a `byval` stack slot on the hot path,
                // then `forget` the `Value::Null` left behind to omit inline drop glue.
                fn fallback(value: &mut Value<'_>) -> Result<$source> {
                    let value = mem::take(value);
                    match value {
                        $($pat_rest => $expr_rest,)*
                        #[allow(unreachable_patterns)]
                        Value::Varchar(Some(ref v), ..) => {
                            <$source as AsValue>::parse(v).with_context(|| {
                                format!("While parsing a {} from `{}`", any::type_name::<$source>(), v)
                            })
                        }
                        #[allow(unreachable_patterns)]
                        Value::Json(Some(serde_json::Value::String(ref v)), ..) => {
                            <$source as AsValue>::parse(v)
                        }
                        Value::Unknown(Some(ref v), ..) => {
                            <$source as AsValue>::parse(v).with_context(|| {
                                format!("While parsing a {} from `{}`", any::type_name::<$source>(), v)
                            })
                        }
                        _ => Err(anyhow!(
                            "Cannot convert {value:?} to {}",
                            any::type_name::<$source>(),
                        )),
                    }
                }
                match value.as_value() {
                    $fast_pat => Ok($unwrap),
                    mut other => {
                        let res = fallback(&mut other);
                        mem::forget(other);
                        res
                    }
                }
            }
            fn parse(input: impl AsRef<str>) -> Result<Self> {
                $extract(input.as_ref())
            }
        }
    };
}

impl_as_value!(
    bool,
    Value::Boolean,
    |input: &str| {
        match input {
            x if x.eq_ignore_ascii_case("true") || x.eq_ignore_ascii_case("t") || x.eq("1") => Ok(true),
            x if x.eq_ignore_ascii_case("false") || x.eq_ignore_ascii_case("f") || x.eq("0") => Ok(false),
            _  => return Err(anyhow!("Cannot parse boolean from `{input}`"))
        }
    },
    Value::Int8(Some(v), ..) => Ok(v != 0),
    Value::Int16(Some(v), ..) => Ok(v != 0),
    Value::Int32(Some(v), ..) => Ok(v != 0),
    Value::Int64(Some(v), ..) => Ok(v != 0),
    Value::Int128(Some(v), ..) => Ok(*v != 0),
    Value::UInt8(Some(v), ..) => Ok(v != 0),
    Value::UInt16(Some(v), ..) => Ok(v != 0),
    Value::UInt32(Some(v), ..) => Ok(v != 0),
    Value::UInt64(Some(v), ..) => Ok(v != 0),
    Value::UInt128(Some(v), ..) => Ok(*v != 0),
    Value::Json(Some(serde_json::Value::Bool(v)), ..) => Ok(v),
    Value::Json(Some(serde_json::Value::Number(v)), ..) => {
        let n = v.as_i64();
        if n == Some(0) {
            Ok(false)
        } else if n == Some(1) {
            Ok(true)
        } else {
            Err(anyhow!("Cannot convert json number `{v:?}` to bool"))
        }
    },
);

impl_as_value!(
    f32,
    Value::Float32,
    |input: &str| Ok(input.parse::<f32>()?),
    Value::Float64(Some(v), ..) => Ok(v as _),
    Value::Decimal(Some(v), ..) => Ok(v.try_into()?),
    Value::Json(Some(serde_json::Value::Number(v)), ..) => {
        let Some(v) = v.as_f64() else {
            return Err(anyhow!("Cannot convert json number `{v:?}` to f32"));
        };
        Ok(v as _)
    }
);

impl_as_value!(
    f64,
    Value::Float64,
    |input: &str| Ok(input.parse::<f64>()?),
    Value::Float32(Some(v), ..) => Ok(v as _),
    Value::Decimal(Some(v), ..) => Ok(v.try_into()?),
    Value::Json(Some(serde_json::Value::Number(v)), ..) => {
        let Some(v) = v.as_f64() else {
            return Err(anyhow!("Cannot convert json number `{v:?}` to f64"));
        };
        Ok(v)
    }
);

impl_as_value!(
    char,
    Value::Char,
    |input: &str| {
        if input.chars().count() != 1 {
            return Err(anyhow!("Cannot convert `{input:?}` to char"))
        }
        Ok(input.chars().next().expect("Should have one character"))
    },
    Value::Varchar(Some(v), ..) => {
        if v.chars().count() != 1 {
            return Err(anyhow!(
                "Cannot convert varchar `{}` to char because it has more than one character",
                truncate_long!(v)
            ))
        }
        Ok(v.chars().next().unwrap())
    },
    Value::Json(Some(serde_json::Value::String(v)), ..) => {
        if v.chars().count() != 1 {
            return Err(anyhow!(
                "Cannot convert json `{}` to char because it has more than one character",
                truncate_long!(v)
            ))
        }
        Ok(v.chars().next().unwrap())
    },
    value @ (
        Value::Int8(Some(..), ..)
        | Value::Int16(Some(..), ..)
        | Value::Int32(Some(..), ..)
        | Value::Int64(Some(..), ..)
        | Value::Int128(Some(..), ..)
        | Value::UInt8(Some(..), ..)
        | Value::UInt16(Some(..), ..)
        | Value::UInt32(Some(..), ..)
        | Value::UInt64(Some(..), ..)
        | Value::UInt128(Some(..), ..)
    ) => {
        let code = u32::try_from_value(value.clone()).map_err(|_| {
            anyhow!("Cannot convert {value:?} to char because it is not a valid unicode scalar value")
        })?;
        char::from_u32(code).ok_or_else(|| {
            anyhow!("Cannot convert {value:?} to char because it is not a valid unicode scalar value")
        })
    }
);

impl_as_value!(
    String,
    Value::Varchar,
    |input: &str| {
        Ok(input.into())
    },
    v @ (
        Value::Int8(Some(..), ..)
        | Value::Int16(Some(..), ..)
        | Value::Int32(Some(..), ..)
        | Value::Int64(Some(..), ..)
        | Value::Int128(Some(..), ..)
        | Value::UInt8(Some(..), ..)
        | Value::UInt16(Some(..), ..)
        | Value::UInt32(Some(..), ..)
        | Value::UInt64(Some(..), ..)
        | Value::UInt128(Some(..), ..)
        | Value::Float32(Some(..), ..)
        | Value::Float64(Some(..), ..)
        | Value::Decimal(Some(..), ..)
        | Value::Char(Some(..), ..)
        | Value::Date(Some(..), ..)
        | Value::Time(Some(..), ..)
        | Value::Timestamp(Some(..), ..)
        | Value::TimestampWithTimezone(Some(..), ..)
        | Value::Uuid(Some(..), ..)
    ) => Ok(v.to_string()),
    Value::Json(Some(serde_json::Value::String(v)), ..) => Ok(v),
);

impl AsValue for &str {
    fn as_empty_value() -> ValueOwned {
        String::as_empty_value()
    }
    #[inline(always)]
    fn as_value<'a>(self) -> Value<'a>
    where
        Self: 'a,
    {
        Value::Varchar(Some(self.into()))
    }
}

impl AsValue for Box<[u8]> {
    #[inline(always)]
    fn as_empty_value() -> ValueOwned {
        Value::Blob(None)
    }
    fn as_value<'a>(self) -> Value<'a>
    where
        Self: 'a,
    {
        Value::Blob(Some(Cow::Owned(self.into_vec())))
    }
    fn try_from_value(value: impl AsValue) -> Result<Self>
    where
        Self: Sized,
    {
        let value = value.as_value();
        match value {
            Value::Blob(Some(v), ..) => Ok(v.into_owned().into_boxed_slice()),
            Value::Varchar(Some(v), ..) => <Self as AsValue>::parse(v),
            Value::Unknown(Some(v), ..) => <Self as AsValue>::parse(v),
            other => Err(anyhow!(
                "Cannot convert {other:?} to {}",
                any::type_name::<Self>()
            )),
        }
    }
    fn parse(input: impl AsRef<str>) -> Result<Self> {
        let mut input = input.as_ref();
        if input.starts_with("\\x") {
            input = &input[2..];
        }
        let filter_x = input.contains('x');
        let result = if filter_x {
            hex::decode(input.chars().filter(|c| *c != 'x').collect::<String>())
        } else {
            hex::decode(input)
        }
        .map(Into::into)
        .context(format!(
            "While decoding `{}` as {}",
            truncate_long!(input),
            any::type_name::<Self>()
        ))?;
        Ok(result)
    }
}

impl AsValue for &Box<[u8]> {
    fn as_empty_value() -> ValueOwned {
        Box::<[u8]>::as_empty_value()
    }
    fn as_value<'a>(self) -> Value<'a>
    where
        Self: 'a,
    {
        Value::Blob(Some(Cow::Borrowed(self)))
    }
}

impl_as_value!(Interval, Value::Interval, |mut input: &str| {
    let make_context = || anyhow!("Cannot parse interval from `{}`", truncate_long!(input));
    match input.chars().peekable().peek() {
        Some(v) if *v == '"' || *v == '\'' => {
            input = &input[1..];
            if !input.ends_with(*v) {
                return Err(make_context());
            }
            input = input.trim_end_matches(*v);
        }
        _ => {}
    };
    let mut interval = Interval::ZERO;
    loop {
        let mut cur = input;
        let Ok(count) = extract_number::<true>(&mut cur).parse::<i128>() else {
            break;
        };
        cur = cur.trim_start();
        let unit = consume_while(&mut cur, char::is_ascii_alphabetic);
        if unit.is_empty() {
            break;
        }
        match unit {
            x if x.eq_ignore_ascii_case("y")
                || x.eq_ignore_ascii_case("year")
                || x.eq_ignore_ascii_case("years") =>
            {
                interval += Interval::from_years(count as _)
            }
            x if x.eq_ignore_ascii_case("mon")
                || x.eq_ignore_ascii_case("mons")
                || x.eq_ignore_ascii_case("month")
                || x.eq_ignore_ascii_case("months") =>
            {
                interval += Interval::from_months(count as _)
            }
            x if x.eq_ignore_ascii_case("d")
                || x.eq_ignore_ascii_case("day")
                || x.eq_ignore_ascii_case("days") =>
            {
                interval += Interval::from_days(count as _)
            }
            x if x.eq_ignore_ascii_case("h")
                || x.eq_ignore_ascii_case("hour")
                || x.eq_ignore_ascii_case("hours") =>
            {
                interval += Interval::from_hours(count as _)
            }
            x if x.eq_ignore_ascii_case("min")
                || x.eq_ignore_ascii_case("mins")
                || x.eq_ignore_ascii_case("minute")
                || x.eq_ignore_ascii_case("minutes") =>
            {
                interval += Interval::from_mins(count as _)
            }
            x if x.eq_ignore_ascii_case("s")
                || x.eq_ignore_ascii_case("sec")
                || x.eq_ignore_ascii_case("secs")
                || x.eq_ignore_ascii_case("second")
                || x.eq_ignore_ascii_case("seconds") =>
            {
                interval += Interval::from_secs(count as _)
            }
            x if x.eq_ignore_ascii_case("micro")
                || x.eq_ignore_ascii_case("micros")
                || x.eq_ignore_ascii_case("microsecond")
                || x.eq_ignore_ascii_case("microseconds") =>
            {
                interval += Interval::from_micros(count as _)
            }
            x if x.eq_ignore_ascii_case("ns")
                || x.eq_ignore_ascii_case("nano")
                || x.eq_ignore_ascii_case("nanos")
                || x.eq_ignore_ascii_case("nanosecond")
                || x.eq_ignore_ascii_case("nanoseconds") =>
            {
                interval += Interval::from_nanos(count as _)
            }
            _ => return Err(make_context()),
        }
        input = cur.trim_start();
    }
    let neg = if Some('-') == input.chars().next() {
        input = input[1..].trim_ascii_start();
        true
    } else {
        false
    };
    let mut time_interval = Interval::ZERO;
    let num = extract_number::<true>(&mut input);
    if !num.is_empty() {
        let num = num.parse::<u64>().with_context(make_context)?;
        time_interval += Interval::from_hours(num as _);
        if Some(':') == input.chars().next() {
            input = &input[1..];
            let num = extract_number::<false>(&mut input).parse::<u64>()?;
            if input.is_empty() {
                return Err(make_context());
            }
            time_interval += Interval::from_mins(num as _);
            if Some(':') == input.chars().next() {
                input = &input[1..];
                let num = extract_number::<false>(&mut input)
                    .parse::<u64>()
                    .with_context(make_context)?;
                time_interval += Interval::from_secs(num as _);
                if Some('.') == input.chars().next() {
                    input = &input[1..];
                    let fraction = extract_number::<false>(&mut input);
                    if fraction.is_empty() || fraction.len() > 9 {
                        return Err(anyhow!("Sub-nanosecond precision cannot be represented"))
                            .with_context(make_context);
                    }
                    let nanos = fraction
                        .parse::<i128>()
                        .with_context(make_context)?
                        .saturating_mul(10_i128.pow((9 - fraction.len()) as u32));
                    time_interval += Interval::from_nanos(nanos);
                }
            }
        }
        if neg {
            interval -= time_interval;
        } else {
            interval += time_interval;
        }
    }
    if !input.is_empty() {
        return Err(make_context());
    }
    Ok(interval)
},);

impl_as_value!(std::time::Duration, Value::Interval, |v| {
    <Interval as AsValue>::parse(v).map(Into::into)
},);

impl_as_value!(time::Duration, Value::Interval, |v| {
    <Interval as AsValue>::parse(v).map(Into::into)
},);

impl_as_value!(Uuid, Value::Uuid, |input: &str| {
    let uuid = Uuid::parse_str(input)
        .with_context(|| format!("Cannot parse a uuid value from `{}`", truncate_long!(input)))?;
    Ok(uuid)
},);

/// Parses `$value` against each of the given formats, returning the first
/// successful parse and advancing `$value` past the consumed input.
///
/// Yields `None` when no format matches, and deliberately never builds an `anyhow::Error`.
macro_rules! parse_time {
    ($value: ident, $($formats:literal),+ $(,)?) => {
        'value: {
            for format in [$($formats,)+] {
                let Ok(format) = parse_borrowed::<2>(format) else {
                    break 'value None;
                };
                let mut parsed = time::parsing::Parsed::new();
                let remaining = parsed.parse_items($value.as_bytes(), &format);
                if let Ok(remaining) = remaining {
                    if let Ok(result) = parsed.try_into() {
                        $value = &$value[($value.len() - remaining.len())..];
                        break 'value Some(result);
                    }
                }
            }
            None
        }
    }
}

impl_as_value!(
    time::Date,
    Value::Date,
    |input: &str| {
        let mut value = input;
        let mut result: time::Date = parse_time!(value, "[year]-[month]-[day]")
            .ok_or_else(|| anyhow!("Cannot parse `{}` as time::Date", truncate_long!(input)))?;
        {
            let mut attempt = value.trim_start();
            let suffix = consume_while(&mut attempt, char::is_ascii_alphabetic);
            if suffix.eq_ignore_ascii_case("bc") {
                result =
                    time::Date::from_calendar_date(-(result.year() - 1), result.month(), result.day())?;
                value = attempt;
            }
            if suffix.eq_ignore_ascii_case("ad") {
                value = attempt
            }
        }
        if !value.is_empty() {
            return Err(anyhow!("Cannot parse `{}` as time::Date", truncate_long!(input)))
        }
        Ok(result)
    },
    Value::Timestamp(Some(v), ..) => {
        if v.time() != time::Time::MIDNIGHT {
            return Err(anyhow!("Timestamp {v:?} cannot be converted to date because the time part is not midnight"))
        }
        Ok(v.date())
    },
);

impl_as_value!(
    time::Time,
    Value::Time,
    |mut input: &str| {
        let result: time::Time = parse_time!(
            input,
            "[hour]:[minute]:[second].[subsecond]",
            "[hour]:[minute]:[second]",
            "[hour]:[minute]",
        )
        .ok_or_else(|| anyhow!("Cannot parse `{}` as time::Time", truncate_long!(input)))?;
        if !input.is_empty() {
            return Err(anyhow!("Cannot parse `{}` as time::Time", truncate_long!(input)))
        }
        Ok(result)
    },
    Value::Interval(Some(v), ..) => {
        let (h, m, s, ns) = v.as_hmsns();
        if h < 0 {
            return Err(anyhow!(
                "Cannot convert negative interval `{v:?}` to `time::Time`"
            ));
        }
        time::Time::from_hms_nano(h as _, m as _, s as _, ns as _)
            .map_err(|e| anyhow!("Cannot convert interval `{v:?}` to time: {e:?}"))
    },
);

impl_as_value!(
    time::PrimitiveDateTime,
    Value::Timestamp,
    |mut input: &str| {
        let result: time::PrimitiveDateTime = parse_time!(
            input,
            "[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond]",
            "[year]-[month]-[day]T[hour]:[minute]:[second]",
            "[year]-[month]-[day]T[hour]:[minute]",
            "[year]-[month]-[day] [hour]:[minute]:[second].[subsecond]",
            "[year]-[month]-[day] [hour]:[minute]:[second]",
            "[year]-[month]-[day] [hour]:[minute]",
        )
        .ok_or_else(|| {
            anyhow!(
                "Cannot parse `{}` as time::PrimitiveDateTime",
                truncate_long!(input)
            )
        })?;
        if !input.is_empty() {
            return Err(anyhow!(
                "Cannot parse `{}` as time::PrimitiveDateTime",
                truncate_long!(input)
            ));
        }
        Ok(result)
    },
);

impl_as_value! { @copy UtcDateTime {
    #[inline(always)]
    fn as_empty_value() -> ValueOwned {
        time::PrimitiveDateTime::as_empty_value()
    }
    fn as_value<'a>(self) -> Value<'a>
    where
        Self: 'a,
    {
        PrimitiveDateTime::new(self.date(), self.time()).as_value()
    }
    fn try_from_value(value: impl AsValue) -> Result<Self>
    where
        Self: Sized,
    {
        PrimitiveDateTime::try_from_value(value).map(|v| Self::new(v.date(), v.time()))
    }
} }

impl_as_value!(
    time::OffsetDateTime,
    Value::TimestampWithTimezone,
    |mut input: &str| {
        let original = input;
        if let Some(result) = parse_time!(
            input,
            "[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond][offset_hour sign:mandatory]:[offset_minute]:[offset_second]",
            "[year]-[month]-[day]T[hour]:[minute]:[second][offset_hour sign:mandatory]:[offset_minute]:[offset_second]",
            "[year]-[month]-[day]T[hour]:[minute][offset_hour sign:mandatory]:[offset_minute]:[offset_second]",
            "[year]-[month]-[day] [hour]:[minute]:[second].[subsecond][offset_hour sign:mandatory]:[offset_minute]:[offset_second]",
            "[year]-[month]-[day] [hour]:[minute]:[second][offset_hour sign:mandatory]:[offset_minute]:[offset_second]",
            "[year]-[month]-[day] [hour]:[minute][offset_hour sign:mandatory]:[offset_minute]:[offset_second]",
            "[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond][offset_hour sign:mandatory]:[offset_minute]",
            "[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond][offset_hour sign:mandatory]",
            "[year]-[month]-[day]T[hour]:[minute]:[second][offset_hour sign:mandatory]:[offset_minute]",
            "[year]-[month]-[day]T[hour]:[minute]:[second][offset_hour sign:mandatory]",
            "[year]-[month]-[day]T[hour]:[minute][offset_hour sign:mandatory]:[offset_minute]",
            "[year]-[month]-[day]T[hour]:[minute][offset_hour sign:mandatory]",
            "[year]-[month]-[day] [hour]:[minute]:[second].[subsecond][offset_hour sign:mandatory]:[offset_minute]",
            "[year]-[month]-[day] [hour]:[minute]:[second].[subsecond][offset_hour sign:mandatory]",
            "[year]-[month]-[day] [hour]:[minute]:[second][offset_hour sign:mandatory]:[offset_minute]",
            "[year]-[month]-[day] [hour]:[minute]:[second][offset_hour sign:mandatory]",
            "[year]-[month]-[day] [hour]:[minute][offset_hour sign:mandatory]:[offset_minute]",
            "[year]-[month]-[day] [hour]:[minute][offset_hour sign:mandatory]",
        ) {
            if input.is_empty() {
                return Ok(result);
            }
        }
        if let Ok(result) = <PrimitiveDateTime as AsValue>::parse(original) {
            return Ok(result.assume_utc());
        }
        Err(anyhow!("Cannot parse `{}` as time::OffsetDateTime", truncate_long!(original)))
    },
    Value::Timestamp(Some(timestamp), ..) => Ok(timestamp.assume_utc()),
);

#[cfg(feature = "chrono")]
impl_as_value! { @copy chrono::NaiveDate {
    fn as_empty_value() -> ValueOwned {
        Value::Date(None)
    }
    fn as_value<'a>(self) -> Value<'a>
    where
        Self: 'a,
    {
        Value::Date(
            'date: {
                time::Date::from_calendar_date(
                    self.year(),
                    number_to_month!(
                        self.month(),
                        break 'date Err(anyhow!("Unexpected month value {}", self.month()))
                    ),
                    self.day() as _,
                )
                .map_err(Into::into)
            }
            .inspect_err(|e| {
                log::error!("Could not create a Value::Date from chrono::NaiveDate: {e:?}");
            })
            .ok(),
        )
    }
    fn try_from_value(value: impl AsValue) -> Result<Self>
    where
        Self: Sized,
    {
        let value = <time::Date as AsValue>::try_from_value(value)
            .context("Could not create a chrono::NaiveDate")?;
        chrono::NaiveDate::from_ymd_opt(
            value.year(),
            month_to_number!(value.month()),
            value.day() as _,
        )
        .context("Could not create chrono::NaiveDate: from_ymd_opt returned None")
    }
} }

#[cfg(feature = "chrono")]
impl_as_value! { @copy chrono::NaiveTime {
    fn as_empty_value() -> ValueOwned {
        Value::Time(None)
    }
    fn as_value<'a>(self) -> Value<'a>
    where
        Self: 'a,
    {
        Value::Time(
            time::Time::from_hms_nano(
                self.hour() as _,
                self.minute() as _,
                self.second() as _,
                self.nanosecond() as _,
            )
            .inspect_err(|e| {
                log::error!("Could not create a Value::Time from chrono::NaiveTime: {e:?}",)
            })
            .ok(),
        )
    }
    fn try_from_value(value: impl AsValue) -> Result<Self>
    where
        Self: Sized,
    {
        let value = <time::Time as AsValue>::try_from_value(value)
            .context("Could not create a chrono::NaiveTime")?;
        Self::from_hms_nano_opt(
            value.hour() as _,
            value.minute() as _,
            value.second() as _,
            value.nanosecond() as _,
        )
        .context("Could not create chrono::NaiveTime: from_hms_nano_opt returned None")
    }
} }

#[cfg(feature = "chrono")]
impl_as_value! { @copy chrono::NaiveDateTime {
    fn as_empty_value() -> ValueOwned {
        Value::Timestamp(None)
    }
    fn as_value<'a>(self) -> Value<'a>
    where
        Self: 'a,
    {
        Value::Timestamp(
            'value: {
                let Ok(date) = AsValue::try_from_value(self.date().as_value()) else {
                    break 'value Err(anyhow!(
                        "Failed to convert the date part from chrono::NaiveDate to time::Date",
                    ));
                };
                let Ok(time) = AsValue::try_from_value(self.time().as_value()) else {
                    break 'value Err(anyhow!(
                        "Failed to convert the time part from chrono::NaiveTime to time::Time",
                    ));
                };
                Ok(time::PrimitiveDateTime::new(date, time))
            }
            .inspect_err(|e| {
                log::error!(
                    "Could not create a Value::Timestamp from chrono::NaiveDateTime: {e:?}",
                );
            })
            .ok(),
        )
    }
    fn try_from_value(value: impl AsValue) -> Result<Self>
    where
        Self: Sized,
    {
        let value = <time::PrimitiveDateTime as AsValue>::try_from_value(value)
            .context("Could not create a chrono::NaiveDateTime")?;
        let date = AsValue::try_from_value(value.date().as_value())
            .context("Could not convert date part of chrono::NaiveDateTime")?;
        let time = AsValue::try_from_value(value.time().as_value())
            .context("Could not convert time part of chrono::NaiveDateTime")?;
        Ok(Self::new(date, time))
    }
} }

#[cfg(feature = "chrono")]
impl_as_value! { @copy chrono::DateTime<chrono::FixedOffset> {
    fn as_empty_value() -> ValueOwned {
        Value::TimestampWithTimezone(None)
    }
    fn as_value<'a>(self) -> Value<'a>
    where
        Self: 'a,
    {
        Value::TimestampWithTimezone(
            'value: {
                let Ok(date) = AsValue::try_from_value(self.date_naive().as_value()) else {
                    break 'value Err(anyhow!(
                        "Failed to convert the date part from chrono::NaiveDate to time::Date",
                    ));
                };
                let Ok(time) = AsValue::try_from_value(self.time().as_value()) else {
                    break 'value Err(anyhow!(
                        "Failed to convert the time part from chrono::NaiveTime to time::Time",
                    ));
                };
                let Ok(offset) =
                    time::UtcOffset::from_whole_seconds(self.offset().fix().local_minus_utc())
                else {
                    break 'value Err(anyhow!("Failed to convert the offset part from"));
                };
                Ok(time::OffsetDateTime::new_in_offset(date, time, offset))
            }
            .inspect_err(|e| {
                log::error!(
                    "Could not create a Value::Timestamp from chrono::NaiveDateTime: {e:?}",
                );
            })
            .ok(),
        )
    }
    fn try_from_value(value: impl AsValue) -> Result<Self>
    where
        Self: Sized,
    {
        let value = <time::OffsetDateTime as AsValue>::try_from_value(value)
            .context("Could not create a chrono::DateTime")?;
        let utc = value.to_offset(time::UtcOffset::UTC);
        let date = AsValue::try_from_value(utc.date().as_value())
            .context("Could not convert date part of chrono::DateTime")?;
        let time = AsValue::try_from_value(utc.time().as_value())
            .context("Could not convert time part of chrono::DateTime")?;
        let date_time = chrono::NaiveDateTime::new(date, time);
        let offset = chrono::FixedOffset::east_opt(value.offset().whole_seconds())
            .context("Could not convert UTC offset part of chrono::DateTime")?;
        Ok(Self::from_naive_utc_and_offset(date_time, offset))
    }
} }

#[cfg(feature = "chrono")]
impl_as_value! { @copy chrono::DateTime<chrono::Utc> {
    fn as_empty_value() -> ValueOwned {
        Value::TimestampWithTimezone(None)
    }
    fn as_value<'a>(self) -> Value<'a>
    where
        Self: 'a,
    {
        let odt = self
            .timestamp_nanos_opt()
            .and_then(|nanos| time::OffsetDateTime::from_unix_timestamp_nanos(nanos as i128).ok());
        if odt.is_none() {
            log::error!(
                "Could not create a Value::TimestampWithTimezone from chrono::DateTime<Utc>: {self:?}",
            );
        }
        Value::TimestampWithTimezone(odt)
    }
    fn try_from_value(value: impl AsValue) -> Result<Self> {
        let odt = <time::OffsetDateTime as AsValue>::try_from_value(value)?;
        let utc_odt = odt.to_offset(time::UtcOffset::UTC);
        let secs = utc_odt.unix_timestamp();
        let nanos = utc_odt.nanosecond();
        Self::from_timestamp(secs, nanos)
            .ok_or_else(|| anyhow!("Timestamp out of range for chrono::DateTime<Utc>"))
    }
} }

impl_as_value! { @copy Decimal {
    #[inline(always)]
    fn as_empty_value() -> ValueOwned {
        Value::Decimal(None, 0, 0)
    }
    fn as_value<'a>(self) -> Value<'a>
    where
        Self: 'a,
    {
        Value::Decimal(Some(self), 0, self.scale() as _)
    }
    fn try_from_value(value: impl AsValue) -> Result<Self> {
        let value = value.as_value();
        match value {
            Value::Decimal(Some(v), ..) => Ok(v),
            Value::Int8(Some(v), ..) => Ok(Decimal::new(v as i64, 0)),
            Value::Int16(Some(v), ..) => Ok(Decimal::new(v as i64, 0)),
            Value::Int32(Some(v), ..) => Ok(Decimal::new(v as i64, 0)),
            Value::Int64(Some(v), ..) => Ok(Decimal::new(v, 0)),
            Value::UInt8(Some(v), ..) => Ok(Decimal::new(v as i64, 0)),
            Value::UInt16(Some(v), ..) => Ok(Decimal::new(v as i64, 0)),
            Value::UInt32(Some(v), ..) => Ok(Decimal::new(v as i64, 0)),
            Value::UInt64(Some(v), ..) => Decimal::from_u64(v)
                .ok_or_else(|| anyhow!("Value {v}: u64 does not fit into Decimal")),
            Value::Float32(Some(v), ..) => {
                Decimal::from_f32(v).ok_or_else(|| anyhow!("Cannot convert {value:?} to Decimal"))
            }
            Value::Float64(Some(v), ..) => {
                Decimal::from_f64(v).ok_or_else(|| anyhow!("Cannot convert {value:?} to Decimal"))
            }
            Value::Json(Some(serde_json::Value::Number(v)), ..) => {
                if let Some(v) = v.as_f64()
                    && let Some(v) = Decimal::from_f64(v)
                {
                    Ok(v)
                } else {
                    Err(anyhow!(
                        "Value {v} from json number is out of range for Decimal",
                    ))
                }
            }
            Value::Unknown(Some(v), ..) => Self::parse(&v),
            Value::Varchar(Some(v)) => Self::parse(&v),
            _ => Err(anyhow!("Cannot convert {value:?} to Decimal")),
        }
    }
    fn parse(input: impl AsRef<str>) -> Result<Self> {
        let input = input.as_ref();
        Ok(input.parse::<Decimal>().with_context(|| {
            anyhow!(
                "Cannot parse a decimal value from `{}`",
                truncate_long!(input)
            )
        })?)
    }
} }

impl<const W: u8, const S: u8> AsValue for FixedDecimal<W, S> {
    #[inline(always)]
    fn as_empty_value() -> ValueOwned {
        Decimal::as_empty_value()
    }
    fn as_value<'a>(self) -> Value<'a>
    where
        Self: 'a,
    {
        Value::Decimal(Some(self.0), W, self.0.scale() as _)
    }
    fn try_from_value(value: impl AsValue) -> Result<Self>
    where
        Self: Sized,
    {
        Ok(Self(Decimal::try_from_value(value)?))
    }
    fn parse(input: impl AsRef<str>) -> Result<Self> {
        <Decimal as AsValue>::parse(input).map(Into::into)
    }
}

impl<const W: u8, const S: u8> AsValue for &FixedDecimal<W, S> {
    fn as_empty_value() -> ValueOwned {
        FixedDecimal::<W, S>::as_empty_value()
    }
    fn as_value<'a>(self) -> Value<'a>
    where
        Self: 'a,
    {
        Value::Decimal(Some(self.0), W, self.0.scale() as _)
    }
}

impl<T: AsValue, const N: usize> AsValue for [T; N] {
    fn as_empty_value() -> ValueOwned {
        Value::Array(
            None,
            Box::new(CollectionType {
                element: T::as_empty_value(),
                len: N as u32,
            }),
        )
    }
    fn as_value<'a>(self) -> Value<'a>
    where
        Self: 'a,
    {
        Value::Array(
            Some(self.into_iter().map(AsValue::as_value).collect()),
            Box::new(CollectionType {
                element: T::as_empty_value(),
                len: N as u32,
            }),
        )
    }
    fn try_from_value(value: impl AsValue) -> Result<Self> {
        let value = value.as_value();
        fn convert_iter<T: AsValue, const N: usize>(
            iter: impl IntoIterator<Item: AsValue>,
        ) -> Result<[T; N]> {
            iter.into_iter()
                .map(|v| T::try_from_value(v.as_value()))
                .collect::<Result<Vec<_>>>()?
                .try_into()
                .map_err(|v: Vec<T>| {
                    anyhow!(
                        "Expected array of length {N}, got {} elements ({})",
                        v.len(),
                        any::type_name::<[T; N]>()
                    )
                })
        }
        match value {
            Value::Varchar(Some(v), ..)
                if matches!(T::as_empty_value(), Value::Char(..)) && v.chars().count() == N =>
            {
                convert_iter(v.chars())
            }
            Value::List(Some(v), ..) | Value::Array(Some(v), ..) if v.len() == N => convert_iter(v),
            Value::Json(Some(serde_json::Value::Array(v))) if v.len() == N => convert_iter(v),
            Value::Unknown(Some(v)) => <Self as AsValue>::parse(v),
            _ => Err(anyhow!(
                "Cannot convert {value:?} to array {}",
                any::type_name::<Self>()
            )),
        }
    }
}

impl<'r, T, const N: usize> AsValue for &'r [T; N]
where
    &'r T: AsValue,
{
    fn as_empty_value() -> ValueOwned {
        Value::Array(
            None,
            Box::new(CollectionType {
                element: <&'r T>::as_empty_value(),
                len: N as u32,
            }),
        )
    }
    fn as_value<'a>(self) -> Value<'a>
    where
        Self: 'a,
    {
        Value::Array(
            Some(self.iter().map(AsValue::as_value).collect()),
            Box::new(CollectionType {
                element: <&'r T>::as_empty_value(),
                len: N as u32,
            }),
        )
    }
}

macro_rules! impl_as_value {
    (@reference $source:ty) => {
        impl<'r, T> AsValue for &'r $source where &'r T: AsValue {
            fn as_empty_value() -> ValueOwned {
                Value::List(
                    None,
                    Box::new(CollectionType { element: <&'r T>::as_empty_value(), len: 0 }),
                )
            }
            fn as_value<'a>(self) -> Value<'a> where Self: 'a {
                Value::List(
                    Some(self.iter().map(AsValue::as_value).collect()),
                    Box::new(CollectionType { element: <&'r T>::as_empty_value(), len: 0 }),
                )
            }
        }
    };
    ($source:ident) => {
        impl<T: AsValue> AsValue for $source<T> {
            fn as_empty_value() -> ValueOwned {
                Value::List(
                    None,
                    Box::new(CollectionType { element: T::as_empty_value(), len: 0 }),
                )
            }
            fn as_value<'a>(self) -> Value<'a>
            where
                Self: 'a,
            {
                Value::List(
                    Some(self.into_iter().map(AsValue::as_value).collect()),
                    Box::new(CollectionType { element: T::as_empty_value(), len: 0 }),
                )
            }
            fn try_from_value(value: impl AsValue) -> Result<Self> {
                let value = value.as_value();
                match value {
                    Value::List(None, ..) => Ok(Default::default()),
                    Value::List(Some(v), ..) | Value::Array(Some(v), ..) => {
                        v.into_iter().map(T::try_from_value).collect()
                    }
                    Value::Json(Some(serde_json::Value::Array(v)), ..) => {
                        v.into_iter().map(T::try_from_value).collect()
                    }
                    _ => Err(anyhow!(
                        "Cannot convert {value:?} to {}",
                        any::type_name::<Self>(),
                    )),
                }
            }
        }
        impl_as_value!(@reference $source<T>);
    };
}
impl_as_value!(Vec);
impl_as_value!(VecDeque);
impl_as_value!(LinkedList);
impl_as_value!(@reference [T]);

macro_rules! impl_as_value {
    ($source:ident, $($key_trait:ident),+) => {
        impl<K: AsValue $(+ $key_trait)+, V: AsValue> AsValue for $source<K, V> {
            fn as_empty_value() -> ValueOwned {
                Value::Map(
                    None,
                    Box::new(MapType {
                        key: K::as_empty_value(),
                        value: V::as_empty_value(),
                    }),
                )
            }
            fn as_value<'a>(self) -> Value<'a> where Self: 'a {
                Value::Map(
                    Some(Box::new(
                        self.into_iter()
                            .map(|(k, v)| (k.as_value(), v.as_value()))
                            .collect(),
                    )),
                    Box::new(MapType {
                        key: K::as_empty_value(),
                        value: V::as_empty_value(),
                    }),
                )
            }
            fn try_from_value(value: impl AsValue) -> Result<Self> {
                let value = value.as_value();
                match value {
                    Value::Map(None, ..) => Ok(Default::default()),
                    Value::Map(Some(v), ..) => v
                        .into_iter()
                        .map(|(k, v)| Ok((K::try_from_value(k)?, V::try_from_value(v)?)))
                        .collect(),
                    Value::Json(Some(serde_json::Value::Object(v)), ..) => v
                        .into_iter()
                        .map(|(k, v)| Ok((K::try_from_value(k)?, V::try_from_value(v)?)))
                        .collect(),
                    _ => Err(anyhow!(
                        "Cannot convert {value:?} to {}",
                        any::type_name::<Self>(),
                    )),
                }
            }
        }
        impl<'r, K, V> AsValue for &'r $source<K, V>
        where
            &'r K: AsValue,
            &'r V: AsValue,
        {
            fn as_empty_value() -> ValueOwned {
                Value::Map(
                    None,
                    Box::new(MapType {
                        key: <&'r K>::as_empty_value(),
                        value: <&'r V>::as_empty_value(),
                    }),
                )
            }
            fn as_value<'a>(self) -> Value<'a> where Self: 'a {
                Value::Map(
                    Some(Box::new(
                        self.iter().map(|(k, v)| (k.as_value(), v.as_value())).collect(),
                    )),
                    Box::new(MapType {
                        key: <&'r K>::as_empty_value(),
                        value: <&'r V>::as_empty_value(),
                    }),
                )
            }
        }
    }
}
impl_as_value!(BTreeMap, Ord);
impl_as_value!(HashMap, Eq, Hash);

impl AsValue for Cow<'_, str> {
    #[inline(always)]
    fn as_empty_value() -> ValueOwned {
        Value::Varchar(None)
    }
    #[inline(always)]
    fn as_value<'a>(self) -> Value<'a>
    where
        Self: 'a,
    {
        Value::Varchar(Some(self))
    }
    #[inline(always)]
    fn try_from_value(value: impl AsValue) -> Result<Self>
    where
        Self: Sized,
    {
        String::try_from_value(value).map(Into::into)
    }
    fn parse(input: impl AsRef<str>) -> Result<Self>
    where
        Self: Sized,
    {
        <String as AsValue>::parse(input).map(Into::into)
    }
}

impl AsValue for &Cow<'_, str> {
    fn as_empty_value() -> ValueOwned {
        <Cow<'_, str>>::as_empty_value()
    }
    #[inline(always)]
    fn as_value<'a>(self) -> Value<'a>
    where
        Self: 'a,
    {
        Value::Varchar(Some(Cow::Borrowed(self)))
    }
}

impl AsValue for Cow<'_, Value<'_>> {
    fn as_empty_value() -> ValueOwned {
        Value::Null
    }
    #[inline(always)]
    fn as_value<'a>(self) -> Value<'a>
    where
        Self: 'a,
    {
        match self {
            Cow::Owned(value) => value,
            Cow::Borrowed(value) => value.as_value(),
        }
    }
    #[inline(always)]
    fn try_from_value(value: impl AsValue) -> Result<Self> {
        Ok(Cow::Owned(value.as_value().into_owned()))
    }
}

impl AsValue for &Cow<'_, Value<'_>> {
    fn as_empty_value() -> ValueOwned {
        <Cow<'_, Value<'_>>>::as_empty_value()
    }
    #[inline(always)]
    fn as_value<'a>(self) -> Value<'a>
    where
        Self: 'a,
    {
        self.as_ref().as_value()
    }
}

impl<T: AsValue> AsValue for Option<T> {
    #[inline(always)]
    fn as_empty_value() -> ValueOwned {
        T::as_empty_value()
    }
    #[inline(always)]
    fn as_value<'a>(self) -> Value<'a>
    where
        Self: 'a,
    {
        match self {
            Some(v) => v.as_value(),
            None => T::as_empty_value(),
        }
    }
    #[inline(always)]
    fn try_from_value(value: impl AsValue) -> Result<Self> {
        #[cold]
        #[inline(never)]
        // Return `Option<Result<T>>` instead of `Result<Option<T>>` so the cold call does not
        // share `try_from_value`'s `sret` slot, and take `&mut Value` so `val` stays in registers.
        fn fallback<T: AsValue>(value: &mut Value<'_>) -> Option<Result<T>> {
            let value = mem::take(value);
            (!value.is_null()).then(|| <T as AsValue>::try_from_value(value))
        }
        match value.as_value() {
            val @ Value::Null => {
                // Suppress `Value` drop glue once `val` is known to be `Value::Null`.
                mem::forget(val);
                Ok(None)
            }
            val => {
                // Compare against `T::as_empty_value()`'s constant discriminant so LLVM
                // constant-folds `val.is_null()` to `T`'s single variant instead of a
                // 28-entry jump table, materializing `other` only on the cold path.
                let empty = T::as_empty_value();
                let same = mem::discriminant(&val) == mem::discriminant(&empty);
                let drop_empty = matches!(
                    empty,
                    Value::Array(..)
                        | Value::List(..)
                        | Value::Map(..)
                        | Value::Struct(..)
                        | Value::Json(..)
                );
                if !drop_empty {
                    // Scalar empty values own no heap data; forgetting elides drop glue.
                    mem::forget(empty);
                }
                if same {
                    if val.is_null() {
                        if !drop_empty {
                            mem::forget(val);
                        }
                        Ok(None)
                    } else {
                        <T as AsValue>::try_from_value(val).map(Some)
                    }
                } else {
                    let mut other = val;
                    let res = fallback(&mut other).transpose();
                    mem::forget(other);
                    res
                }
            }
        }
    }
    fn parse(input: impl AsRef<str>) -> Result<Self>
    where
        Self: Sized,
    {
        let mut value = input.as_ref();
        let result = consume_while(&mut value, |v| v.is_alphanumeric() || *v == '_');
        if result.eq_ignore_ascii_case("null") {
            return Ok(None);
        };
        T::parse(input).map(Some)
    }
}

impl<'r, T> AsValue for &'r Option<T>
where
    &'r T: AsValue,
{
    fn as_empty_value() -> ValueOwned {
        <&'r T>::as_empty_value()
    }
    fn as_value<'a>(self) -> Value<'a>
    where
        Self: 'a,
    {
        match self {
            Some(value) => value.as_value(),
            None => <&'r T>::as_empty_value(),
        }
    }
}

// TODO: Use the macro below once box_into_inner is stabilized
impl<T: AsValue> AsValue for Box<T> {
    fn as_empty_value() -> ValueOwned {
        T::as_empty_value()
    }
    fn as_value<'a>(self) -> Value<'a>
    where
        Self: 'a,
    {
        (*self).as_value()
    }
    fn try_from_value(value: impl AsValue) -> Result<Self> {
        Ok(Self::new(<T as AsValue>::try_from_value(value)?))
    }
    fn parse(input: impl AsRef<str>) -> Result<Self>
    where
        Self: Sized,
    {
        T::parse(input).map(Self::new)
    }
}

impl<'r, T> AsValue for &'r Box<T>
where
    &'r T: AsValue,
{
    fn as_empty_value() -> ValueOwned {
        <&'r T>::as_empty_value()
    }
    fn as_value<'a>(self) -> Value<'a>
    where
        Self: 'a,
    {
        self.as_ref().as_value()
    }
}

macro_rules! impl_as_value {
    ($source:ident) => {
        impl<T: AsValue> AsValue for $source<T> {
            fn as_empty_value() -> ValueOwned {
                T::as_empty_value()
            }
            fn as_value<'a>(self) -> Value<'a>
            where
                Self: 'a,
            {
                $source::<T>::into_inner(self).as_value()
            }
            fn try_from_value(value: impl AsValue) -> Result<Self> {
                Ok($source::new(<T as AsValue>::try_from_value(value)?))
            }
        }
    };
}
// impl_as_value!(Box);
impl_as_value!(Cell);

impl<T: AsValue + Copy> AsValue for &Cell<T> {
    fn as_empty_value() -> ValueOwned {
        T::as_empty_value()
    }
    fn as_value<'a>(self) -> Value<'a>
    where
        Self: 'a,
    {
        self.get().as_value()
    }
}

impl_as_value!(RefCell);

impl<T> AsValue for &RefCell<T>
where
    for<'r> &'r T: AsValue,
{
    fn as_empty_value() -> ValueOwned {
        <&T>::as_empty_value()
    }
    fn as_value<'a>(self) -> Value<'a>
    where
        Self: 'a,
    {
        // A borrow of the guarded data must not escape the guard.
        (&*self.borrow()).as_value().into_owned()
    }
}

impl<T: AsValue> AsValue for RwLock<T> {
    fn as_empty_value() -> ValueOwned {
        T::as_empty_value()
    }
    fn as_value<'a>(self) -> Value<'a>
    where
        Self: 'a,
    {
        self.into_inner()
            .expect("Error occurred while trying to take the content of the RwLock")
            .as_value()
    }
    fn try_from_value(value: impl AsValue) -> Result<Self> {
        Ok(RwLock::new(<T as AsValue>::try_from_value(value)?))
    }
    fn parse(input: impl AsRef<str>) -> Result<Self>
    where
        Self: Sized,
    {
        T::parse(input).map(Self::new)
    }
}

impl<T> AsValue for &RwLock<T>
where
    for<'r> &'r T: AsValue,
{
    fn as_empty_value() -> ValueOwned {
        <&T>::as_empty_value()
    }
    fn as_value<'a>(self) -> Value<'a>
    where
        Self: 'a,
    {
        (&*self
            .read()
            .expect("Error occurred while trying to read the RwLock"))
            .as_value()
            .into_owned()
    }
}

macro_rules! impl_as_value {
    ($source:ident) => {
        impl<T: AsValue + ToOwned<Owned = impl AsValue>> AsValue for $source<T> {
            fn as_empty_value() -> ValueOwned {
                T::as_empty_value()
            }
            fn as_value<'a>(self) -> Value<'a>
            where
                Self: 'a,
            {
                $source::try_unwrap(self)
                    .map(|v| v.as_value())
                    .unwrap_or_else(|v| v.as_ref().to_owned().as_value().into_owned())
            }
            fn try_from_value(value: impl AsValue) -> Result<Self> {
                Ok($source::new(<T as AsValue>::try_from_value(value)?))
            }
            fn parse(input: impl AsRef<str>) -> Result<Self>
            where
                Self: Sized,
            {
                T::parse(input).map(Self::new)
            }
        }
        impl<'r, T> AsValue for &'r $source<T>
        where
            &'r T: AsValue,
        {
            fn as_empty_value() -> ValueOwned {
                <&'r T>::as_empty_value()
            }
            fn as_value<'a>(self) -> Value<'a>
            where
                Self: 'a,
            {
                self.as_ref().as_value()
            }
        }
    };
}
impl_as_value!(Arc);
impl_as_value!(Rc);

impl AsValue for serde_json::Value {
    fn as_empty_value() -> ValueOwned {
        Value::Json(None)
    }
    fn as_value<'a>(self) -> Value<'a>
    where
        Self: 'a,
    {
        Value::Json(Some(self))
    }
    fn try_from_value(value: impl AsValue) -> Result<Self>
    where
        Self: Sized,
    {
        let value = value.as_value();
        Ok(if let Value::Json(v) = value {
            match v {
                Some(v) => v,
                None => Self::Null,
            }
        } else {
            return Err(anyhow!(
                "Cannot convert non json tank::Value to serde_json::Value",
            ));
        })
    }
}

impl AsValue for &serde_json::Value {
    fn as_empty_value() -> ValueOwned {
        serde_json::Value::as_empty_value()
    }
    fn as_value<'a>(self) -> Value<'a>
    where
        Self: 'a,
    {
        Value::Json(Some(self.clone()))
    }
}

impl<'r, T: ?Sized> AsValue for &&'r T
where
    &'r T: AsValue,
{
    fn as_empty_value() -> ValueOwned {
        <&'r T>::as_empty_value()
    }
    fn as_value<'a>(self) -> Value<'a>
    where
        Self: 'a,
    {
        (*self).as_value()
    }
}
