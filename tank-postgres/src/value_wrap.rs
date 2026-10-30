use crate::interval_wrap::IntervalWrap;
use anyhow::anyhow;
use bytes::BytesMut;
use core::hash::BuildHasher;
use postgres_protocol::types::{ArrayDimension, array_to_sql};
use postgres_types::{IsNull, Kind, ToSql, Type, to_sql_checked};
use rust_decimal::{Decimal, prelude::FromPrimitive};
use std::error::Error;
use tank_core::{Value, ValueOwned};
use wtx::{
    codec::Encode,
    database::{
        StmtCmd, Typed,
        client::postgres::{Postgres, PostgresEncodeWrapper, Ty},
    },
};

/// A `ToSql` adapter over a borrowed tank [`Value`].
#[derive(Debug)]
struct ValueSql<'a>(&'a Value<'a>);

impl<'a> ValueSql<'a> {
    fn flatten_array(
        values: &'a [Value<'a>],
        mut element_type: &'a Value<'a>,
    ) -> (Vec<&'a Value<'a>>, Vec<ArrayDimension>, Type) {
        let mut dimensions = Vec::new();
        let mut current_level = values;
        let mut total_capacity = 1;
        loop {
            let len = current_level.len();
            dimensions.push(ArrayDimension {
                len: len as i32,
                lower_bound: 1,
            });
            total_capacity *= len;
            if len == 0 {
                total_capacity = 0;
                break;
            }
            match current_level.first() {
                Some(Value::Array(Some(inner), ..) | Value::List(Some(inner), ..)) => {
                    current_level = inner
                }
                _ => break,
            }
        }
        loop {
            match element_type {
                Value::Array(.., t) | Value::List(.., t) => element_type = &t.element,
                _ => break,
            }
        }
        let pg_type = Self::value_to_postgres_type(element_type);
        let mut collector = Vec::with_capacity(total_capacity);
        Self::flatten_recursive(values, &mut collector);
        (collector, dimensions, pg_type)
    }

    fn flatten_recursive(values: &'a [Value<'a>], collector: &mut Vec<&'a Value<'a>>) {
        for value in values {
            match value {
                Value::Array(Some(inner), ..) | Value::List(Some(inner), ..) => {
                    Self::flatten_recursive(inner, collector);
                }
                Value::Array(None, ..) | Value::List(None, ..) => {}
                _ => collector.push(value),
            }
        }
    }

    fn value_to_postgres_type(value: &Value) -> Type {
        assert!(matches!(Type::BPCHAR_ARRAY.kind(), Kind::Array(..)));
        match value {
            Value::Boolean(..) => Type::BOOL,
            Value::Int8(..) => Type::INT2,
            Value::Int16(..) => Type::INT2,
            Value::Int32(..) => Type::INT4,
            Value::Int64(..) => Type::INT8,
            Value::Int128(..) => Type::NUMERIC,
            Value::UInt8(..) => Type::INT2,
            Value::UInt16(..) => Type::INT4,
            Value::UInt32(..) => Type::INT8,
            Value::UInt64(..) => Type::NUMERIC,
            Value::UInt128(..) => Type::NUMERIC,
            Value::Float32(..) => Type::FLOAT4,
            Value::Float64(..) => Type::FLOAT8,
            Value::Decimal(.., _, _) => Type::NUMERIC,
            Value::Char(..) => Type::BPCHAR,
            Value::Varchar(..) => Type::TEXT,
            Value::Blob(..) => Type::BYTEA,
            Value::Date(..) => Type::DATE,
            Value::Time(..) => Type::TIME,
            Value::Timestamp(..) => Type::TIMESTAMP,
            Value::TimestampWithTimezone(..) => Type::TIMESTAMPTZ,
            Value::Interval(..) => Type::INTERVAL,
            Value::Uuid(..) => Type::UUID,
            Value::Array(.., t) | Value::List(.., t) => match &t.element {
                Value::Boolean(..) => Type::BOOL_ARRAY,
                Value::Int8(..) => Type::INT2_ARRAY,
                Value::Int16(..) => Type::INT2_ARRAY,
                Value::Int32(..) => Type::INT4_ARRAY,
                Value::Int64(..) => Type::INT8_ARRAY,
                Value::Int128(..) => Type::NUMERIC_ARRAY,
                Value::UInt8(..) => Type::INT2_ARRAY,
                Value::UInt16(..) => Type::INT4_ARRAY,
                Value::UInt32(..) => Type::INT8_ARRAY,
                Value::UInt64(..) => Type::NUMERIC_ARRAY,
                Value::UInt128(..) => Type::NUMERIC_ARRAY,
                Value::Float32(..) => Type::FLOAT4_ARRAY,
                Value::Float64(..) => Type::FLOAT8_ARRAY,
                Value::Decimal(..) => Type::NUMERIC_ARRAY,
                Value::Char(..) => Type::BPCHAR_ARRAY,
                Value::Varchar(..) => Type::TEXT_ARRAY,
                Value::Blob(..) => Type::BYTEA_ARRAY,
                Value::Date(..) => Type::DATE_ARRAY,
                Value::Time(..) => Type::TIME_ARRAY,
                Value::Timestamp(..) => Type::TIMESTAMP_ARRAY,
                Value::TimestampWithTimezone(..) => Type::TIMESTAMPTZ_ARRAY,
                Value::Interval(..) => Type::INTERVAL_ARRAY,
                Value::Uuid(..) => Type::UUID_ARRAY,
                Value::Array(..) | Value::List(..) => Self::value_to_postgres_type(&t.element),
                Value::Map(..) | Value::Json(..) | Value::Struct(..) => Type::JSON_ARRAY,
                _ => Type::UNKNOWN,
            },
            Value::Map(..) | Value::Json(..) | Value::Struct(..) => Type::JSON,
            _ => Type::UNKNOWN,
        }
    }
}

impl ToSql for ValueSql<'_> {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        match self.0 {
            Value::Null => None::<String>.to_sql(ty, out),
            Value::Boolean(v) => v.to_sql(ty, out),
            Value::Int8(v) => v.map(|v| v as i16).to_sql(ty, out),
            Value::Int16(v) => v.to_sql(ty, out),
            Value::Int32(v) => v.to_sql(ty, out),
            Value::Int64(v) => v.to_sql(ty, out),
            Value::Int128(v) => v
                .as_deref()
                .copied()
                .map(Decimal::from_i128)
                .to_sql(ty, out),
            Value::UInt8(v) => v.map(|v| v as i16).to_sql(ty, out),
            Value::UInt16(v) => v.map(|v| v as i32).to_sql(ty, out),
            Value::UInt32(v) => v.map(|v| v as i64).to_sql(ty, out),
            Value::UInt64(v) => v.map(Decimal::from_u64).to_sql(ty, out),
            Value::UInt128(v) => v
                .as_deref()
                .copied()
                .map(Decimal::from_u128)
                .to_sql(ty, out),
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
            Value::Array(v, element) | Value::List(v, element) => match v {
                Some(v) => {
                    let (vector, dimensions, element_type) =
                        Self::flatten_array(v, &element.element);
                    array_to_sql(
                        dimensions,
                        element_type.oid(),
                        vector.into_iter().map(|v| ValueSql(v)),
                        |e, w| match e.to_sql(&element_type, w)? {
                            IsNull::No => Ok(postgres_protocol::IsNull::No),
                            IsNull::Yes => Ok(postgres_protocol::IsNull::Yes),
                        },
                        out,
                    )?;
                    Ok(IsNull::No)
                }
                None => None::<Vec<ValueSql>>.to_sql(ty, out),
            },
            _ => Err(anyhow!("Value variant `{:?}` is not supported by Postgres", self.0).into()),
        }
    }

    fn accepts(_ty: &Type) -> bool {
        true
    }

    to_sql_checked!();
}

/// A single parameter to bind, pre-encoded into the PostgreSQL binary format.
///
/// The value is serialized once with the existing `postgres-types` codecs, so
/// the wtx encoder only has to copy bytes around. A slice of these implements
/// [`wtx::database::RecordValues`] through wtx's blanket `&[T]` impl.
#[derive(Debug, Clone)]
pub(crate) struct TankParam {
    pub(crate) ty: Ty,
    pub(crate) is_null: bool,
    pub(crate) bytes: BytesMut,
}

impl TankParam {
    fn new(value: &Value<'_>) -> tank_core::Result<Self> {
        let mut param = Self {
            ty: Ty::Unknown,
            is_null: true,
            bytes: BytesMut::new(),
        };
        param.encode_value(value)?;
        Ok(param)
    }

    fn encode_value(&mut self, value: &Value<'_>) -> tank_core::Result<()> {
        let pg_ty = ValueSql::value_to_postgres_type(value);
        self.bytes.clear();
        self.is_null = match ValueSql(value).to_sql(&pg_ty, &mut self.bytes) {
            Ok(IsNull::Yes) => true,
            Ok(IsNull::No) => false,
            Err(e) => {
                return Err(anyhow!("Could not encode parameter {value:?}: {e}"));
            }
        };
        self.ty = Ty::from(pg_ty.oid());
        Ok(())
    }

    /// Encodes a list of tank values into bindable parameters.
    pub(crate) fn encode_all(
        values: &[ValueOwned],
        params: &mut Vec<Self>,
    ) -> tank_core::Result<()> {
        params.truncate(values.len());
        for (index, value) in values.iter().enumerate() {
            if let Some(param) = params.get_mut(index) {
                param.encode_value(value)?;
            } else {
                params.push(Self::new(value)?);
            }
        }
        Ok(())
    }
}

impl Encode<Postgres<wtx::Error>> for TankParam {
    #[inline]
    fn encode(&self, ew: &mut PostgresEncodeWrapper<'_>) -> Result<(), wtx::Error> {
        ew.buffer()
            .extend_from_copyable_slice(&self.bytes)
            .map_err(wtx::Error::from)?;
        Ok(())
    }

    #[inline]
    fn is_null(&self) -> bool {
        self.is_null
    }
}

impl Typed<Postgres<wtx::Error>> for TankParam {
    #[inline]
    fn runtime_ty(&self) -> Option<Ty> {
        Some(self.ty)
    }

    #[inline]
    fn static_ty() -> Option<Ty> {
        None
    }
}

/// A statement command carrying both the SQL text and the parameter types.
///
/// `wtx` caches prepared statements by the hash returned from
/// [`wtx::database::StmtCmd`]. Hashing only the SQL would reuse a statement
/// whose parameter OIDs were inferred from a previous binding of the same SQL
/// with different types, producing a wire-level size mismatch. Including the
/// parameter OIDs in the hash keeps distinct bindings distinct.
#[derive(Debug)]
pub(crate) struct TankStmt<'a> {
    pub(crate) sql: &'a str,
    params: &'a [TankParam],
}

impl<'a> TankStmt<'a> {
    #[inline]
    pub(crate) fn new(sql: &'a str, params: &'a [TankParam]) -> Self {
        Self { sql, params }
    }
}

impl StmtCmd for TankStmt<'_> {
    #[inline]
    fn cmd(&self) -> Option<&str> {
        Some(self.sql)
    }

    #[inline]
    fn hash(&self, hasher: &mut impl BuildHasher) -> u64 {
        let mut h = hasher.hash_one(self.sql);
        for param in self.params {
            let ty = u32::from(param.ty);
            h = h
                .wrapping_mul(0x100_0000_01b3)
                .wrapping_add(hasher.hash_one(ty));
        }
        h
    }
}
