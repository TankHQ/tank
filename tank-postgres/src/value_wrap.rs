use crate::{
    interval_wrap::IntervalWrap,
    util::{flatten_array, value_to_postgres_type},
};
use anyhow::anyhow;
use bytes::{BufMut, BytesMut};
use postgres_protocol::types::array_to_sql;
use postgres_types::{IsNull, ToSql, Type};
use rust_decimal::{Decimal, prelude::FromPrimitive};
use tank_core::{Value, ValueOwned};
use wtx::{
    codec::Encode,
    database::Typed,
    database::client::postgres::{Postgres, PostgresEncodeWrapper, Ty},
};

/// A `ToSql` adapter over a borrowed tank [`Value`].
#[derive(Debug)]
struct ValueSql<'a>(&'a Value<'a>);

impl ToSql for ValueSql<'_> {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        match self.0 {
            Value::Null => None::<String>.to_sql(ty, out),
            Value::Boolean(v) => v.to_sql(ty, out),
            Value::Int8(v) => v.map(|v| v as i16).to_sql(ty, out),
            Value::Int16(v) => v.to_sql(ty, out),
            Value::Int32(v) => v.to_sql(ty, out),
            Value::Int64(v) => v.to_sql(ty, out),
            Value::Int128(v) => v.as_deref().copied().map(Decimal::from_i128).to_sql(ty, out),
            Value::UInt8(v) => v.map(|v| v as i16).to_sql(ty, out),
            Value::UInt16(v) => v.map(|v| v as i32).to_sql(ty, out),
            Value::UInt32(v) => v.map(|v| v as i64).to_sql(ty, out),
            Value::UInt64(v) => v.map(Decimal::from_u64).to_sql(ty, out),
            Value::UInt128(v) => v.as_deref().copied().map(Decimal::from_u128).to_sql(ty, out),
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
                    let (vector, dimensions, element_type) = flatten_array(v, &element.element);
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
            _ => Err(anyhow!(
                "tank::Value variant `{:?}` is not supported by Postgres",
                self.0
            )
            .into()),
        }
    }

    fn accepts(_ty: &Type) -> bool {
        true
    }

    postgres_types::to_sql_checked!();
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
    fn new(value: &Value<'_>) -> anyhow::Result<Self> {
        let mut param = Self {
            ty: Ty::Unknown,
            is_null: true,
            bytes: BytesMut::new(),
        };
        param.encode_value(value)?;
        Ok(param)
    }

    fn encode_value(&mut self, value: &Value<'_>) -> anyhow::Result<()> {
        let pg_ty = value_to_postgres_type(value);
        self.bytes.clear();
        self.is_null = match ValueSql(value).to_sql(&pg_ty, &mut self.bytes) {
            Ok(IsNull::Yes) => true,
            Ok(IsNull::No) => false,
            Err(e) => {
                return Err(anyhow!("Could not encode parameter {value:?}: {e}"));
            }
        };
        self.ty = ty_to_wtx(pg_ty.oid());
        Ok(())
    }

    /// Encodes a list of tank values into bindable parameters.
    pub(crate) fn encode_all(values: &[ValueOwned], params: &mut Vec<Self>) -> anyhow::Result<()> {
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
    pub(crate) fn new(sql: &'a str, params: &'a [TankParam]) -> Self {
        Self { sql, params }
    }
}

impl wtx::database::StmtCmd for TankStmt<'_> {
    fn cmd(&self) -> Option<&str> {
        Some(self.sql)
    }

    fn hash(&self, hasher: &mut impl core::hash::BuildHasher) -> u64 {
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

/// Maps a PostgreSQL type OID to the `wtx` type enum.
///
/// `wtx` does not publicly expose an OID → `Ty` constructor, so the common
/// types tank produces are listed explicitly and everything else falls back to
/// `Ty::Custom`, which carries the raw OID verbatim.
fn ty_to_wtx(oid: u32) -> Ty {
    match oid {
        16 => Ty::Bool,
        17 => Ty::Bytea,
        18 => Ty::Char,
        19 => Ty::Name,
        20 => Ty::Int8,
        21 => Ty::Int2,
        23 => Ty::Int4,
        25 => Ty::Text,
        26 => Ty::Oid,
        114 => Ty::Json,
        700 => Ty::Float4,
        701 => Ty::Float8,
        705 => Ty::Unknown,
        1042 => Ty::Bpchar,
        1043 => Ty::Varchar,
        1082 => Ty::Date,
        1083 => Ty::Time,
        1114 => Ty::Timestamp,
        1184 => Ty::Timestamptz,
        1186 => Ty::Interval,
        1700 => Ty::Numeric,
        2950 => Ty::Uuid,
        1000 => Ty::BoolArray,
        1001 => Ty::ByteaArray,
        1002 => Ty::CharArray,
        1003 => Ty::NameArray,
        1005 => Ty::Int2Array,
        1007 => Ty::Int4Array,
        1009 => Ty::TextArray,
        1014 => Ty::BpcharArray,
        1015 => Ty::VarcharArray,
        1016 => Ty::Int8Array,
        1021 => Ty::Float4Array,
        1022 => Ty::Float8Array,
        1115 => Ty::TimestampArray,
        1182 => Ty::DateArray,
        1183 => Ty::TimeArray,
        1185 => Ty::TimestamptzArray,
        1187 => Ty::IntervalArray,
        1231 => Ty::NumericArray,
        2951 => Ty::UuidArray,
        other => Ty::Custom(other),
    }
}

/// Keeps `BufMut` referenced (used transitively by the `ToSql` impls).
#[allow(dead_code)]
fn _keep(_: &mut dyn BufMut) {}
