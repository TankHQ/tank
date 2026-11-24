use rust_decimal::Decimal;
use scylla::{
    cluster::metadata::{CollectionType, ColumnType, NativeType},
    deserialize::{
        FrameSlice,
        value::{DeserializeValue, UdtIterator},
    },
    errors::{DeserializationError, SerializationError, TypeCheckError},
    serialize::{
        value::SerializeValue,
        writers::{CellWriter, WrittenCellProof},
    },
    value::{
        CqlDecimal, CqlDecimalBorrowed, CqlDuration, CqlTimestamp, CqlVarint, CqlVarintBorrowed,
    },
};
use std::{
    borrow::Cow,
    collections::{HashMap, HashSet},
    io::{self, Error, ErrorKind},
    net::IpAddr,
};
use tank_core::{
    AsValue, CollectionType as TankCollectionType, Interval, MapType, StructType, TableRef, Value,
    ValueOwned,
};
use time::{Date, OffsetDateTime, PrimitiveDateTime, Time};
use uuid::Uuid;

#[derive(Default, Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct ValueWrap(pub(crate) ValueOwned);

impl AsValue for ValueWrap {
    fn as_empty_value() -> ValueOwned {
        Value::Unknown(None)
    }

    fn as_value<'a>(self) -> Value<'a>
    where
        Self: 'a,
    {
        self.0
    }

    fn decode_value(value: impl AsValue) -> tank_core::Result<Self> {
        Ok(Self(value.as_value().into_owned()))
    }
}

impl SerializeValue for ValueWrap {
    fn serialize<'b>(
        &self,
        ty: &ColumnType,
        writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        let value = self.0.clone();
        let error = SerializationError::new(Error::new(
            ErrorKind::InvalidData,
            format!("Could not serialize {value:?} as ColumnType::UserDefinedType"),
        ));
        fn do_serialize<'b, V: AsValue + SerializeValue>(
            value: ValueOwned,
            ty: &ColumnType,
            writer: CellWriter<'b>,
        ) -> Result<WrittenCellProof<'b>, SerializationError> {
            Option::<V>::decode_value(value)
                .map_err(|e| {
                    SerializationError::new(Error::new(ErrorKind::InvalidData, format!("{}", e)))
                })?
                .serialize(ty, writer)
        }
        fn conversion_error(error: impl std::fmt::Display) -> SerializationError {
            SerializationError::new(Error::new(ErrorKind::InvalidData, format!("{error:#}")))
        }
        fn unsupported(ty: &ColumnType) -> SerializationError {
            SerializationError::new(Error::new(
                ErrorKind::InvalidData,
                format!("Unsupported ScyllaDB type {ty:?}"),
            ))
        }
        match ty {
            ColumnType::Native(t) => match t {
                NativeType::Ascii => do_serialize::<String>(value, ty, writer),
                NativeType::Boolean => do_serialize::<bool>(value, ty, writer),
                NativeType::Blob => do_serialize::<Vec<u8>>(value, ty, writer),
                NativeType::Counter => do_serialize::<Vec<u8>>(value, ty, writer),
                NativeType::Date => do_serialize::<Date>(value, ty, writer),
                NativeType::Decimal => {
                    if self.0.is_null() {
                        return Ok(writer.set_null());
                    }
                    let v = Decimal::decode_value(value).map_err(|e| {
                        SerializationError::new(Error::new(
                            ErrorKind::InvalidData,
                            format!("{}", e),
                        ))
                    })?;
                    CqlDecimal::from_signed_be_bytes_slice_and_exponent(
                        &v.mantissa().to_be_bytes(),
                        v.scale() as _,
                    )
                    .serialize(ty, writer)
                }
                NativeType::Double => do_serialize::<f64>(value, ty, writer),
                NativeType::Duration => Option::<Interval>::decode_value(value)
                    .map_err(conversion_error)?
                    .map(|v| CqlDuration {
                        months: v.months,
                        days: v.days,
                        nanoseconds: v.nanos,
                    })
                    .serialize(ty, writer),
                NativeType::Float => do_serialize::<f32>(value, ty, writer),
                NativeType::Int => do_serialize::<i32>(value, ty, writer),
                NativeType::BigInt => do_serialize::<i64>(value, ty, writer),
                NativeType::Text => do_serialize::<String>(value, ty, writer),
                NativeType::Timestamp => Option::<PrimitiveDateTime>::decode_value(value)
                    .map_err(conversion_error)?
                    .map(|v| CqlTimestamp::from(v.assume_utc()))
                    .serialize(ty, writer),
                NativeType::Inet => Option::<IpAddr>::decode_value(value)
                    .map_err(conversion_error)?
                    .serialize(ty, writer),
                NativeType::SmallInt => do_serialize::<i16>(value, ty, writer),
                NativeType::TinyInt => do_serialize::<i8>(value, ty, writer),
                NativeType::Time => do_serialize::<Time>(value, ty, writer),
                NativeType::Timeuuid => do_serialize::<Uuid>(value, ty, writer),
                NativeType::Uuid => do_serialize::<Uuid>(value, ty, writer),
                NativeType::Varint => {
                    fn signed(v: i128) -> CqlVarint {
                        CqlVarint::from_signed_bytes_be(v.to_be_bytes().to_vec())
                    }
                    fn unsigned(v: u128) -> CqlVarint {
                        let bytes = v.to_be_bytes();
                        if bytes[0] & 0x80 == 0 {
                            CqlVarint::from_signed_bytes_be(bytes.to_vec())
                        } else {
                            let mut signed = Vec::with_capacity(17);
                            signed.push(0);
                            signed.extend_from_slice(&bytes);
                            CqlVarint::from_signed_bytes_be(signed)
                        }
                    }
                    let varint = match value {
                        Value::Int8(Some(v), ..) => Some(signed(v as i128)),
                        Value::Int16(Some(v), ..) => Some(signed(v as i128)),
                        Value::Int32(Some(v), ..) => Some(signed(v as i128)),
                        Value::Int64(Some(v), ..) => Some(signed(v as i128)),
                        Value::Int128(Some(v), ..) => Some(signed(*v)),
                        Value::UInt8(Some(v), ..) => Some(unsigned(v as u128)),
                        Value::UInt16(Some(v), ..) => Some(unsigned(v as u128)),
                        Value::UInt32(Some(v), ..) => Some(unsigned(v as u128)),
                        Value::UInt64(Some(v), ..) => Some(unsigned(v as u128)),
                        Value::UInt128(Some(v), ..) => Some(unsigned(*v)),
                        Value::Int8(None, ..)
                        | Value::Int16(None, ..)
                        | Value::Int32(None, ..)
                        | Value::Int64(None, ..)
                        | Value::Int128(None, ..)
                        | Value::UInt8(None, ..)
                        | Value::UInt16(None, ..)
                        | Value::UInt32(None, ..)
                        | Value::UInt64(None, ..)
                        | Value::UInt128(None, ..)
                        | Value::Null => None,
                        other => {
                            return Err(SerializationError::new(Error::new(
                                ErrorKind::InvalidData,
                                format!("Cannot serialize {other:?} as a ScyllaDB varint"),
                            )));
                        }
                    };
                    varint.serialize(ty, writer)
                }
                _ => Err(unsupported(ty)),
            },
            ColumnType::Collection { frozen: _, typ } => match typ {
                CollectionType::List(..) => do_serialize::<Vec<ValueWrap>>(value, ty, writer),
                CollectionType::Map(..) => {
                    do_serialize::<HashMap<ValueWrap, ValueWrap>>(value, ty, writer)
                }
                CollectionType::Set(..) => do_serialize::<Vec<ValueWrap>>(value, ty, writer),
                _ => Err(unsupported(ty)),
            },
            ColumnType::Vector {
                typ: _,
                dimensions: _,
            } => do_serialize::<Vec<ValueWrap>>(value, ty, writer),
            ColumnType::UserDefinedType {
                frozen: _,
                definition,
            } => {
                if let Value::Struct(value, ..) = value {
                    let value = value.unwrap_or_default();
                    let mut builder = writer.into_value_builder();
                    for (field_name, field_type) in &*definition.field_types {
                        let sub_writer = builder.make_sub_writer();
                        if let Some((_, value)) =
                            value.iter().find(|(k, _)| k.as_str() == field_name)
                        {
                            ValueWrap(value.clone()).serialize(&field_type, sub_writer)?;
                        } else {
                            sub_writer.set_null();
                        }
                    }
                    builder.finish().map_err(|_| error)
                } else {
                    return Err(error);
                }
            }
            ColumnType::Tuple(_) => Err(unsupported(ty)),
            _ => Err(unsupported(ty)),
        }
    }
}

impl<'frame, 'metadata> DeserializeValue<'frame, 'metadata> for ValueWrap {
    fn type_check(_typ: &ColumnType) -> Result<(), TypeCheckError> {
        Ok(())
    }

    fn deserialize(
        ty: &'metadata ColumnType<'metadata>,
        v: Option<FrameSlice<'frame>>,
    ) -> Result<Self, DeserializationError> {
        let value = match ty {
            ColumnType::Native(native_type) => match native_type {
                NativeType::Boolean => Value::Boolean(DeserializeValue::deserialize(ty, v)?),
                NativeType::TinyInt => Value::Int8(DeserializeValue::deserialize(ty, v)?),
                NativeType::SmallInt => Value::Int16(DeserializeValue::deserialize(ty, v)?),
                NativeType::Int => Value::Int32(DeserializeValue::deserialize(ty, v)?),
                NativeType::BigInt => Value::Int64(DeserializeValue::deserialize(ty, v)?),
                NativeType::Counter => Value::Int64(DeserializeValue::deserialize(ty, v)?),
                NativeType::Varint => {
                    let mut unsigned = false;
                    let value = if let Some(varint) =
                        <Option<CqlVarintBorrowed> as DeserializeValue>::deserialize(ty, v)?
                    {
                        let mut bytes = varint.as_signed_bytes_be_slice();
                        let mut len = bytes.len();
                        if len == 17 && bytes[0] == 0 {
                            len = 16;
                            unsigned = true;
                            bytes = &bytes[1..];
                        }
                        if len > 16 {
                            return Err(DeserializationError::new(io::Error::new(
                                io::ErrorKind::InvalidData,
                                "Could not deserialize NativeType::Varint as Value::Int128: overflow (more than 16 bytes)",
                            )));
                        }
                        let is_negative = len > 0 && (bytes[0] & 0x80) != 0;
                        let mut padded = [if is_negative { 0xFFu8 } else { 0u8 }; 16];
                        padded[(16 - len)..].copy_from_slice(bytes);
                        let num = i128::from_be_bytes(padded);
                        Some(num)
                    } else {
                        None
                    };
                    if unsigned {
                        Value::UInt128(value.map(|v| Box::new(v as u128)))
                    } else {
                        Value::Int128(value.map(Box::new))
                    }
                }
                NativeType::Float => Value::Float32(DeserializeValue::deserialize(ty, v)?),
                NativeType::Double => Value::Float64(DeserializeValue::deserialize(ty, v)?),
                NativeType::Decimal => {
                    if let Some(d) =
                        <Option<CqlDecimalBorrowed> as DeserializeValue>::deserialize(ty, v)?
                    {
                        let error = |details: Cow<'_, str>| {
                            DeserializationError::new(io::Error::new(
                                io::ErrorKind::InvalidData,
                                format!(
                                    "Could not deserialize NativeType::Decimal as Value::Decimal{}{}",
                                    if !details.is_empty() { ": " } else { "" },
                                    details
                                ),
                            ))
                        };
                        let (bytes, mut scale) = d.as_signed_be_bytes_slice_and_exponent();
                        let len = bytes.len();
                        if len > 16 {
                            return Err(error("overflow (more than 16 bytes)".into()));
                        }
                        let pad_len = 16 - len;
                        let is_negative = len > 0 && (bytes[0] & 0x80) != 0;
                        let mut padded = [if is_negative { 0xFFu8 } else { 0u8 }; 16];
                        padded[pad_len..].copy_from_slice(bytes);
                        let mut num = i128::from_be_bytes(padded);
                        if scale < 0 {
                            let Some(scaled) = 10_i128
                                .checked_pow(scale.unsigned_abs())
                                .and_then(|p| num.checked_mul(p))
                            else {
                                return Err(error(
                                    format!("overflow (while applying the scale {scale})").into(),
                                ));
                            };
                            scale = 0;
                            num = scaled;
                        }
                        if scale > u8::MAX as _ {
                            return Err(error(
                                format!("overflow (scale {scale} is too big)").into(),
                            ));
                        }
                        let scale = scale as u8;
                        let value = Decimal::try_from_i128_with_scale(num, scale as _)
                            .map_err(|e| error(format!("{e:?} (mantissa: {num})").into()))?;
                        Value::Decimal(Some(value), 0, scale)
                    } else {
                        Value::Decimal(None, 0, 0)
                    }
                }
                NativeType::Ascii => Value::Varchar(
                    <Option<String> as DeserializeValue>::deserialize(ty, v)?.map(Into::into),
                ),
                NativeType::Text => Value::Varchar(
                    <Option<String> as DeserializeValue>::deserialize(ty, v)?.map(Into::into),
                ),
                NativeType::Blob => Value::Blob(
                    <Option<Vec<u8>> as DeserializeValue>::deserialize(ty, v)?.map(Into::into),
                ),
                NativeType::Date => Value::Date(DeserializeValue::deserialize(ty, v)?),
                NativeType::Time => Value::Time(DeserializeValue::deserialize(ty, v)?),
                NativeType::Timestamp => Value::Timestamp(
                    <Option<CqlTimestamp> as DeserializeValue>::deserialize(ty, v)?
                        .map(|v| {
                            OffsetDateTime::from_unix_timestamp_nanos((v.0 as i128) * 1_000_000)
                                .map(|v| PrimitiveDateTime::new(v.date(), v.time()))
                        })
                        .transpose()
                        .map_err(DeserializationError::new)?,
                ),
                NativeType::Duration => Value::Interval(
                    <Option<CqlDuration> as DeserializeValue>::deserialize(ty, v)?.map(|v| {
                        Interval {
                            months: v.months as _,
                            days: v.days as _,
                            nanos: v.nanoseconds as _,
                        }
                    }),
                ),
                NativeType::Inet => {
                    Value::IpAddr(<Option<IpAddr> as DeserializeValue>::deserialize(ty, v)?)
                }
                NativeType::Timeuuid => Value::Uuid(DeserializeValue::deserialize(ty, v)?),
                NativeType::Uuid => Value::Uuid(DeserializeValue::deserialize(ty, v)?),
                _ => {
                    let error = DeserializationError::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("Unexpected type {ty:?} from ScyllaDB"),
                    ));
                    log::error!("{error:#}");
                    return Err(error);
                }
            },
            ColumnType::Collection { frozen: _, typ } => match typ {
                CollectionType::List(elem_type) => Value::List(
                    <Option<Vec<ValueWrap>> as DeserializeValue>::deserialize(ty, v)?
                        .map(|v| v.into_iter().map(|v| v.0).collect()),
                    Box::new(TankCollectionType {
                        element: Self::deserialize(elem_type, None)?.0,
                        len: 0,
                    }),
                ),
                CollectionType::Map(k_type, v_type) => Value::Map(
                    <Option<HashMap<ValueWrap, ValueWrap>> as DeserializeValue>::deserialize(
                        ty, v,
                    )?
                    .map(|v| Box::new(v.into_iter().map(|(k, v)| (k.0, v.0)).collect())),
                    Box::new(MapType {
                        key: Self::deserialize(k_type, None)?.0,
                        value: Self::deserialize(v_type, None)?.0,
                    }),
                ),
                CollectionType::Set(elem_type) => Value::List(
                    <Option<HashSet<ValueWrap>> as DeserializeValue>::deserialize(ty, v)?
                        .map(|v| v.into_iter().map(|v| v.0).collect()),
                    Box::new(TankCollectionType {
                        element: Self::deserialize(elem_type, None)?.0,
                        len: 0,
                    }),
                ),
                _ => {
                    return Err(DeserializationError::new(Error::new(
                        ErrorKind::InvalidData,
                        format!("Unexpected collection type {ty:?}"),
                    )));
                }
            },
            ColumnType::Vector { typ, dimensions } => Value::Array(
                <Option<Vec<ValueWrap>> as DeserializeValue>::deserialize(ty, v)?
                    .map(|v| v.into_iter().map(|v| v.0).collect()),
                Box::new(TankCollectionType {
                    element: Self::deserialize(typ, None)?.0,
                    len: *dimensions as _,
                }),
            ),
            ColumnType::UserDefinedType {
                frozen: _,
                definition,
            } => {
                let mut type_ref = TableRef::new(definition.name.to_string().into());
                type_ref.schema = definition.keyspace.to_string().into();
                let fields = UdtIterator::deserialize(ty, v)?
                    .map(|((name, ty), res)| {
                        res.and_then(|v| {
                            let val = Option::<ValueWrap>::deserialize(ty, v.flatten())?
                                .unwrap_or_default()
                                .0;
                            Ok((name.clone().into_owned(), val))
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let ty = fields
                    .iter()
                    .map(|(name, value)| (name.clone(), value.as_null()))
                    .collect();
                Value::Struct(
                    if v.is_none() {
                        None
                    } else {
                        Some(fields.into_boxed_slice())
                    },
                    Box::new(StructType {
                        fields: ty,
                        name: type_ref,
                    }),
                )
            }
            ColumnType::Tuple(elem_types) => Value::Array(
                <Option<Vec<ValueWrap>> as DeserializeValue>::deserialize(ty, v)?
                    .map(|v| v.into_iter().map(|v| v.0).collect()),
                Box::new(TankCollectionType {
                    element: Value::Unknown(None),
                    len: elem_types.len() as _,
                }),
            ),
            _ => {
                return Err(DeserializationError::new(Error::new(
                    ErrorKind::InvalidData,
                    format!("Unexpected type {ty:?}"),
                )));
            }
        };
        Ok(ValueWrap(value))
    }
}

impl From<ValueOwned> for ValueWrap {
    fn from(value: ValueOwned) -> Self {
        Self(value)
    }
}

impl From<ValueWrap> for ValueOwned {
    fn from(value: ValueWrap) -> Self {
        value.0
    }
}
