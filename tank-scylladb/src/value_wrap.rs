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
    value::{Counter, CqlDecimalBorrowed, CqlDuration, CqlTimestamp, CqlVarintBorrowed},
};
use std::{
    array,
    borrow::Cow,
    collections::{HashMap, HashSet},
    io::{Error, ErrorKind},
};
use tank_core::{AsValue, Interval, TableRef, Value};
use time::{Date, OffsetDateTime, PrimitiveDateTime, Time};
use uuid::Uuid;

#[derive(Default, Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct ValueWrap(pub(crate) Value);

impl From<Value> for ValueWrap {
    fn from(value: tank_core::Value) -> Self {
        Self(value)
    }
}

impl From<ValueWrap> for Value {
    fn from(value: ValueWrap) -> Self {
        value.0
    }
}

impl AsValue for ValueWrap {
    fn as_empty_value() -> Value {
        Value::Unknown(None)
    }
    fn as_value(self) -> Value {
        self.0
    }
    fn try_from_value(value: Value) -> tank_core::Result<Self> {
        Ok(Self(value))
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
            value: Value,
            ty: &ColumnType,
            writer: CellWriter<'b>,
        ) -> Result<WrittenCellProof<'b>, SerializationError> {
            V::try_from_value(value)
                .map_err(|e| {
                    SerializationError::new(Error::new(ErrorKind::InvalidData, format!("{}", e)))
                })?
                .serialize(ty, writer)
        }
        match ty {
            ColumnType::Native(t) => match t {
                NativeType::Ascii => do_serialize::<String>(value, ty, writer),
                NativeType::Boolean => do_serialize::<bool>(value, ty, writer),
                NativeType::Blob => do_serialize::<Vec<u8>>(value, ty, writer),
                NativeType::Counter => Counter(i64::try_from_value(value).map_err(|e| {
                    SerializationError::new(Error::new(ErrorKind::InvalidData, format!("{}", e)))
                })?)
                .serialize(ty, writer),
                NativeType::Date => do_serialize::<Date>(value, ty, writer),
                NativeType::Decimal => {
                    let decimal = Decimal::try_from_value(value).map_err(|e| {
                        SerializationError::new(Error::new(
                            ErrorKind::InvalidData,
                            format!("{}", e),
                        ))
                    })?;
                    CqlDecimalBorrowed::from_signed_be_bytes_slice_and_exponent(
                        &decimal.mantissa().to_be_bytes(),
                        decimal.scale() as _,
                    )
                    .serialize(ty, writer)
                }
                NativeType::Double => do_serialize::<f64>(value, ty, writer),
                NativeType::Duration => todo!(),
                NativeType::Float => do_serialize::<f32>(value, ty, writer),
                NativeType::Int => do_serialize::<i32>(value, ty, writer),
                NativeType::BigInt => do_serialize::<i64>(value, ty, writer),
                NativeType::Text => do_serialize::<String>(value, ty, writer),
                NativeType::Timestamp => todo!(),
                NativeType::Inet => todo!(),
                NativeType::SmallInt => do_serialize::<i16>(value, ty, writer),
                NativeType::TinyInt => do_serialize::<i8>(value, ty, writer),
                NativeType::Time => do_serialize::<Time>(value, ty, writer),
                NativeType::Timeuuid => do_serialize::<Uuid>(value, ty, writer),
                NativeType::Uuid => do_serialize::<Uuid>(value, ty, writer),
                NativeType::Varint => todo!(),
                _ => todo!(),
            },
            ColumnType::Collection { frozen: _, typ } => match typ {
                CollectionType::List(..) => do_serialize::<Vec<ValueWrap>>(value, ty, writer),
                CollectionType::Map(..) => {
                    do_serialize::<HashMap<ValueWrap, ValueWrap>>(value, ty, writer)
                }
                CollectionType::Set(..) => do_serialize::<Vec<ValueWrap>>(value, ty, writer),
                _ => todo!(),
            },
            ColumnType::Vector { typ: _, dimensions } => {
                do_serialize::<Vec<ValueWrap>>(value, ty, writer)
            }
            ColumnType::UserDefinedType {
                frozen: _,
                definition,
            } => {
                if let Value::Struct(value, ..) = value {
                    let value = value.unwrap_or_default();
                    let mut builder = writer.into_value_builder();
                    for (field_name, field_type) in &*definition.field_types {
                        let sub_writer = builder.make_sub_writer();
                        if let Some((k, value)) =
                            value.iter().find(|(k, v)| k.as_str() == field_name)
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
            ColumnType::Tuple(column_types) => todo!(),
            _ => todo!(),
        }
    }
}

impl<'frame, 'metadata> DeserializeValue<'frame, 'metadata> for ValueWrap {
    fn type_check(typ: &ColumnType) -> Result<(), TypeCheckError> {
        Ok(())
    }
    fn deserialize(
        ty: &'metadata ColumnType<'metadata>,
        v: Option<FrameSlice<'frame>>,
    ) -> Result<Self, DeserializationError> {
        let value = match ty {
            ColumnType::Native(native_type) => match native_type {
                NativeType::Ascii => Value::Varchar(DeserializeValue::deserialize(ty, v)?),
                NativeType::Boolean => Value::Boolean(DeserializeValue::deserialize(ty, v)?),
                NativeType::Blob => Value::Blob(
                    <Option<Vec<u8>> as DeserializeValue>::deserialize(ty, v)?.map(Into::into),
                ),
                NativeType::Counter => todo!(),
                NativeType::Date => Value::Date(DeserializeValue::deserialize(ty, v)?),
                NativeType::Decimal => Value::Decimal(
                    <Option<CqlDecimalBorrowed> as DeserializeValue>::deserialize(ty, v)?.map(
                        |v| {
                            let (bytes, scale) = v.as_signed_be_bytes_slice_and_exponent();
                            let num = i128::from_be_bytes(array::from_fn(|i| {
                                if i < 16 { bytes[i] } else { 0 }
                            }));
                            Decimal::from_i128_with_scale(num, scale as _)
                        },
                    ),
                    0,
                    0,
                ),
                NativeType::Double => Value::Float64(DeserializeValue::deserialize(ty, v)?),
                NativeType::Duration => Value::Interval(
                    <Option<CqlDuration> as DeserializeValue>::deserialize(ty, v)?.map(|v| {
                        Interval {
                            months: v.months as _,
                            days: v.days as _,
                            nanos: v.nanoseconds as _,
                        }
                    }),
                ),
                NativeType::Float => Value::Float32(DeserializeValue::deserialize(ty, v)?),
                NativeType::Int => Value::Int32(DeserializeValue::deserialize(ty, v)?),
                NativeType::BigInt => Value::Int64(DeserializeValue::deserialize(ty, v)?),
                NativeType::Text => Value::Varchar(DeserializeValue::deserialize(ty, v)?),
                NativeType::Timestamp => Value::Timestamp(
                    <Option<CqlTimestamp> as DeserializeValue>::deserialize(ty, v)?.map(|v| {
                        OffsetDateTime::from_unix_timestamp_nanos(v.0 as _).map(
                            |v| PrimitiveDateTime::new(v.date(), v.time())
                        )
                    })
                        .transpose()
                        .map_err(DeserializationError::new)?,
                ),
                NativeType::Inet => todo!(),
                NativeType::SmallInt => Value::Int16(DeserializeValue::deserialize(ty, v)?),
                NativeType::TinyInt => Value::Int8(DeserializeValue::deserialize(ty, v)?),
                NativeType::Time => Value::Time(DeserializeValue::deserialize(ty, v)?),
                NativeType::Timeuuid => Value::Uuid(DeserializeValue::deserialize(ty, v)?),
                NativeType::Uuid => Value::Uuid(DeserializeValue::deserialize(ty, v)?),
                NativeType::Varint => Value::Int128(
                    <Option<CqlVarintBorrowed> as DeserializeValue>::deserialize(ty, v)
                        .map(|v| {
                            v.map(|v| {
                                let bytes = v.as_signed_bytes_be_slice();
                                if bytes.len() > 16 {
                                    return Err(DeserializationError::new(Error::new(
                                        ErrorKind::InvalidData,
                                        "The varint value cannot be represented as a 128 bit integer"
                                    )));
                                }
                                Ok(i128::from_be_bytes(array::from_fn(|i| {
                                    if i < 16 { bytes[i] } else { 0 }
                                })))
                            })
                            .transpose()
                        })
                        .flatten()?,
                ),
                _ => todo!(),
            },
            ColumnType::Collection { frozen, typ } => match typ {
                CollectionType::List(elem_type) => Value::List(
                    <Option<Vec<ValueWrap>> as DeserializeValue>::deserialize(ty, v)?
                        .map(|v|  v.into_iter().map(|v| v.0).collect()),
                    Self::deserialize(elem_type, None)?.0.into(),
                ),
                CollectionType::Map(k_type, v_type) => Value::Map(
                    <Option<HashMap<ValueWrap, ValueWrap>> as DeserializeValue>::deserialize(ty, v)?
                        .map(|v|  v.into_iter().map(|(k,v)| (k.0,v.0)).collect()),
                    Self::deserialize(k_type, None)?.0.into(),
                    Self::deserialize(v_type, None)?.0.into(),
                ),
                CollectionType::Set(elem_type) => Value::List(
                    <Option<HashSet<ValueWrap>> as DeserializeValue>::deserialize(ty, v)?
                        .map(|v|  v.into_iter().map(|v| v.0).collect()),
                    Self::deserialize(elem_type, None)?.0.into(),
                ),
                _ => return Err(DeserializationError::new(Error::new(
                    ErrorKind::InvalidData,
                    format!("Unexpected collection type {ty:?}")
                )))
            }
            ColumnType::Vector { typ, dimensions } => Value::Array(
                <Option<Vec<ValueWrap>> as DeserializeValue>::deserialize(ty, v)?
                    .map(|v|  v.into_iter().map(|v| v.0).collect()),
                Self::deserialize(typ, None)?.0.into(),
                *dimensions as _
            ),
            ColumnType::UserDefinedType { frozen, definition } => {
                let type_ref = TableRef {
                    schema: definition.keyspace.to_string().into(),
                    name: definition.name.to_string().into(),
                    alias: Cow::Borrowed(""),
                };
                let fields = UdtIterator::deserialize(ty, v)?
                    .map(|((name, ty), res)| {
                        res.and_then(|v| {
                            let val = Option::<ValueWrap>::deserialize(ty, v.flatten())?.unwrap_or_default().0;
                            Ok((name.clone().into_owned(), val))
                        })
                     })
                    .collect::<Result<Vec<_>, _>>()?;
                let ty = fields.iter().map(|(name, value)| {
                    (name.clone(), value.as_null())
                }).collect();
                Value::Struct(if v.is_none() {None} else {Some(fields)}, ty, type_ref)
            },
            ColumnType::Tuple(elem_types) => Value::Array(
                <Option<Vec<ValueWrap>> as DeserializeValue>::deserialize(ty, v)?
                    .map(|v|  v.into_iter().map(|v| v.0).collect()),
                Value::Unknown(None).into(),
                elem_types.len() as _
            ),
            _ =>  {
                return Err(DeserializationError::new(Error::new(
                    ErrorKind::InvalidData,
                    format!("Unexpected type {ty:?}")
                )));
            }
        };
        Ok(ValueWrap(value))
    }
}
