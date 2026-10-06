use crate::{MongoDBDriver, MongoDBPrepared, MongoDBSqlWriter, value_to_bson};
use mongodb::bson::{self, Binary, Bson, Document, spec::BinarySubtype};
use std::{collections::HashMap, mem};
use tank_core::{AsValue, Context, DynQuery, Expression, Interval, SqlValueWriter, Value};
use time::{Date, OffsetDateTime, PrimitiveDateTime, Time};
use uuid::Uuid;

macro_rules! write_value_fn {
    ($fn_name:ident, $ty:ty, $bson:path) => {
        fn $fn_name(&self, _context: &mut Context, out: &mut DynQuery, value: $ty) {
            let Some(target) = out
                .as_prepared::<MongoDBDriver>()
                .and_then(MongoDBPrepared::current_bson)
            else {
                log::error!(
                    "Failed to get the bson in MongoDBSqlWriter::{}",
                    stringify!($fn_name)
                );
                return;
            };
            *target = $bson(value as _);
        }
    };
}

impl SqlValueWriter for MongoDBSqlWriter {
    fn write_value(&self, _context: &mut Context, out: &mut DynQuery, value: &Value) {
        let Some(target) = out
            .as_prepared::<MongoDBDriver>()
            .and_then(MongoDBPrepared::current_bson)
        else {
            log::error!("Failed to get the bson while writing the value {value:?}");
            return;
        };
        *target = match value_to_bson(value) {
            Ok(v) => v,
            Err(e) => {
                let error = e.context(format!("While writing the value {value:?}"));
                log::error!("{error:#}");
                return;
            }
        };
    }

    fn write_null(&self, _context: &mut Context, out: &mut DynQuery) {
        let Some(target) = out
            .as_prepared::<MongoDBDriver>()
            .and_then(MongoDBPrepared::current_bson)
        else {
            log::error!("Failed to get the bson in MongoDBSqlWriter::write_value_none");
            return;
        };
        *target = Bson::Null;
    }

    write_value_fn!(write_bool, bool, Bson::Boolean);
    write_value_fn!(write_value_i8, i8, Bson::Int32);
    write_value_fn!(write_value_i16, i16, Bson::Int32);
    write_value_fn!(write_value_i32, i32, Bson::Int32);
    write_value_fn!(write_value_i64, i64, Bson::Int64);
    write_value_fn!(write_value_u8, u8, Bson::Int32);
    write_value_fn!(write_value_u16, u16, Bson::Int32);
    write_value_fn!(write_value_u32, u32, Bson::Int64);
    write_value_fn!(write_value_f32, f32, Bson::Double);
    write_value_fn!(write_value_f64, f64, Bson::Double);

    fn write_value_i128(&self, context: &mut Context, out: &mut DynQuery, value: i128) {
        match i64::try_from_value(value.as_value()) {
            Ok(v) => self.write_value_i64(context, out, v),
            Err(e) => {
                log::error!("{e:#}");
                return;
            }
        }
    }

    fn write_value_u64(&self, context: &mut Context, out: &mut DynQuery, value: u64) {
        match i64::try_from_value(value.as_value()) {
            Ok(v) => self.write_value_i64(context, out, v),
            Err(e) => {
                log::error!("{e:#}");
                return;
            }
        }
    }

    fn write_value_u128(&self, context: &mut Context, out: &mut DynQuery, value: u128) {
        match i64::try_from_value(value.as_value()) {
            Ok(v) => self.write_value_i64(context, out, v),
            Err(e) => {
                log::error!("{e:#}");
                return;
            }
        }
    }

    fn write_string(&self, _context: &mut Context, out: &mut DynQuery, value: &str) {
        let Some(target) = out
            .as_prepared::<MongoDBDriver>()
            .and_then(MongoDBPrepared::current_bson)
        else {
            log::error!("Failed to get the bson in MongoDBSqlWriter::write_value_string");
            return;
        };
        *target = Bson::String(value.into());
    }

    fn write_blob(&self, _context: &mut Context, out: &mut DynQuery, value: &[u8]) {
        let Some(target) = out
            .as_prepared::<MongoDBDriver>()
            .and_then(MongoDBPrepared::current_bson)
        else {
            log::error!("Failed to get the bson in MongoDBSqlWriter::write_value_blob");
            return;
        };
        *target = Bson::Binary(Binary {
            subtype: BinarySubtype::Generic,
            bytes: value.to_vec(),
        });
    }

    fn write_date(&self, _context: &mut Context, out: &mut DynQuery, value: &Date) {
        let Some(target) = out
            .as_prepared::<MongoDBDriver>()
            .and_then(MongoDBPrepared::current_bson)
        else {
            log::error!("Failed to get the bson in MongoDBSqlWriter::write_value_date");
            return;
        };
        let midnight = time::Time::MIDNIGHT;
        let date_time = PrimitiveDateTime::new(*value, midnight).assume_utc();
        *target = Bson::DateTime(bson::DateTime::from_millis(
            (date_time.unix_timestamp_nanos() / 1_000_000) as _,
        ))
    }

    fn write_time(&self, _context: &mut Context, out: &mut DynQuery, value: &Time) {
        let Some(target) = out
            .as_prepared::<MongoDBDriver>()
            .and_then(MongoDBPrepared::current_bson)
        else {
            log::error!("Failed to get the bson in MongoDBSqlWriter::write_value_time");
            return;
        };
        *target = Bson::String(Value::Time(Some(*value)).to_string())
    }

    fn write_timestamp(
        &self,
        _context: &mut Context,
        out: &mut DynQuery,
        value: &PrimitiveDateTime,
    ) {
        let Some(target) = out
            .as_prepared::<MongoDBDriver>()
            .and_then(MongoDBPrepared::current_bson)
        else {
            log::error!("Failed to get the bson in MongoDBSqlWriter::write_value_timestamp");
            return;
        };
        let ms = value.assume_utc().unix_timestamp_nanos() / 1_000_000;
        *target = Bson::DateTime(bson::DateTime::from_millis(ms as _));
    }

    fn write_timestamptz(
        &self,
        _context: &mut Context,
        out: &mut DynQuery,
        value: &OffsetDateTime,
    ) {
        let Some(target) = out
            .as_prepared::<MongoDBDriver>()
            .and_then(MongoDBPrepared::current_bson)
        else {
            log::error!("Failed to get the bson in MongoDBSqlWriter::write_value_timestamptz");
            return;
        };
        let ms = value.to_utc().unix_timestamp_nanos() / 1_000_000;
        *target = Bson::DateTime(bson::DateTime::from_millis(ms as _));
    }

    fn write_interval(&self, _context: &mut Context, _out: &mut DynQuery, _value: &Interval) {
        log::error!("MongoDB does not support interval types");
        return;
    }

    fn write_uuid(&self, _context: &mut Context, out: &mut DynQuery, value: &Uuid) {
        let Some(target) = out
            .as_prepared::<MongoDBDriver>()
            .and_then(MongoDBPrepared::current_bson)
        else {
            log::error!("Failed to get the bson in MongoDBSqlWriter::write_value_uuid");
            return;
        };
        *target = Bson::Binary(Binary {
            subtype: BinarySubtype::Uuid,
            bytes: value.as_bytes().to_vec(),
        });
    }

    fn write_list(
        &self,
        context: &mut Context,
        out: &mut DynQuery,
        value: &mut dyn Iterator<Item = &dyn Expression>,
        _ty: Option<&Value>,
        _is_array: bool,
    ) {
        let Some(target) = out
            .as_prepared::<MongoDBDriver>()
            .and_then(MongoDBPrepared::current_bson)
        else {
            log::error!("Failed to get the bson in MongoDBSqlWriter::write_list");
            return;
        };
        let Some(values) = value
            .map(|v| {
                let mut q = Self::make_prepared();
                v.write_query(self, context, &mut q);
                let Some(bson) = q
                    .as_prepared::<MongoDBDriver>()
                    .and_then(MongoDBPrepared::current_bson)
                else {
                    return None;
                };
                Some(mem::take(bson))
            })
            .collect::<Option<_>>()
        else {
            log::error!("Failed to get the bson in MongoDBSqlWriter::write_expression_list");
            return;
        };
        *target = Bson::Array(values);
    }

    fn write_tuple(
        &self,
        context: &mut Context,
        out: &mut DynQuery,
        value: &mut dyn Iterator<Item = &dyn Expression>,
    ) {
        let Some(target) = out
            .as_prepared::<MongoDBDriver>()
            .and_then(MongoDBPrepared::current_bson)
        else {
            log::error!("Failed to get the bson in MongoDBSqlWriter::write_expression_tuple");
            return;
        };
        let Some(values) = value
            .map(|v| {
                let mut q = Self::make_prepared();
                v.write_query(self, context, &mut q);
                let Some(bson) = q
                    .as_prepared::<MongoDBDriver>()
                    .and_then(MongoDBPrepared::current_bson)
                else {
                    return None;
                };
                Some(mem::take(bson))
            })
            .collect::<Option<_>>()
        else {
            log::error!("Failed to get the bson in MongoDBSqlWriter::write_expression_tuple");
            return;
        };
        *target = Bson::Array(values);
    }

    fn write_map(&self, _context: &mut Context, out: &mut DynQuery, value: &HashMap<Value, Value>) {
        let Some(target) = out
            .as_prepared::<MongoDBDriver>()
            .and_then(MongoDBPrepared::current_bson)
        else {
            log::error!("Failed to get the bson in MongoDBSqlWriter::write_value_map");
            return;
        };
        let mut doc = Document::new();
        for (k, v) in value.iter() {
            let Ok(k) = String::try_from_value(k.clone()) else {
                log::error!("Unexpected tank::Value key: {k:?}, it is not convertible to String");
                return;
            };
            let v = match value_to_bson(v) {
                Ok(v) => v,
                Err(e) => {
                    let error = e.context(format!("While converting value {v:?} to bson"));
                    log::error!("{error:#}");
                    return;
                }
            };
            doc.insert(k, v);
        }
        *target = Bson::Document(doc);
    }

    fn write_struct(&self, _context: &mut Context, out: &mut DynQuery, value: &[(String, Value)]) {
        let Some(target) = out
            .as_prepared::<MongoDBDriver>()
            .and_then(MongoDBPrepared::current_bson)
        else {
            log::error!("Failed to get the bson in MongoDBSqlWriter::write_value_struct");
            return;
        };
        let mut doc = Document::new();
        for (k, v) in value.iter() {
            let v = match value_to_bson(v) {
                Ok(v) => v,
                Err(e) => {
                    let error = e.context(format!("While converting value {v:?} to bson"));
                    log::error!("{error:#}");
                    return;
                }
            };
            doc.insert(k, v);
        }
        *target = Bson::Document(doc);
    }
}
