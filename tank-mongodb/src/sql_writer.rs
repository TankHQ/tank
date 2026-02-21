use crate::{
    AggregatePayload, BatchPayload, CreateCollectionPayload, CreateDatabasePayload, DeletePayload,
    DropCollectionPayload, DropDatabasePayload, FieldType, FindManyPayload, FindOnePayload,
    InsertManyPayload, InsertOnePayload, IsField, MongoDBDriver, MongoDBPrepared, NegateNumber,
    Payload, RowWrap, UpsertPayload, WriteMatchExpression, like_to_regex, value_to_bson,
};
use mongodb::{
    Namespace,
    bson::{self, Binary, Bson, Document, Regex, doc, spec::BinarySubtype},
    options::{
        AggregateOptions, CreateCollectionOptions, DeleteOptions, FindOneOptions, FindOptions,
        InsertManyOptions, InsertOneOptions, UpdateModifications, UpdateOptions,
    },
};
use std::{borrow::Cow, collections::HashMap, f64, iter, mem, sync::Arc};
use tank_core::{
    AsValue, BinaryOp, BinaryOpType, ColumnRef, Context, Dataset, DynQuery, Entity, ErrorContext,
    Expression, FindOrder, Fragment, Interval, IsAggregateFunction, IsAsterisk, Operand, Order,
    Result, SelectQuery, SqlWriter, TableRef, UnaryOp, UnaryOpType, Value, print_timer,
    truncate_long,
};
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

#[derive(Default)]
pub struct MongoDBSqlWriter {}

impl MongoDBSqlWriter {
    pub fn make_prepared() -> DynQuery {
        DynQuery::Prepared(Box::new(MongoDBPrepared::default()))
    }

    pub fn make_unmatchable() -> Document {
        doc! {
            "_id": { "$exists": false }
        }
    }

    pub fn make_namespace(table_ref: &TableRef) -> Namespace {
        Namespace {
            db: table_ref.schema.to_string(),
            coll: table_ref.name.to_string(),
        }
    }

    pub(crate) fn prepare_query(query: &mut DynQuery, context: &mut Context, payload: Payload) {
        if let Some(prepared) = query.as_prepared::<MongoDBDriver>() {
            if let Err(e) = prepared.add_payload(payload) {
                let e = e.context("While preparing the query (adding payload)");
                log::error!("{e:#}",);
            };
            prepared.count = context.counter;
        } else {
            if !query.is_empty() {
                log::error!(
                    "The query is not empty, MongoDBSqlWriter::switch_to_prepared will drop the content",
                );
            }
            *query = DynQuery::Prepared(Box::new(MongoDBPrepared::new(payload, context.counter)));
        }
    }

    pub fn expression_binary_op_key(value: BinaryOpType) -> &'static str {
        let result = match value {
            BinaryOpType::Indexing => "$arrayElemAt",
            BinaryOpType::Cast => "",
            BinaryOpType::Multiplication => "$multiply",
            BinaryOpType::Division => "$divide",
            BinaryOpType::Remainder => "$mod",
            BinaryOpType::Addition => "$add",
            BinaryOpType::Subtraction => "$subtract",
            BinaryOpType::ShiftLeft => "",
            BinaryOpType::ShiftRight => "",
            BinaryOpType::BitwiseAnd => "$bitAnd",
            BinaryOpType::BitwiseOr => "$bitOr",
            BinaryOpType::In => "$in",
            BinaryOpType::NotIn => "$nin",
            BinaryOpType::Is => "$eq",
            BinaryOpType::IsNot => "$ne",
            BinaryOpType::Like => "",
            BinaryOpType::NotLike => "",
            BinaryOpType::Regexp => "$regexMatch",
            BinaryOpType::NotRegexp => "",
            BinaryOpType::Glob => "",
            BinaryOpType::NotGlob => "",
            BinaryOpType::Equal => "$eq",
            BinaryOpType::NotEqual => "$ne",
            BinaryOpType::Less => "$lt",
            BinaryOpType::Greater => "$gt",
            BinaryOpType::LessEqual => "$lte",
            BinaryOpType::GreaterEqual => "$gte",
            BinaryOpType::And => "$and",
            BinaryOpType::Or => "$or",
            BinaryOpType::Alias => "",
        };
        if result.is_empty() {
            log::error!("MongoDB does not support {value:?} binary operator");
        }
        result
    }
}

impl SqlWriter for MongoDBSqlWriter {
    fn as_dyn(&self) -> &dyn SqlWriter {
        self
    }

    fn write_identifier(
        &self,
        _context: &mut Context,
        out: &mut DynQuery,
        value: &str,
        _quoted: bool,
    ) {
        let out = if let Some(target) = out
            .as_prepared::<MongoDBDriver>()
            .and_then(MongoDBPrepared::current_bson)
        {
            *target = Bson::String(String::new());
            let Bson::String(value) = target else {
                unreachable!("It must be a string here");
            };
            value
        } else {
            out.buffer()
        };
        out.push('$');
        out.push_str(value);
    }

    fn write_column_ref(&self, context: &mut Context, out: &mut DynQuery, value: &ColumnRef) {
        self.write_identifier(context, out, &value.name, false);
    }

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
                log::error!(
                    "{:#}",
                    e.context(format!("While writing the value {value:?}"))
                );
                return;
            }
        };
    }

    fn write_value_none(&self, _context: &mut Context, out: &mut DynQuery) {
        let Some(target) = out
            .as_prepared::<MongoDBDriver>()
            .and_then(MongoDBPrepared::current_bson)
        else {
            log::error!("Failed to get the bson in MongoDBSqlWriter::write_value_none");
            return;
        };
        *target = Bson::Null;
    }

    write_value_fn!(write_value_bool, bool, Bson::Boolean);
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

    fn write_value_string(&self, _context: &mut Context, out: &mut DynQuery, value: &str) {
        let Some(target) = out
            .as_prepared::<MongoDBDriver>()
            .and_then(MongoDBPrepared::current_bson)
        else {
            log::error!("Failed to get the bson in MongoDBSqlWriter::write_value_string");
            return;
        };
        *target = Bson::String(value.into());
    }

    fn write_value_blob(&self, _context: &mut Context, out: &mut DynQuery, value: &[u8]) {
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

    fn write_value_date(
        &self,
        _context: &mut Context,
        out: &mut DynQuery,
        value: &Date,
        _timestamp: bool,
    ) {
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

    fn write_value_time(
        &self,
        _context: &mut Context,
        out: &mut DynQuery,
        value: &Time,
        _timestamp: bool,
    ) {
        let Some(target) = out
            .as_prepared::<MongoDBDriver>()
            .and_then(MongoDBPrepared::current_bson)
        else {
            log::error!("Failed to get the bson in MongoDBSqlWriter::write_value_time");
            return;
        };
        let mut out = String::new();
        print_timer(
            &mut out,
            "",
            value.hour() as _,
            value.minute(),
            value.second(),
            value.nanosecond(),
        );
        *target = Bson::String(out)
    }

    fn write_value_timestamp(
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

    fn write_value_timestamptz(
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

    fn write_value_interval(&self, _context: &mut Context, _out: &mut DynQuery, _value: &Interval) {
        log::error!("MongoDB does not support interval types");
        return;
    }

    fn write_value_uuid(&self, _context: &mut Context, out: &mut DynQuery, value: &Uuid) {
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

    fn write_value_list(
        &self,
        _context: &mut Context,
        out: &mut DynQuery,
        value: &mut dyn Iterator<Item = &Value>,
        _ty: &Value,
        _elem_ty: &Value,
    ) {
        let Some(target) = out
            .as_prepared::<MongoDBDriver>()
            .and_then(MongoDBPrepared::current_bson)
        else {
            log::error!("Failed to get the bson in MongoDBSqlWriter::write_value_list");
            return;
        };
        let list = match value.map(value_to_bson).collect::<Result<_>>() {
            Ok(v) => v,
            Err(e) => {
                log::error!(
                    "{:#}",
                    e.context("While MongoDBSqlWriter::write_value_list")
                );
                return;
            }
        };
        *target = Bson::Array(list);
    }

    fn write_value_map(
        &self,
        _context: &mut Context,
        out: &mut DynQuery,
        value: &HashMap<Value, Value>,
    ) {
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
                    log::error!(
                        "{:#}",
                        e.context(format!("While converting value {v:?} to bson"))
                    );
                    return;
                }
            };
            doc.insert(k, v);
        }
        *target = Bson::Document(doc);
    }

    fn write_value_struct(
        &self,
        _context: &mut Context,
        out: &mut DynQuery,
        value: &Vec<(String, Value)>,
    ) {
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
                    log::error!(
                        "{:#}",
                        e.context(format!("While converting value {v:?} to bson"))
                    );
                    return;
                }
            };
            doc.insert(k, v);
        }
        *target = Bson::Document(doc);
    }

    fn write_expression_unary_op(
        &self,
        context: &mut Context,
        out: &mut DynQuery,
        value: &UnaryOp<&dyn Expression>,
    ) {
        match value.op {
            UnaryOpType::Negative => {
                let mut matcher = NegateNumber::default();
                if value.arg.accept_visitor(&mut matcher, self, context, out) {
                    self.write_value(context, out, &matcher.value);
                } else {
                    // TODO: Change when MongoDB introduces a better way to handle negative
                    BinaryOp {
                        op: BinaryOpType::Multiplication,
                        lhs: Operand::LitInt(-1),
                        rhs: value.arg,
                    }
                    .write_query(self, context, out);
                }
            }
            UnaryOpType::Not => {
                value.arg.write_query(self, context, out);
                let Some(target) = out
                    .as_prepared::<MongoDBDriver>()
                    .and_then(MongoDBPrepared::current_bson)
                else {
                    log::error!(
                        "Failed to get the bson in MongoDBSqlWriter::write_expression_unary_op after writing the argument"
                    );
                    return;
                };
                *target = doc! { "$not": [mem::take(target)] }.into();
            }
        }
    }

    fn write_expression_binary_op(
        &self,
        context: &mut Context,
        out: &mut DynQuery,
        value: &BinaryOp<&dyn Expression, &dyn Expression>,
    ) {
        match value.op {
            BinaryOpType::ShiftLeft => {
                return BinaryOp {
                    op: BinaryOpType::Multiplication,
                    lhs: value.lhs,
                    rhs: Operand::Call("POW", &[&Operand::LitInt(2), value.rhs]),
                }
                .write_query(self, context, out);
            }
            BinaryOpType::ShiftRight => {
                return Operand::Call(
                    "FLOOR",
                    &[&BinaryOp {
                        op: BinaryOpType::Division,
                        lhs: value.lhs,
                        rhs: Operand::Call("POW", &[&Operand::LitInt(2), value.rhs]),
                    }],
                )
                .write_query(self, context, out);
            }
            BinaryOpType::NotLike | BinaryOpType::NotRegexp | BinaryOpType::NotGlob => {
                return UnaryOp {
                    op: UnaryOpType::Not,
                    arg: BinaryOp {
                        op: match value.op {
                            BinaryOpType::NotLike => BinaryOpType::Like,
                            BinaryOpType::NotRegexp => BinaryOpType::Regexp,
                            BinaryOpType::NotGlob => BinaryOpType::Glob,
                            _ => unreachable!(),
                        },
                        lhs: value.lhs,
                        rhs: value.rhs,
                    },
                }
                .write_query(self, context, out);
            }
            BinaryOpType::Alias => return value.lhs.write_query(self, context, out),
            _ => {}
        }
        let Some(document) = out
            .as_prepared::<MongoDBDriver>()
            .and_then(MongoDBPrepared::switch_to_document)
        else {
            log::error!(
                "The query provided to write_expression_binary_op (out) does not have a document to write the content into"
            );
            return;
        };
        let lhs = {
            let mut lhs = MongoDBSqlWriter::make_prepared();
            value.lhs.write_query(self, context, &mut lhs);
            let Some(lhs) = lhs
                .as_prepared::<MongoDBDriver>()
                .and_then(MongoDBPrepared::current_bson)
                .map(mem::take)
            else {
                log::error!(
                    "Unexpected error while rendering the lhs of the binary expression, failed to get the bson object"
                );
                return;
            };
            lhs
        };
        let mut rhs = {
            let mut rhs = MongoDBSqlWriter::make_prepared();
            value.rhs.write_query(self, context, &mut rhs);
            let Some(rhs) = rhs
                .as_prepared::<MongoDBDriver>()
                .and_then(MongoDBPrepared::current_bson)
                .map(mem::take)
            else {
                log::error!(
                    "Unexpected error while rendering the rhs of the binary expression, failed to get the bson object"
                );
                return;
            };
            rhs
        };
        let mut op = value.op;
        if value.op == BinaryOpType::Like {
            let Bson::String(pattern) = rhs else {
                log::error!(
                    "MongoDB can handle LIKE operations but only if the pattern is a string literal (to transform it in $regexMatch)"
                );
                return;
            };
            op = BinaryOpType::Regexp;
            rhs = Bson::RegularExpression(Regex {
                pattern: like_to_regex(&pattern).into(),
                options: Default::default(),
            });
        }
        let key = Self::expression_binary_op_key(op).to_string();
        document.insert(
            key,
            match op {
                BinaryOpType::Regexp => Bson::Document(doc! {
                    "input": lhs,
                    "regex": rhs,
                }),
                _ => Bson::Array(vec![lhs, rhs]),
            },
        );
    }

    fn write_expression_call(
        &self,
        context: &mut Context,
        out: &mut DynQuery,
        function: &str,
        args: &[&dyn Expression],
    ) {
        let Some(document) = out
            .as_prepared::<MongoDBDriver>()
            .and_then(MongoDBPrepared::switch_to_document)
        else {
            log::error!(
                "The query provided to write_expression_call (out) does not have a document to write the content into"
            );
            return;
        };
        let function = match function {
            s if s.eq_ignore_ascii_case("abs") => "$abs",
            s if s.eq_ignore_ascii_case("acos") => "$acos",
            s if s.eq_ignore_ascii_case("asin") => "$asin",
            s if s.eq_ignore_ascii_case("atan") => "$atan",
            s if s.eq_ignore_ascii_case("atan2") => "$atan2",
            s if s.eq_ignore_ascii_case("avg") => "$avg",
            s if s.eq_ignore_ascii_case("ceil") => "$ceil",
            s if s.eq_ignore_ascii_case("cos") => "$cos",
            s if s.eq_ignore_ascii_case("count") => {
                return self.write_expression_call(context, out, "sum", &[&Operand::LitInt(1)]);
            }
            s if s.eq_ignore_ascii_case("exp") => "$exp",
            s if s.eq_ignore_ascii_case("floor") => "$floor",
            s if s.eq_ignore_ascii_case("log") => "$ln",
            s if s.eq_ignore_ascii_case("log10") => "$log",
            s if s.eq_ignore_ascii_case("max") => "$max",
            s if s.eq_ignore_ascii_case("min") => "$min",
            s if s.eq_ignore_ascii_case("pow") => "$pow",
            s if s.eq_ignore_ascii_case("round") => "$round",
            s if s.eq_ignore_ascii_case("sin") => "$sin",
            s if s.eq_ignore_ascii_case("sqrt") => "$sqrt",
            s if s.eq_ignore_ascii_case("sum") => "$sum",
            s if s.eq_ignore_ascii_case("tan") => "$tan",
            _ => {
                log::error!("Unknown function: ${function}");
                return;
            }
        };
        let len = args.len();
        let mut query = Self::make_prepared();
        if len == 1 {
            args[0].write_query(self, context, &mut query);
        } else {
            self.write_expression_list(
                context,
                &mut query,
                &mut args.iter().map(|v| v as &dyn Expression),
            );
        };
        let Some(arg) = query
            .as_prepared::<MongoDBDriver>()
            .and_then(MongoDBPrepared::current_bson)
            .map(mem::take)
        else {
            log::error!("The query returned from write_query (out) does not have a current bson");
            return;
        };
        document.insert(function, arg);
    }

    fn write_expression_list(
        &self,
        context: &mut Context,
        out: &mut DynQuery,
        value: &mut dyn Iterator<Item = &dyn Expression>,
    ) {
        let Some(target) = out
            .as_prepared::<MongoDBDriver>()
            .and_then(MongoDBPrepared::current_bson)
        else {
            log::error!("Failed to get the bson in MongoDBSqlWriter::write_expression_list");
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

    fn write_expression_tuple(
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

    fn write_expression_operand_question_mark(&self, context: &mut Context, out: &mut DynQuery) {
        let Some(target) = out
            .as_prepared::<MongoDBDriver>()
            .and_then(MongoDBPrepared::current_bson)
        else {
            log::error!(
                "Failed to get the bson in MongoDBSqlWriter::write_expression_operand_question_mark"
            );
            return;
        };
        *target = Bson::String(format!("$$param_{}", context.counter));
        context.counter += 1;
    }

    fn write_expression_operand_current_timestamp_ms(
        &self,
        _context: &mut Context,
        out: &mut DynQuery,
    ) {
        let Some(target) = out
            .as_prepared::<MongoDBDriver>()
            .and_then(MongoDBPrepared::current_bson)
        else {
            log::error!(
                "Failed to get the bson in MongoDBSqlWriter::write_expression_operand_current_timestamp_ms"
            );
            return;
        };
        *target = doc! { "$toLong": "$$NOW" }.into();
    }

    fn write_create_schema<E>(&self, out: &mut DynQuery, _if_not_exists: bool)
    where
        Self: Sized,
        E: Entity,
    {
        Self::prepare_query(
            out,
            &mut Context::empty(),
            CreateDatabasePayload {
                table: E::table().clone(),
            }
            .into(),
        );
    }

    fn write_drop_schema<E>(&self, out: &mut DynQuery, _if_exists: bool)
    where
        Self: Sized,
        E: Entity,
    {
        Self::prepare_query(
            out,
            &mut Context::empty(),
            DropDatabasePayload {
                table: E::table().clone(),
            }
            .into(),
        );
    }

    fn write_create_table<E>(&self, out: &mut DynQuery, _if_not_exists: bool)
    where
        Self: Sized,
        E: Entity,
    {
        let table = E::table().clone();
        let name = table.full_name();
        Self::prepare_query(
            out,
            &mut Context::empty(),
            CreateCollectionPayload {
                table: E::table().clone(),
                options: CreateCollectionOptions::builder()
                    .comment(Bson::String(format!("Tank: create collection {name}")))
                    .build(),
            }
            .into(),
        );
    }

    fn write_drop_table<E>(&self, out: &mut DynQuery, _if_exists: bool)
    where
        Self: Sized,
        E: Entity,
    {
        Self::prepare_query(
            out,
            &mut Context::empty(),
            DropCollectionPayload {
                table: E::table().clone(),
            }
            .into(),
        );
    }

    fn write_select<'a, Data>(&self, out: &mut DynQuery, query: &impl SelectQuery<Data>)
    where
        Self: Sized,
        Data: Dataset + 'a,
    {
        let (Some(table), where_expr) = (query.get_from(), query.get_where()) else {
            log::error!("The query does not have the FROM clause");
            return;
        };
        let table = table.table_ref();
        if table.name.is_empty() {
            log::error!(
                "The table is not specified in the dataset (if it is a JOIN, MongoDB does not support it)"
            );
            return;
        }
        let mut context = Context::fragment(Fragment::SqlSelect);
        context.quote_identifiers = false;
        let name = table.full_name();
        let limit = query.get_limit();
        let mut group_by = query.get_group_by().peekable();
        let mut group = Document::new();
        let mut is_aggregate = group_by.peek().is_some();
        macro_rules! update_group {
            ($column:expr, $name:expr, $bson:expr, $is_aggregate:expr) => {
                if $is_aggregate {
                    group.insert($name, $bson);
                    is_aggregate = true;
                } else {
                    group
                        .entry("_id".into())
                        .or_insert(Document::new().into())
                        .as_document_mut()
                        .expect("Field _id should be a document")
                        .insert($name, $bson);
                }
            };
        }
        fn get_name(expression: impl Expression, qualify: bool) -> String {
            expression.as_identifier(&mut Context {
                qualify_columns: qualify,
                quote_identifiers: false,
                ..Default::default()
            })
        }
        let mut project = Some(Document::new());
        let mut is_asterisk = true;
        for column in query.get_select() {
            if is_asterisk {
                if !column.accept_visitor(
                    &mut IsAsterisk,
                    self,
                    &mut context,
                    &mut Default::default(),
                ) {
                    is_asterisk = false
                } else {
                    continue;
                }
            }
            let name = get_name(&column, false);
            let mut query = Self::make_prepared();
            column.write_query(self, &mut context, &mut query);
            let Some(mut bson) = query
                .as_prepared::<MongoDBDriver>()
                .and_then(MongoDBPrepared::current_bson)
                .map(mem::take)
            else {
                log::error!(
                    "Failed to get the bson in MongoDBSqlWriter::write_select while rendering the column {}",
                    truncate_long!(&name, true)
                );
                return;
            };
            let aggregate_function =
                column.accept_visitor(&mut IsAggregateFunction, self, &mut context, out);
            update_group!(column, name.clone(), bson.clone(), aggregate_function);
            if aggregate_function {
                bson = Bson::Int32(1);
            } else if column.accept_visitor(&mut IsField::default(), self, &mut context, out) {
                bson = Bson::String(format!("$_id.{name}"))
            }
            if let Some(project) = &mut project {
                project.insert(name, bson);
            }
        }
        if is_asterisk {
            project = None;
        }
        let where_expr = if let Some(where_expr) = where_expr {
            let mut context = context.switch_fragment(Fragment::SqlSelectWhere);
            let mut query = Self::make_prepared();
            where_expr.accept_visitor(
                &mut WriteMatchExpression::new(),
                self,
                &mut context.current,
                &mut query,
            );
            let Some(Bson::Document(document)) = query
                .as_prepared::<MongoDBDriver>()
                .and_then(MongoDBPrepared::current_bson)
                .map(mem::take)
            else {
                log::error!(
                    "Failed to get the bson in MongoDBSqlWriter::write_select while rendering the WHERE clause"
                );
                return;
            };
            document
        } else {
            Default::default()
        };
        for column in group_by {
            let name = get_name(&column, false);
            let mut context = context.switch_fragment(Fragment::SqlSelectGroupBy);
            let mut query = Self::make_prepared();
            column.write_query(self, &mut context.current, &mut query);
            let Some(bson) = query
                .as_prepared::<MongoDBDriver>()
                .and_then(MongoDBPrepared::current_bson)
                .map(mem::take)
            else {
                log::error!(
                    "Failed to get the bson in MongoDBSqlWriter::write_select while rendering the {} column",
                    truncate_long!(&name, true)
                );
                return;
            };
            update_group!(
                column,
                name,
                bson,
                column.accept_visitor(&mut IsAggregateFunction, self, &mut context.current, out)
            );
        }
        let known_columns = Arc::new(group.keys().collect::<Vec<_>>());
        let mut having = Bson::Null;
        if let Some(condition) = query.get_having() {
            let mut context = context.switch_fragment(Fragment::SqlSelectHaving);
            let mut context = context.current.switch_table("_id".into());
            let mut query = Self::make_prepared();
            let mut matcher = WriteMatchExpression {
                known_columns: known_columns.clone(),
                ..Default::default()
            };
            condition.accept_visitor(&mut matcher, self, &mut context.current, &mut query);
            let Some(bson) = query
                .as_prepared::<MongoDBDriver>()
                .and_then(MongoDBPrepared::current_bson)
                .map(mem::take)
            else {
                log::error!(
                    "Failed to get the bson in MongoDBSqlWriter::write_select while rendering the HAVING clause"
                );
                return;
            };
            having = bson;
        }
        let mut sort = Document::new();
        {
            for order in query.get_order_by() {
                let mut context = context.switch_fragment(Fragment::SqlSelectOrderBy);
                let is_asc = {
                    let find_order = &mut FindOrder::default();
                    order.accept_visitor(find_order, self, &mut context.current, out);
                    find_order.order == Order::ASC
                };
                let mut is_field = IsField {
                    known_columns: known_columns.clone(),
                    ..Default::default()
                };
                order.accept_visitor(&mut is_field, self, &mut context.current, out);
                let is_column = |c| {
                    group
                        .get_document("_id")
                        .map(|v| v.keys().find(|v| *v == c).is_some())
                        .unwrap_or_default()
                };
                let field = match is_field.field {
                    FieldType::None => {
                        log::error!(
                            "Unexpected ordering on {:?}, {:?}",
                            order,
                            known_columns.clone()
                        );
                        return;
                    }
                    FieldType::Identifier(v) => v,
                    FieldType::Column(v) => {
                        let mut context = if is_aggregate && is_column(&v.name) {
                            context.current.switch_table("_id".into())
                        } else {
                            context
                        };
                        v.as_identifier(&mut context.current)
                    }
                };
                sort.insert(field, Bson::Int32(if is_asc { 1 } else { -1 }));
            }
        }
        if !is_aggregate && let Some(project) = &mut project {
            for (_k, v) in project.iter_mut() {
                if let Bson::String(value) = v
                    && value.starts_with("$_id.")
                {
                    *v = Bson::Int32(1);
                }
            }
        }
        let payload: Payload = if is_aggregate {
            let mut pipeline = Vec::new();
            if !where_expr.is_empty() {
                pipeline.push(doc! { "$match": where_expr });
            }
            if !group.is_empty() {
                group.entry("_id".into()).or_insert_with(|| {
                    project = None;
                    Bson::Null.into()
                });
                pipeline.push(doc! { "$group": group });
            }
            if !matches!(having, Bson::Null) {
                pipeline.push(doc! { "$match": having });
            }
            if !sort.is_empty() {
                pipeline.push(doc! { "$sort": sort });
            }
            if let Some(limit) = limit {
                pipeline.push(doc! { "$limit": limit });
            }
            if let Some(project) = project
                && !project.is_empty()
            {
                pipeline.push(doc! { "$project": project })
            }
            AggregatePayload {
                table,
                pipeline: pipeline.into(),
                options: AggregateOptions::builder()
                    .comment(Bson::String(format!("Tank: aggregate on {name}")))
                    .build(),
            }
            .into()
        } else if limit == Some(1) {
            FindOnePayload {
                table,
                filter: where_expr.into(),
                options: FindOneOptions::builder()
                    .comment(Bson::String(format!("Tank: select one entity from {name}")))
                    .projection(project)
                    .build(),
            }
            .into()
        } else {
            FindManyPayload {
                table,
                filter: where_expr.into(),
                options: FindOptions::builder()
                    .comment(Bson::String(format!("Tank: select entities from {name}")))
                    .projection(project)
                    .sort(if !sort.is_empty() { Some(sort) } else { None })
                    .limit(limit.map(|v| v as _))
                    .build(),
            }
            .into()
        };
        Self::prepare_query(out, &mut context, payload);
    }

    fn write_insert<'b, E>(
        &self,
        out: &mut DynQuery,
        entities: impl IntoIterator<Item = &'b E>,
        update: bool,
    ) where
        Self: Sized,
        E: Entity + 'b,
    {
        let table = E::table().clone();
        let name = table.full_name();
        let mut entities = entities.into_iter().peekable();
        let Some(entity) = entities.next() else {
            return;
        };
        let single = entities.peek().is_none();
        let mut context = Context::fragment(Fragment::SqlInsertInto);
        context.quote_identifiers = false;
        let payload: Payload = match (update, single) {
            (false, true) => InsertOnePayload {
                table,
                row: entity.row_labeled(),
                options: InsertOneOptions::builder()
                    .comment(Bson::String(format!("Tank: insert one entity in {name}")))
                    .build(),
            }
            .into(),
            (false, false) => {
                let rows = iter::chain(
                    iter::once(entity.row_labeled()),
                    entities.map(Entity::row_labeled),
                )
                .collect::<Vec<_>>();
                InsertManyPayload {
                    table,
                    rows,
                    options: InsertManyOptions::builder()
                        .comment(Bson::String(format!("Tank: insert entities in {name}")))
                        .build(),
                }
                .into()
            }
            (true, _) => {
                let mut values = iter::chain(iter::once(entity), entities).filter_map(|entity| {
                    let mut query = Self::make_prepared();
                    entity.primary_key_expr().accept_visitor(
                        &mut WriteMatchExpression::new(),
                        self,
                        &mut context,
                        &mut query,
                    );
                    let Some(Bson::Document(filter)) = query
                        .as_prepared::<MongoDBDriver>()
                        .and_then(MongoDBPrepared::current_bson)
                        .map(mem::take)
                    else {
                        log::error!(
                            "Failed to get the bson in MongoDBSqlWriter::write_insert while rendering the primary key condition"
                        );
                        return None;
                    };
                    let modifications: Document = match RowWrap(Cow::Owned(entity.row_labeled()))
                        .try_into()
                        .with_context(|| "While rendering the entity to create a upsert query")
                    {
                        Ok(v) => v,
                        Err(e) => {
                            log::error!("{e:?}");
                            return None;
                        }
                    };
                    Some((
                        entity,
                        filter,
                        UpdateModifications::Document(doc! { "$set": modifications }),
                    ))
                });
                if single {
                    let Some((_, filter, modifications)) = values.next() else {
                        return;
                    };
                    UpsertPayload {
                        table,
                        filter: Bson::Document(filter),
                        modifications,
                        options: UpdateOptions::builder()
                            .upsert(true)
                            .comment(Bson::String(format!("Tank: update one entity in {name}")))
                            .build(),
                    }
                    .into()
                } else {
                    let values = values
                        .into_iter()
                        .map(|(entity, filter, modifications)| {
                            let table = entity.table_ref();
                            UpsertPayload {
                                table,
                                filter: filter.into(),
                                modifications,
                                options: UpdateOptions::builder()
                                    .comment(Bson::String(format!(
                                        "Tank: update entities in {name}"
                                    )))
                                    .upsert(true)
                                    .build(),
                            }
                            .into()
                        })
                        .collect::<Vec<_>>();
                    BatchPayload {
                        batch: values,
                        options: Default::default(),
                    }
                    .into()
                }
            }
        };
        Self::prepare_query(out, &mut context, payload);
    }

    fn write_delete<E>(&self, out: &mut DynQuery, condition: impl Expression)
    where
        Self: Sized,
        E: Entity,
    {
        let table = E::table().clone();
        let name = table.full_name();
        let mut context = Context::fragment(Fragment::SqlDeleteFromWhere);
        context.quote_identifiers = false;
        Self::prepare_query(
            out,
            &mut context,
            DeletePayload {
                table,
                filter: Default::default(),
                options: DeleteOptions::builder()
                    .comment(Bson::String(format!("Tank: delete entities from {name}")))
                    .build(),
                single: false,
            }
            .into(),
        );
        condition.accept_visitor(&mut WriteMatchExpression::new(), self, &mut context, out);
        let Some(prepared) = out.as_prepared::<MongoDBDriver>() else {
            return;
        };
        prepared.count = context.counter;
    }
}
