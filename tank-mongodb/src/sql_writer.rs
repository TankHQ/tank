use crate::{
    BatchPayload, CreateCollectionPayload, CreateDatabasePayload, DeletePayload,
    DropCollectionPayload, DropDatabasePayload, FindManyPayload, FindOnePayload, InsertManyPayload,
    InsertOnePayload, IsFieldCondition, MongoDBDriver, MongoDBPrepared, Payload, RowWrap,
    UpsertPayload, value_to_bson,
};
use mongodb::{
    Namespace,
    bson::{self, Binary, Bson, Document, doc, spec::BinarySubtype},
    options::{
        CreateCollectionOptions, DeleteOptions, FindOneOptions, FindOptions, InsertManyOptions,
        InsertOneOptions, UpdateModifications, UpdateOptions,
    },
};
use std::{borrow::Cow, collections::HashMap, f64, iter, mem};
use tank_core::{
    AsValue, BinaryOp, BinaryOpType, ColumnRef, Context, DataSet, DynQuery, Entity, ErrorContext,
    Expression, Fragment, Interval, IsFalse, IsTrue, Result, SelectQuery, SqlWriter, TableRef,
    Value, print_timer,
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
                    "Failed to get the bson objec in MongoDBSqlWriter::{}",
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

    pub(crate) fn prepare_query(query: &mut DynQuery, payload: Payload) {
        if let Some(prepared) = query.as_prepared::<MongoDBDriver>() {
            if let Err(e) = prepared.add_payload(payload) {
                let e = e.context("While preparing the query (adding payload)");
                log::error!("{e:#}",);
            };
        } else {
            if !query.is_empty() {
                log::error!(
                    "The query is not empty, MongoDBSqlWriter::switch_to_prepared will drop the content",
                );
            }
            *query = DynQuery::Prepared(Box::new(MongoDBPrepared::new(payload)));
        }
    }

    pub fn expression_binary_op_key(&self, value: BinaryOpType) -> &'static str {
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
            BinaryOpType::IsNot => "",
            BinaryOpType::Like => "",
            BinaryOpType::NotLike => "",
            BinaryOpType::Regexp => "$regex",
            BinaryOpType::NotRegexp => "$regex",
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

    pub fn write_filter_expression(
        &self,
        context: &mut Context,
        out: &mut DynQuery,
        condition: impl Expression,
    ) {
        if condition.matches(&mut IsFalse)
            && let Some(prepared) = out.as_prepared::<MongoDBDriver>()
            && let Some(target) = prepared.current_bson()
        {
            *target = Bson::Document(Self::make_unmatchable())
        } else if condition.matches(&mut IsTrue)
            && let Some(prepared) = out.as_prepared::<MongoDBDriver>()
            && let Some(target) = prepared.current_bson()
        {
            *target = Bson::Document(Default::default());
        } else if let matcher = &mut IsFieldCondition::new()
            && condition.matches(matcher)
            && let Some(prepared) = out.as_prepared::<MongoDBDriver>()
            && let Some(target) = prepared.current_bson()
        {
            *target = Bson::Document(mem::take(&mut matcher.condition))
        } else {
            condition.write_query(self, context, out);
        }
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
        let Some(target) = out
            .as_prepared::<MongoDBDriver>()
            .and_then(MongoDBPrepared::current_bson)
        else {
            log::error!("Failed to get the bson objec in MongoDBSqlWriter::write_identifier",);
            return;
        };
        *target = Bson::String(value.into());
    }

    fn write_column_ref(&self, context: &mut Context, out: &mut DynQuery, value: &ColumnRef) {
        let Some(target) = out
            .as_prepared::<MongoDBDriver>()
            .and_then(MongoDBPrepared::current_bson)
        else {
            log::error!("Failed to get the bson objec in MongoDBSqlWriter::write_column_ref");
            return;
        };
        let mut column =
            String::with_capacity(value.schema.len() + value.table.len() + value.name.len() + 2);
        if context.qualify_columns && !value.table.is_empty() {
            if !value.schema.is_empty() {
                column.push_str(&value.schema);
                column.push('.');
            }
            column.push_str(&value.table);
            column.push('.');
        }
        column.push_str(&value.name);
        *target = Bson::String(column);
    }

    fn write_value(&self, _context: &mut Context, out: &mut DynQuery, value: &Value) {
        let Some(target) = out
            .as_prepared::<MongoDBDriver>()
            .and_then(MongoDBPrepared::current_bson)
        else {
            log::error!("Failed to get the bson objec while writing the value {value:?}");
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
            log::error!("Failed to get the bson objec in MongoDBSqlWriter::write_value_none");
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
            log::error!("Failed to get the bson objec in MongoDBSqlWriter::write_value_string");
            return;
        };
        *target = Bson::String(value.into());
    }

    fn write_value_blob(&self, _context: &mut Context, out: &mut DynQuery, value: &[u8]) {
        let Some(target) = out
            .as_prepared::<MongoDBDriver>()
            .and_then(MongoDBPrepared::current_bson)
        else {
            log::error!("Failed to get the bson objec in MongoDBSqlWriter::write_value_blob");
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
            log::error!("Failed to get the bson objec in MongoDBSqlWriter::write_value_date");
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
            log::error!("Failed to get the bson objec in MongoDBSqlWriter::write_value_time");
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
            log::error!("Failed to get the bson objec in MongoDBSqlWriter::write_value_timestamp");
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
            log::error!(
                "Failed to get the bson objec in MongoDBSqlWriter::write_value_timestamptz"
            );
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
            log::error!("Failed to get the bson objec in MongoDBSqlWriter::write_value_uuid");
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
            log::error!("Failed to get the bson objec in MongoDBSqlWriter::write_value_list");
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
            log::error!("Failed to get the bson objec in MongoDBSqlWriter::write_value_map");
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
            log::error!("Failed to get the bson objec in MongoDBSqlWriter::write_value_struct");
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

    fn write_expression_binary_op(
        &self,
        context: &mut Context,
        out: &mut DynQuery,
        value: &BinaryOp<&dyn Expression, &dyn Expression>,
    ) {
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
                // Unreachable
                log::error!(
                    "Unexpected error while rendering the lhs of the binary expression, the query does not have a current bson"
                );
                return;
            };
            lhs
        };
        let rhs = {
            let mut rhs = MongoDBSqlWriter::make_prepared();
            value.rhs.write_query(self, context, &mut rhs);
            let Some(rhs) = rhs
                .as_prepared::<MongoDBDriver>()
                .and_then(MongoDBPrepared::current_bson)
                .map(mem::take)
            else {
                // Unreachable
                log::error!(
                    "Unexpected error while rendering the rhs of the binary expression, the query does not have a current bson"
                );
                return;
            };
            rhs
        };
        let key = self.expression_binary_op_key(value.op).to_string();
        document.insert(key, Bson::Array(vec![lhs, rhs]));
    }

    fn write_expression_call(
        &self,
        context: &mut Context,
        out: &mut DynQuery,
        function: &str,
        args: &[&dyn Expression],
    ) {
        let Some(mut document) = out
            .as_prepared::<MongoDBDriver>()
            .and_then(MongoDBPrepared::switch_to_document)
            .map(mem::take)
        else {
            log::error!(
                "The query provided to write_expression_call (out) does not have a document to write the content into"
            );
            return;
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
        match function {
            "ABS" => document.insert("$abs", arg),
            "ACOS" => document.insert("$acos", arg),
            "ASIN" => document.insert("$asin", arg),
            "ATAN" => document.insert("$atan", arg),
            "ATAN2" => document.insert("$atan2", arg),
            "AVG" => document.insert("$atan2", arg),
            "CEIL" => document.insert("$ceil", arg),
            "COS" => document.insert("$cos", arg),
            "EXP" => document.insert("$exp", arg),
            "FLOOR" => document.insert("$floor", arg),
            "LOG" => document.insert("$ln", arg),
            "LOG10" => document.insert("$log", arg),
            "MAX" => document.insert("$max", arg),
            "MIN" => document.insert("$min", arg),
            "POW" => document.insert("$pow", arg),
            "ROUND" => document.insert("$round", arg),
            "SIN" => document.insert("$sin", arg),
            "SQRT" => document.insert("$sqrt", arg),
            "TAN" => document.insert("$tan", arg),
            _ => None,
        };
    }

    fn write_create_schema<E>(&self, out: &mut DynQuery, _if_not_exists: bool)
    where
        Self: Sized,
        E: Entity,
    {
        Self::prepare_query(
            out,
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
            DropCollectionPayload {
                table: E::table().clone(),
            }
            .into(),
        );
    }

    fn write_select<'a, Data>(&self, out: &mut DynQuery, query: &impl SelectQuery<Data>)
    where
        Self: Sized,
        Data: DataSet + 'a,
    {
        let (Some(table), Some(condition)) = (query.get_from(), query.get_where_condition()) else {
            log::error!("The query does not have the FROM or WHERE part");
            return;
        };
        let table = table.table_ref();
        let name = table.full_name();
        let limit = query.get_limit();
        Self::prepare_query(
            out,
            if limit == Some(1) {
                FindOnePayload {
                    table,
                    filter: Default::default(),
                    options: FindOneOptions::builder()
                        .comment(Bson::String(format!("Tank: select one entity from {name}")))
                        .build(),
                }
                .into()
            } else {
                FindManyPayload {
                    table,
                    filter: Default::default(),
                    options: FindOptions::builder()
                        .comment(Bson::String(format!("Tank: select entities from {name}")))
                        .limit(limit.map(|v| v as _))
                        .build(),
                }
                .into()
            },
        );
        let mut context = Context::fragment(Fragment::SqlSelectWhere);
        self.write_filter_expression(&mut context, out, condition);
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
                    let mut context = Context::fragment(Fragment::SqlInsertInto);
                    self.write_filter_expression(&mut context, &mut query, entity.primary_key_expr());
                    let Some(Bson::Document(filter)) = query
                        .as_prepared::<MongoDBDriver>()
                        .and_then(MongoDBPrepared::current_bson)
                        .map(mem::take)
                    else {
                        // Unreachable
                        log::error!(
                            "Unexpected error while rendering the primary key expression for upsert, the query does not have a current bson"
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
                    Some((entity, filter, UpdateModifications::Document(doc! { "$set": modifications })))
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
        Self::prepare_query(out, payload);
    }

    fn write_delete<E>(&self, out: &mut DynQuery, condition: impl Expression)
    where
        Self: Sized,
        E: Entity,
    {
        let table = E::table().clone();
        let name = table.full_name();
        Self::prepare_query(
            out,
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
        let mut context = Context::fragment(Fragment::SqlDeleteFromWhere);
        self.write_filter_expression(&mut context, out, condition);
    }
}
