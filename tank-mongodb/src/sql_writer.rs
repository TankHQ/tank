use crate::{
    AggregatePayload, BatchPayload, CreateCollectionPayload, CreateDatabasePayload, DeletePayload,
    DropCollectionPayload, DropDatabasePayload, FindManyPayload, FindOnePayload, InsertManyPayload,
    InsertOnePayload, IsFieldCondition, MongoDBDriver, MongoDBPrepared, Payload, RowWrap,
    UpsertPayload, value_to_bson,
};
use mongodb::{
    Namespace,
    bson::{self, Binary, Bson, Document, doc, spec::BinarySubtype},
    options::{
        AggregateOptions, CreateCollectionOptions, DeleteOptions, FindOneOptions, FindOptions,
        InsertManyOptions, InsertOneOptions, UpdateModifications, UpdateOptions,
    },
};
use std::{borrow::Cow, collections::HashMap, f64, iter, mem};
use tank_core::{
    AsValue, BinaryOp, BinaryOpType, ColumnRef, Context, DataSet, DynQuery, Entity, ErrorContext,
    Expression, FindOrder, Fragment, Interval, IsAggregateFunction, IsFalse, IsTrue, Operand,
    Order, Result, SelectQuery, SqlWriter, TableRef, Value, print_timer, truncate_long,
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
            BinaryOpType::LessEqual => "$le",
            BinaryOpType::GreaterEqual => "$ge",
            BinaryOpType::And => "$and",
            BinaryOpType::Or => "$or",
            BinaryOpType::Alias => "",
        };
        if result.is_empty() {
            log::error!("MongoDB does not support {value:?} binary operator");
        }
        result
    }

    pub fn write_match_expression(
        &self,
        context: &mut Context,
        out: &mut DynQuery,
        condition: impl Expression,
        table: Cow<'static, str>,
    ) {
        if condition.matches(&mut IsFalse, self)
            && let Some(prepared) = out.as_prepared::<MongoDBDriver>()
            && let Some(target) = prepared.current_bson()
        {
            *target = Bson::Document(Self::make_unmatchable())
        } else if condition.matches(&mut IsTrue, self)
            && let Some(prepared) = out.as_prepared::<MongoDBDriver>()
            && let Some(target) = prepared.current_bson()
        {
            *target = Bson::Document(Default::default());
        } else if let matcher = &mut IsFieldCondition::with_table(table)
            && condition.matches(matcher, self)
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
                    "Unexpected error while rendering the lhs of the binary expression, failed to get the bson object"
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
                    "Unexpected error while rendering the rhs of the binary expression, failed to get the bson object"
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
        let (Some(table), Some(condition)) = (query.get_from(), query.get_where()) else {
            log::error!("The query does not have the FROM or WHERE part");
            return;
        };
        let mut context = Context::fragment(Fragment::SqlSelect);
        let table = table.table_ref();
        let name = table.full_name();
        let limit = query.get_limit();
        let mut group_by = query.get_group_by().peekable();
        let mut group: Document = Document::new();
        let mut is_aggregate = group_by.peek().is_some();
        macro_rules! update_group {
            ($column:expr, $name:expr, $bson:expr) => {
                if $column.matches(&mut IsAggregateFunction, self) {
                    group.insert($name, $bson);
                    is_aggregate = true;
                } else if is_aggregate {
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
            expression.as_written(&mut Context {
                qualify_columns: qualify,
                quote_identifiers: false,
                ..Default::default()
            })
        }
        let mut project = Document::new();
        for column in query.get_select() {
            let mut query = Self::make_prepared();
            column.write_query(self, &mut context, &mut query);
            let name = get_name(&column, false);
            let Some(bson) = query
                .as_prepared::<MongoDBDriver>()
                .and_then(MongoDBPrepared::current_bson)
                .map(mem::take)
            else {
                // Unreachable
                log::error!(
                    "Unexpected error while rendering the column {} in select query, failed to get the bson object",
                    truncate_long!(&name, true)
                );
                return;
            };
            update_group!(column, name.clone(), bson.clone());
            project.insert(name, bson);
        }
        let filter = {
            let mut context = context.switch_fragment(Fragment::SqlSelectWhere);
            let mut query = Self::make_prepared();
            self.write_match_expression(
                &mut context.current,
                &mut query,
                condition,
                Default::default(),
            );
            let Some(Bson::Document(document)) = query
                .as_prepared::<MongoDBDriver>()
                .and_then(MongoDBPrepared::current_bson)
                .map(mem::take)
            else {
                // Unreachable
                log::error!(
                    "Unexpected error while rendering the where clause in a select query, failed to get the bson document object"
                );
                return;
            };
            document
        };
        for column in group_by {
            let mut context = context.switch_fragment(Fragment::SqlSelectGroupBy);
            let mut query = Self::make_prepared();
            column.write_query(self, &mut context.current, &mut query);
            let name = get_name(&column, false);
            let Some(bson) = query
                .as_prepared::<MongoDBDriver>()
                .and_then(MongoDBPrepared::current_bson)
                .map(mem::take)
            else {
                // Unreachable
                log::error!(
                    "Unexpected error while rendering the column {} in select query, failed to get the bson object",
                    truncate_long!(&name, true)
                );
                return;
            };
            update_group!(column, name, bson);
        }
        let mut having = Bson::Null;
        if let Some(condition) = query.get_having() {
            let mut context = context.switch_fragment(Fragment::SqlSelectHaving);
            let mut query = Self::make_prepared();
            self.write_match_expression(&mut context.current, &mut query, condition, "_id".into());
            // condition.write_query(self, &mut context.current, &mut query);
            let Some(bson) = query
                .as_prepared::<MongoDBDriver>()
                .and_then(MongoDBPrepared::current_bson)
                .map(mem::take)
            else {
                // Unreachable
                log::error!(
                    "Unexpected error while rendering the column {} in select query, failed to get the bson object",
                    truncate_long!(&name, true)
                );
                return;
            };
            having = bson;
        }
        let mut sort = Document::new();
        {
            for order in query.get_order_by() {
                let find_order = &mut FindOrder::default();
                order.matches(find_order, self);
                sort.insert(
                    get_name(&order, false),
                    Bson::Int32(if find_order.order == Order::DESC {
                        -1
                    } else {
                        -1
                    }),
                );
            }
        }
        let payload: Payload = if is_aggregate {
            let mut pipeline = Vec::new();
            if !filter.is_empty() {
                pipeline.push(doc! { "$match": filter });
            }
            if !group.is_empty() {
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
            pipeline.push(doc! { "$project": project });
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
                filter: filter.into(),
                options: FindOneOptions::builder()
                    .comment(Bson::String(format!("Tank: select one entity from {name}")))
                    .projection(Some(project))
                    .build(),
            }
            .into()
        } else {
            FindManyPayload {
                table,
                filter: filter.into(),
                options: FindOptions::builder()
                    .comment(Bson::String(format!("Tank: select entities from {name}")))
                    .limit(limit.map(|v| v as _))
                    .build(),
            }
            .into()
        };
        Self::prepare_query(out, payload);
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
                    self.write_match_expression(&mut context, &mut query, entity.primary_key_expr(), Default::default());
                    let Some(Bson::Document(filter)) = query
                        .as_prepared::<MongoDBDriver>()
                        .and_then(MongoDBPrepared::current_bson)
                        .map(mem::take)
                    else {
                        // Unreachable
                        log::error!(
                            "Unexpected error while rendering the primary key expression for upsert, failed to get the bson object"
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
        self.write_match_expression(&mut context, out, condition, Default::default());
    }
}
