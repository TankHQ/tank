use crate::{
    FieldConditionMatcher, InsertManyPayload, InsertOnePayload, MongoDBDriver, MongoDBPrepared,
    Payload,
};
use mongodb::bson::Bson;
use std::{collections::BTreeMap, iter, mem};
use tank_core::{
    BinaryOp, BinaryOpType, ColumnDef, Context, DynQuery, Entity, Expression, Fragment,
    QueryMetadata, QueryType, SqlWriter,
};

#[derive(Default)]
pub struct MongoDBSqlWriter {}

impl MongoDBSqlWriter {
    pub fn make_prepared() -> DynQuery {
        DynQuery::Prepared(Box::new(MongoDBPrepared::default()))
    }

    pub fn switch_to_prepared(query: &mut DynQuery) -> &mut MongoDBPrepared {
        if query.as_prepared::<MongoDBDriver>().is_none() {
            let mut prepared = MongoDBPrepared::default();
            prepared.metadata = mem::take(query.metadata_mut());
            *query = DynQuery::Prepared(Box::new(prepared));
        }
        let Some(prepared) = query.as_prepared::<MongoDBDriver>() else {
            unreachable!("Expected to be the MongoDBPrepared here");
        };
        prepared
    }

    pub fn expression_binary_op_key(&self, value: BinaryOpType) -> &'static str {
        let value = match value {
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
            BinaryOpType::Is => "",
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
        if value.is_empty() {
            log::error!("MongoDB does not support {value} binary operator");
        }
        value
    }
}

impl SqlWriter for MongoDBSqlWriter {
    fn as_dyn(&self) -> &dyn SqlWriter {
        self
    }

    fn write_column_overridden_type(
        &self,
        _context: &mut Context,
        out: &mut DynQuery,
        _column: &ColumnDef,
        types: &BTreeMap<&'static str, &'static str>,
    ) {
        if let Some(t) = types
            .iter()
            .find_map(|(k, v)| if *k == "mongodb" { Some(v) } else { None })
        {
            out.push_str(t);
        }
    }

    // fn write_identifier_quoted(&self, context: &mut Context, out: &mut DynQuery, value: &str) {
    //     out.push_str(value);
    // }

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
        if context.fragment == Fragment::SqlSelectWhere
            && let matcher = &mut FieldConditionMatcher::default()
            && value.matches(matcher)
        {
            // Specific case for a field condition { "field": condition }
            *document = mem::take(&mut matcher.field_condition);
            return;
        }
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

    fn write_insert<'b, E>(
        &self,
        out: &mut DynQuery,
        entities: impl IntoIterator<Item = &'b E>,
        update: bool,
    ) where
        Self: Sized,
        E: Entity + 'b,
    {
        let table = E::table();
        self.update_metadata(
            out,
            QueryMetadata {
                table: table.clone(),
                count: Some(0),
                query_type: if update {
                    QueryType::Upsert
                } else {
                    QueryType::InsertInto
                }
                .into(),
            }
            .into(),
        );
        let prepared = Self::switch_to_prepared(out);
        let mut rows = entities.into_iter().map(Entity::row_labeled).peekable();
        let Some(row) = rows.next() else {
            return;
        };
        let single = rows.peek().is_none();
        prepared.payload = match (update, single) {
            (false, true) => {
                let payload = InsertOnePayload {
                    row,
                    options: Default::default(),
                };
                Payload::InsertOne(payload)
            }
            (false, false) => {
                let payload = InsertManyPayload {
                    rows: iter::chain(iter::once(row), rows).collect(),
                    options: Default::default(),
                };
                Payload::InsertMany(payload)
            }
            (true, _) => {
                // let payload = UpsertPayload {
                //     rows: iter::chain(iter::once(row), rows).collect(),
                //     options: Default::default(),
                // };
                Payload::Upsert(Default::default())
            }
        };
    }

    fn write_delete<E>(&self, out: &mut DynQuery, condition: impl Expression)
    where
        Self: Sized,
        E: Entity,
    {
        let prepared = Self::switch_to_prepared(out);
        prepared.payload = Payload::Delete(Default::default());
        let table = E::table();
        self.update_metadata(
            out,
            QueryMetadata {
                table: table.clone(),
                count: None,
                query_type: QueryType::DeleteFrom.into(),
            }
            .into(),
        );
        let mut context = Context::fragment(Fragment::SqlDeleteFromWhere);
        condition.write_query(self, &mut context, out);
    }
}
