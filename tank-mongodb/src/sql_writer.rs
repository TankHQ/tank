use crate::{MongoDBDriver, MongoDBPrepared, Payload};
use std::{collections::BTreeMap, mem};
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
        let Some(prepared) = out.as_prepared::<MongoDBDriver>() else {
            log::error!(
                "Tried to write a binary expression into a query which is not the MongoDB prepared variant"
            );
            return;
        };
        let Some(document) = prepared.current_document() else {
            log::error!(
                "MongoDBPrepared::current_document returned None, no document to write the binary expression into"
            );
            return;
        };
        let op = match value.op {
            BinaryOpType::Indexing => todo!(),
            BinaryOpType::Cast => todo!(),
            BinaryOpType::Multiplication => todo!(),
            BinaryOpType::Division => todo!(),
            BinaryOpType::Remainder => todo!(),
            BinaryOpType::Addition => todo!(),
            BinaryOpType::Subtraction => todo!(),
            BinaryOpType::ShiftLeft => todo!(),
            BinaryOpType::ShiftRight => todo!(),
            BinaryOpType::BitwiseAnd => todo!(),
            BinaryOpType::BitwiseOr => todo!(),
            BinaryOpType::In => todo!(),
            BinaryOpType::NotIn => todo!(),
            BinaryOpType::Is => todo!(),
            BinaryOpType::IsNot => todo!(),
            BinaryOpType::Like => todo!(),
            BinaryOpType::NotLike => todo!(),
            BinaryOpType::Regexp => todo!(),
            BinaryOpType::NotRegexp => todo!(),
            BinaryOpType::Glob => todo!(),
            BinaryOpType::NotGlob => todo!(),
            BinaryOpType::Equal => "$eq",
            BinaryOpType::NotEqual => "$ne",
            BinaryOpType::Less => "$lt",
            BinaryOpType::Greater => "$gt",
            BinaryOpType::LessEqual => "$lte",
            BinaryOpType::GreaterEqual => "$gte",
            BinaryOpType::And => todo!(),
            BinaryOpType::Or => todo!(),
            BinaryOpType::Alias => todo!(),
        };
        let mut rhs = MongoDBSqlWriter::make_prepared();
        value.rhs.write_query(self, context, &mut rhs);
        let rhs = mem::take(
            rhs.as_prepared::<MongoDBDriver>()
                .unwrap()
                .current_document()
                .unwrap(),
        );
        let mut context = context.switch_fragment(Fragment::JsonKey);
        let mut key = DynQuery::new(String::new());
        value.lhs.write_query(self, &mut context.current, &mut key);
        let key = mem::take(key.buffer());
        document.insert(key, rhs);
    }

    // fn write_select<'a, Data>(&self, out: &mut DynQuery, query: &impl SelectQuery<Data>)
    // where
    //     Self: Sized,
    //     Data: DataSet + 'a,
    // {
    //     Self::switch_to_prepared(out);
    //     let columns = query.get_select();
    //     let Some(from) = query.get_from() else {
    //         return;
    //     };
    //     let limit = query.get_limit();
    //     self.update_metadata(
    //         out,
    //         QueryMetadata {
    //             table: from.table_ref(),
    //             limit,
    //             query_type: QueryType::Select.into(),
    //         }
    //         .into(),
    //     );
    //     let mut context = Context::new(Fragment::SqlSelectWhere, Data::qualified_columns());
    //     for condition in query.get_where_condition() {
    //         condition.write_query(self, &mut context, out);
    //     }
    // }

    fn write_delete<E>(&self, out: &mut DynQuery, condition: impl Expression)
    where
        Self: Sized,
        E: Entity,
    {
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
        let prepared = Self::switch_to_prepared(out);
        let mut payload = Default::default();
        prepared.payload = Payload::Delete(payload);
        condition.write_query(self, &mut context, out);
    }
}
