use crate::{MongoDBDriver, MongoDBPrepared};
use std::{collections::BTreeMap, mem};
use tank_core::{
    BinaryOp, BinaryOpType, ColumnDef, Context, DataSet, DynQuery, Expression, Fragment,
    QueryMetadata, QueryType, SelectQuery, SqlWriter,
};

#[derive(Default)]
pub struct MongoDBSqlWriter {}

impl MongoDBSqlWriter {
    pub fn make_prepared() -> DynQuery {
        DynQuery::Prepared(Box::new(MongoDBPrepared::default()))
    }
    pub fn switch_to_prepared(query: &mut DynQuery) -> &mut MongoDBPrepared {
        if query.as_prepared::<MongoDBDriver>().is_none() {
            *query = DynQuery::Prepared(Box::new(MongoDBPrepared::default()));
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

    // fn write_expression_binary_op(
    //     &self,
    //     context: &mut Context,
    //     out: &mut DynQuery,
    //     value: &BinaryOp<&dyn Expression, &dyn Expression>,
    // ) {
    //     let op = match value.op {
    //         BinaryOpType::Indexing => todo!(),
    //         BinaryOpType::Cast => todo!(),
    //         BinaryOpType::Multiplication => todo!(),
    //         BinaryOpType::Division => todo!(),
    //         BinaryOpType::Remainder => todo!(),
    //         BinaryOpType::Addition => todo!(),
    //         BinaryOpType::Subtraction => todo!(),
    //         BinaryOpType::ShiftLeft => todo!(),
    //         BinaryOpType::ShiftRight => todo!(),
    //         BinaryOpType::BitwiseAnd => todo!(),
    //         BinaryOpType::BitwiseOr => todo!(),
    //         BinaryOpType::In => todo!(),
    //         BinaryOpType::NotIn => todo!(),
    //         BinaryOpType::Is => todo!(),
    //         BinaryOpType::IsNot => todo!(),
    //         BinaryOpType::Like => todo!(),
    //         BinaryOpType::NotLike => todo!(),
    //         BinaryOpType::Regexp => todo!(),
    //         BinaryOpType::NotRegexp => todo!(),
    //         BinaryOpType::Glob => todo!(),
    //         BinaryOpType::NotGlob => todo!(),
    //         BinaryOpType::Equal => "$eq",
    //         BinaryOpType::NotEqual => "$ne",
    //         BinaryOpType::Less => "$lt",
    //         BinaryOpType::Greater => "$gt",
    //         BinaryOpType::LessEqual => "$lte",
    //         BinaryOpType::GreaterEqual => "$gte",
    //         BinaryOpType::And => todo!(),
    //         BinaryOpType::Or => todo!(),
    //         BinaryOpType::Alias => todo!(),
    //     };
    //     let mut rhs;
    //     let rhs = {
    //         rhs = MongoDBSqlWriter::make_prepared();
    //         value.rhs.write_query(self, context, &mut rhs);
    //         Self::switch_to_prepared(&mut rhs)
    //     };
    //     if context.fragment == Fragment::SqlSelectWhere {
    //         let mut context = context.switch_fragment(Fragment::JsonKey);
    //         let mut key = DynQuery::new(String::new());
    //         value.lhs.write_query(self, &mut context.current, &mut key);
    //         let key = mem::take(key.buffer());
    //         Self::switch_to_prepared(out)
    //             .find
    //             .insert(key, mem::take(&mut rhs.current));
    //     }
    // }

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
}
