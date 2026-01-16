use bson::{Document, doc};
use serde_json::{Map as JsonMap, Value as JsonValue};
use std::{collections::BTreeMap, mem};
use tank_core::{
    BinaryOp, BinaryOpType, ColumnDef, Context, DataSet, Entity, Expression, Fragment, QueryData,
    QueryMetadata, RawQuery, SqlWriter,
};

#[derive(Default)]
pub struct MongoDBSqlWriter {}

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

    fn write_expression_binary_op(
        &self,
        context: &mut Context,
        out: &mut DynQuery,
        value: &BinaryOp<&dyn Expression, &dyn Expression>,
    ) {
        let doc = out.switch_to_document();
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
        let rhs = {
            let mut doc = DynQuery::new_document();
            value.rhs.write_query(self, context, &mut doc);
            mem::take(doc.switch_to_document())
        };
        if context.fragment == Fragment::DocMatchCriteria {
            let mut context = context.switch_fragment(Fragment::DocMatchCriteriaKey);
            let mut key = DynQuery::new(Default::default());
            value.lhs.write_query(self, &mut context.current, &mut key);
            let key = mem::take(key.buffer());
            doc.insert(key, rhs);
        }
    }

    fn write_select<'a, Data>(&self, out: &mut DynQuery, query: &impl QueryData<Data>)
    where
        Self: Sized,
        Data: DataSet + 'a,
    {
        let columns = query.get_select();
        let Some(from) = query.get_from() else {
            return;
        };
        let limit = query.get_limit();
        self.update_table_ref(
            out,
            QueryMetadata {
                table: from.table_ref(),
                limit,
            }
            .into(),
        );
        let mut has_order_by = false;
        let mut context = Context::new(Fragment::DocMatchCriteria, Data::qualified_columns());
        for column in query.get_where_condition() {
            column.write_query(self, &mut context, out);
        }
    }

    fn write_insert<'b, E>(
        &self,
        out: &mut DynQuery,
        entities: impl IntoIterator<Item = &'b E>,
        _update: bool,
    ) where
        Self: Sized,
        E: Entity + 'b,
    {
        let mut docs = Vec::<JsonValue>::new();
        for ent in entities.into_iter() {
            let row = ent.row_filtered();
            let mut map = JsonMap::new();
            for (k, v) in row.into_iter() {
                map.insert(k.to_string(), tank_value_to_json(&v));
            }
            docs.push(JsonValue::Object(map));
        }
        if docs.is_empty() {
            return;
        }
        let name = E::table().full_name();
        let payload = if docs.len() == 1 {
            docs.into_iter().next().unwrap()
        } else {
            JsonValue::Array(docs)
        };
        if !out.is_empty() {
            out.push('\n');
        }
        out.push_str(&format!("MONGO:INSERT {} {};", name, payload.to_string()));
    }
}
