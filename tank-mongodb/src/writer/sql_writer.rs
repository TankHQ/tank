use crate::{
    AggregatePayload, BatchPayload, CreateCollectionPayload, CreateDatabasePayload, DeletePayload,
    DropCollectionPayload, DropDatabasePayload, FieldType, FindManyPayload, FindOnePayload,
    InsertManyPayload, InsertOnePayload, IsField, MongoDBDriver, MongoDBPrepared, Payload, RowWrap,
    UpsertPayload, WriteMatchExpression,
};
use mongodb::{
    Namespace,
    bson::{Bson, Document, doc},
    options::{
        AggregateOptions, CreateCollectionOptions, DeleteOptions, FindOneOptions, FindOptions,
        InsertManyOptions, InsertOneOptions, UpdateModifications, UpdateOptions,
    },
};
use std::{borrow::Cow, iter, mem, sync::Arc};
use tank_core::{
    AsEntity, BinaryOpType, Context, Dataset, DynQuery, Entity, ErrorContext, Expression, FindOrder,
    Fragment, IsAggregateFunction, IsAsterisk, IsConstant, Order, SelectQuery, SqlCoreWriter,
    SqlWriter, TableRef, truncate_long,
};

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
                let error = e.context("While preparing the query (adding payload)");
                log::error!("{error:#}",);
            };
            prepared.count = context.counter;
        } else {
            if !query.is_empty() {
                log::error!(
                    "The query is not empty, MongoDBSqlWriter::prepare_query will drop the content",
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
        let name = table.full_name(self.separator());
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
        let Some(table) = query.get_from() else {
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
        let name = table.full_name(self.separator());
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
        let mut aggregate_aliases: Vec<(Bson, String)> = Vec::new();
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
            if !aggregate_function
                && column.accept_visitor(&mut IsConstant, self, &mut context, out)
            {
                bson = doc! { "$literal": bson }.into();
            }
            if aggregate_function {
                aggregate_aliases.push((bson.clone(), name.clone()));
            }
            update_group!(column, name.clone(), bson.clone(), aggregate_function);
            if aggregate_function {
                bson = Bson::String(format!("${name}"));
            } else if column.accept_visitor(&mut IsField::default(), self, &mut context, out) {
                bson = Bson::String(format!("$_id.{name}"))
            }
            if let Some(project) = &mut project {
                project.insert(name, bson);
            }
        }
        let aggregate_aliases = Arc::new(aggregate_aliases);
        if is_asterisk {
            project = None;
        }
        let where_expr = if let Some(where_expr) = query.get_where() {
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
            let Some(mut bson) = query
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
            let aggregate_function =
                column.accept_visitor(&mut IsAggregateFunction, self, &mut context.current, out);
            if !aggregate_function
                && column.accept_visitor(&mut IsConstant, self, &mut context.current, out)
            {
                bson = doc! { "$literal": bson }.into();
            }
            update_group!(column, name, bson, aggregate_function);
        }
        let known_columns = Arc::new(group.keys().collect::<Vec<_>>());
        let mut having = Bson::Null;
        if let Some(condition) = query.get_having() {
            let mut context = context.switch_fragment(Fragment::SqlSelectHaving);
            let mut context = context.current.switch_table("_id".into());
            let mut query = Self::make_prepared();
            let mut matcher = WriteMatchExpression {
                known_columns: known_columns.clone(),
                aggregate_aliases: aggregate_aliases.clone(),
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
                            "Unexpected ordering on {}, known columns: {:?}",
                            order.as_identifier(&mut Default::default()),
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
        let limit = query.get_limit();
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
                    .sort(if !sort.is_empty() { Some(sort) } else { None })
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

    fn write_insert<It>(&self, out: &mut DynQuery, entities: It, update: bool)
    where
        Self: Sized,
        It: IntoIterator,
        It::Item: AsEntity,
    {
        let table = <It::Item as AsEntity>::Entity::table().clone();
        let name = table.full_name(self.separator());
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
                row: entity.as_entity().row(),
                options: InsertOneOptions::builder()
                    .comment(Bson::String(format!("Tank: insert one entity in {name}")))
                    .build(),
            }
            .into(),
            (false, false) => {
                let rows = iter::chain(
                    iter::once(entity.as_entity().row()),
                    entities.map(|e| e.as_entity().row()),
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
                    entity.as_entity().primary_key_expr().accept_visitor(
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
                    let modifications: Document = match RowWrap(Cow::Owned(entity.as_entity().row()))
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
                            let table = entity.as_entity().table_ref();
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
        let name = table.full_name(self.separator());
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
