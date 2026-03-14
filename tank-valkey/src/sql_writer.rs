use crate::{IsField, IsPKCondition, ValkeyDriver, ValkeyPrepared, ValueWrap};
use redis::Cmd;
use std::{borrow::Cow, fmt::Write};
use tank_core::{
    Context, CreateSchemaQuery, CreateTableQuery, Dataset, DropSchemaQuery, DropTableQuery,
    DynQuery, Entity, Expression, Fragment, IsAsterisk, SelectQuery, SqlWriter, TableRef, Value,
    column_def,
};

#[derive(Default)]
pub struct ValkeySqlWriter {}

impl ValkeySqlWriter {
    pub fn make_prepared() -> DynQuery {
        DynQuery::Prepared(Box::new(ValkeyPrepared::default()))
    }

    pub fn make_context(fragment: Fragment) -> Context {
        Context {
            counter: 0,
            fragment,
            table_ref: Default::default(),
            qualify_columns: false,
            quote_identifiers: false,
        }
    }

    pub(crate) fn prepare_query<'a>(
        query: &'a mut DynQuery,
        _context: &mut Context,
    ) -> &'a mut ValkeyPrepared {
        if query.as_prepared::<ValkeyDriver>().is_none() {
            *query = Self::make_prepared();
        }
        let Some(prepared) = query.as_prepared::<ValkeyDriver>() else {
            unreachable!();
        };
        prepared
    }
}

impl SqlWriter for ValkeySqlWriter {
    fn as_dyn(&self) -> &dyn SqlWriter {
        self
    }

    fn separator(&self) -> &str {
        ":"
    }

    fn write_table_ref(&self, context: &mut Context, out: &mut DynQuery, value: &TableRef) {
        if self.is_alias_declaration(context) || value.alias.is_empty() {
            if !value.schema.is_empty() {
                self.write_identifier(context, out, &value.schema, context.quote_identifiers);
                out.push_str(self.separator());
            }
            self.write_identifier(context, out, &value.name, context.quote_identifiers);
        }
        if !value.alias.is_empty() {
            let _ = write!(out, " {}", value.alias);
        }
    }
    fn write_value_string(&self, _context: &mut Context, out: &mut DynQuery, value: &str) {
        out.push_str(value);
    }

    fn write_create_schema(&self, out: &mut DynQuery, _query: &impl CreateSchemaQuery) {
        Self::prepare_query(out, &mut Default::default());
    }

    fn write_drop_schema(&self, out: &mut DynQuery, _query: &impl DropSchemaQuery) {
        Self::prepare_query(out, &mut Default::default());
    }

    fn write_create_table<E>(&self, out: &mut DynQuery, _query: &impl CreateTableQuery<E>)
    where
        Self: Sized,
        E: Entity,
    {
        Self::prepare_query(out, &mut Default::default());
    }

    fn write_drop_table<E>(&self, out: &mut DynQuery, _query: &impl DropTableQuery<E>)
    where
        Self: Sized,
        E: Entity,
    {
        log::error!("Valkey/Redis does not implement drop table, it must be done separately");
        Self::prepare_query(out, &mut Default::default());
    }

    fn write_select<'a, Data>(&self, out: &mut DynQuery, query: &impl SelectQuery<Data>)
    where
        Self: Sized,
        Data: Dataset + 'a,
    {
        let (Some(table), Some(where_expr)) = (query.get_from(), query.get_where()) else {
            log::error!("The query does not have the FROM or WHERE clause");
            return;
        };
        let table = table.table_ref();
        if table.name.is_empty() {
            log::error!(
                "The table is not specified in the dataset (if it is a JOIN, Valkey/Redis does not support it)"
            );
            return;
        }
        let mut context = Self::make_context(Fragment::SqlSelect);
        let mut is_pk_condition =
            IsPKCondition::new(table.full_name(self.separator()).into_owned());
        if !where_expr.accept_visitor(
            &mut is_pk_condition,
            self,
            &mut context.switch_fragment(Fragment::SqlSelectWhere).current,
            &mut Default::default(),
        ) {
            log::error!("Valkey/Redis can only query using the primary key expression of a table");
            return;
        }
        let prepared = Self::prepare_query(out, &mut context);
        let mut columns = query.get_select();
        let columns_count = columns.clone().into_iter().count();
        let key = is_pk_condition.key.as_str();
        if columns_count == 0
            || columns_count == 1
                && columns.next().unwrap().accept_visitor(
                    &mut IsAsterisk,
                    self,
                    &mut context,
                    &mut Default::default(),
                )
        {
            prepared.commands.push(Cmd::hgetall(key));
            return;
        }
        let mut is_field = IsField::default();
        for column in columns {
            let id = column.as_identifier(&mut context);
            if !column.accept_visitor(&mut is_field, self, &mut context, &mut Default::default()) {
                log::error!("Valkey/Redis can only query columns, found: {id}",);
                return;
            }
            let Some(column_def) = column_def(&is_field.field, &table) else {
                log::error!(
                    "Valkey/Redis can only query known columns, {id} was not defined in the entity",
                );
                return;
            };
            prepared.columns.push(column_def);
            let ty = &column_def.value;
            match ty {
                v if v.is_scalar() => prepared.commands.push(Cmd::hget(key, id)),
                Value::Array(.., ty, _) | Value::List(.., ty) => {
                    if !ty.is_scalar() {
                        log::error!(
                            "Valkey/Redis can only query lists with scalar values, found: {ty:?}"
                        );
                        return;
                    }
                    prepared
                        .commands
                        .push(Cmd::lrange(format!("{key}:{id}"), 0, -1));
                }
                Value::Map(.., k_ty, v_ty) => {
                    if !k_ty.is_scalar() || !v_ty.is_scalar() {
                        log::error!(
                            "Valkey/Redis can only query maps with scalar key and value (they are encoded as HSET), found: {ty:?}"
                        );
                        return;
                    }
                    prepared
                        .commands
                        .push(Cmd::hgetall(format!("{key}{}{id}", self.separator())));
                }
                _ => {
                    log::error!("Valkey/Redis cannot query columns of type {ty:?}");
                    return;
                }
            };
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
        let table = E::table();
        let mut context = Self::make_context(Fragment::SqlInsertInto);
        let prepared = Self::prepare_query(out, &mut context);
        for entity in entities.into_iter() {
            let row = entity.row_filtered();
            let mut is_pk_condition =
                IsPKCondition::new(table.full_name(self.separator()).into_owned());
            entity.primary_key_expr().accept_visitor(
                &mut is_pk_condition,
                self,
                &mut context,
                &mut Default::default(),
            );
            let key = is_pk_condition.key;
            prepared.commands.push(Cmd::hset_multiple(
                &key,
                row.iter()
                    .filter_map(|(k, v)| {
                        if v.is_scalar() && !v.is_null() {
                            (k, ValueWrap(Cow::Borrowed(v))).into()
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<_>>()
                    .as_slice(),
            ));
            for (k, v) in row.iter().filter_map(|(k, v)| {
                if !v.is_scalar() || v.same_type(&Value::Json(None)) {
                    (k, ValueWrap(Cow::Borrowed(v))).into()
                } else {
                    None
                }
            }) {
                let key = format!("{key}:{k}");
                let value = v.0.as_ref();
                match value {
                    Value::Array(Some(..), ..) | Value::List(Some(..), ..) => prepared
                        .commands
                        .push(Cmd::rpush(key, ValueWrap(Cow::Borrowed(value)))),
                    Value::Map(Some(value), ..) => prepared.commands.push(Cmd::hset_multiple(
                        key,
                        value
                            .iter()
                            .map(|(k, v)| {
                                (ValueWrap(Cow::Borrowed(k)), ValueWrap(Cow::Borrowed(v)))
                            })
                            .collect::<Vec<_>>()
                            .as_slice(),
                    )),
                    Value::Struct(Some(value), ..) => prepared.commands.push(Cmd::hset_multiple(
                        key,
                        value
                            .iter()
                            .map(|(k, v)| (k, ValueWrap(Cow::Borrowed(v))))
                            .collect::<Vec<_>>()
                            .as_slice(),
                    )),
                    _ => {}
                };
            }
        }
    }

    fn write_delete<E>(&self, out: &mut DynQuery, condition: impl Expression)
    where
        Self: Sized,
        E: Entity,
    {
        let table = E::table();
        let mut context = Self::make_context(Fragment::SqlDeleteFrom);
        let mut is_pk_condition =
            IsPKCondition::new(table.full_name(self.separator()).into_owned());
        if !condition.accept_visitor(
            &mut is_pk_condition,
            self,
            &mut context
                .switch_fragment(Fragment::SqlDeleteFromWhere)
                .current,
            &mut Default::default(),
        ) {
            log::error!(
                "Valkey/Redis can only delete using the primary key conditions, found: {condition:?}"
            );
            return;
        }
        let prepared = Self::prepare_query(out, &mut context);
        let key = is_pk_condition.key;
        prepared.commands.push(Cmd::del(&key));
        for column in E::columns().iter().filter(|c| !c.value.is_scalar()) {
            let child_key = format!("{key}{}{}", self.separator(), column.name());
            prepared.commands.push(Cmd::del(child_key));
        }
    }
}
