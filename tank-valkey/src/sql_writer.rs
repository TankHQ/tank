use crate::{IsField, IsPKCondition, ValkeyDriver, ValkeyPrepared, ValueWrap};
use redis::Cmd;
use std::{fmt::Write, mem};
use tank_core::{
    AsValue, Case, ColumnDef, Context, Dataset, DynQuery, Expression, Fragment, InsertIntoQuery,
    IsAsterisk, SelectQuery, SqlCoreWriter, SqlExpressionWriter, SqlFragmentWriter, SqlValueWriter,
    SqlWriter, TableRef, Value, column_def, future::Either,
};

pub struct ValkeySqlWriter {
    pub(crate) separator: &'static str,
    pub(crate) keys_with_names: bool,
}

impl ValkeySqlWriter {
    pub fn new(separator: &'static str, keys_with_names: bool) -> Self {
        Self {
            separator,
            keys_with_names,
        }
    }

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

impl SqlCoreWriter for ValkeySqlWriter {
    fn as_dyn(&self) -> &dyn SqlWriter {
        self
    }

    fn separator(&self) -> &str {
        &self.separator
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
}

impl SqlValueWriter for ValkeySqlWriter {
    fn write_string(&self, _context: &mut Context, out: &mut DynQuery, value: &str) {
        out.push_str(value);
    }
}

impl SqlExpressionWriter for ValkeySqlWriter {
    fn write_case(&self, _context: &mut Context, _out: &mut DynQuery, _value: &Case) {
        log::error!("Valkey does not support CASE expressions");
    }
}

impl SqlFragmentWriter for ValkeySqlWriter {}

impl SqlWriter for ValkeySqlWriter {
    fn write_create_schema(&self, out: &mut DynQuery, _table: &TableRef, _if_not_exists: bool)
    where
        Self: Sized,
    {
        Self::prepare_query(out, &mut Default::default());
    }

    fn write_drop_schema(&self, out: &mut DynQuery, _table: &TableRef, _if_exists: bool)
    where
        Self: Sized,
    {
        Self::prepare_query(out, &mut Default::default());
    }

    fn write_create_table(&self, out: &mut DynQuery, _table: &TableRef, _if_not_exists: bool)
    where
        Self: Sized,
    {
        Self::prepare_query(out, &mut Default::default());
    }

    fn write_column_comment(
        &self,
        _context: &mut Context,
        _out: &mut DynQuery,
        _column: &ColumnDef,
    ) {
    }

    fn write_drop_table(&self, out: &mut DynQuery, _table: &TableRef, _if_exists: bool)
    where
        Self: Sized,
    {
        log::error!("Valkey/Redis does not implement drop table, it must be done separately");
        Self::prepare_query(out, &mut Default::default());
    }

    fn write_select<'a, Data>(&self, out: &mut DynQuery, query: impl SelectQuery<Data>)
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
        let mut is_pk_condition = IsPKCondition::new(
            self.keys_with_names,
            table.full_name(self.separator()).into_owned(),
            table.primary_key,
        );
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
                Value::Array(_, t, ..) | Value::List(_, t, ..) => {
                    if !t.element.is_scalar() {
                        log::error!(
                            "Valkey/Redis can only query lists with scalar values, found: {ty:?}"
                        );
                        return;
                    }
                    prepared.commands.push(Cmd::lrange(
                        format!("{key}{}{id}", self.separator),
                        0,
                        -1,
                    ));
                }
                Value::Map(_, t, ..) => {
                    if !t.key.is_scalar() || !t.value.is_scalar() {
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

    fn write_insert(&self, out: &mut DynQuery, query: impl InsertIntoQuery)
    where
        Self: Sized,
    {
        let table = query.get_table();
        let columns = query.get_columns();
        let Either::Left(rows) = query.get_values() else {
            log::error!("Valkey/Redis does not support INSERT ... SELECT");
            return;
        };
        if table.primary_key.is_empty() {
            log::error!(
                "Valkey/Redis needs the primary key of {} to build the keys, use the `Entity::table()` as target",
                table.full_name(self.separator())
            );
            return;
        }
        let mut context = Self::make_context(Fragment::SqlInsertInto);
        let prepared = Self::prepare_query(out, &mut context);
        for row in rows {
            let values = row.collect::<Vec<_>>();
            let fields = || columns.iter().map(ColumnDef::name).zip(&values);
            // Same key as `IsPKCondition` builds from a primary key condition.
            let mut key = table.full_name(self.separator()).into_owned();
            for column in table.primary_key {
                let Some((_, value)) = fields().find(|(name, _)| *name == column.name()) else {
                    log::error!(
                        "The row has no value for the primary key column `{}`",
                        column.name()
                    );
                    return;
                };
                if !key.is_empty() {
                    key.push_str(self.separator());
                }
                if self.keys_with_names {
                    key.push_str(column.name());
                    key.push_str(self.separator());
                }
                let mut rendered = DynQuery::new(key);
                self.write_value(&mut context, &mut rendered, value);
                key = mem::take(rendered.buffer());
            }
            prepared.commands.push(Cmd::hset_multiple(
                &key,
                fields()
                    .filter_map(|(k, v)| {
                        if v.is_scalar() && !v.is_null() {
                            (k, ValueWrap(v.as_value())).into()
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<_>>()
                    .as_slice(),
            ));
            for (k, v) in fields().filter_map(|(k, v)| {
                if !v.is_scalar() || v.same_type(&Value::Json(None)) {
                    (k, ValueWrap(v.as_value())).into()
                } else {
                    None
                }
            }) {
                let key = format!("{key}{}{k}", self.separator);
                let value = v.0;
                match value {
                    Value::Array(Some(ref v), ..) if !v.is_empty() => {
                        prepared.commands.push(Cmd::rpush(key, ValueWrap(value)))
                    }
                    Value::List(Some(ref v), ..) if !v.is_empty() => {
                        prepared.commands.push(Cmd::rpush(key, ValueWrap(value)))
                    }
                    Value::Map(Some(v), ..) if !v.is_empty() => {
                        prepared.commands.push(Cmd::hset_multiple(
                            key,
                            v.iter()
                                .map(|(k, v)| (ValueWrap(k.as_value()), ValueWrap(v.as_value())))
                                .collect::<Vec<_>>()
                                .as_slice(),
                        ))
                    }
                    Value::Struct(Some(v), ..) if !v.is_empty() => {
                        prepared.commands.push(Cmd::hset_multiple(
                            key,
                            v.iter()
                                .map(|(k, v)| (k, ValueWrap(v.as_value())))
                                .collect::<Vec<_>>()
                                .as_slice(),
                        ))
                    }
                    _ => {}
                };
            }
        }
    }

    fn write_delete(&self, out: &mut DynQuery, from: &dyn Dataset, condition: impl Expression)
    where
        Self: Sized,
    {
        let table = from.table_ref();
        let mut context = Self::make_context(Fragment::SqlDeleteFrom);
        let mut is_pk_condition = IsPKCondition::new(
            self.keys_with_names,
            table.full_name(self.separator()).into_owned(),
            table.primary_key,
        );
        if !condition.accept_visitor(
            &mut is_pk_condition,
            self,
            &mut context
                .switch_fragment(Fragment::SqlDeleteFromWhere)
                .current,
            &mut Default::default(),
        ) {
            log::error!(
                "Valkey/Redis can only delete using the primary key conditions, found: {}",
                condition.as_identifier(&mut Default::default())
            );
            return;
        }
        let prepared = Self::prepare_query(out, &mut context);
        let key = is_pk_condition.key;
        prepared.commands.push(Cmd::del(&key));
        for column in table.columns.iter().filter(|c| !c.value.is_scalar()) {
            let child_key = format!("{key}{}{}", self.separator(), column.name());
            prepared.commands.push(Cmd::del(child_key));
        }
    }
}
