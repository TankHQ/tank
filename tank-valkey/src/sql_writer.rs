use crate::{IsField, IsPKCondition, ValkeyDriver, ValkeyPrepared, ValueWrap};
use anyhow::anyhow;
use redis::Cmd;
use std::{borrow::Cow, fmt::Write, hint, mem};
use tank_core::{
    AnyQuery, AsValue, Case, ColumnDef, Context, Dataset, Expression, Fragment, InsertIntoQuery,
    IsAsterisk, LiteralValue, Result, SelectQuery, SqlCoreWriter, SqlExpressionWriter,
    SqlFragmentWriter, SqlValueWriter, SqlWriter, TableRef, Value, future::Either,
};

pub struct ValkeySqlWriter {
    pub(crate) separator: &'static str,
    pub(crate) keys_with_names: bool,
}

impl ValkeySqlWriter {
    pub const fn new(separator: &'static str, keys_with_names: bool) -> Self {
        Self {
            separator,
            keys_with_names,
        }
    }

    /// Values stored in the entity hash itself, unlike nested collections kept in child keys.
    const fn is_hash_field(value: &Value<'_>) -> bool {
        value.is_scalar() || matches!(value, Value::Json(..))
    }

    /// Queues the command that reads one column, and registers it for decoding.
    fn push_column(
        &self,
        prepared: &mut ValkeyPrepared,
        key: &str,
        column: &ColumnDef,
    ) -> Result<()> {
        let id = column.name();
        let ty = &column.value;
        let child_key = || format!("{key}{}{id}", self.separator());
        match ty {
            v if Self::is_hash_field(v) => prepared.commands.push(Cmd::hget(key, id)),
            Value::Array(_, t, ..) | Value::List(_, t, ..) => {
                if !Self::is_hash_field(&t.element) {
                    let error = anyhow!(
                        "Valkey/Redis can only query lists with scalar values, found: {ty:?}"
                    );
                    log::error!("{error:#}");
                    return Err(error);
                }
                prepared.commands.push(Cmd::lrange(child_key(), 0, -1));
            }
            Value::Map(_, t, ..) => {
                if !Self::is_hash_field(&t.key) || !Self::is_hash_field(&t.value) {
                    let error = anyhow!(
                        "Valkey/Redis can only query maps with scalar key and value (they are encoded as HSET), found: {ty:?}"
                    );
                    log::error!("{error:#}");
                    return Err(error);
                }
                prepared.commands.push(Cmd::hgetall(child_key()));
            }
            Value::Struct(_, t, ..) => {
                if !t.fields.iter().all(|(_, v)| Self::is_hash_field(v)) {
                    let error = anyhow!(
                        "Valkey/Redis can only query structs with scalar fields (they are encoded as HSET), found: {ty:?}"
                    );
                    log::error!("{error:#}");
                    return Err(error);
                }
                prepared.commands.push(Cmd::hgetall(child_key()));
            }
            _ => {
                let error = anyhow!("Valkey/Redis cannot query columns of type {ty:?}");
                log::error!("{error:#}");
                return Err(error);
            }
        };
        prepared.columns.push(Cow::Owned(column.clone()));
        Ok(())
    }

    pub fn make_prepared() -> AnyQuery {
        AnyQuery::Prepared(Box::new(ValkeyPrepared::default()))
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
        query: &'a mut AnyQuery,
        _context: &mut Context,
    ) -> &'a mut ValkeyPrepared {
        loop {
            if let Some(prepared) = query.as_prepared::<ValkeyDriver>() {
                return prepared;
            }
            *query = Self::make_prepared();
        }
    }
}

impl SqlCoreWriter for ValkeySqlWriter {
    fn as_dyn(&self) -> &dyn SqlWriter {
        self
    }

    fn separator(&self) -> &str {
        &self.separator
    }

    fn write_table_ref(&self, context: &mut Context, out: &mut AnyQuery, value: &TableRef) {
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
    fn write_string(&self, _context: &mut Context, out: &mut AnyQuery, value: &str) {
        out.push_str(value);
    }
}

impl SqlExpressionWriter for ValkeySqlWriter {
    fn write_case(&self, _context: &mut Context, _out: &mut AnyQuery, _value: &Case) {
        log::error!("Valkey does not support CASE expressions");
    }
}

impl SqlFragmentWriter for ValkeySqlWriter {}

impl SqlWriter for ValkeySqlWriter {
    fn write_create_schema(&self, out: &mut AnyQuery, _table: &TableRef, _if_not_exists: bool)
    where
        Self: Sized,
    {
        Self::prepare_query(out, &mut Default::default());
    }

    fn write_drop_schema(&self, out: &mut AnyQuery, _table: &TableRef, _if_exists: bool)
    where
        Self: Sized,
    {
        Self::prepare_query(out, &mut Default::default());
    }

    fn write_create_table(&self, out: &mut AnyQuery, _table: &TableRef, _if_not_exists: bool)
    where
        Self: Sized,
    {
        Self::prepare_query(out, &mut Default::default());
    }

    fn write_column_comment(
        &self,
        _context: &mut Context,
        _out: &mut AnyQuery,
        _column: &ColumnDef,
    ) {
    }

    fn write_drop_table(&self, out: &mut AnyQuery, _table: &TableRef, _if_exists: bool)
    where
        Self: Sized,
    {
        log::warn!("Valkey/Redis does not implement drop table, it must be done separately");
        Self::prepare_query(out, &mut Default::default());
    }

    fn write_select<'a, Data>(&self, out: &mut AnyQuery, query: impl SelectQuery<Data>)
    where
        Self: Sized,
        Data: Dataset + 'a,
    {
        let mut context = Self::make_context(Fragment::SqlSelect);
        let prepared = Self::prepare_query(out, &mut context);
        let (Some(table), Some(where_expr)) = (query.get_from(), query.get_where()) else {
            let error = anyhow!("Valkey/Redis needs both the FROM and WHERE clauses to query");
            log::error!("{error:#}");
            prepared.error = Some(error);
            return;
        };
        let table = table.table_ref();
        if table.name.is_empty() {
            let error = anyhow!(
                "The table is not specified in the dataset (if it's a JOIN, Valkey/Redis does not support it)"
            );
            log::error!("{error:#}");
            prepared.error = Some(error);
            return;
        }
        let mut is_pk_condition = IsPKCondition::new(
            self.keys_with_names,
            table.full_name(self.separator()).into_owned(),
            &table,
        );
        if !where_expr.accept_visitor(
            &mut is_pk_condition,
            self,
            &mut context.switch_fragment(Fragment::SqlSelectWhere).current,
            &mut Default::default(),
        ) {
            let error = anyhow!(
                "Valkey/Redis can only query using the primary key expression of a table, found: `{}`",
                where_expr.as_identifier(&mut Default::default())
            );
            log::error!("{error:#}");
            prepared.error = Some(error);
            return;
        }
        let columns = query.get_select();
        let columns_count = columns.clone().into_iter().count();
        let key = is_pk_condition.key.as_str();
        let single_asterisk = columns_count == 1
            && columns.clone().next().is_some_and(|column| {
                column.accept_visitor(&mut IsAsterisk, self, &mut context, &mut Default::default())
            });
        if single_asterisk {
            if table.columns.is_empty() {
                let error = anyhow!(
                    "Valkey/Redis cannot expand `*` because the table has no column definitions"
                );
                log::error!("{error:#}");
                prepared.error = Some(error);
                return;
            }
            for column in table.columns.iter() {
                if let Err(error) = self.push_column(prepared, key, column) {
                    prepared.error = Some(error);
                    return;
                }
            }
            return;
        }
        if columns_count == 0 {
            let error = anyhow!("Valkey/Redis cannot run a SELECT without columns");
            log::error!("{error:#}");
            prepared.error = Some(error);
            return;
        }
        let mut is_field = IsField::default();
        for column in columns {
            let id = column.as_identifier(&mut context);
            if !column.accept_visitor(&mut is_field, self, &mut context, &mut Default::default()) {
                let error = anyhow!("Valkey/Redis can only query columns, found: {id}");
                log::error!("{error:#}");
                prepared.error = Some(error);
                return;
            }
            let is_column = |c: &&ColumnDef| c.name() == is_field.field;
            let found = match &table.columns {
                Cow::Borrowed(defined) => defined.iter().find(is_column).map(Cow::Borrowed),
                Cow::Owned(defined) => defined.iter().find(is_column).cloned().map(Cow::Owned),
            };
            let Some(column_def) = found else {
                let error = anyhow!(
                    "Valkey/Redis can only query known columns, {id} was not defined in the entity"
                );
                log::error!("{error:#}");
                prepared.error = Some(error);
                return;
            };
            if let Err(error) = self.push_column(prepared, key, &column_def) {
                prepared.error = Some(error);
                return;
            }
        }
    }

    fn write_insert(&self, out: &mut AnyQuery, query: impl InsertIntoQuery)
    where
        Self: Sized,
    {
        let table = query.get_table();
        let columns = query.get_columns();
        let update = query.get_update();
        let mut context = Self::make_context(Fragment::SqlInsertInto);
        let prepared = Self::prepare_query(out, &mut context);
        let Either::Left(rows) = query.get_values() else {
            let error = anyhow!("Valkey/Redis does not support INSERT ... SELECT");
            log::error!("{error:#}");
            prepared.error = Some(error);
            return;
        };
        if table.primary_key().next().is_none() {
            let error = anyhow!(
                "Valkey/Redis needs the primary key of {} to build the keys, use the `Entity::table()` as target",
                table.full_name(self.separator())
            );
            log::error!("{error:#}");
            prepared.error = Some(error);
            return;
        }
        for row in rows {
            let values = match row.map(LiteralValue::new).collect::<Result<Vec<_>>>() {
                Ok(values) => values,
                Err(error) => {
                    hint::cold_path();
                    let error = error.context("Cannot insert into Valkey/Redis");
                    log::error!("{error:#}");
                    prepared.error = Some(error);
                    return;
                }
            };
            let fields = || columns.iter().map(ColumnDef::name).zip(&values);
            // Same key as `IsPKCondition` builds from a primary key condition.
            let mut key = table.full_name(self.separator()).into_owned();
            for name in table.primary_key().map(ColumnDef::name) {
                let Some((_, value)) = fields().find(|(field, _)| *field == name) else {
                    let error = anyhow!(
                        "Cannot insert into Valkey/Redis: the row has no value for the primary key column `{name}`"
                    );
                    log::error!("{error:#}");
                    prepared.error = Some(error);
                    return;
                };
                if !key.is_empty() {
                    key.push_str(self.separator());
                }
                if self.keys_with_names {
                    key.push_str(name);
                    key.push_str(self.separator());
                }
                let mut rendered = AnyQuery::new(key);
                self.write_value(&mut context, &mut rendered, value);
                key = mem::take(rendered.buffer());
            }
            if update {
                // Saving over an existing entity must replace it (delete it first)
                prepared.commands.push(Cmd::del(&key));
                for column in table
                    .columns
                    .iter()
                    .filter(|c| !Self::is_hash_field(&c.value))
                {
                    prepared.commands.push(Cmd::del(format!(
                        "{key}{}{}",
                        self.separator(),
                        column.name()
                    )));
                }
            }
            prepared.commands.push(Cmd::hset_multiple(
                &key,
                fields()
                    .filter_map(|(k, v)| {
                        if Self::is_hash_field(v) && !v.is_null() {
                            (k, ValueWrap(v.as_value())).into()
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<_>>()
                    .as_slice(),
            ));
            for (k, value) in fields().filter(|(_, v)| !Self::is_hash_field(v)) {
                let key = format!("{key}{}{k}", self.separator);
                let valid = |values: &[Value<'_>]| {
                    values
                        .iter()
                        .all(|v| Self::is_hash_field(v) && !v.is_null())
                };
                match value {
                    Value::Array(Some(v), ..) | Value::List(Some(v), ..) if !v.is_empty() => {
                        if !valid(v) {
                            let error = anyhow!(
                                "Valkey/Redis cannot store `{k}`: collections cannot contain null or nested values"
                            );
                            log::error!("{error:#}");
                            prepared.error = Some(error);
                            return;
                        }
                        prepared
                            .commands
                            .push(Cmd::rpush(key, ValueWrap(value.as_value())));
                    }
                    Value::Map(Some(v), ..) if !v.is_empty() => {
                        if !v.iter().all(|(k, v)| {
                            Self::is_hash_field(k)
                                && !k.is_null()
                                && Self::is_hash_field(v)
                                && !v.is_null()
                        }) {
                            let error = anyhow!(
                                "Valkey/Redis cannot store `{k}`: maps cannot contain null or nested keys or values"
                            );
                            log::error!("{error:#}");
                            prepared.error = Some(error);
                            return;
                        }
                        prepared.commands.push(Cmd::hset_multiple(
                            key,
                            v.iter()
                                .map(|(k, v)| (ValueWrap(k.as_value()), ValueWrap(v.as_value())))
                                .collect::<Vec<_>>()
                                .as_slice(),
                        ));
                    }
                    Value::Struct(Some(v), ..) if !v.is_empty() => {
                        if !v
                            .iter()
                            .all(|(_, v)| Self::is_hash_field(v) && !v.is_null())
                        {
                            let error = anyhow!(
                                "Valkey/Redis cannot store `{k}`: structs cannot contain null or nested values"
                            );
                            log::error!("{error:#}");
                            prepared.error = Some(error);
                            return;
                        }
                        prepared.commands.push(Cmd::hset_multiple(
                            key,
                            v.iter()
                                .map(|(k, v)| (k, ValueWrap(v.as_value())))
                                .collect::<Vec<_>>()
                                .as_slice(),
                        ));
                    }
                    _ => {}
                };
            }
        }
    }

    fn write_delete(&self, out: &mut AnyQuery, from: &dyn Dataset, condition: impl Expression)
    where
        Self: Sized,
    {
        let table = from.table_ref();
        let mut context = Self::make_context(Fragment::SqlDeleteFrom);
        let prepared = Self::prepare_query(out, &mut context);
        let mut is_pk_condition = IsPKCondition::new(
            self.keys_with_names,
            table.full_name(self.separator()).into_owned(),
            &table,
        );
        if !condition.accept_visitor(
            &mut is_pk_condition,
            self,
            &mut context
                .switch_fragment(Fragment::SqlDeleteFromWhere)
                .current,
            &mut Default::default(),
        ) {
            let error = anyhow!(
                "Valkey/Redis can only delete using the primary key conditions, found: `{}`",
                condition.as_identifier(&mut Default::default())
            );
            log::error!("{error:#}");
            prepared.error = Some(error);
            return;
        }
        let key = is_pk_condition.key;
        prepared.commands.push(Cmd::del(&key));
        for column in table
            .columns
            .iter()
            .filter(|c| !Self::is_hash_field(&c.value))
        {
            let child_key = format!("{key}{}{}", self.separator(), column.name());
            prepared.commands.push(Cmd::del(child_key));
        }
    }
}
