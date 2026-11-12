use crate::{
    ColumnDef, Context, Driver, DynQuery, Executor, Expression, Fragment, Query, RawQuery, Result,
    Row, RowOwned, RowsAffected, Sql, TableRef, Value, future::Either, log_error, stream::Stream,
    truncate_long, writer::SqlWriter,
};
use anyhow::anyhow;
use futures::{FutureExt, StreamExt};
use log::Level;
use std::{
    future::{self, Future},
    pin::pin,
};

pub trait AsEntity {
    type Entity: Entity;
    fn as_entity(&self) -> &Self::Entity;
}

/// Database entity mapping.
///
/// Use `#[derive(Entity)]` to implement this trait.
pub trait Entity: AsEntity + Expression {
    /// Primary key type. A tuple of field types (or single type) forming the PK.
    type PrimaryKey<'a>
    where
        Self: 'a;

    /// Table reference matching the `#[tank(...)]` attributes.
    fn table() -> &'static TableRef;

    /// All column definitions in declaration order.
    fn columns() -> &'static [ColumnDef];

    /// Primary key column definitions. Empty if no PK defined.
    fn primary_key_def() -> &'static [&'static ColumnDef];

    /// Extract PK value(s) from `self`.
    fn primary_key(&self) -> Self::PrimaryKey<'_>;

    /// Build an expression matching the PK of `self`.
    fn primary_key_expr(&self) -> impl Expression;

    /// Unique constraint definitions, each a list of columns.
    fn unique_defs() -> &'static [&'static [&'static ColumnDef]];

    /// Values of all persisted columns in declaration order, borrowing from `self` where possible.
    fn row_values(&self) -> impl ExactSizeIterator<Item = Value<'_>>;

    /// Full row representation with column labels.
    fn row(&self) -> RowOwned {
        RowOwned::new(
            Self::columns()
                .iter()
                .map(|v| v.name().to_string())
                .collect(),
            self.row_values().map(Value::into_owned).collect(),
        )
    }

    /// Reconstruct `Self` from a labeled row.
    ///
    /// Fails if columns are missing or type conversion fails.
    fn from_row(row: impl Row) -> Result<Self>
    where
        Self: Sized;

    /// Create table (and optional schema).
    ///
    /// - `if_not_exists`: Emits `IF NOT EXISTS` if supported.
    /// - `create_schema`: Attempts schema creation first.
    fn create_table(
        executor: &mut impl Executor,
        if_not_exists: bool,
        create_schema: bool,
    ) -> impl Future<Output = Result<()>> + Send
    where
        Self: Sized,
    {
        async move {
            let mut query = DynQuery::with_capacity(2048);
            let writer = executor.driver().sql_writer();
            if create_schema && !Self::table().schema.is_empty() {
                writer.write_create_schema(&mut query, Self::table(), true);
            }
            if !executor.accepts_multiple_statements() && !query.is_empty() {
                let mut q = query.into_query(executor.driver());
                executor.execute(&mut q).await?;
                query = q.into();
                query.clear(); // Reuse the buffer
            }
            writer.write_create_table(&mut query, Self::table(), if_not_exists);
            for column in Self::columns().iter().filter(|c| !c.comment.is_empty()) {
                if !executor.accepts_multiple_statements() && !query.is_empty() {
                    let mut q = query.into_query(executor.driver());
                    executor.execute(&mut q).await?;
                    query = q.into();
                    query.clear(); // Reuse the buffer
                }
                writer.write_column_comment(
                    &mut Context::new(Fragment::SqlCommentOnColumn, true),
                    &mut query,
                    column,
                );
            }
            if !query.is_empty() {
                executor.execute(query).await?;
            }
            Ok(())
        }
    }

    /// Drop the table (and optional schema).
    ///
    /// - `if_exists`: Emits `IF EXISTS` if supported.
    /// - `drop_schema`: Drops schema *after* table removal (if empty).
    fn drop_table(
        executor: &mut impl Executor,
        if_exists: bool,
        drop_schema: bool,
    ) -> impl Future<Output = Result<()>> + Send
    where
        Self: Sized,
    {
        async move {
            let mut query = DynQuery::with_capacity(256);
            let writer = executor.driver().sql_writer();
            writer.write_drop_table(&mut query, Self::table(), if_exists);
            if drop_schema && !Self::table().schema.is_empty() {
                if !executor.accepts_multiple_statements() {
                    let mut q = query.into_query(executor.driver());
                    executor.execute(&mut q).await?;
                    // To reuse the allocated buffer
                    query = q.into();
                    query.clear();
                }
                writer.write_drop_schema(&mut query, Self::table(), true);
            }
            executor.execute(query).await.map(|_| ())
        }
    }

    /// Insert a single entity.
    fn insert_one<E: AsEntity>(
        executor: &mut impl Executor,
        entity: E,
    ) -> impl Future<Output = Result<RowsAffected>> + Send {
        let mut query = DynQuery::default();
        executor.driver().sql_writer().write_insert(
            &mut query,
            Sql()
                .insert_into(E::Entity::table())
                .values([entity.as_entity()]),
        );
        executor.execute(query)
    }

    /// Bulk insert entities.
    fn insert_many<It>(
        executor: &mut impl Executor,
        items: It,
    ) -> impl Future<Output = Result<RowsAffected>> + Send
    where
        Self: Sized,
        It: IntoIterator + Send,
        It::IntoIter: Send,
        It::Item: AsEntity,
    {
        executor.append(items)
    }

    /// Prepare (but do not yet run) a SQL select query.
    ///
    /// Returns the prepared statement.
    fn prepare_find<Exec: Executor>(
        executor: &mut Exec,
        condition: impl Expression,
        limit: Option<u32>,
    ) -> impl Future<Output = Result<Query<Exec::Driver>>> + Send {
        let builder = Sql()
            .select(Self::columns())
            .from(Self::table())
            .where_expr(condition)
            .limit(limit);
        let writer = executor.driver().sql_writer();
        let mut query = DynQuery::default();
        writer.write_select(&mut query, &builder);
        async {
            if let DynQuery::Raw(RawQuery(sql)) = query {
                executor.prepare(sql).await
            } else {
                Ok(query.into())
            }
        }
    }

    /// Finds the first entity matching a condition expression.
    ///
    /// Returns `Ok(None)` if no row matches.
    fn find_one(
        executor: &mut impl Executor,
        condition: impl Expression,
    ) -> impl Future<Output = Result<Option<Self>>> + Send
    where
        Self: Sized,
    {
        let stream = Self::find_many(executor, condition, Some(1));
        async move { pin!(stream).into_future().map(|(v, _)| v).await.transpose() }
    }

    /// Streams entities matching a condition.
    ///
    /// `limit` restricts the maximum number of rows returned at a database level if `Some`
    /// (if supported by the driver, unlimited otherwise).
    fn find_many(
        executor: &mut impl Executor,
        condition: impl Expression,
        limit: Option<u32>,
    ) -> impl Stream<Item = Result<Self>> + Send
    where
        Self: Sized,
    {
        let builder = Sql()
            .select(Self::columns())
            .from(Self::table())
            .where_expr(condition)
            .limit(limit);
        executor
            .fetch(builder.build(&executor.driver()))
            .map(|result| result.and_then(Self::from_row))
    }

    /// Deletes all entities matching a condition.
    ///
    /// Returns the number of deleted rows.
    fn delete_many(
        executor: &mut impl Executor,
        condition: impl Expression,
    ) -> impl Future<Output = Result<RowsAffected>> + Send
    where
        Self: Sized,
    {
        let mut query = DynQuery::with_capacity(128);
        executor
            .driver()
            .sql_writer()
            .write_delete(&mut query, Self::table(), condition);
        executor.execute(query)
    }

    /// Saves the entity (insert or update if available) based on primary key presence.
    ///
    /// Errors:
    /// - Missing PK in the table.
    /// - Execution failures from underlying driver.
    fn save(&self, executor: &mut impl Executor) -> impl Future<Output = Result<()>> + Send
    where
        Self: Sized,
        for<'s> &'s Self: AsEntity,
    {
        if Self::primary_key_def().is_empty() {
            return Either::Left(future::ready(
                Err(anyhow!(
                    "Cannot save an entity without a primary key, it would always result in an insert",
                ))
                .inspect_err(log_error),
            ));
        }
        let mut query = DynQuery::default();
        executor.driver().sql_writer().write_insert(
            &mut query,
            Sql().insert_into(Self::table()).values([self]).update(),
        );
        let sql = query.as_str();
        let context = format!("While saving using the query {}", truncate_long!(sql));
        Either::Right(executor.execute(query).map(|mut v| {
            if let Ok(result) = v
                && let Some(affected) = result.rows_affected
                && affected > 2
            {
                v = Err(anyhow!(
                    "The driver returned affected rows: {affected} (expected <= 2)"
                ));
            }
            match v {
                Ok(_) => Ok(()),
                Err(e) => Err(e.context(context)),
            }
        }))
    }

    /// Deletes this entity instance via its primary key.
    ///
    /// Errors:
    /// - Missing PK in the table.
    /// - If not exactly one row was deleted.
    /// - Execution failures from underlying driver.
    fn delete(&self, executor: &mut impl Executor) -> impl Future<Output = Result<()>> + Send
    where
        Self: Sized,
    {
        if Self::primary_key_def().is_empty() {
            return Either::Left(future::ready(
                Err(anyhow!(
                    "Cannot delete an entity without a primary key, it would delete nothing",
                ))
                .inspect_err(log_error),
            ));
        }
        Either::Right(
            Self::delete_many(executor, self.primary_key_expr()).map(|v| {
                v.and_then(|v| {
                    if let Some(affected) = v.rows_affected {
                        if affected != 1 {
                            let error = anyhow!(
                                "The query deleted {affected} rows instead of the expected 1"
                            );
                            log::log!(
                                if affected == 0 {
                                    Level::Info
                                } else {
                                    Level::Error
                                },
                                "{error}",
                            );
                            return Err(error);
                        }
                    }
                    Ok(())
                })
            }),
        )
    }
}
