use crate::{
    ColumnDef, Context, DataSet, Driver, Error, Executor, Expression, Query, Result, Row,
    RowLabeled, RowsAffected, TableRef, Value, future::Either, stream::Stream, writer::SqlWriter,
};
use futures::{FutureExt, StreamExt, TryFutureExt};
use log::Level;
use std::{
    future::{self, Future},
    pin::pin,
};

/// A table-mapped record with schema and CRUD helpers.
pub trait Entity {
    /// Primary key type. Tuple of the types of the fields forming the primary key.
    type PrimaryKey<'a>;

    /// Returns the table reference backing this entity.
    fn table() -> &'static TableRef;

    /// Returns all declared column definitions in declaration order.
    fn columns() -> &'static [ColumnDef];

    /// Iterator over columns forming the primary key. Empty iterator means no PK.
    fn primary_key_def() -> impl ExactSizeIterator<Item = &'static ColumnDef>;

    /// Extracts the primary key value(s) from `self`.
    fn primary_key(&self) -> Self::PrimaryKey<'_>;

    /// Returns an iterator over unique constraint definitions.
    fn unique_defs()
    -> impl ExactSizeIterator<Item = impl ExactSizeIterator<Item = &'static ColumnDef>>;

    /// Returns a filtered mapping of column name to value, typically excluding
    /// auto-generated or default-only columns.
    fn row_filtered(&self) -> Box<[(&'static str, Value)]>;

    /// Returns a full `Row` representation including all persisted columns.
    fn row_full(&self) -> Row;

    /// Constructs `Self` from a labeled database row.
    ///
    /// Error if mandatory columns are missing or type conversion fails.
    fn from_row(row: RowLabeled) -> Result<Self>
    where
        Self: Sized;

    /// Creates the underlying table (and optionally schema) if requested.
    ///
    /// Parameters:
    /// - `if_not_exists`: guards against existing table (if drivers support it, otherwise just create table).
    /// - `create_schema`: attempt to create schema prior to table creation (if drivers support it).
    fn create_table(
        executor: &mut impl Executor,
        if_not_exists: bool,
        create_schema: bool,
    ) -> impl Future<Output = Result<()>> + Send
    where
        Self: Sized,
    {
        let mut query = String::with_capacity(2048);
        let writer = executor.driver().sql_writer();
        if create_schema && !Self::table().schema().is_empty() {
            writer.write_create_schema::<Self>(&mut query, true);
        }
        writer.write_create_table::<Self>(&mut query, if_not_exists);
        // TODO: Remove boxed() once https://github.com/rust-lang/rust/issues/100013 is fixed
        async { executor.execute(query).boxed().await.map(|_| ()) }
    }

    /// Drops the underlying table (and optionally schema) if requested.
    ///
    /// Parameters:
    /// - `if_exists`: guards against missing table (if drivers support it, otherwise just drop table).
    /// - `drop_schema`: attempt to drop schema after table removal (if drivers support it).
    fn drop_table(
        executor: &mut impl Executor,
        if_exists: bool,
        drop_schema: bool,
    ) -> impl Future<Output = Result<()>> + Send
    where
        Self: Sized,
    {
        let mut query = String::with_capacity(256);
        let writer = executor.driver().sql_writer();
        writer.write_drop_table::<Self>(&mut query, if_exists);
        if drop_schema && !Self::table().schema().is_empty() {
            writer.write_drop_schema::<Self>(&mut query, true);
        }
        // TODO: Remove boxed() once https://github.com/rust-lang/rust/issues/100013 is fixed
        async { executor.execute(query).boxed().await.map(|_| ()) }
    }

    /// Inserts a single entity row.
    ///
    /// Returns rows affected (expected: 1 on success).
    fn insert_one(
        executor: &mut impl Executor,
        entity: &impl Entity,
    ) -> impl Future<Output = Result<RowsAffected>> + Send {
        let mut query = String::with_capacity(128);
        executor
            .driver()
            .sql_writer()
            .write_insert(&mut query, [entity], false);
        executor.execute(query)
    }

    /// Multiple insert for a homogeneous iterator of entities.
    ///
    /// Returns the number of rows inserted.
    fn insert_many<'a, It>(
        executor: &mut impl Executor,
        items: It,
    ) -> impl Future<Output = Result<RowsAffected>> + Send
    where
        Self: 'a + Sized,
        It: IntoIterator<Item = &'a Self> + Send,
    {
        executor.append(items)
    }

    /// Prepare (but do not yet run) a SQL select query.
    ///
    /// Returns the prepared statement.
    fn prepare_find<Exec: Executor>(
        executor: &mut Exec,
        condition: &impl Expression,
        limit: Option<u32>,
    ) -> impl Future<Output = Result<Query<Exec::Driver>>> {
        Self::table().prepare(executor, Self::columns(), condition, limit)
    }

    /// Finds an entity by primary key.
    ///
    /// Returns `Ok(None)` if no row matches.
    fn find_pk(
        executor: &mut impl Executor,
        primary_key: &Self::PrimaryKey<'_>,
    ) -> impl Future<Output = Result<Option<Self>>> + Send
    where
        Self: Sized;

    /// Finds the first entity matching a condition expression.
    ///
    /// Returns `Ok(None)` if no row matches.
    fn find_one(
        executor: &mut impl Executor,
        condition: &impl Expression,
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
        Self::table()
            .select(
                executor,
                Self::columns()
                    .iter()
                    .map(|c| &c.column_ref as &dyn Expression),
                condition,
                limit,
            )
            .map(|result| result.and_then(Self::from_row))
    }

    /// Deletes exactly one entity by primary key.
    ///
    /// Returns rows affected (0 if not found).
    fn delete_one(
        executor: &mut impl Executor,
        primary_key: Self::PrimaryKey<'_>,
    ) -> impl Future<Output = Result<RowsAffected>> + Send
    where
        Self: Sized;

    /// Deletes all entities matching a condition.
    ///
    /// Returns the number of rows deleted.
    fn delete_many(
        executor: &mut impl Executor,
        condition: &impl Expression,
    ) -> impl Future<Output = Result<RowsAffected>> + Send
    where
        Self: Sized,
    {
        let mut query = String::with_capacity(128);
        executor
            .driver()
            .sql_writer()
            .write_delete::<Self>(&mut query, condition);
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
    {
        if Self::primary_key_def().len() == 0 {
            let error = Error::msg(
                "Cannot save a entity without a primary key, it would always result in a insert",
            );
            log::error!("{:#}", error);
            return Either::Left(future::ready(Err(error)));
        }
        let mut query = String::with_capacity(512);
        executor
            .driver()
            .sql_writer()
            .write_insert(&mut query, [self], true);
        Either::Right(executor.execute(query).map_ok(|_| ()))
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
        if Self::primary_key_def().len() == 0 {
            let error =
                Error::msg("Cannot delete a entity without a primary key, it would delete nothing");
            log::error!("{:#}", error);
            return Either::Left(future::ready(Err(error)));
        }
        Either::Right(Self::delete_one(executor, self.primary_key()).map(|v| {
            v.and_then(|v| {
                if let Some(affected) = v.rows_affected {
                    if affected != 1 {
                        let error = Error::msg(format!(
                            "The query deleted {affected} rows instead of the expected 1"
                        ));
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
        }))
    }
}

impl<E: Entity> DataSet for E {
    /// Indicates whether column names should be fully qualified with schema and table name.
    ///
    /// For entities this returns `false` to keep queries concise, for joins it returns `true`.
    fn qualified_columns() -> bool
    where
        Self: Sized,
    {
        false
    }

    /// Writes the table reference into the out string.
    fn write_query(&self, writer: &dyn SqlWriter, context: &mut Context, out: &mut String) {
        Self::table().write_query(writer, context, out);
    }
}
