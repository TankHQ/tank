use crate::{AnyQuery, BoxQueryBuilder, Driver, NA, Query, SqlWriter, TableRef};
use std::marker::PhantomData;

pub struct DropTableQueryBuilder<Exists> {
    pub(crate) table: TableRef,
    pub(crate) if_exists: bool,
    pub(crate) _e: PhantomData<Exists>,
}

impl DropTableQueryBuilder<NA> {
    pub fn if_exists(self) -> DropTableQueryBuilder<bool> {
        DropTableQueryBuilder {
            table: self.table,
            if_exists: true,
            _e: Default::default(),
        }
    }
}

impl<E> DropTableQueryBuilder<E> {
    pub const fn get_table(&self) -> &TableRef {
        &self.table
    }

    pub const fn get_exists(&self) -> bool {
        self.if_exists
    }
}

/// DROP TABLE [IF EXISTS] table
///
/// Use `Sql().drop_table(table)` to start.
pub trait DropTableQuery {
    /// Get the table metadata.
    fn get_table(&self) -> &TableRef;
    /// SQL Fragment `IF EXISTS` is present.
    fn get_exists(&self) -> bool;
}

impl<E> DropTableQuery for DropTableQueryBuilder<E> {
    fn get_table(&self) -> &TableRef {
        self.get_table()
    }

    fn get_exists(&self) -> bool {
        self.get_exists()
    }
}

impl<E> DropTableQueryBuilder<E> {
    /// Builds the query for `driver`.
    pub fn build<D: Driver>(&self, driver: &D) -> Query<D> {
        let mut query = AnyQuery::default();
        self.write_into(&driver.sql_writer(), &mut query);
        query.into()
    }

    /// Writes the statement into an existing query.
    pub fn write_into(&self, writer: &impl SqlWriter, out: &mut AnyQuery) {
        writer.write_drop_table(out, &self.table, self.if_exists);
    }

    /// Erases the builder into a [`BoxQueryBuilder`], to store it and run it later on any executor.
    pub fn boxed(self) -> BoxQueryBuilder
    where
        Self: 'static,
    {
        BoxQueryBuilder::new(move |writer, out| self.write_into(writer, out))
    }
}
