use crate::{AnyQuery, Driver, NA, QueryBuilder, SqlWriter, TableRef};
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

impl<E> QueryBuilder for DropTableQueryBuilder<E> {
    fn build_into<D: Driver>(&self, driver: &D, out: &mut AnyQuery) {
        driver
            .sql_writer()
            .write_drop_table(out, &self.table, self.if_exists);
    }
}
