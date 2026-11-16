use crate::{AnyQuery, Driver, NA, Query, SqlWriter, TableRef};
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
    pub fn get_table(&self) -> &TableRef {
        &self.table
    }

    pub fn get_exists(&self) -> bool {
        self.if_exists
    }

    pub fn build<D: Driver>(&self, driver: &D) -> Query<D> {
        let mut query = AnyQuery::default();
        self.build_into(driver, &mut query);
        query.into()
    }

    pub fn build_into<D: Driver>(&self, driver: &D, out: &mut AnyQuery) {
        let writer = driver.sql_writer();
        writer.write_drop_table(out, &self.table, self.if_exists);
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
    /// Build query.
    fn build<D: Driver>(&self, driver: &D) -> Query<D>;
    /// Build query into existing buffer.
    fn build_into<D: Driver>(&self, driver: &D, out: &mut AnyQuery);
}

impl<E> DropTableQuery for DropTableQueryBuilder<E> {
    fn get_table(&self) -> &TableRef {
        self.get_table()
    }

    fn get_exists(&self) -> bool {
        self.get_exists()
    }

    fn build<D: Driver>(&self, driver: &D) -> Query<D> {
        self.build(driver)
    }

    fn build_into<D: Driver>(&self, driver: &D, out: &mut AnyQuery) {
        self.build_into(driver, out);
    }
}
