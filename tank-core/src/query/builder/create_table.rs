use crate::{Driver, DynQuery, NA, SqlWriter, TableRef};
use std::marker::PhantomData;

pub struct CreateTableQueryBuilder<Exists> {
    pub(crate) table: TableRef,
    pub(crate) if_not_exists: bool,
    pub(crate) _e: PhantomData<Exists>,
}

impl CreateTableQueryBuilder<NA> {
    pub fn if_not_exists(self) -> CreateTableQueryBuilder<bool> {
        CreateTableQueryBuilder {
            table: self.table,
            if_not_exists: true,
            _e: Default::default(),
        }
    }
}

impl<E> CreateTableQueryBuilder<E> {
    pub fn get_table(&self) -> &TableRef {
        &self.table
    }

    pub fn get_not_exists(&self) -> bool {
        self.if_not_exists
    }

    pub fn build<D: Driver>(&self, driver: &D) -> DynQuery {
        let mut query = DynQuery::default();
        self.build_into(driver, &mut query);
        query
    }

    pub fn build_into<D: Driver>(&self, driver: &D, out: &mut DynQuery) {
        let writer = driver.sql_writer();
        writer.write_create_table(out, &self.table, self.if_not_exists);
    }
}

/// CREATE TABLE query builder.
///
/// Use `Sql.create_table(Entity::table())` to start, the table metadata carries the columns and constraints.
pub trait CreateTableQuery {
    /// Get the table metadata.
    fn get_table(&self) -> &TableRef;
    /// SQL Fragment `IF NOT EXISTS` is present.
    fn get_not_exists(&self) -> bool;
    /// Build query.
    fn build<D: Driver>(&self, driver: &D) -> DynQuery;
    /// Build query into existing buffer.
    fn build_into<D: Driver>(&self, driver: &D, out: &mut DynQuery);
}

impl<E> CreateTableQuery for CreateTableQueryBuilder<E> {
    fn get_table(&self) -> &TableRef {
        self.get_table()
    }

    fn get_not_exists(&self) -> bool {
        self.get_not_exists()
    }

    fn build<D: Driver>(&self, driver: &D) -> DynQuery {
        self.build(driver)
    }

    fn build_into<D: Driver>(&self, driver: &D, out: &mut DynQuery) {
        self.build_into(driver, out);
    }
}
