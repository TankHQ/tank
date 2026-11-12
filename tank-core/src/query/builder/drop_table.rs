use crate::{Driver, DynQuery, Entity, NA, SqlWriter};
use std::marker::PhantomData;

pub struct DropTableQueryBuilder<Table: Entity, Exists> {
    pub(crate) if_exists: bool,
    pub(crate) _table: PhantomData<Table>,
    pub(crate) _e: PhantomData<Exists>,
}

impl<T: Entity> DropTableQueryBuilder<T, NA> {
    pub fn if_exists(self) -> DropTableQueryBuilder<T, bool> {
        DropTableQueryBuilder {
            if_exists: true,
            _table: self._table,
            _e: Default::default(),
        }
    }
}

impl<T: Entity, E> DropTableQueryBuilder<T, E> {
    pub fn get_exists(&self) -> bool {
        self.if_exists
    }

    pub fn build<D: Driver>(&self, driver: &D) -> DynQuery {
        let writer = driver.sql_writer();
        let mut query = DynQuery::default();
        writer.write_drop_table::<T>(&mut query, self.if_exists);
        query
    }

    pub fn build_into<D: Driver>(&self, driver: &D, out: &mut DynQuery) {
        let writer = driver.sql_writer();
        writer.write_drop_table::<T>(out, self.if_exists);
    }
}

/// DROP TABLE query builder.
///
/// Use `QueryBuilder::new().drop_table::<Entity>()` to start.
pub trait DropTableQuery<Table: Entity> {
    fn get_exists(&self) -> bool;
    /// Build query.
    fn build<D: Driver>(&self, driver: &D) -> DynQuery;
    /// Build query into existing buffer.
    fn build_into<D: Driver>(&self, driver: &D, out: &mut DynQuery);
}

impl<T: Entity, E> DropTableQuery<T> for DropTableQueryBuilder<T, E> {
    fn get_exists(&self) -> bool {
        self.get_exists()
    }

    fn build<D: Driver>(&self, driver: &D) -> DynQuery {
        self.build(driver)
    }

    fn build_into<D: Driver>(&self, driver: &D, out: &mut DynQuery) {
        self.build_into(driver, out);
    }
}
