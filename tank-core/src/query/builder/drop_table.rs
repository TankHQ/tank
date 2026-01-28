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

    pub fn build<D: Driver>(&self, driver: &D) -> String {
        let writer = driver.sql_writer();
        let mut query = DynQuery::default();
        writer.write_drop_table::<T>(&mut query, self.if_exists);
        query.into()
    }

    pub fn build_into<D: Driver>(&self, driver: &D, out: &mut DynQuery) {
        let writer = driver.sql_writer();
        writer.write_drop_table::<T>(out, self.if_exists);
    }
}

pub trait DropTableQuery<Table: Entity> {
    fn get_exists(&self) -> bool;
}

impl<T: Entity, E> DropTableQuery<T> for DropTableQueryBuilder<T, E> {
    fn get_exists(&self) -> bool {
        self.get_exists()
    }
}
