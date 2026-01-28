use crate::{Driver, DynQuery, Entity, NA, SqlWriter};
use std::marker::PhantomData;

pub struct CreateTableQueryBuilder<Table: Entity, Exists> {
    pub(crate) if_not_exists: bool,
    pub(crate) _table: PhantomData<Table>,
    pub(crate) _e: PhantomData<Exists>,
}

impl<T: Entity> CreateTableQueryBuilder<T, NA> {
    pub fn if_not_exists(self) -> CreateTableQueryBuilder<T, bool> {
        CreateTableQueryBuilder {
            if_not_exists: true,
            _table: self._table,
            _e: Default::default(),
        }
    }
}

impl<T: Entity, E> CreateTableQueryBuilder<T, E> {
    pub fn get_not_exists(&self) -> bool {
        self.if_not_exists
    }

    pub fn build<D: Driver>(&self, driver: &D) -> String {
        let writer = driver.sql_writer();
        let mut query = DynQuery::default();
        writer.write_create_table::<T>(&mut query, self.if_not_exists);
        query.into()
    }

    pub fn build_into<D: Driver>(&self, driver: &D, out: &mut DynQuery) {
        let writer = driver.sql_writer();
        writer.write_create_table::<T>(out, self.if_not_exists);
    }
}

pub trait CreateTableQuery<Table: Entity> {
    fn get_not_exists(&self) -> bool;
}

impl<T: Entity, E> CreateTableQuery<T> for CreateTableQueryBuilder<T, E> {
    fn get_not_exists(&self) -> bool {
        self.get_not_exists()
    }
}
