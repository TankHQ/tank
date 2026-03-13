use crate::{DynQuery, Entity, SqlWriter};
use std::marker::PhantomData;

pub struct CreateTableQueryBuilder<Table: Entity> {
    pub(crate) if_not_exists: bool,
    pub(crate) _table: PhantomData<fn() -> Table>,
}

impl<T: Entity> CreateTableQueryBuilder<T> {
    pub fn if_not_exists(self) -> CreateTableQueryBuilder<T> {
        CreateTableQueryBuilder {
            if_not_exists: true,
            _table: self._table,
        }
    }
}

impl<T: Entity> CreateTableQueryBuilder<T> {
    pub fn get_not_exists(&self) -> bool {
        self.if_not_exists
    }

    pub fn build(&self, writer: &impl SqlWriter) -> DynQuery {
        let mut query = DynQuery::default();
        self.build_into(writer, &mut query);
        query
    }

    pub fn build_into(&self, writer: &impl SqlWriter, out: &mut DynQuery) {
        writer.write_create_table::<T>(out, self);
    }
}

pub trait CreateTableQuery<Table: Entity> {
    fn get_not_exists(&self) -> bool;

    fn build(&self, writer: &impl SqlWriter) -> DynQuery;
    fn build_into(&self, writer: &impl SqlWriter, out: &mut DynQuery);
}

impl<T: Entity> CreateTableQuery<T> for CreateTableQueryBuilder<T> {
    fn get_not_exists(&self) -> bool {
        self.get_not_exists()
    }

    fn build(&self, writer: &impl SqlWriter) -> DynQuery {
        self.build(writer)
    }

    fn build_into(&self, writer: &impl SqlWriter, out: &mut DynQuery) {
        self.build_into(writer, out)
    }
}

impl<T: Entity> CreateTableQuery<T> for bool {
    fn get_not_exists(&self) -> bool {
        *self
    }

    fn build(&self, writer: &impl SqlWriter) -> DynQuery {
        DynQuery::default()
    }

    fn build_into(&self, writer: &impl SqlWriter, out: &mut DynQuery) {
        writer.write_create_table::<T>(out, self);
    }
}
