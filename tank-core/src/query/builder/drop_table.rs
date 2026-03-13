use crate::{DynQuery, Entity, SqlWriter};
use std::marker::PhantomData;

pub struct DropTableQueryBuilder<Table: Entity> {
    pub(crate) if_exists: bool,
    pub(crate) _table: PhantomData<fn() -> Table>,
}

impl<T: Entity> DropTableQueryBuilder<T> {
    pub fn if_exists(self) -> DropTableQueryBuilder<T> {
        DropTableQueryBuilder {
            if_exists: true,
            _table: self._table,
        }
    }
}

impl<T: Entity> DropTableQueryBuilder<T> {
    pub fn get_exists(&self) -> bool {
        self.if_exists
    }

    pub fn build(&self, writer: &impl SqlWriter) -> DynQuery {
        let mut query = DynQuery::default();
        self.build_into(writer, &mut query);
        query
    }

    pub fn build_into(&self, writer: &impl SqlWriter, out: &mut DynQuery) {
        writer.write_drop_table::<T>(out, self);
    }
}

pub trait DropTableQuery<Table: Entity> {
    fn get_exists(&self) -> bool;

    fn build(&self, writer: &impl SqlWriter) -> DynQuery;
    fn build_into(&self, writer: &impl SqlWriter, out: &mut DynQuery);
}

impl<T: Entity> DropTableQuery<T> for DropTableQueryBuilder<T> {
    fn get_exists(&self) -> bool {
        self.get_exists()
    }

    fn build(&self, writer: &impl SqlWriter) -> DynQuery {
        self.build(writer)
    }

    fn build_into(&self, writer: &impl SqlWriter, out: &mut DynQuery) {
        self.build_into(writer, out)
    }
}
