use crate::{
    AnyQuery, BoxQueryBuilder, ColumnDef, Driver, NA, PrimaryKeyType, Query, SqlWriter, TableRef,
};
use std::{borrow::Cow, marker::PhantomData};

pub struct CreateTableQueryBuilder<Exists, Columns = NA, PrimaryKey = NA> {
    pub(crate) table: TableRef,
    pub(crate) if_not_exists: bool,
    pub(crate) _e: PhantomData<(Exists, Columns, PrimaryKey)>,
}

impl<C, P> CreateTableQueryBuilder<NA, C, P> {
    pub fn if_not_exists(self) -> CreateTableQueryBuilder<bool, C, P> {
        CreateTableQueryBuilder {
            table: self.table,
            if_not_exists: true,
            _e: PhantomData,
        }
    }
}

impl<E> CreateTableQueryBuilder<E, NA, NA> {
    /// Specify the columns of the table by providing a list of `ColumnDef` objects
    pub fn columns(
        mut self,
        columns: impl IntoIterator<Item = ColumnDef>,
    ) -> CreateTableQueryBuilder<E, bool, NA> {
        self.table.columns = Cow::Owned(columns.into_iter().collect());
        self.table.unique = Cow::Borrowed(&[]);
        let marked = self
            .table
            .columns
            .iter()
            .enumerate()
            .filter(|(_, c)| c.primary_key != PrimaryKeyType::None)
            .map(|(i, _)| i)
            .collect();
        self.update_primary_key(marked);
        CreateTableQueryBuilder {
            table: self.table,
            if_not_exists: self.if_not_exists,
            _e: PhantomData,
        }
    }
}

impl<E> CreateTableQueryBuilder<E, bool, NA> {
    /// Set the primary key, in key order, from column names (replaces the columns marked as key).
    pub fn primary_key<Name: AsRef<str>>(
        mut self,
        names: impl IntoIterator<Item = Name>,
    ) -> CreateTableQueryBuilder<E, bool, bool> {
        let indices = self.indices(names);
        self.update_primary_key(indices);
        CreateTableQueryBuilder {
            table: self.table,
            if_not_exists: self.if_not_exists,
            _e: PhantomData,
        }
    }
}

impl<E, P> CreateTableQueryBuilder<E, bool, P> {
    /// Add a unique constraint from column names.
    pub fn unique<Name: AsRef<str>>(mut self, names: impl IntoIterator<Item = Name>) -> Self {
        let indices = self.indices(names);
        self.table.unique.to_mut().push(indices.into());
        self
    }
}

impl<E, C, P> CreateTableQueryBuilder<E, C, P> {
    pub const fn get_table(&self) -> &TableRef {
        &self.table
    }

    pub const fn get_not_exists(&self) -> bool {
        self.if_not_exists
    }

    /// Set the key indices and give the PK columns the matching role.
    fn update_primary_key(&mut self, indices: Vec<usize>) {
        let role = if indices.len() == 1 {
            PrimaryKeyType::PrimaryKey
        } else {
            PrimaryKeyType::PartOfPrimaryKey
        };
        for (i, column) in self.table.columns.to_mut().iter_mut().enumerate() {
            column.primary_key = if indices.contains(&i) {
                role
            } else {
                PrimaryKeyType::None
            };
        }
        self.table.primary_key = indices.into();
    }

    fn indices<Name: AsRef<str>>(&self, names: impl IntoIterator<Item = Name>) -> Vec<usize> {
        names
            .into_iter()
            .filter_map(|name| {
                let name = name.as_ref();
                let index = self.table.columns.iter().position(|c| c.name() == name);
                if index.is_none() {
                    log::error!("Column `{name}` is not defined in the table");
                }
                index
            })
            .collect()
    }
}

/// CREATE TABLE [IF NOT EXISTS] table (columns, [PRIMARY KEY (columns)], [UNIQUE (columns)], ...)
///
/// Use `Sql().create_table(Entity::table())` to start, the table metadata carries the columns and constraints.
pub trait CreateTableQuery {
    /// Get the table metadata.
    fn get_table(&self) -> &TableRef;
    /// SQL Fragment `IF NOT EXISTS` is present.
    fn get_not_exists(&self) -> bool;
}

impl<E, C, P> CreateTableQuery for CreateTableQueryBuilder<E, C, P> {
    fn get_table(&self) -> &TableRef {
        self.get_table()
    }

    fn get_not_exists(&self) -> bool {
        self.get_not_exists()
    }
}

impl<E, C, P> CreateTableQueryBuilder<E, C, P> {
    /// Builds the query for `driver`.
    pub fn build<D: Driver>(&self, driver: &D) -> Query<D> {
        let mut query = AnyQuery::default();
        self.write_into(&driver.sql_writer(), &mut query);
        query.into()
    }

    /// Writes the statement into an existing query.
    pub fn write_into(&self, writer: &impl SqlWriter, out: &mut AnyQuery) {
        writer.write_create_table(out, &self.table, self.if_not_exists);
    }

    /// Erases the builder into a [`BoxQueryBuilder`], to store it and run it later on any executor.
    pub fn boxed(self) -> BoxQueryBuilder
    where
        Self: 'static,
    {
        BoxQueryBuilder::new(move |writer, out| self.write_into(writer, out))
    }
}
