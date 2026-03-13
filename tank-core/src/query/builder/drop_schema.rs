use crate::{DynQuery, SqlWriter};
use std::borrow::Cow;

pub struct DropSchemaQueryBuilder {
    pub(crate) schema: Cow<'static, str>,
    pub(crate) if_exists: bool,
}

impl DropSchemaQueryBuilder {
    pub fn new(schema: Cow<'static, str>) -> Self {
        Self {
            schema,
            if_exists: false,
        }
    }

    pub fn if_exists(self) -> DropSchemaQueryBuilder {
        DropSchemaQueryBuilder {
            schema: self.schema,
            if_exists: true,
        }
    }
}

impl DropSchemaQueryBuilder {
    pub fn get_schema(&self) -> &str {
        &self.schema
    }

    pub fn get_if_exists(&self) -> bool {
        self.if_exists
    }

    pub fn build(&self, writer: &impl SqlWriter) -> DynQuery {
        let mut query = DynQuery::default();
        self.build_into(writer, &mut query);
        query
    }

    pub fn build_into(&self, writer: &impl SqlWriter, out: &mut DynQuery) {
        writer.write_drop_schema(out, self);
    }
}

pub trait DropSchemaQuery {
    fn get_schema(&self) -> &str;
    fn get_if_exists(&self) -> bool;

    fn build(&self, writer: &impl SqlWriter) -> DynQuery;
    fn build_into(&self, writer: &impl SqlWriter, out: &mut DynQuery);
}

impl DropSchemaQuery for DropSchemaQueryBuilder {
    fn get_schema(&self) -> &str {
        self.get_schema()
    }

    fn get_if_exists(&self) -> bool {
        self.get_if_exists()
    }

    fn build(&self, writer: &impl SqlWriter) -> DynQuery {
        self.build(writer)
    }

    fn build_into(&self, writer: &impl SqlWriter, out: &mut DynQuery) {
        self.build_into(writer, out)
    }
}
