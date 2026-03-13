use crate::{DynQuery, SqlWriter};
use std::borrow::Cow;

pub struct CreateSchemaQueryBuilder {
    pub(crate) schema: Cow<'static, str>,
    pub(crate) if_not_exists: bool,
}

impl CreateSchemaQueryBuilder {
    pub fn new(schema: Cow<'static, str>) -> Self {
        Self {
            schema,
            if_not_exists: false,
        }
    }

    pub fn if_not_exists(self) -> CreateSchemaQueryBuilder {
        CreateSchemaQueryBuilder {
            schema: self.schema,
            if_not_exists: true,
        }
    }
}

impl CreateSchemaQueryBuilder {
    pub fn get_schema(&self) -> &str {
        &self.schema
    }

    pub fn get_if_not_exists(&self) -> bool {
        self.if_not_exists
    }

    pub fn build(&self, writer: &impl SqlWriter) -> DynQuery {
        let mut query = DynQuery::default();
        self.build_into(writer, &mut query);
        query
    }

    pub fn build_into(&self, writer: &impl SqlWriter, out: &mut DynQuery) {
        writer.write_create_schema(out, self);
    }
}

pub trait CreateSchemaQuery {
    fn get_schema(&self) -> &str;
    fn get_if_not_exists(&self) -> bool;

    fn build(&self, writer: &impl SqlWriter) -> DynQuery;
    fn build_into(&self, writer: &impl SqlWriter, out: &mut DynQuery);
}

impl CreateSchemaQuery for CreateSchemaQueryBuilder {
    fn get_schema(&self) -> &str {
        self.get_schema()
    }

    fn get_if_not_exists(&self) -> bool {
        self.get_if_not_exists()
    }

    fn build(&self, writer: &impl SqlWriter) -> DynQuery {
        self.build(writer)
    }

    fn build_into(&self, writer: &impl SqlWriter, out: &mut DynQuery) {
        self.build_into(writer, out)
    }
}
