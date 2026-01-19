use crate::TableRef;
use std::borrow::Cow;

#[derive(Clone, Copy, Debug)]
pub enum QueryType {
    Select,
    InsertInto,
    DeleteFrom,
    CreateTable,
    DropTable,
    CreateSchema,
    DropSchema,
}

#[derive(Default, Clone, Debug)]
pub struct QueryMetadata {
    pub table: TableRef,
    pub limit: Option<u32>,
    pub query_type: Option<QueryType>,
}

impl<'s> From<QueryMetadata> for Cow<'s, QueryMetadata> {
    fn from(value: QueryMetadata) -> Self {
        Cow::Owned(value)
    }
}

impl<'s> From<&'s QueryMetadata> for Cow<'s, QueryMetadata> {
    fn from(value: &'s QueryMetadata) -> Self {
        Cow::Borrowed(value)
    }
}
