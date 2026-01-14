use crate::TableRef;
use std::borrow::Cow;

#[derive(Default, Clone, Debug)]
pub struct QueryMetadata {
    pub table: TableRef,
    pub limit: Option<u32>,
}

impl QueryMetadata {
    pub fn from_table(table: TableRef) -> Self {
        QueryMetadata { table, limit: None }
    }
    pub fn from_limit(limit: Option<u32>) -> Self {
        QueryMetadata {
            table: Default::default(),
            limit,
        }
    }
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
