use mongodb::{
    bson::Document,
    options::{DeleteOptions, FindOneOptions, FindOptions, InsertManyOptions, InsertOneOptions},
};
use std::fmt::{self, Display, Formatter};
use tank_core::{AsValue, Error, Prepared, QueryMetadata, Result, RowLabeled, Value};

#[derive(Default, Debug)]
pub struct FindOnePayload {
    pub(crate) matching: Document,
    pub(crate) options: FindOneOptions,
}

#[derive(Default, Debug)]
pub struct FindPayload {
    pub(crate) matching: Document,
    pub(crate) options: FindOptions,
}

#[derive(Debug)]
pub struct InsertOnePayload {
    pub(crate) row: RowLabeled,
    pub(crate) options: InsertOneOptions,
}

#[derive(Debug)]
pub struct InsertManyPayload {
    pub(crate) rows: Vec<RowLabeled>,
    pub(crate) options: InsertManyOptions,
}

#[derive(Default, Debug)]
pub struct DeletePayload {
    pub(crate) matching: Document,
    pub(crate) options: DeleteOptions,
}

#[derive(Debug)]
pub enum Payload {
    Fragment(Document),
    FindOne(FindOnePayload),
    Find(FindPayload),
    InsertOne(InsertOnePayload),
    InsertMany(InsertManyPayload),
    Delete(DeletePayload),
}

impl Default for Payload {
    fn default() -> Self {
        Self::Fragment(Default::default())
    }
}

#[derive(Default, Debug)]
pub struct MongoDBPrepared {
    pub(crate) payload: Payload,
    pub(crate) params: Vec<Value>,
    pub(crate) index: u64,
    pub(crate) metadata: QueryMetadata,
    pub(crate) current: Document,
}

impl MongoDBPrepared {
    pub fn new() -> Self {
        Default::default()
    }
    pub fn current_document(&mut self) -> Option<&mut Document> {
        match &mut self.payload {
            Payload::Fragment(v) => Some(v),
            Payload::FindOne(v) => Some(&mut v.matching),
            Payload::Find(v) => Some(&mut v.matching),
            Payload::InsertOne(..) => None,
            Payload::InsertMany(..) => None,
            Payload::Delete(v) => Some(&mut v.matching),
        }
    }
}

impl Prepared for MongoDBPrepared {
    fn as_any(self: Box<Self>) -> Box<dyn std::any::Any> {
        self
    }

    fn clear_bindings(&mut self) -> Result<&mut Self> {
        self.params.clear();
        self.index = 0;
        Ok(self)
    }

    fn bind(&mut self, value: impl AsValue) -> Result<&mut Self> {
        self.bind_index(value, self.index)
    }

    fn bind_index(&mut self, value: impl AsValue, index: u64) -> Result<&mut Self> {
        if index as usize >= self.params.len() {
            self.params.resize_with((index + 1) as _, Default::default);
        }
        let target = self
            .params
            .get_mut(index as usize)
            .ok_or(Error::msg(format!("Index {index} cannot be bound")))?;
        *target = value.as_value();
        self.index = index + 1;
        Ok(self)
    }

    fn metadata(&self) -> &QueryMetadata {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut QueryMetadata {
        &mut self.metadata
    }
}

impl Display for MongoDBPrepared {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str("MongoDBPrepared")
    }
}
