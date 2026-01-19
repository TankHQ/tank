use mongodb::{
    bson::Document,
    options::{FindOneOptions, FindOptions, InsertManyOptions, InsertOneOptions},
};
use std::fmt::{self, Display, Formatter};
use tank_core::{AsValue, Error, Prepared, QueryMetadata, Result, Value};

#[derive(Debug)]
pub enum Options {
    Find(FindOptions),
    FindOne(FindOneOptions),
    InsertMany(InsertManyOptions),
    InsertOne(InsertOneOptions),
}

impl Default for Options {
    fn default() -> Self {
        Options::Find(Default::default())
    }
}

#[derive(Default, Debug)]
pub struct FindPayload {
    pub(crate) find: Document,
    pub(crate) options: Options,
}

#[derive(Debug)]
pub struct InsertPayload {
    pub(crate) documents: Vec<Document>,
    pub(crate) options: Options,
}

#[derive(Debug)]
pub enum Payload {
    Find(FindPayload),
    Insert(InsertPayload),
}

impl Default for Payload {
    fn default() -> Self {
        Payload::Find(Default::default())
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
