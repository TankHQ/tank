use mongodb::{
    bson::{Bson, Document},
    options::{
        DeleteOptions, FindOneOptions, FindOptions, InsertManyOptions, InsertOneOptions,
        UpdateModifications, UpdateOptions,
    },
};
use std::fmt::{self, Display, Formatter};
use tank_core::{AsValue, Error, Prepared, QueryMetadata, Result, RowLabeled, Value};

#[derive(Default, Debug)]
pub struct FindOnePayload {
    pub(crate) matching: Bson,
    pub(crate) options: FindOneOptions,
}

#[derive(Default, Debug)]
pub struct FindPayload {
    pub(crate) matching: Bson,
    pub(crate) options: FindOptions,
}

#[derive(Default, Debug)]
pub struct InsertOnePayload {
    pub(crate) row: RowLabeled,
    pub(crate) options: InsertOneOptions,
}

#[derive(Default, Debug)]
pub struct InsertManyPayload {
    pub(crate) rows: Vec<RowLabeled>,
    pub(crate) options: InsertManyOptions,
}

#[derive(Default, Debug)]
pub struct UpsertPayload {
    pub(crate) matching: Bson,
    pub(crate) modifications: Option<UpdateModifications>,
    pub(crate) options: UpdateOptions,
}

#[derive(Default, Debug)]
pub struct DeletePayload {
    pub(crate) matching: Bson,
    pub(crate) options: DeleteOptions,
}

#[derive(Debug)]
pub enum Payload {
    Fragment(Bson),
    FindOne(FindOnePayload),
    Find(FindPayload),
    InsertOne(InsertOnePayload),
    InsertMany(InsertManyPayload),
    Upsert(UpsertPayload),
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
}

impl MongoDBPrepared {
    pub fn new() -> Self {
        Default::default()
    }
    pub fn current_bson(&mut self) -> Option<&mut Bson> {
        match &mut self.payload {
            Payload::Fragment(v) => Some(v),
            Payload::FindOne(v) => Some(&mut v.matching),
            Payload::Find(v) => Some(&mut v.matching),
            Payload::InsertOne(..) => None,
            Payload::InsertMany(..) => None,
            Payload::Upsert(v) => Some(&mut v.matching),
            Payload::Delete(v) => Some(&mut v.matching),
        }
    }
    pub fn switch_to_document(&mut self) -> Option<&mut Document> {
        self.current_bson().map(|v| {
            if !matches!(v, Bson::Document(..)) {
                *v = Bson::Document(Document::default());
            }
            let Bson::Document(document) = v else {
                unreachable!();
            };
            document
        })
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
