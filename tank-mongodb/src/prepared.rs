use crate::{Payload, value_to_bson};
use mongodb::bson::{Bson, Document};
use std::{
    fmt::{self, Display, Formatter, Write},
    mem,
};
use tank_core::{AsValue, Error, Prepared, Result, Value};

#[derive(Default, Debug)]
pub struct MongoDBPrepared {
    payload: Payload,
    pub(crate) count: u32,
    pub(crate) params: Vec<Value>,
    pub(crate) index: u64,
}

impl MongoDBPrepared {
    pub fn new(payload: Payload, count: u32) -> Self {
        Self {
            payload,
            count,
            params: Default::default(),
            index: Default::default(),
        }
    }
    pub fn get_payload(&self) -> &Payload {
        &self.payload
    }
    pub fn add_payload(&mut self, payload: Payload) -> Result<()> {
        self.payload.add_payload(payload)
    }
    pub fn current_bson(&mut self) -> Option<&mut Bson> {
        self.payload.current_bson_mut()
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
    pub(crate) fn take_params(&mut self) -> Result<Option<Document>> {
        self.index = 0;
        if self.count == 0 {
            Ok(None)
        } else {
            let mut doc = Document::new();
            for (i, v) in mem::take(&mut self.params).into_iter().enumerate() {
                doc.insert(format!("param_{i}"), value_to_bson(&v)?);
            }
            Ok(Some(doc))
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
    fn is_empty(&self) -> bool {
        self.payload.is_empty()
    }
}

impl Display for MongoDBPrepared {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str("MongoDBPrepared (")?;
        f.write_str(match self.payload {
            Payload::Fragment(..) => "fragment",
            Payload::FindOne(..) => "find one",
            Payload::FindMany(..) => "find",
            Payload::InsertOne(..) => "insert one",
            Payload::InsertMany(..) => "insert many",
            Payload::Upsert(..) => "upsert",
            Payload::Delete(..) => "delete",
            Payload::CreateCollection(..) => "create collection",
            Payload::DropCollection(..) => "drop collection",
            Payload::CreateDatabase(..) => "create database",
            Payload::DropDatabase(..) => "drop database",
            Payload::Aggregate(..) => "aggregate",
            Payload::Batch(..) => "batch",
        })?;
        f.write_char(')')?;
        Ok(())
    }
}
