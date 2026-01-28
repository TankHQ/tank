use crate::RowWrap;
use mongodb::{
    Namespace,
    bson::{Bson, Document},
    options::{
        BulkWriteOptions, DeleteManyModel, DeleteOptions, FindOneOptions, FindOptions,
        InsertManyOptions, InsertOneModel, InsertOneOptions, UpdateModifications, UpdateOneModel,
        UpdateOptions, WriteModel,
    },
};
use std::borrow::Cow;
use tank_core::{Error, Result, RowLabeled, truncate_long};

#[derive(Default, Debug)]
pub struct FindOnePayload {
    pub(crate) filter: Bson,
    pub(crate) options: FindOneOptions,
}

#[derive(Default, Debug)]
pub struct FindManyPayload {
    pub(crate) filter: Bson,
    pub(crate) options: FindOptions,
}

#[derive(Debug)]
pub struct InsertOnePayload {
    pub(crate) namespace: Namespace,
    pub(crate) row: RowLabeled,
    pub(crate) options: InsertOneOptions,
}
impl Default for InsertOnePayload {
    fn default() -> Self {
        Self {
            namespace: Namespace::new("", ""),
            row: Default::default(),
            options: Default::default(),
        }
    }
}

#[derive(Debug)]
pub struct InsertManyPayload {
    pub(crate) namespace: Namespace,
    pub(crate) rows: Vec<RowLabeled>,
    pub(crate) options: InsertManyOptions,
}
impl Default for InsertManyPayload {
    fn default() -> Self {
        Self {
            namespace: Namespace::new("", ""),
            rows: Default::default(),
            options: Default::default(),
        }
    }
}

#[derive(Debug)]
pub struct UpsertPayload {
    pub(crate) namespace: Namespace,
    pub(crate) filter: Bson,
    pub(crate) modifications: UpdateModifications,
    pub(crate) options: UpdateOptions,
}
impl Default for UpsertPayload {
    fn default() -> Self {
        Self {
            namespace: Namespace::new("", ""),
            filter: Default::default(),
            modifications: UpdateModifications::Document(Document::default()),
            options: Default::default(),
        }
    }
}

#[derive(Debug)]
pub struct DeletePayload {
    pub(crate) namespace: Namespace,
    pub(crate) filter: Bson,
    pub(crate) options: DeleteOptions,
}
impl Default for DeletePayload {
    fn default() -> Self {
        Self {
            namespace: Namespace::new("", ""),
            filter: Default::default(),
            options: Default::default(),
        }
    }
}

#[derive(Default, Debug)]
pub struct BatchPayload {
    pub(crate) batch: Vec<Payload>,
    pub(crate) options: BulkWriteOptions,
}

#[derive(Debug)]
pub enum Payload {
    Fragment(Bson),
    FindOne(FindOnePayload),
    FindMany(FindManyPayload),
    InsertOne(InsertOnePayload),
    InsertMany(InsertManyPayload),
    Upsert(UpsertPayload),
    Delete(DeletePayload),
    Batch(BatchPayload),
}
impl Payload {
    pub fn current_bson(&mut self) -> Option<&mut Bson> {
        match self {
            Payload::Fragment(v) => Some(v),
            Payload::FindOne(v) => Some(&mut v.filter),
            Payload::FindMany(v) => Some(&mut v.filter),
            Payload::InsertOne(..) => None,
            Payload::InsertMany(..) => None,
            Payload::Upsert(v) => Some(&mut v.filter),
            Payload::Delete(v) => Some(&mut v.filter),
            Payload::Batch(BatchPayload { batch, .. }) => {
                batch.last_mut().and_then(Payload::current_bson)
            }
        }
    }
    pub fn add_payload(&mut self, payload: Payload) -> Result<()> {
        match self {
            Payload::Fragment(..) => *self = payload,
            Payload::Batch(BatchPayload { batch, .. }) => match payload {
                Payload::Fragment(..)
                | Payload::InsertOne(..)
                | Payload::InsertMany(..)
                | Payload::Upsert(..)
                | Payload::Delete(..) => {
                    batch.push(payload);
                }
                Payload::Batch(BatchPayload {
                    batch: payloads, ..
                }) => {
                    for payload in payloads {
                        batch.push(payload);
                    }
                }
                _ => {
                    return Err(Error::msg(format!(
                        "Cannot add into a batch {}",
                        truncate_long!(format!("{payload:?}"), true)
                    )));
                }
            },
            _ => {
                *self = Payload::Batch(Default::default());
                return self.add_payload(payload);
            }
        }
        Ok(())
    }
    pub fn as_write_models(&self) -> Option<WriteModel> {
        match self {
            Payload::Fragment(..) => None,
            Payload::FindOne(..) => None,
            Payload::FindMany(..) => None,
            Payload::InsertOne(payload) => {
                let Some(document): Option<Document> =
                    RowWrap(Cow::Borrowed(&payload.row)).try_into().ok()
                else {
                    return None;
                };
                Some(
                    InsertOneModel::builder()
                        .namespace(payload.namespace.clone())
                        .document(document)
                        .build()
                        .into(),
                )
            }
            Payload::InsertMany(payload) => None,
            Payload::Upsert(payload) => {
                let Bson::Document(filter) = &payload.filter else {
                    return None;
                };
                Some(
                    UpdateOneModel::builder()
                        .namespace(payload.namespace.clone())
                        .filter(filter.clone())
                        .update(payload.modifications.clone())
                        .upsert(true)
                        .build()
                        .into(),
                )
            }
            Payload::Delete(payload) => {
                let Bson::Document(filter) = &payload.filter else {
                    return None;
                };
                Some(
                    DeleteManyModel::builder()
                        .namespace(payload.namespace.clone())
                        .filter(filter.clone())
                        .build()
                        .into(),
                )
            }
            Payload::Batch(..) => None,
        }
    }
}
impl Default for Payload {
    fn default() -> Self {
        Self::Fragment(Bson::Document(Default::default()))
    }
}
