use crate::{RowWrap, bson_is_empty};
use mongodb::{
    Namespace,
    bson::{Bson, Document},
    options::{
        AggregateOptions, BulkWriteOptions, CreateCollectionOptions, DeleteManyModel,
        DeleteOptions, FindOneOptions, FindOptions, InsertManyOptions, InsertOneModel,
        InsertOneOptions, UpdateModifications, UpdateOneModel, UpdateOptions, WriteModel,
    },
};
use std::borrow::Cow;
use tank_core::{Error, Result, RowLabeled, TableRef, truncate_long};

#[derive(Default, Debug)]
pub struct FindOnePayload {
    pub(crate) table: TableRef,
    pub(crate) filter: Bson,
    pub(crate) options: FindOneOptions,
}

#[derive(Default, Debug)]
pub struct FindManyPayload {
    pub(crate) table: TableRef,
    pub(crate) filter: Bson,
    pub(crate) options: FindOptions,
}

#[derive(Debug)]
pub struct InsertOnePayload {
    pub(crate) table: TableRef,
    pub(crate) row: RowLabeled,
    pub(crate) options: InsertOneOptions,
}

#[derive(Debug)]
pub struct InsertManyPayload {
    pub(crate) table: TableRef,
    pub(crate) rows: Vec<RowLabeled>,
    pub(crate) options: InsertManyOptions,
}

#[derive(Debug)]
pub struct UpsertPayload {
    pub(crate) table: TableRef,
    pub(crate) filter: Bson,
    pub(crate) modifications: UpdateModifications,
    pub(crate) options: UpdateOptions,
}

#[derive(Default, Debug)]
pub struct DeletePayload {
    pub(crate) table: TableRef,
    pub(crate) filter: Bson,
    pub(crate) options: DeleteOptions,
    pub(crate) single: bool,
}

#[derive(Default, Debug)]
pub struct CreateCollectionPayload {
    pub(crate) table: TableRef,
    pub(crate) options: CreateCollectionOptions,
}

#[derive(Default, Debug)]
pub struct DropCollectionPayload {
    pub(crate) table: TableRef,
}

#[derive(Default, Debug)]
pub struct CreateDatabasePayload {
    pub(crate) table: TableRef,
}

#[derive(Default, Debug)]
pub struct DropDatabasePayload {
    pub(crate) table: TableRef,
}

#[derive(Default, Debug)]
pub struct AggregatePayload {
    pub(crate) table: TableRef,
    pub(crate) pipeline: Bson,
    pub(crate) options: AggregateOptions,
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
    CreateCollection(CreateCollectionPayload),
    DropCollection(DropCollectionPayload),
    CreateDatabase(CreateDatabasePayload),
    DropDatabase(DropDatabasePayload),
    Aggregate(AggregatePayload),
    Batch(BatchPayload),
}
impl Payload {
    pub fn namespace(&self) -> Namespace {
        let table = match self {
            Payload::Fragment(..) => return Namespace::new("", ""),
            Payload::FindOne(payload) => &payload.table,
            Payload::FindMany(payload) => &payload.table,
            Payload::InsertOne(payload) => &payload.table,
            Payload::InsertMany(payload) => &payload.table,
            Payload::Upsert(payload) => &payload.table,
            Payload::Delete(payload) => &payload.table,
            Payload::CreateCollection(payload) => &payload.table,
            Payload::DropCollection(payload) => &payload.table,
            Payload::CreateDatabase(payload) => &payload.table,
            Payload::DropDatabase(payload) => &payload.table,
            Payload::Aggregate(payload) => &payload.table,
            Payload::Batch(..) => return Namespace::new("", ""),
        };
        Namespace::new(table.schema.to_string(), table.name.to_string())
    }
    pub fn current_bson(&self) -> Option<&Bson> {
        match self {
            Payload::Fragment(v) => Some(v),
            Payload::FindOne(v) => Some(&v.filter),
            Payload::FindMany(v) => Some(&v.filter),
            Payload::InsertOne(..) => None,
            Payload::InsertMany(..) => None,
            Payload::Upsert(v) => Some(&v.filter),
            Payload::Delete(v) => Some(&v.filter),
            Payload::CreateCollection(..) => None,
            Payload::DropCollection(..) => None,
            Payload::CreateDatabase(..) => None,
            Payload::DropDatabase(..) => None,
            Payload::Aggregate(v) => Some(&v.pipeline),
            Payload::Batch(BatchPayload { batch, .. }) => {
                batch.last().and_then(Payload::current_bson)
            }
        }
    }
    pub fn current_bson_mut(&mut self) -> Option<&mut Bson> {
        match self {
            Payload::Fragment(v) => Some(v),
            Payload::FindOne(v) => Some(&mut v.filter),
            Payload::FindMany(v) => Some(&mut v.filter),
            Payload::InsertOne(..) => None,
            Payload::InsertMany(..) => None,
            Payload::Upsert(v) => Some(&mut v.filter),
            Payload::Delete(v) => Some(&mut v.filter),
            Payload::CreateCollection(..) => None,
            Payload::DropCollection(..) => None,
            Payload::CreateDatabase(..) => None,
            Payload::DropDatabase(..) => None,
            Payload::Aggregate(v) => Some(&mut v.pipeline),
            Payload::Batch(BatchPayload { batch, .. }) => {
                batch.last_mut().and_then(Payload::current_bson_mut)
            }
        }
    }
    pub fn add_payload(&mut self, payload: Payload) -> Result<()> {
        match self {
            Payload::Fragment(bson) if !matches!(bson, Bson::Document(..)) => *self = payload,
            Payload::CreateCollection(CreateCollectionPayload { table, .. })
                if *table.schema == payload.table().schema =>
            {
                // The collection is automatically created
                *self = payload
            }
            Payload::CreateDatabase(CreateDatabasePayload { table, .. })
                if *table == payload.table() =>
            {
                // The database is automatically created
                *self = payload
            }
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
                if let Payload::DropCollection(DropCollectionPayload { ref table }) = payload
                    && *table == self.table()
                {
                    // The collection will be dropped, the previous query would have no effect
                    *self = payload;
                } else if let Payload::DropDatabase(DropDatabasePayload { ref table }) = payload
                    && table.schema == self.table().schema
                {
                    // The database will be dropped, the previous query would have no effect
                    *self = payload;
                } else {
                    *self = Payload::Batch(Default::default());
                    return self.add_payload(payload);
                }
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
                        .namespace(self.namespace())
                        .document(document)
                        .build()
                        .into(),
                )
            }
            Payload::InsertMany(..) => None,
            Payload::Upsert(payload) => {
                let Bson::Document(filter) = &payload.filter else {
                    return None;
                };
                Some(
                    UpdateOneModel::builder()
                        .namespace(self.namespace())
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
                        .namespace(self.namespace())
                        .filter(filter.clone())
                        .build()
                        .into(),
                )
            }
            Payload::CreateCollection(..) => None,
            Payload::DropCollection(..) => None,
            Payload::CreateDatabase(..) => None,
            Payload::DropDatabase(..) => None,
            Payload::Aggregate(..) => None,
            Payload::Batch(..) => None,
        }
    }
    pub fn is_empty(&self) -> bool {
        if let Payload::Batch(payload) = self {
            if payload.batch.is_empty() {
                return true;
            } else if payload.batch.len() > 1 {
                return false;
            }
        }
        self.current_bson()
            .map(|v| bson_is_empty(v))
            .unwrap_or_default()
    }
    pub fn table(&self) -> TableRef {
        match self {
            Payload::Fragment(..) => Default::default(),
            Payload::FindOne(payload) => payload.table.clone(),
            Payload::FindMany(payload) => payload.table.clone(),
            Payload::InsertOne(payload) => payload.table.clone(),
            Payload::InsertMany(payload) => payload.table.clone(),
            Payload::Upsert(payload) => payload.table.clone(),
            Payload::Delete(payload) => payload.table.clone(),
            Payload::CreateCollection(payload) => payload.table.clone(),
            Payload::DropCollection(payload) => payload.table.clone(),
            Payload::CreateDatabase(payload) => payload.table.clone(),
            Payload::DropDatabase(payload) => payload.table.clone(),
            Payload::Aggregate(payload) => payload.table.clone(),
            Payload::Batch(payload) => payload.batch.last().map(Payload::table).unwrap_or_default(),
        }
    }
}
impl Default for Payload {
    fn default() -> Self {
        Self::Fragment(Bson::Document(Default::default()))
    }
}

impl From<FindOnePayload> for Payload {
    fn from(value: FindOnePayload) -> Self {
        Payload::FindOne(value)
    }
}

impl From<FindManyPayload> for Payload {
    fn from(value: FindManyPayload) -> Self {
        Payload::FindMany(value)
    }
}

impl From<InsertOnePayload> for Payload {
    fn from(value: InsertOnePayload) -> Self {
        Payload::InsertOne(value)
    }
}

impl From<InsertManyPayload> for Payload {
    fn from(value: InsertManyPayload) -> Self {
        Payload::InsertMany(value)
    }
}

impl From<UpsertPayload> for Payload {
    fn from(value: UpsertPayload) -> Self {
        Payload::Upsert(value)
    }
}

impl From<DeletePayload> for Payload {
    fn from(value: DeletePayload) -> Self {
        Payload::Delete(value)
    }
}

impl From<CreateCollectionPayload> for Payload {
    fn from(value: CreateCollectionPayload) -> Self {
        Payload::CreateCollection(value)
    }
}

impl From<DropCollectionPayload> for Payload {
    fn from(value: DropCollectionPayload) -> Self {
        Payload::DropCollection(value)
    }
}

impl From<CreateDatabasePayload> for Payload {
    fn from(value: CreateDatabasePayload) -> Self {
        Payload::CreateDatabase(value)
    }
}

impl From<DropDatabasePayload> for Payload {
    fn from(value: DropDatabasePayload) -> Self {
        Payload::DropDatabase(value)
    }
}

impl From<AggregatePayload> for Payload {
    fn from(value: AggregatePayload) -> Self {
        Payload::Aggregate(value)
    }
}

impl From<BatchPayload> for Payload {
    fn from(value: BatchPayload) -> Self {
        Payload::Batch(value)
    }
}
