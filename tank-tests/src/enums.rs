#![allow(unused_imports)]
use rust_decimal::{Decimal, prelude::FromPrimitive};
use std::{
    borrow::Cow,
    cell::{Cell, RefCell},
    i128,
    ops::Deref,
    pin::pin,
    sync::{Arc, LazyLock},
};
use tank::{
    AsValue, Driver, DynQuery, Entity, Error, Executor, FixedDecimal, Query, QueryBuilder,
    QueryResult, RawQuery, Result, RowsAffected, SqlWriter, Value, cols, expr,
    stream::{StreamExt, TryStreamExt},
};
use time::{Date, Time, macros::date};
use tokio::sync::Mutex;
use uuid::Uuid;

static MUTEX: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum SomeEnum {
    FirstValue,
    SecondValue,
}
impl AsValue for SomeEnum {
    fn as_empty_value() -> Value {
        Value::Varchar(None)
    }
    fn as_value(self) -> Value {
        match self {
            Self::FirstValue => Value::Varchar(Some("first".into())),
            Self::SecondValue => Value::Varchar(Some("second".into())),
        }
    }
    fn try_from_value(value: Value) -> Result<Self>
    where
        Self: Sized,
    {
        if let Value::Varchar(Some(v), ..) = value.try_as(&Value::Varchar(None))? {
            match v.deref() {
                "first" => return Ok(Self::FirstValue),
                "second" => return Ok(Self::SecondValue),
                _ => {}
            }
        }
        Err(Error::msg("Could not decode SomeEnum from value"))
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum AnotherEnum {
    Alpha,
    Bravo,
    Charlie,
    Delta,
}
impl AsValue for AnotherEnum {
    fn as_empty_value() -> Value {
        Value::Int8(None)
    }
    fn as_value(self) -> Value {
        match self {
            Self::Alpha => Value::Int8(Some(0)),
            Self::Bravo => Value::Int8(Some(1)),
            Self::Charlie => Value::Int8(Some(2)),
            Self::Delta => Value::Int8(Some(3)),
        }
    }
    fn try_from_value(value: Value) -> Result<Self>
    where
        Self: Sized,
    {
        if let Value::Int8(Some(v), ..) = value.try_as(&Value::Int8(None))? {
            match v {
                0 => return Ok(Self::Alpha),
                1 => return Ok(Self::Bravo),
                2 => return Ok(Self::Charlie),
                3 => return Ok(Self::Delta),
                _ => {}
            }
        }
        Err(Error::msg("Could not decode AnotherEnum from value"))
    }
}

#[derive(Entity, PartialEq, Debug)]
#[tank(primary_key = (id, another_enum))]
struct Entry {
    id: i32,
    some_enum: Option<SomeEnum>,
    #[tank(clustering_key)]
    another_enum: AnotherEnum,
}

pub async fn enums<E: Executor>(executor: &mut E) {
    let _lock = MUTEX.lock().await;

    // Setup
    Entry::drop_table(executor, true, false)
        .await
        .expect("Failed to drop Table table");
    Entry::create_table(executor, true, true)
        .await
        .expect("Failed to create Table table");

    // Operations
    let mut entry = Entry {
        id: 1,
        some_enum: Some(SomeEnum::FirstValue),
        another_enum: AnotherEnum::Delta,
    };
    entry.save(executor).await.expect("Failed to save entry");
    let value = Entry::find_one(executor, true)
        .await
        .expect("Failed to read entry");
    assert_eq!(
        value,
        Some(Entry {
            id: 1,
            some_enum: Some(SomeEnum::FirstValue),
            another_enum: AnotherEnum::Delta,
        })
    );

    entry.some_enum = None;
    entry
        .save(executor)
        .await
        .expect("Failed to save again entry");
    Entry::insert_many(
        executor,
        &[
            Entry {
                id: 1,
                some_enum: Some(SomeEnum::SecondValue),
                another_enum: AnotherEnum::Charlie,
            },
            Entry {
                id: 1,
                some_enum: None,
                another_enum: AnotherEnum::Bravo,
            },
        ],
    )
    .await
    .expect("Could not insert multiple entities");
    let entries = executor
        .fetch(
            QueryBuilder::new()
                .select(cols!(*))
                .from(Entry::table())
                .where_expr(expr!(id == 1))
                .order_by(cols!(Entry::another_enum ASC))
                .build(&executor.driver()),
        )
        .map_ok(Entry::from_row)
        .map(Result::flatten)
        .try_collect::<Vec<_>>()
        .await
        .expect("Could not query multiple entities");
    assert_eq!(
        entries,
        [
            Entry {
                id: 1,
                some_enum: None,
                another_enum: AnotherEnum::Bravo,
            },
            Entry {
                id: 1,
                some_enum: Some(SomeEnum::SecondValue),
                another_enum: AnotherEnum::Charlie,
            },
            Entry {
                id: 1,
                some_enum: None,
                another_enum: AnotherEnum::Delta,
            },
        ]
    );
}
