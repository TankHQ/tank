#![allow(unused_imports)]
use std::collections::BTreeMap;
use std::str::FromStr;
use std::sync::{Arc, LazyLock};
use tank::{Entity, Executor, expr};
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::silent_logs;

static MUTEX: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

#[derive(Entity, PartialEq, Debug, Clone)]
#[tank(schema = "testing", primary_key = Self::key)]
pub struct KV {
    pub key: String,
    pub string_val: String,
    pub int_val: i64,
    pub small_int: i16,
    pub unsigned_val: u32,
    pub float_val: f32,
    pub double_val: f64,
    pub boolean_val: bool,
    pub opt_string: Option<String>,
    pub opt_int: Option<i64>,
    pub opt_float: Option<f64>,
    pub opt_bool: Option<bool>,
    pub uuid: Uuid,
    pub opt_uuid: Option<Uuid>,
    #[cfg(not(feature = "disable-arrays"))]
    pub fixed_bytes: [u8; 16],
    #[cfg(not(feature = "disable-arrays"))]
    pub numbers: [i32; 4],
    #[cfg(not(feature = "disable-lists"))]
    pub tags: Vec<String>,
    #[cfg(not(feature = "disable-lists"))]
    pub scores: Vec<i32>,
    #[cfg(not(feature = "disable-lists"))]
    pub floats: Option<Vec<f64>>,
    #[cfg(not(feature = "disable-lists"))]
    pub shared_strings: Arc<Vec<String>>,
    #[cfg(not(feature = "disable-maps"))]
    pub metadata: BTreeMap<String, String>,
    #[cfg(not(feature = "disable-maps"))]
    pub counters: Option<BTreeMap<String, i64>>,
}

pub async fn kv_storage<E: Executor>(executor: &mut E) {
    let _lock = MUTEX.lock().await;

    // Setup
    silent_logs! {
        // Silent logs for Valkey/Redis
        KV::drop_table(executor, true, false)
            .await
            .expect("Failed to drop KV table");
    }
    KV::create_table(executor, false, true)
        .await
        .expect("Failed to create KV table");

    // Insert values
    let entries = vec![KV {
        key: "first".into(),
        string_val: "hello".into(),
        int_val: 42,
        small_int: -7,
        unsigned_val: 99,
        float_val: 3.14,
        double_val: 9.87654321,
        boolean_val: true,
        opt_string: Some("optional".into()),
        opt_int: None,
        opt_float: Some(1.5),
        opt_bool: None,
        uuid: Uuid::from_str("c41af414-54cf-49cb-96c6-364e9c42f294").unwrap(),
        opt_uuid: Some(Uuid::from_str("b1cacbcb-b3e0-4502-bbcd-d4ef014d189b").unwrap()),
        #[cfg(not(feature = "disable-arrays"))]
        fixed_bytes: *b"abcdefghijklmnop",
        #[cfg(not(feature = "disable-arrays"))]
        numbers: [1, 2, 3, 4],
        #[cfg(not(feature = "disable-lists"))]
        tags: vec!["kv".into(), "test".into(), "valkey".into()],
        #[cfg(not(feature = "disable-lists"))]
        scores: vec![10, 20, 30],
        #[cfg(not(feature = "disable-lists"))]
        floats: Some(vec![1.1, 2.2, 3.3]),
        #[cfg(not(feature = "disable-lists"))]
        shared_strings: Arc::new(vec!["a".into(), "b".into(), "c".into()]),
        #[cfg(not(feature = "disable-maps"))]
        metadata: BTreeMap::from_iter([
            ("env".into(), "test".into()),
            ("region".into(), "eu".into()),
        ]),
        #[cfg(not(feature = "disable-maps"))]
        counters: Some(BTreeMap::from_iter([
            ("views".into(), 100),
            ("likes".into(), 5),
        ])),
    }];

    let result = KV::insert_many(executor, entries.iter())
        .await
        .expect("Failed to insert KV entity");

    if let Some(rows) = result.rows_affected {
        assert_eq!(rows, 1);
    }

    // Query
    let name = KV::find_one(executor, expr!(KV::key == "first"))
        .await
        .expect("Failed to query KV");
    assert_eq!(
        name,
        Some(KV {
            key: "first".into(),
            string_val: "hello".into(),
            int_val: 42,
            small_int: -7,
            unsigned_val: 99,
            float_val: 3.14,
            double_val: 9.87654321,
            boolean_val: true,
            opt_string: Some("optional".into()),
            opt_int: None,
            opt_float: Some(1.5),
            opt_bool: None,
            uuid: Uuid::from_str("c41af414-54cf-49cb-96c6-364e9c42f294").unwrap(),
            opt_uuid: Some(Uuid::from_str("b1cacbcb-b3e0-4502-bbcd-d4ef014d189b").unwrap()),
            #[cfg(not(feature = "disable-arrays"))]
            fixed_bytes: *b"abcdefghijklmnop",
            #[cfg(not(feature = "disable-arrays"))]
            numbers: [1, 2, 3, 4],
            #[cfg(not(feature = "disable-lists"))]
            tags: vec!["kv".into(), "test".into(), "valkey".into()],
            #[cfg(not(feature = "disable-lists"))]
            scores: vec![10, 20, 30],
            #[cfg(not(feature = "disable-lists"))]
            floats: Some(vec![1.1, 2.2, 3.3]),
            #[cfg(not(feature = "disable-lists"))]
            shared_strings: Arc::new(vec!["a".into(), "b".into(), "c".into()]),
            #[cfg(not(feature = "disable-maps"))]
            metadata: BTreeMap::from_iter([
                ("env".into(), "test".into()),
                ("region".into(), "eu".into()),
            ]),
            #[cfg(not(feature = "disable-maps"))]
            counters: Some(BTreeMap::from_iter([
                ("views".into(), 100),
                ("likes".into(), 5),
            ])),
        })
    );
}
