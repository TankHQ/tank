#![allow(unused_imports)]
use std::{
    borrow::Cow,
    cell::{Cell, RefCell},
    collections::{self, BTreeMap, HashMap, VecDeque},
    ops::Deref,
    rc::Rc,
    sync::{Arc, LazyLock},
};
use tank::{Entity, Executor};
use time::Time;
use tokio::sync::Mutex;
use uuid::Uuid;

static MUTEX: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

#[derive(Entity)]
struct InsaneNullFields {
    #[cfg(all(
        not(feature = "disable-arrays"),
        not(feature = "disable-lists"),
        not(feature = "disable-maps")
    ))]
    red: Option<Vec<Option<Vec<HashMap<Cow<'static, str>, BTreeMap<u128, Option<Vec<[i8; 2]>>>>>>>>,
    #[cfg(all(
        not(feature = "disable-arrays"),
        not(feature = "disable-lists"),
        not(feature = "disable-maps")
    ))]
    yellow: std::rc::Rc<
        Option<
            Rc<
                std::cell::RefCell<
                    Vec<
                        Option<
                            Arc<
                                std::collections::HashMap<
                                    Box<u64>,
                                    collections::VecDeque<
                                        Option<Arc<[Cell<Option<Option<uuid::Uuid>>>; 2]>>,
                                    >,
                                >,
                            >,
                        >,
                    >,
                >,
            >,
        >,
    >,
    #[cfg(not(feature = "disable-lists"))]
    blue: Vec<Option<Arc<VecDeque<i32>>>>,
    green: Box<RefCell<Arc<Box<Arc<Arc<Arc<RefCell<time::Time>>>>>>>>,
}

pub async fn insane<E: Executor>(executor: &mut E) {
    let _lock = MUTEX.lock().await;

    // Setup
    InsaneNullFields::drop_table(executor, true, false)
        .await
        .expect("Failed to drop ComplexNullFields table");
    InsaneNullFields::create_table(executor, true, true)
        .await
        .expect("Failed to create ComplexNullFields table");

    // Insane 1
    InsaneNullFields::delete_many(executor, &true)
        .await
        .expect("Failed to clear the ComplexNullFields table");
    let entity = InsaneNullFields {
        #[cfg(all(
            not(feature = "disable-arrays"),
            not(feature = "disable-lists"),
            not(feature = "disable-maps")
        ))]
        red: vec![
            vec![HashMap::from_iter([
                (
                    "the first key".into(),
                    BTreeMap::from_iter([(
                        9941492349876,
                        vec![[0, 1], [4, 2], [-3, 5], [1, 1]].into(),
                    )]),
                ),
                (
                    "the second key".into(),
                    BTreeMap::from_iter([(443234, vec![[7, 8], [-4, 2]].into())]),
                ),
                ("the third key".into(), BTreeMap::from_iter([(0, None)])),
                (
                    "the third key".into(),
                    BTreeMap::from_iter([(1, vec![].into())]),
                ),
            ])]
            .into(),
            None,
            vec![].into(),
            None,
            vec![HashMap::new()].into(),
        ]
        .into(),
        #[cfg(all(
            not(feature = "disable-arrays"),
            not(feature = "disable-lists"),
            not(feature = "disable-maps")
        ))]
        yellow: Rc::new(None),
        #[cfg(not(feature = "disable-lists"))]
        blue: vec![
            Some(Arc::new([-1, -2, -3, -4, -5].into())),
            Some(Arc::new([66, 77].into())),
            None,
            Some(Arc::new(vec![].into())),
            None,
            None,
        ],
        green: Box::new(RefCell::new(Arc::new(Box::new(Arc::new(Arc::new(
            Arc::new(RefCell::new(
                Time::from_hms(14, 25, 59).expect("Correct time"),
            )),
        )))))),
    };
    InsaneNullFields::insert_one(executor, &entity)
        .await
        .expect("Failed to save insane 1");
    #[allow(unused_variables)]
    let loaded = InsaneNullFields::find_one(executor, &true)
        .await
        .expect("Failed to query insane 1")
        .expect("Failed to find insane 1");
    #[cfg(all(
        not(feature = "disable-arrays"),
        not(feature = "disable-lists"),
        not(feature = "disable-maps")
    ))]
    assert_eq!(loaded.red, entity.red);
    #[cfg(all(
        not(feature = "disable-arrays"),
        not(feature = "disable-lists"),
        not(feature = "disable-maps")
    ))]
    assert_eq!(loaded.yellow, entity.yellow);
    #[cfg(not(feature = "disable-lists"))]
    assert_eq!(loaded.blue, entity.blue);
    assert_eq!(loaded.green, entity.green);

    // Insane 2
    InsaneNullFields::delete_many(executor, &true)
        .await
        .expect("Failed to clear the ComplexNullFields table");
    let entity = InsaneNullFields {
        #[cfg(all(
            not(feature = "disable-arrays"),
            not(feature = "disable-lists"),
            not(feature = "disable-maps")
        ))]
        red: None,
        #[cfg(all(
            not(feature = "disable-arrays"),
            not(feature = "disable-lists"),
            not(feature = "disable-maps")
        ))]
        yellow: Rc::new(Some(Rc::new(RefCell::new(vec![
            Arc::new(HashMap::from_iter([
                (844710.into(), vec![].into()),
                (5.into(), vec![None].into()),
                (994700000.into(), vec![None, None, None, None, None].into()),
                (
                    9000.into(),
                    vec![
                        Arc::new([
                            Cell::new(None),
                            Cell::new(Some(Some(
                                Uuid::parse_str("cd6c7a05-8b7d-4ee9-8b9c-0e39380b4dac").unwrap(),
                            ))),
                        ])
                        .into(),
                        Arc::new([
                            Cell::new(Some(Some(
                                Uuid::parse_str("d2dfa11e-32bb-4896-ba49-f9d51296f9da").unwrap(),
                            ))),
                            Cell::new(Some(Some(
                                Uuid::parse_str("11a314b3-972f-4d20-8e0d-583a168d1d05").unwrap(),
                            ))),
                        ])
                        .into(),
                    ]
                    .into(),
                ),
            ]))
            .into(),
        ])))),
        #[cfg(not(feature = "disable-lists"))]
        blue: vec![],
        green: Box::new(RefCell::new(Arc::new(Box::new(Arc::new(Arc::new(
            Arc::new(RefCell::new(
                Time::from_hms(11, 59, 59).expect("Correct time"),
            )),
        )))))),
    };
    InsaneNullFields::insert_one(executor, &entity)
        .await
        .expect("Failed to save insane 2");
    let loaded = InsaneNullFields::find_one(executor, &true)
        .await
        .expect("Failed to query insane 2")
        .expect("Failed to find insane 2");
    #[cfg(all(
        not(feature = "disable-arrays"),
        not(feature = "disable-lists"),
        not(feature = "disable-maps")
    ))]
    assert_eq!(loaded.red, entity.red);
    #[cfg(all(
        not(feature = "disable-arrays"),
        not(feature = "disable-lists"),
        not(feature = "disable-maps")
    ))]
    assert_eq!(loaded.yellow, entity.yellow);
    #[cfg(not(feature = "disable-lists"))]
    assert_eq!(loaded.blue, entity.blue);
    #[cfg(all(
        not(feature = "disable-arrays"),
        not(feature = "disable-lists"),
        not(feature = "disable-maps")
    ))]
    assert_eq!(
        loaded.yellow.deref().as_ref().unwrap().borrow()[0]
            .as_ref()
            .unwrap()
            .get(&9000)
            .unwrap()[0]
            .as_ref()
            .unwrap()
            .deref()[1]
            .get()
            .unwrap()
            .unwrap()
            .to_string(),
        "cd6c7a05-8b7d-4ee9-8b9c-0e39380b4dac"
    );
    assert_eq!(loaded.green, entity.green);
}
