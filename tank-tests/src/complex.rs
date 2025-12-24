#![allow(unused_imports)]
use std::{
    collections::{BTreeMap, LinkedList, VecDeque},
    sync::LazyLock,
    time::Duration,
};
use tank::{Entity, Executor};
use tokio::sync::Mutex;

#[derive(Default)]
struct TankUnsupported {
    field: i32,
}
#[derive(Entity)]
struct ComplexNullFields {
    #[cfg(not(feature = "disable-arrays"))]
    first: Option<[Option<f64>; 8]>,
    #[cfg(all(
        not(feature = "disable-lists"),
        not(feature = "disable-intervals"),
        not(feature = "disable-nested-collections"),
    ))]
    second: Option<Vec<Option<Duration>>>,
    third: Option<Box<[u8]>>,
    #[cfg(all(
        not(feature = "disable-arrays"),
        not(feature = "disable-maps"),
        not(feature = "disable-nested-collections"),
    ))]
    fourth: Option<Box<BTreeMap<String, Option<[Option<i128>; 3]>>>>,
    #[cfg(all(
        not(feature = "disable-lists"),
        not(feature = "disable-maps"),
        not(feature = "disable-nested-collections"),
    ))]
    fifth: LinkedList<Option<VecDeque<Option<BTreeMap<i32, Option<i32>>>>>>,
    #[tank(ignore)]
    sixth: TankUnsupported,
}
impl Default for ComplexNullFields {
    fn default() -> Self {
        Self {
            #[cfg(not(feature = "disable-arrays"))]
            first: None,
            #[cfg(all(
                not(feature = "disable-lists"),
                not(feature = "disable-intervals"),
                not(feature = "disable-nested-collections"),
            ))]
            second: None,
            third: None,
            #[cfg(all(
                not(feature = "disable-arrays"),
                not(feature = "disable-maps"),
                not(feature = "disable-nested-collections"),
            ))]
            fourth: None,
            #[cfg(all(
                not(feature = "disable-lists"),
                not(feature = "disable-maps"),
                not(feature = "disable-nested-collections"),
            ))]
            fifth: Default::default(),
            sixth: TankUnsupported { field: 777 },
        }
    }
}
static MUTEX: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

pub async fn complex<E: Executor>(executor: &mut E) {
    let _lock = MUTEX.lock().await;

    // Setup
    ComplexNullFields::drop_table(executor, true, false)
        .await
        .expect("Failed to drop ComplexNullFields table");
    ComplexNullFields::create_table(executor, true, true)
        .await
        .expect("Failed to create ComplexNullFields table");

    // Complex 1
    ComplexNullFields::delete_many(executor, &true)
        .await
        .expect("Failed to clear the ComplexNullFields table");
    let entity = ComplexNullFields {
        #[cfg(not(feature = "disable-arrays"))]
        first: None,
        #[cfg(all(
            not(feature = "disable-lists"),
            not(feature = "disable-intervals"),
            not(feature = "disable-nested-collections"),
        ))]
        second: Some(vec![
            None,
            None,
            Duration::from_millis(15).into(),
            Duration::from_micros(22).into(),
            None,
            Duration::from_micros(99).into(),
            Duration::from_micros(0).into(),
            Duration::from_secs(24).into(),
            None,
            None,
            None,
        ]),
        third: Some(Box::new([0x75, 0xAA, 0x30, 0x77])),
        #[cfg(all(
            not(feature = "disable-arrays"),
            not(feature = "disable-maps"),
            not(feature = "disable-nested-collections"),
        ))]
        fourth: Some(Box::new(BTreeMap::from_iter([
            ("aa".into(), Some([19314.into(), 241211.into(), None])),
            (
                "bb".into(),
                Some([165536.into(), 23311090.into(), 30001.into()]),
            ),
            ("cc".into(), None),
            ("dd".into(), Some([None, None, None])),
            ("ee".into(), Some([None, 777.into(), None])),
        ]))),
        #[cfg(all(
            not(feature = "disable-lists"),
            not(feature = "disable-maps"),
            not(feature = "disable-nested-collections"),
        ))]
        fifth: LinkedList::from_iter([]),
        sixth: Default::default(),
    };
    ComplexNullFields::insert_one(executor, &entity)
        .await
        .expect("Failed to save complex 1");
    let entity = ComplexNullFields::find_one(executor, &true)
        .await
        .expect("Failed to query complex 1")
        .expect("Failed to find complex 1");
    #[cfg(not(feature = "disable-arrays"))]
    assert_eq!(entity.first, None);
    #[cfg(all(
        not(feature = "disable-lists"),
        not(feature = "disable-intervals"),
        not(feature = "disable-nested-collections"),
    ))]
    assert_eq!(
        entity.second,
        Some(vec![
            None,
            None,
            Duration::from_millis(15).into(),
            Duration::from_micros(22).into(),
            None,
            Duration::from_micros(99).into(),
            Duration::from_micros(0).into(),
            Duration::from_secs(24).into(),
            None,
            None,
            None,
        ])
    );
    assert_eq!(*entity.third.unwrap(), [0x75, 0xAA, 0x30, 0x77]);
    #[cfg(all(
        not(feature = "disable-arrays"),
        not(feature = "disable-maps"),
        not(feature = "disable-nested-collections"),
    ))]
    assert_eq!(
        *entity.fourth.unwrap(),
        BTreeMap::from_iter([
            ("aa".into(), Some([19314.into(), 241211.into(), None])),
            (
                "bb".into(),
                Some([165536.into(), 23311090.into(), 30001.into()]),
            ),
            ("cc".into(), None),
            ("dd".into(), Some([None, None, None])),
            ("ee".into(), Some([None, 777.into(), None])),
        ])
    );
    #[cfg(all(
        not(feature = "disable-lists"),
        not(feature = "disable-maps"),
        not(feature = "disable-nested-collections"),
    ))]
    assert_eq!(entity.fifth.len(), 0);

    // Complex 2
    ComplexNullFields::delete_many(executor, &true)
        .await
        .expect("Failed to clear the ComplexNullFields table");
    let entity = ComplexNullFields {
        #[cfg(not(feature = "disable-arrays"))]
        first: Some([
            0.5.into(),
            None,
            (-99.5).into(),
            100.0.into(),
            0.0.into(),
            #[cfg(not(feature = "disable-infinity"))]
            f64::NEG_INFINITY.into(),
            #[cfg(feature = "disable-infinity")]
            f64::MIN.into(),
            None,
            777.777.into(),
        ]),
        #[cfg(all(
            not(feature = "disable-lists"),
            not(feature = "disable-intervals"),
            not(feature = "disable-nested-collections"),
        ))]
        second: None,
        third: None,
        #[cfg(all(
            not(feature = "disable-arrays"),
            not(feature = "disable-maps"),
            not(feature = "disable-nested-collections"),
        ))]
        fourth: None,
        #[cfg(all(
            not(feature = "disable-lists"),
            not(feature = "disable-maps"),
            not(feature = "disable-nested-collections"),
        ))]
        fifth: LinkedList::from_iter([
            None,
            None,
            None,
            None,
            None,
            Some(
                vec![
                    Some(BTreeMap::from_iter([
                        (1, Some(11)),
                        (2, Some(22)),
                        (3, None),
                        (4, None),
                        (5, Some(55)),
                    ])),
                    None,
                ]
                .into(),
            ),
            None,
        ]),
        sixth: Default::default(),
    };
    ComplexNullFields::insert_one(executor, &entity)
        .await
        .expect("Failed to save complex 2");
    let loaded = ComplexNullFields::find_one(executor, &true)
        .await
        .expect("Failed to query complex 2")
        .expect("Failed to find complex 2");
    #[cfg(not(feature = "disable-arrays"))]
    assert_eq!(loaded.first, entity.first);
    #[cfg(all(
        not(feature = "disable-lists"),
        not(feature = "disable-intervals"),
        not(feature = "disable-nested-collections"),
    ))]
    assert_eq!(loaded.second, None);
    assert_eq!(loaded.third, None);
    #[cfg(all(
        not(feature = "disable-arrays"),
        not(feature = "disable-maps"),
        not(feature = "disable-nested-collections"),
    ))]
    assert_eq!(loaded.fourth, None);
    #[cfg(all(
        not(feature = "disable-lists"),
        not(feature = "disable-maps"),
        not(feature = "disable-nested-collections"),
    ))]
    assert_eq!(loaded.fifth, entity.fifth);
    assert_eq!(loaded.sixth.field, 777);
}
