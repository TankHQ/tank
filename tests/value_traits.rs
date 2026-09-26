#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;
    use std::{
        borrow::Cow,
        collections::{HashMap, HashSet, hash_map::DefaultHasher},
        hash::{Hash, Hasher},
    };
    use tank_core::{AsValue, Interval, TableRef, Value};
    use time::Month;
    use uuid::Uuid;

    #[test]
    fn value_is_null() {
        assert!(Value::Null.is_null());
        assert!(Value::Boolean(None).is_null());
        assert!(Value::Int8(None).is_null());
        assert!(Value::Int16(None).is_null());
        assert!(Value::Int32(None).is_null());
        assert!(Value::Int64(None).is_null());
        assert!(Value::Int128(None).is_null());
        assert!(Value::UInt8(None).is_null());
        assert!(Value::UInt16(None).is_null());
        assert!(Value::UInt32(None).is_null());
        assert!(Value::UInt64(None).is_null());
        assert!(Value::UInt128(None).is_null());
        assert!(Value::Float32(None).is_null());
        assert!(Value::Float64(None).is_null());
        assert!(Value::Decimal(None, 0, 0).is_null());
        assert!(Value::Char(None).is_null());
        assert!(Value::Varchar(None).is_null());
        assert!(Value::Blob(None).is_null());
        assert!(Value::Date(None).is_null());
        assert!(Value::Time(None).is_null());
        assert!(Value::Timestamp(None).is_null());
        assert!(Value::TimestampWithTimezone(None).is_null());
        assert!(Value::Interval(None).is_null());
        assert!(Value::Uuid(None).is_null());
        assert!(Value::Array(None, Box::new(Value::Int32(None)), 3).is_null());
        assert!(Value::List(None, Box::new(Value::Int32(None))).is_null());
        assert!(
            Value::Map(
                None,
                Box::new(Value::Varchar(None)),
                Box::new(Value::Int32(None))
            )
            .is_null()
        );
        assert!(Value::Json(None).is_null());
        assert!(Value::Json(Some(serde_json::Value::Null)).is_null());
        assert!(Value::Struct(None, vec![], TableRef::new("t".into()),).is_null());
        assert!(Value::Unknown(None).is_null());

        assert!(!Value::Boolean(Some(false)).is_null());
        assert!(!Value::Int32(Some(0)).is_null());
        assert!(!Value::Varchar(Some("".into())).is_null());
        assert!(!Value::Json(Some(serde_json::json!(42))).is_null());
    }

    #[test]
    fn value_as_null() {
        assert!(Value::Null.as_null().is_null());
        assert_eq!(Value::Boolean(Some(true)).as_null(), Value::Boolean(None));
        assert_eq!(Value::Int8(Some(5)).as_null(), Value::Int8(None));
        assert_eq!(Value::Int16(Some(5)).as_null(), Value::Int16(None));
        assert_eq!(Value::Int32(Some(5)).as_null(), Value::Int32(None));
        assert_eq!(Value::Int64(Some(5)).as_null(), Value::Int64(None));
        assert_eq!(Value::Int128(Some(5)).as_null(), Value::Int128(None));
        assert_eq!(Value::UInt8(Some(5)).as_null(), Value::UInt8(None));
        assert_eq!(Value::UInt16(Some(5)).as_null(), Value::UInt16(None));
        assert_eq!(Value::UInt32(Some(5)).as_null(), Value::UInt32(None));
        assert_eq!(Value::UInt64(Some(5)).as_null(), Value::UInt64(None));
        assert_eq!(Value::UInt128(Some(5)).as_null(), Value::UInt128(None));
        assert_eq!(Value::Float32(Some(1.0)).as_null(), Value::Float32(None));
        assert_eq!(Value::Float64(Some(1.0)).as_null(), Value::Float64(None));
        assert_eq!(
            Value::Decimal(Some(Decimal::from(10)), 10, 2).as_null(),
            Value::Decimal(None, 10, 2)
        );
        assert_eq!(Value::Char(Some('x')).as_null(), Value::Char(None));
        assert_eq!(
            Value::Varchar(Some("hi".into())).as_null(),
            Value::Varchar(None)
        );
        assert_eq!(
            Value::Blob(Some(vec![1, 2].into())).as_null(),
            Value::Blob(None)
        );
        assert_eq!(
            Value::Date(Some(
                time::Date::from_calendar_date(2025, Month::January, 1).unwrap()
            ))
            .as_null(),
            Value::Date(None)
        );
        assert_eq!(
            Value::Time(Some(time::Time::from_hms(0, 0, 0).unwrap())).as_null(),
            Value::Time(None)
        );
        assert_eq!(
            Value::Timestamp(Some(time::PrimitiveDateTime::new(
                time::Date::from_calendar_date(2025, Month::January, 1).unwrap(),
                time::Time::from_hms(0, 0, 0).unwrap(),
            )))
            .as_null(),
            Value::Timestamp(None)
        );
        assert_eq!(
            Value::TimestampWithTimezone(Some(time::OffsetDateTime::now_utc())).as_null(),
            Value::TimestampWithTimezone(None)
        );
        assert_eq!(
            Value::Interval(Some(Interval::from_days(1))).as_null(),
            Value::Interval(None)
        );
        assert_eq!(Value::Uuid(Some(Uuid::nil())).as_null(), Value::Uuid(None));
        assert_eq!(
            Value::Array(
                Some(vec![Value::Int32(Some(1))].into()),
                Box::new(Value::Int32(None)),
                1,
            )
            .as_null(),
            Value::Array(None, Box::new(Value::Int32(None)), 1)
        );
        assert_eq!(
            Value::List(Some(vec![]), Box::new(Value::Boolean(None))).as_null(),
            Value::List(None, Box::new(Value::Boolean(None)))
        );
        assert_eq!(
            Value::Map(
                Some(HashMap::new()),
                Box::new(Value::Varchar(None)),
                Box::new(Value::Int32(None)),
            )
            .as_null(),
            Value::Map(
                None,
                Box::new(Value::Varchar(None)),
                Box::new(Value::Int32(None))
            )
        );
        assert_eq!(
            Value::Json(Some(serde_json::json!({"a": 1}))).as_null(),
            Value::Json(None)
        );
        assert_eq!(
            Value::Struct(
                Some(vec![("id".into(), 1_i32.as_value())]),
                vec![("id".into(), i32::as_empty_value())],
                TableRef::new("t".into()),
            )
            .as_null(),
            Value::Struct(
                None,
                vec![("id".into(), i32::as_empty_value())],
                TableRef::new("t".into()),
            )
        );
        assert!(Value::Unknown(Some("x".into())).as_null().is_null());
    }

    #[test]
    fn value_is_scalar() {
        assert!(Value::Boolean(Some(true)).is_scalar());
        assert!(Value::Int8(Some(1)).is_scalar());
        assert!(Value::Int16(Some(1)).is_scalar());
        assert!(Value::Int32(Some(1)).is_scalar());
        assert!(Value::Int64(Some(1)).is_scalar());
        assert!(Value::Int128(Some(1)).is_scalar());
        assert!(Value::UInt8(Some(1)).is_scalar());
        assert!(Value::UInt16(Some(1)).is_scalar());
        assert!(Value::UInt32(Some(1)).is_scalar());
        assert!(Value::UInt64(Some(1)).is_scalar());
        assert!(Value::UInt128(Some(1)).is_scalar());
        assert!(Value::Float32(Some(1.0)).is_scalar());
        assert!(Value::Float64(Some(1.0)).is_scalar());
        assert!(Value::Decimal(Some(Decimal::from(1)), 0, 0).is_scalar());
        assert!(Value::Char(Some('a')).is_scalar());
        assert!(Value::Varchar(Some("x".into())).is_scalar());
        assert!(Value::Blob(Some(vec![].into())).is_scalar());
        assert!(Value::Date(None).is_scalar());
        assert!(Value::Time(None).is_scalar());
        assert!(Value::Timestamp(None).is_scalar());
        assert!(Value::TimestampWithTimezone(None).is_scalar());
        assert!(Value::Interval(None).is_scalar());
        assert!(Value::Uuid(None).is_scalar());
        assert!(Value::Unknown(None).is_scalar());

        assert!(!Value::Null.is_scalar());
        assert!(!Value::Array(None, Box::new(Value::Int32(None)), 1).is_scalar());
        assert!(!Value::List(None, Box::new(Value::Int32(None))).is_scalar());
        assert!(
            !Value::Map(
                None,
                Box::new(Value::Varchar(None)),
                Box::new(Value::Int32(None))
            )
            .is_scalar()
        );
        assert!(!Value::Json(None).is_scalar());
        assert!(!Value::Struct(None, vec![], TableRef::new("t".into())).is_scalar());
    }

    #[test]
    fn value_same_type() {
        assert!(Value::Int32(Some(1)).same_type(&Value::Int32(Some(99))));
        assert!(Value::Int32(Some(1)).same_type(&Value::Int32(None)));
        assert!(!Value::Int32(Some(1)).same_type(&Value::Int64(Some(1))));

        assert!(Value::Decimal(None, 10, 2).same_type(&Value::Decimal(None, 10, 2)));
        assert!(Value::Decimal(None, 0, 2).same_type(&Value::Decimal(None, 10, 2)));
        assert!(Value::Decimal(None, 10, 0).same_type(&Value::Decimal(None, 10, 2)));
        assert!(!Value::Decimal(None, 10, 2).same_type(&Value::Decimal(None, 8, 2)));
        assert!(!Value::Decimal(None, 10, 2).same_type(&Value::Decimal(None, 10, 3)));

        assert!(
            Value::Array(None, Box::new(Value::Int32(None)), 5).same_type(&Value::Array(
                None,
                Box::new(Value::Int32(None)),
                5
            ))
        );
        assert!(
            !Value::Array(None, Box::new(Value::Int32(None)), 5).same_type(&Value::Array(
                None,
                Box::new(Value::Int32(None)),
                3
            ))
        );
        assert!(
            !Value::Array(None, Box::new(Value::Int32(None)), 5).same_type(&Value::Array(
                None,
                Box::new(Value::Int64(None)),
                5
            ))
        );

        assert!(
            Value::List(None, Box::new(Value::Varchar(None)))
                .same_type(&Value::List(None, Box::new(Value::Varchar(None))))
        );
        assert!(
            !Value::List(None, Box::new(Value::Varchar(None)))
                .same_type(&Value::List(None, Box::new(Value::Int32(None))))
        );

        assert!(
            Value::Map(
                None,
                Box::new(Value::Varchar(None)),
                Box::new(Value::Int32(None))
            )
            .same_type(&Value::Map(
                None,
                Box::new(Value::Varchar(None)),
                Box::new(Value::Int32(None)),
            ))
        );
        assert!(
            !Value::Map(
                None,
                Box::new(Value::Varchar(None)),
                Box::new(Value::Int32(None))
            )
            .same_type(&Value::Map(
                None,
                Box::new(Value::Int32(None)),
                Box::new(Value::Int32(None)),
            ))
        );
    }

    #[test]
    fn value_try_as() {
        let v = Value::Int32(Some(42));
        assert_eq!(v.clone().try_as(&Value::Int32(None)).unwrap(), v);
        assert_eq!(
            Value::Int32(Some(1)).try_as(&Value::Boolean(None)).unwrap(),
            Value::Boolean(Some(true))
        );
        assert_eq!(
            Value::Int32(Some(42)).try_as(&Value::Int8(None)).unwrap(),
            Value::Int8(Some(42))
        );
        assert_eq!(
            Value::Int8(Some(10)).try_as(&Value::Int16(None)).unwrap(),
            Value::Int16(Some(10))
        );
        assert_eq!(
            Value::Int16(Some(100)).try_as(&Value::Int32(None)).unwrap(),
            Value::Int32(Some(100))
        );
        assert_eq!(
            Value::Int32(Some(1000))
                .try_as(&Value::Int64(None))
                .unwrap(),
            Value::Int64(Some(1000))
        );
        assert_eq!(
            Value::Int64(Some(10000))
                .try_as(&Value::Int128(None))
                .unwrap(),
            Value::Int128(Some(10000))
        );
        assert_eq!(
            Value::Int32(Some(5)).try_as(&Value::UInt8(None)).unwrap(),
            Value::UInt8(Some(5))
        );
        assert_eq!(
            Value::Int32(Some(5)).try_as(&Value::UInt16(None)).unwrap(),
            Value::UInt16(Some(5))
        );
        assert_eq!(
            Value::Int32(Some(5)).try_as(&Value::UInt32(None)).unwrap(),
            Value::UInt32(Some(5))
        );
        assert_eq!(
            Value::Int32(Some(5)).try_as(&Value::UInt64(None)).unwrap(),
            Value::UInt64(Some(5))
        );
        assert_eq!(
            Value::Int32(Some(5)).try_as(&Value::UInt128(None)).unwrap(),
            Value::UInt128(Some(5))
        );
        assert_eq!(
            Value::Float64(Some(5.0))
                .try_as(&Value::Float32(None))
                .unwrap(),
            Value::Float32(Some(5.0))
        );
        assert_eq!(
            Value::Float32(Some(5.0))
                .try_as(&Value::Float64(None))
                .unwrap(),
            Value::Float64(Some(5.0))
        );
        assert_eq!(
            Value::Int32(Some(5))
                .try_as(&Value::Decimal(None, 0, 0))
                .unwrap(),
            Value::Decimal(Some(Decimal::from(5)), 0, 0)
        );
        assert_eq!(
            Value::Char(Some('x'))
                .try_as(&Value::Varchar(None))
                .unwrap(),
            Value::Varchar(Some("x".into()))
        );

        assert!(Value::Int32(Some(5)).try_as(&Value::Json(None)).is_err());
        assert!(
            Value::Int32(Some(5))
                .try_as(&Value::Array(None, Box::new(Value::Int32(None)), 1))
                .is_err()
        );

        assert_eq!(
            Value::UInt8(Some(10)).try_as(&Value::UInt8(None)).unwrap(),
            Value::UInt8(Some(10))
        );
        assert_eq!(
            Value::UInt16(Some(20))
                .try_as(&Value::UInt16(None))
                .unwrap(),
            Value::UInt16(Some(20))
        );
        assert_eq!(
            Value::UInt32(Some(30))
                .try_as(&Value::UInt32(None))
                .unwrap(),
            Value::UInt32(Some(30))
        );
        assert_eq!(
            Value::UInt64(Some(40))
                .try_as(&Value::UInt64(None))
                .unwrap(),
            Value::UInt64(Some(40))
        );
        assert_eq!(
            Value::UInt128(Some(50))
                .try_as(&Value::UInt128(None))
                .unwrap(),
            Value::UInt128(Some(50))
        );
        assert_eq!(
            Value::Char(Some('z')).try_as(&Value::Char(None)).unwrap(),
            Value::Char(Some('z'))
        );
        assert_eq!(
            Value::Varchar(Some("hi".into()))
                .try_as(&Value::Varchar(None))
                .unwrap(),
            Value::Varchar(Some("hi".into()))
        );
        assert_eq!(
            Value::Blob(Some(vec![1, 2].into()))
                .try_as(&Value::Blob(None))
                .unwrap(),
            Value::Blob(Some(vec![1, 2].into()))
        );
        let d = time::Date::from_calendar_date(2024, time::Month::January, 1).unwrap();
        assert_eq!(
            Value::Date(Some(d)).try_as(&Value::Date(None)).unwrap(),
            Value::Date(Some(d))
        );
        let t = time::Time::from_hms(12, 30, 0).unwrap();
        assert_eq!(
            Value::Time(Some(t)).try_as(&Value::Time(None)).unwrap(),
            Value::Time(Some(t))
        );
        let ts = time::PrimitiveDateTime::new(d, t);
        assert_eq!(
            Value::Timestamp(Some(ts))
                .try_as(&Value::Timestamp(None))
                .unwrap(),
            Value::Timestamp(Some(ts))
        );
        let tstz = ts.assume_utc();
        assert_eq!(
            Value::TimestampWithTimezone(Some(tstz))
                .try_as(&Value::TimestampWithTimezone(None))
                .unwrap(),
            Value::TimestampWithTimezone(Some(tstz))
        );
        assert_eq!(
            Value::Interval(Some(tank_core::Interval::from_days(5)))
                .try_as(&Value::Interval(None))
                .unwrap(),
            Value::Interval(Some(tank_core::Interval::from_days(5)))
        );
        assert_eq!(
            Value::Uuid(Some(Uuid::nil()))
                .try_as(&Value::Uuid(None))
                .unwrap(),
            Value::Uuid(Some(Uuid::nil()))
        );
    }

    #[test]
    fn value_try_as_remaining_targets() {
        assert!(Value::Int32(Some(1)).try_as(&Value::Char(None)).is_ok());
        assert!(
            Value::Varchar(Some("hi".into()))
                .try_as(&Value::Char(None))
                .is_err()
        );
        assert_eq!(
            Value::Int32(Some(5)).try_as(&Value::Varchar(None)).unwrap(),
            Value::Varchar(Some("5".into()))
        );
        assert!(
            Value::Varchar(Some("zz".into()))
                .try_as(&Value::Char(None))
                .is_err()
        );
        let d = time::Date::from_calendar_date(2024, time::Month::January, 1).unwrap();
        assert_eq!(
            Value::Timestamp(Some(time::PrimitiveDateTime::new(d, time::Time::MIDNIGHT)))
                .try_as(&Value::Date(None))
                .unwrap(),
            Value::Date(Some(d))
        );
        let t = time::Time::from_hms(1, 2, 3).unwrap();
        assert_eq!(
            t.as_value().try_as(&Value::Time(None)).unwrap(),
            Value::Time(Some(t))
        );
        assert!(
            Value::Date(Some(d))
                .try_as(&Value::Timestamp(None))
                .is_err()
        );
        assert_eq!(
            Value::Timestamp(Some(time::PrimitiveDateTime::new(d, t)))
                .try_as(&Value::TimestampWithTimezone(None))
                .unwrap(),
            Value::TimestampWithTimezone(Some(time::PrimitiveDateTime::new(d, t).assume_utc()))
        );
        assert_eq!(
            Value::Timestamp(Some(time::PrimitiveDateTime::new(d, t)))
                .try_as(&Value::Timestamp(None))
                .unwrap(),
            Value::Timestamp(Some(time::PrimitiveDateTime::new(d, t)))
        );
        assert!(
            Value::Interval(Some(Interval::from_secs(3600)))
                .try_as(&Value::Time(None))
                .is_ok()
        );
        assert!(
            Value::Varchar(Some("not uuid".into()))
                .try_as(&Value::Uuid(None))
                .is_err()
        );
        assert!(Value::Int32(Some(1)).try_as(&Value::Date(None)).is_err());
        assert_eq!(
            Value::Blob(Some(vec![1, 2].into()))
                .try_as(&Value::Blob(None))
                .unwrap(),
            Value::Blob(Some(vec![1, 2].into()))
        );
    }

    #[test]
    fn value_partial_eq_complex() {
        assert_eq!(
            Value::Float32(Some(f32::NAN)),
            Value::Float32(Some(f32::NAN))
        );
        assert_eq!(
            Value::Float64(Some(f64::NAN)),
            Value::Float64(Some(f64::NAN))
        );
        assert_ne!(
            Value::Decimal(Some(Decimal::from(1)), 10, 2),
            Value::Decimal(Some(Decimal::from(1)), 10, 3)
        );
        assert_ne!(
            Value::Decimal(Some(Decimal::from(1)), 10, 2),
            Value::Decimal(Some(Decimal::from(1)), 8, 2)
        );
        assert_eq!(
            Value::Unknown(Some("a".into())),
            Value::Unknown(Some("a".into()))
        );
        assert_ne!(Value::Int32(Some(1)), Value::Int64(Some(1)));
        assert_eq!(
            Value::Map(
                None,
                Box::new(Value::Varchar(None)),
                Box::new(Value::Int32(None))
            ),
            Value::Map(
                None,
                Box::new(Value::Varchar(None)),
                Box::new(Value::Int32(None))
            ),
        );
        let mut m = HashMap::new();
        m.insert(Value::Varchar(Some("k".into())), Value::Int32(Some(1)));
        assert_ne!(
            Value::Map(
                Some(HashMap::new()),
                Box::new(Value::Varchar(None)),
                Box::new(Value::Int32(None)),
            ),
            Value::Map(
                Some(m),
                Box::new(Value::Varchar(None)),
                Box::new(Value::Int32(None)),
            ),
        );
        assert_eq!(
            Value::Map(
                Some(HashMap::new()),
                Box::new(Value::Varchar(None)),
                Box::new(Value::Int32(None)),
            ),
            Value::Map(
                Some(HashMap::new()),
                Box::new(Value::Varchar(None)),
                Box::new(Value::Int32(None)),
            ),
        );
        assert_ne!(
            Value::Map(
                Some(HashMap::new()),
                Box::new(Value::Varchar(None)),
                Box::new(Value::Int32(None)),
            ),
            Value::Map(
                None,
                Box::new(Value::Varchar(None)),
                Box::new(Value::Int32(None))
            ),
        );
        assert_eq!(
            Value::Struct(
                Some(vec![("a".into(), 1_i32.as_value())]),
                vec![("a".into(), i32::as_empty_value())],
                TableRef::new("t".into()),
            ),
            Value::Struct(
                Some(vec![("a".into(), 1_i32.as_value())]),
                vec![("a".into(), i32::as_empty_value())],
                TableRef::new("t".into()),
            ),
        );
        assert_ne!(
            Value::Struct(Some(vec![]), vec![], TableRef::new("t1".into()),),
            Value::Struct(Some(vec![]), vec![], TableRef::new("t2".into()),),
        );
        assert_eq!(Value::UInt8(Some(1)), Value::UInt8(Some(1)));
        assert_ne!(Value::UInt8(Some(1)), Value::UInt8(Some(2)));
        assert_eq!(Value::UInt16(Some(10)), Value::UInt16(Some(10)));
        assert_ne!(Value::UInt16(Some(10)), Value::UInt16(Some(20)));
        assert_eq!(Value::UInt32(Some(100)), Value::UInt32(Some(100)));
        assert_eq!(Value::UInt64(Some(1000)), Value::UInt64(Some(1000)));
        assert_eq!(Value::UInt128(Some(10000)), Value::UInt128(Some(10000)));

        let mut m1 = HashMap::new();
        m1.insert(Value::Varchar(Some("a".into())), Value::Int32(Some(1)));
        let mut m2 = HashMap::new();
        m2.insert(Value::Varchar(Some("a".into())), Value::Int32(Some(1)));
        assert_eq!(
            Value::Map(
                Some(m1),
                Box::new(Value::Varchar(None)),
                Box::new(Value::Int32(None))
            ),
            Value::Map(
                Some(m2),
                Box::new(Value::Varchar(None)),
                Box::new(Value::Int32(None))
            ),
        );
    }

    #[test]
    fn value_hash() {
        let mut set = HashSet::new();
        set.insert(Value::Null);
        set.insert(Value::Boolean(Some(true)));
        set.insert(Value::Boolean(None));
        set.insert(Value::Int8(Some(1)));
        set.insert(Value::Int16(Some(2)));
        set.insert(Value::Int32(Some(3)));
        set.insert(Value::Int64(Some(4)));
        set.insert(Value::Int128(Some(5)));
        set.insert(Value::UInt8(Some(6)));
        set.insert(Value::UInt16(Some(7)));
        set.insert(Value::UInt32(Some(8)));
        set.insert(Value::UInt64(Some(9)));
        set.insert(Value::UInt128(Some(10)));
        set.insert(Value::Float32(Some(1.5)));
        set.insert(Value::Float32(None));
        set.insert(Value::Float64(Some(2.5)));
        set.insert(Value::Float64(None));
        set.insert(Value::Decimal(Some(Decimal::from(1)), 10, 2));
        set.insert(Value::Char(Some('a')));
        set.insert(Value::Varchar(Some("hello".into())));
        set.insert(Value::Blob(Some(vec![1, 2].into())));
        set.insert(Value::Uuid(Some(Uuid::nil())));
        set.insert(Value::Json(Some(serde_json::json!(1))));
        set.insert(Value::Array(
            Some(vec![Value::Int32(Some(1))].into()),
            Box::new(Value::Int32(None)),
            1,
        ));
        set.insert(Value::List(
            Some(vec![Value::Int32(Some(1))]),
            Box::new(Value::Int32(None)),
        ));
        set.insert(Value::Map(
            Some(HashMap::new()),
            Box::new(Value::Varchar(None)),
            Box::new(Value::Int32(None)),
        ));
        let mut m_hash = HashMap::new();
        m_hash.insert(Value::Varchar(Some("k".into())), Value::Int32(Some(1)));
        set.insert(Value::Map(
            Some(m_hash),
            Box::new(Value::Varchar(None)),
            Box::new(Value::Int32(None)),
        ));
        set.insert(Value::Map(
            None,
            Box::new(Value::Varchar(None)),
            Box::new(Value::Int32(None)),
        ));
        set.insert(Value::Struct(
            Some(vec![("a".into(), 1_i32.as_value())]),
            vec![("a".into(), i32::as_empty_value())],
            TableRef::new("t".into()),
        ));
        set.insert(Value::Struct(
            None,
            vec![("a".into(), i32::as_empty_value())],
            TableRef::new("t2".into()),
        ));
        set.insert(Value::Unknown(Some("x".into())));
        let d = time::Date::from_calendar_date(2024, time::Month::January, 15).unwrap();
        set.insert(Value::Date(Some(d)));
        let t = time::Time::from_hms(10, 30, 0).unwrap();
        set.insert(Value::Time(Some(t)));
        set.insert(Value::Timestamp(Some(time::PrimitiveDateTime::new(d, t))));
        set.insert(Value::TimestampWithTimezone(Some(
            time::PrimitiveDateTime::new(d, t).assume_utc(),
        )));
        set.insert(Value::Interval(Some(tank_core::Interval::from_days(7))));
        assert!(set.len() >= 25);
    }

    #[test]
    fn value_null_equality_is_reflexive() {
        assert_eq!(Value::Null, Value::Null);
        assert_eq!(Value::Null, Value::Null.clone());
        let mut set = HashSet::new();
        set.insert(Value::Null);
        assert!(set.contains(&Value::Null), "HashSet lost Value::Null");
    }

    #[test]
    fn value_float_hash_matches_equality() {
        let mut f32_set = HashSet::new();
        f32_set.insert(Value::Float32(Some(0.0)));
        f32_set.insert(Value::Float32(Some(-0.0)));
        f32_set.insert(Value::Float32(Some(f32::from_bits(0x7fc0_0000))));
        f32_set.insert(Value::Float32(Some(f32::from_bits(0x7fc0_0001))));
        assert_eq!(f32_set.len(), 2);

        let mut f64_set = HashSet::new();
        f64_set.insert(Value::Float64(Some(0.0)));
        f64_set.insert(Value::Float64(Some(-0.0)));
        f64_set.insert(Value::Float64(Some(f64::from_bits(0x7ff8_0000_0000_0000))));
        f64_set.insert(Value::Float64(Some(f64::from_bits(0x7ff8_0000_0000_0001))));
        assert_eq!(f64_set.len(), 2);
    }

    #[test]
    fn value_collection_hash_matches_equality() {
        let a = Value::Array(
            Some(vec![Value::Int32(Some(1))].into()),
            Box::new(Value::Decimal(None, 0, 2)),
            1,
        );
        let b = Value::Array(
            Some(vec![Value::Int32(Some(1))].into()),
            Box::new(Value::Decimal(None, 10, 2)),
            1,
        );
        let c = Value::Array(
            Some(vec![Value::Int32(Some(1))].into()),
            Box::new(Value::Decimal(None, 8, 2)),
            1,
        );
        assert_ne!(a, b);
        assert_ne!(a, c);
        assert_ne!(b, c);

        assert_eq!(a, a.clone());
        let mut set = HashSet::new();
        set.insert(a.clone());
        assert!(set.contains(&a));

        // Same for List and Map element types.
        let l1 = Value::List(
            Some(vec![Value::Int32(Some(1))]),
            Box::new(Value::Decimal(None, 0, 2)),
        );
        let l2 = Value::List(
            Some(vec![Value::Int32(Some(1))]),
            Box::new(Value::Decimal(None, 10, 2)),
        );
        assert_ne!(l1, l2);

        let m1 = Value::Map(
            Some(HashMap::new()),
            Box::new(Value::Decimal(None, 0, 2)),
            Box::new(Value::Int32(None)),
        );
        let m2 = Value::Map(
            Some(HashMap::new()),
            Box::new(Value::Decimal(None, 10, 2)),
            Box::new(Value::Int32(None)),
        );
        assert_ne!(m1, m2);
    }

    #[test]
    fn value_map_hash_matches_equality() {
        let empty = Value::Map(
            Some(HashMap::new()),
            Box::new(Value::Varchar(None)),
            Box::new(Value::Int32(None)),
        );
        let null = Value::Map(
            None,
            Box::new(Value::Varchar(None)),
            Box::new(Value::Int32(None)),
        );

        let mut empty_hasher = DefaultHasher::new();
        empty.hash(&mut empty_hasher);

        let mut null_hasher = DefaultHasher::new();
        null.hash(&mut null_hasher);

        assert_ne!(empty, null);
        assert_ne!(empty_hasher.finish(), null_hasher.finish());
    }

    #[test]
    fn value_display() {
        assert_eq!(format!("{}", Value::Null), "NULL");
        assert_eq!(format!("{}", Value::Boolean(Some(true))), "true");
        assert_eq!(format!("{}", Value::Int32(Some(42))), "42");
        assert_eq!(format!("{}", Value::Float64(Some(3.14))), "3.14");
        assert_eq!(format!("{}", Value::Varchar(Some("hello".into()))), "hello");
        assert_eq!(format!("{}", Value::Char(Some('x'))), "x");
        assert_eq!(
            format!("{}", Value::Uuid(Some(Uuid::nil()))),
            "00000000-0000-0000-0000-000000000000"
        );
        assert_eq!(format!("{}", Value::UInt64(Some(999))), "999");
        assert_eq!(format!("{}", Value::Int128(Some(-100))), "-100");
        assert_eq!(format!("{}", Value::Boolean(Some(false))), "false");
    }

    #[test]
    fn value_value() {
        assert_eq!(
            Value::try_from_value("hello".as_value()).expect("Could not get a value from a value"),
            Cow::Borrowed("hello").as_value()
        );
        assert_ne!("2hello".as_value(), Cow::Borrowed("1hello").as_value());
        assert_eq!(
            "hello".to_string().as_value(),
            Cow::Borrowed("hello").as_value(),
        );
        assert_ne!(
            Cow::Borrowed("hello3").as_value(),
            "hello4".to_string().as_value(),
        );
        assert!(matches!(Value::as_empty_value(), Value::Null));
        assert_eq!(
            <[u128; 23]>::as_empty_value(),
            Value::Array(None, Box::new(Value::UInt128(None)), 23)
        );
        assert!(Value::parse("some input").is_err());
    }
}
