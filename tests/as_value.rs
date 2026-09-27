#[cfg(test)]
mod tests {
    use quote::ToTokens;
    use rust_decimal::Decimal;
    use std::{
        borrow::Cow,
        cell::{Cell, RefCell},
        collections::HashMap,
        num::*,
        rc::Rc,
        sync::{Arc, RwLock},
    };
    use tank::{AsValue, TableRef, Value};
    use tank_core::{FixedDecimal, Interval};

    #[test]
    fn nonzero_conversions() {
        let v = NonZeroI32::new(42).unwrap().as_value();
        assert_eq!(v, Value::Int32(Some(42)));
        let back = NonZeroI32::try_from_value(v).unwrap();
        assert_eq!(back.get(), 42);
        assert_eq!(NonZeroI32::as_empty_value(), Value::Int32(None));

        let v = NonZeroU64::new(100).unwrap().as_value();
        assert_eq!(v, Value::UInt64(Some(100)));
        let back = NonZeroU64::try_from_value(v).unwrap();
        assert_eq!(back.get(), 100);

        assert!(NonZeroI32::try_from_value(Value::Int32(Some(0))).is_err());
    }

    #[test]
    fn bool_from_various_types() {
        assert_eq!(bool::try_from_value(Value::Int8(Some(1))).unwrap(), true);
        assert_eq!(bool::try_from_value(Value::Int8(Some(0))).unwrap(), false);
        assert_eq!(bool::try_from_value(Value::Int16(Some(1))).unwrap(), true);
        assert_eq!(bool::try_from_value(Value::UInt8(Some(1))).unwrap(), true);
        assert_eq!(bool::try_from_value(Value::UInt16(Some(0))).unwrap(), false);
        assert_eq!(bool::try_from_value(Value::UInt32(Some(1))).unwrap(), true);
        assert_eq!(bool::try_from_value(Value::UInt64(Some(0))).unwrap(), false);
        assert_eq!(bool::try_from_value(Value::UInt128(Some(1))).unwrap(), true);
        assert_eq!(bool::try_from_value(Value::Int128(Some(0))).unwrap(), false);

        assert_eq!(bool::parse("true").unwrap(), true);
        assert_eq!(bool::parse("false").unwrap(), false);
        assert_eq!(bool::parse("T").unwrap(), true);
        assert_eq!(bool::parse("F").unwrap(), false);
        assert_eq!(bool::parse("1").unwrap(), true);
        assert_eq!(bool::parse("0").unwrap(), false);
        assert!(bool::parse("maybe").is_err());

        assert_eq!(
            bool::try_from_value(Value::Json(Some(serde_json::json!(true)))).unwrap(),
            true
        );
        assert_eq!(
            bool::try_from_value(Value::Json(Some(serde_json::json!(0)))).unwrap(),
            false
        );
        assert_eq!(
            bool::try_from_value(Value::Json(Some(serde_json::json!(1)))).unwrap(),
            true
        );
    }

    #[test]
    fn decimal_from_various_types() {
        assert_eq!(
            Decimal::try_from_value(Value::Int8(Some(5))).unwrap(),
            Decimal::new(5, 0)
        );
        assert_eq!(
            Decimal::try_from_value(Value::UInt8(Some(10))).unwrap(),
            Decimal::new(10, 0)
        );
        assert_eq!(
            Decimal::try_from_value(Value::UInt16(Some(20))).unwrap(),
            Decimal::new(20, 0)
        );
        assert_eq!(
            Decimal::try_from_value(Value::UInt32(Some(30))).unwrap(),
            Decimal::new(30, 0)
        );
        assert_eq!(
            Decimal::try_from_value(Value::UInt64(Some(40))).unwrap(),
            Decimal::new(40, 0)
        );
        let large_u64: u64 = 10_000_000_000_000_000_000;
        let dec = Decimal::try_from_value(Value::UInt64(Some(large_u64))).unwrap();
        assert!(
            dec > Decimal::ZERO,
            "Decimal from large u64 should be positive, got: {dec}"
        );
        assert_eq!(dec.to_string(), large_u64.to_string());
        assert!(Decimal::try_from_value(Value::Float32(Some(1.5))).is_ok());
        assert!(Decimal::try_from_value(Value::Float64(Some(2.5))).is_ok());
        assert!(Decimal::try_from_value(Value::Varchar(Some("3.14".into()))).is_ok());
        assert!(Decimal::try_from_value(Value::Unknown(Some("1.23".into()))).is_ok());

        assert!(Decimal::try_from_value(Value::Json(Some(serde_json::json!(42.5)))).is_ok());
    }

    #[test]
    fn integer_from_json() {
        assert_eq!(
            i32::try_from_value(Value::Json(Some(serde_json::json!(42)))).unwrap(),
            42
        );
        assert_eq!(
            i32::try_from_value(Value::Json(Some(serde_json::json!("99")))).unwrap(),
            99
        );
        assert_eq!(
            i32::try_from_value(Value::Json(Some(serde_json::json!(5.0)))).unwrap(),
            5
        );
    }

    #[test]
    fn integer_from_json_boundary() {
        let json =
            |f: f64| serde_json::Value::Number(serde_json::Number::from_f64(f).unwrap()).as_value();
        assert!(i64::try_from_value(json(9_223_372_036_854_775_808.0)).is_err());
        assert!(i32::try_from_value(json(2_147_483_648.0)).is_err());
        assert!(i8::try_from_value(json(128.0)).is_err());
        assert!(u64::try_from_value(json(18_446_744_073_709_551_616.0)).is_err());
        assert!(u8::try_from_value(json(256.0)).is_err());
        assert!(
            i128::try_from_value(json(170_141_183_460_469_231_731_687_303_715_884_105_728.0))
                .is_err()
        );
        assert!(
            u128::try_from_value(json(340_282_366_920_938_463_463_374_607_431_768_211_456.0))
                .is_err()
        );
    }

    #[test]
    fn integer_from_varchar_and_unknown() {
        assert_eq!(
            i32::try_from_value(Value::Varchar(Some("42".into()))).unwrap(),
            42
        );
        assert_eq!(
            i32::try_from_value(Value::Unknown(Some("99".into()))).unwrap(),
            99
        );
    }

    #[test]
    fn integer_from_float64() {
        assert_eq!(i32::try_from_value(Value::Float64(Some(10.0))).unwrap(), 10);
        assert!(i32::try_from_value(Value::Float64(Some(10.5))).is_err());
    }

    #[test]
    fn integer_cross_type_conversions() {
        assert_eq!(i16::try_from_value(Value::Int8(Some(5))).unwrap(), 5);
        assert_eq!(i16::try_from_value(Value::UInt8(Some(200))).unwrap(), 200);
        assert_eq!(i16::try_from_value(Value::UInt16(Some(100))).unwrap(), 100);
        assert_eq!(i64::try_from_value(Value::UInt8(Some(1))).unwrap(), 1);
        assert_eq!(i64::try_from_value(Value::UInt16(Some(2))).unwrap(), 2);
        assert_eq!(i64::try_from_value(Value::UInt32(Some(3))).unwrap(), 3);
        assert_eq!(i64::try_from_value(Value::UInt64(Some(4))).unwrap(), 4);
        assert_eq!(i128::try_from_value(Value::UInt8(Some(1))).unwrap(), 1);
        assert_eq!(
            i128::try_from_value(Value::UInt128(Some(999))).unwrap(),
            999
        );
        assert_eq!(u64::try_from_value(Value::UInt8(Some(1))).unwrap(), 1);
        assert_eq!(u64::try_from_value(Value::UInt16(Some(2))).unwrap(), 2);
        assert_eq!(u64::try_from_value(Value::UInt32(Some(3))).unwrap(), 3);
        assert_eq!(u128::try_from_value(Value::UInt8(Some(1))).unwrap(), 1);
        assert_eq!(u128::try_from_value(Value::UInt64(Some(9))).unwrap(), 9);

        assert!(i8::try_from_value(Value::Int16(Some(200))).is_err());
        assert_eq!(i8::try_from_value(Value::Int16(Some(-128))).unwrap(), -128);
        assert_eq!(i8::try_from_value(Value::Int16(Some(127))).unwrap(), 127);
        assert!(i8::try_from_value(Value::Int16(Some(128))).is_err());
        assert!(i8::try_from_value(Value::Int16(Some(-129))).is_err());

        assert!(u16::try_from_value(Value::Int32(Some(70_000))).is_err());
        assert_eq!(
            u16::try_from_value(Value::Int32(Some(65_535))).unwrap(),
            65_535
        );
        assert!(u16::try_from_value(Value::Int32(Some(-1))).is_err());

        assert!(u8::try_from_value(Value::Int16(Some(256))).is_err());
        assert_eq!(u8::try_from_value(Value::Int16(Some(255))).unwrap(), 255);

        assert!(i64::try_from_value(Value::UInt64(Some(u64::MAX))).is_err());
        assert_eq!(
            i64::try_from_value(Value::UInt64(Some(i64::MAX as u64))).unwrap(),
            i64::MAX
        );

        assert!(i128::try_from_value(Value::UInt128(Some(u128::MAX))).is_err());
        assert_eq!(
            i128::try_from_value(Value::UInt128(Some(i128::MAX as u128))).unwrap(),
            i128::MAX
        );

        assert_eq!(i128::try_from_value(Value::UInt8(Some(255))).unwrap(), 255);
        assert_eq!(u8::try_from_value(Value::Int8(Some(42))).unwrap(), 42);
        assert!(u8::try_from_value(Value::Int8(Some(-1))).is_err());
    }

    #[test]
    fn signed_unsigned_boundaries_match_rust_tryfrom() {
        macro_rules! assert_same {
            ($target:ty, $value:expr) => {{
                let value = $value;
                let expected = <$target as TryFrom<_>>::try_from(match &value {
                    Value::Int8(Some(v), ..) => *v as i128,
                    Value::Int16(Some(v), ..) => *v as i128,
                    Value::Int32(Some(v), ..) => *v as i128,
                    Value::Int64(Some(v), ..) => *v as i128,
                    Value::Int128(Some(v), ..) => *v,
                    Value::UInt8(Some(v), ..) => *v as i128,
                    Value::UInt16(Some(v), ..) => *v as i128,
                    Value::UInt32(Some(v), ..) => *v as i128,
                    Value::UInt64(Some(v), ..) => *v as i128,
                    Value::UInt128(Some(v), ..) => {
                        if *v > i128::MAX as u128 {
                            i128::MAX
                        } else {
                            *v as i128
                        }
                    }
                    _ => unreachable!(),
                });
                let got = <$target as AsValue>::try_from_value(value);
                match (expected, got) {
                    (Ok(e), Ok(g)) => assert_eq!(e, g),
                    (Err(_), Err(_)) => {}
                    (e, g) => panic!("Mismatch: {e:?} vs {g:?}"),
                }
            }};
        }
        assert_same!(i8, Value::Int16(Some(128)));
        assert_same!(i8, Value::Int16(Some(-129)));
        assert_same!(i16, Value::Int32(Some(40_000)));
        assert_same!(i32, Value::Int64(Some(5_000_000_000)));
        assert_same!(u8, Value::Int16(Some(300)));
        assert_same!(u16, Value::Int32(Some(70_000)));
        assert_same!(u32, Value::Int64(Some(5_000_000_000)));
        assert_same!(i64, Value::Int128(Some(i64::MAX as i128 + 1)));
        assert_same!(i16, Value::UInt16(Some(40_000)));
        assert_same!(i32, Value::UInt32(Some(3_000_000_000)));
        assert_same!(i64, Value::UInt64(Some(u64::MAX)));
    }

    #[test]
    fn integer_decimal_conversions() {
        assert_eq!(
            i32::try_from_value(Value::Decimal(Some(Decimal::new(42, 0)), 0, 0)).unwrap(),
            42
        );
        assert!(i32::try_from_value(Value::Decimal(Some(Decimal::new(155, 1)), 0, 0)).is_err());
        assert_eq!(
            i64::try_from_value(Value::Decimal(Some(Decimal::new(100, 0)), 0, 0)).unwrap(),
            100
        );
        assert_eq!(
            u64::try_from_value(Value::Decimal(Some(Decimal::new(50, 0)), 0, 0)).unwrap(),
            50
        );
    }

    #[test]
    fn float_conversions() {
        assert!(
            f32::try_from_value(Value::Decimal(
                Some(rust_decimal::Decimal::new(15, 1)),
                0,
                0
            ))
            .is_ok()
        );
        assert_eq!(f32::try_from_value(Value::Float64(Some(2.5))).unwrap(), 2.5);
        assert_eq!(f64::try_from_value(Value::Float32(Some(1.5))).unwrap(), 1.5);
        assert!(
            f64::try_from_value(Value::Decimal(
                Some(rust_decimal::Decimal::new(25, 1)),
                0,
                0
            ))
            .is_ok()
        );
        assert!(f32::try_from_value(Value::Json(Some(serde_json::json!(1.5)))).is_ok());
        assert!(f64::try_from_value(Value::Json(Some(serde_json::json!(2.5)))).is_ok());
    }

    #[test]
    fn string_from_various_types() {
        assert_eq!(
            String::try_from_value(Value::Int32(Some(42))).unwrap(),
            "42"
        );
        assert_eq!(
            String::try_from_value(Value::Float64(Some(3.14))).unwrap(),
            "3.14"
        );
        assert_eq!(String::try_from_value(Value::Char(Some('x'))).unwrap(), "x");
        assert!(String::try_from_value(Value::Uuid(Some(uuid::Uuid::nil()))).is_ok());
        assert_eq!(
            String::try_from_value(Value::Json(Some(serde_json::json!("hi")))).unwrap(),
            "hi"
        );
    }

    #[test]
    fn char_conversions() {
        assert_eq!(
            char::try_from_value(Value::Varchar(Some("a".into()))).unwrap(),
            'a'
        );
        assert!(char::try_from_value(Value::Varchar(Some("ab".into()))).is_err());
        assert_eq!(
            char::try_from_value(Value::Json(Some(serde_json::json!("z")))).unwrap(),
            'z'
        );
        assert_eq!(
            char::try_from_value(Value::Varchar(Some("é".into()))).unwrap(),
            'é'
        );
        assert_eq!(
            char::try_from_value(Value::Varchar(Some("€".into()))).unwrap(),
            '€'
        );
        assert_eq!(
            char::try_from_value(Value::Json(Some(serde_json::json!("é")))).unwrap(),
            'é'
        );
        assert_eq!(char::parse("é").unwrap(), 'é');
    }

    #[test]
    fn blob_parse() {
        let v = Box::<[u8]>::try_from_value(Value::Varchar(Some("deadbeef".into()))).unwrap();
        assert_eq!(v.as_ref(), &[0xde, 0xad, 0xbe, 0xef]);
        let v2 = Box::<[u8]>::parse("\\xCAFE").unwrap();
        assert_eq!(v2.as_ref(), &[0xCA, 0xFE]);
    }

    #[test]
    fn interval_parse() {
        let i = Interval::parse("'1 year 2 months 3 days'").unwrap();
        assert_eq!(i.months, 14); // 12 + 2
        assert_eq!(i.days, 3);

        let i2 = Interval::parse("5 hours 30 minutes").unwrap();
        assert!(!i2.is_zero());

        let i3 = Interval::parse("01:30:00").unwrap();
        assert!(!i3.is_zero());
    }

    #[test]
    fn date_parse() {
        let d = <time::Date as AsValue>::parse("2024-06-15").unwrap();
        assert_eq!(
            d,
            time::Date::from_calendar_date(2024, time::Month::June, 15).unwrap()
        );

        let bc = <time::Date as AsValue>::parse("0044-03-15 BC").unwrap();
        assert!(bc.year() < 0);
        let ad = <time::Date as AsValue>::parse("2024-06-15 AD").unwrap();
        assert_eq!(ad, d);
        assert!(<time::Date as AsValue>::parse("2024-06-15 trailing").is_err());
        assert!(<time::Date as AsValue>::parse("not a date").is_err());

        let ts = time::PrimitiveDateTime::new(d, time::Time::MIDNIGHT);
        assert_eq!(
            time::Date::try_from_value(Value::Timestamp(Some(ts))).unwrap(),
            d
        );
    }

    #[test]
    fn time_parse() {
        let t = <time::Time as AsValue>::parse("14:30:00").unwrap();
        assert_eq!(t, time::Time::from_hms(14, 30, 0).unwrap());
        assert_eq!(
            <time::Time as AsValue>::parse("14:30").unwrap(),
            time::Time::from_hms(14, 30, 0).unwrap()
        );
        assert_eq!(
            <time::Time as AsValue>::parse("14:30:00.5").unwrap(),
            time::Time::from_hms_nano(14, 30, 0, 500_000_000).unwrap()
        );
        assert!(<time::Time as AsValue>::parse("14:30:00 trailing").is_err());
        assert!(
            time::Time::try_from_value(Value::Interval(Some(Interval::from_mins(-30)))).is_err()
        );
    }

    #[test]
    fn timestamp_parse() {
        let ts = <time::PrimitiveDateTime as AsValue>::parse("2024-06-15T14:30:00").unwrap();
        let d = time::Date::from_calendar_date(2024, time::Month::June, 15).unwrap();
        let t = time::Time::from_hms(14, 30, 0).unwrap();
        assert_eq!(ts, time::PrimitiveDateTime::new(d, t));
        assert_eq!(
            <time::PrimitiveDateTime as AsValue>::parse("2024-06-15 14:30").unwrap(),
            time::PrimitiveDateTime::new(d, time::Time::from_hms(14, 30, 0).unwrap())
        );
        assert_eq!(
            <time::PrimitiveDateTime as AsValue>::parse("2024-06-15T14:30:00.5").unwrap(),
            time::PrimitiveDateTime::new(
                d,
                time::Time::from_hms_nano(14, 30, 0, 500_000_000).unwrap()
            )
        );
        assert!(<time::PrimitiveDateTime as AsValue>::parse("2024-06-15T14:30:00 x").is_err());
    }

    #[test]
    fn offset_datetime_parse() {
        let odt = <time::OffsetDateTime as AsValue>::parse("2024-06-15T14:30:00+05:00").unwrap();
        assert_eq!(odt.offset().whole_hours(), 5);
        let utc = <time::OffsetDateTime as AsValue>::parse("2024-06-15T14:30:00").unwrap();
        assert_eq!(utc.offset().whole_seconds(), 0);
        assert!(<time::OffsetDateTime as AsValue>::parse("not a timestamp").is_err());
    }

    #[test]
    fn seconds_offset_datetime() {
        for (text, seconds) in [
            ("2024-06-15T14:30:00+00:19:32", 19 * 60 + 32),
            ("2024-06-15 14:30:00-00:00:45", -(45)),
            ("2024-06-15T14:30:00+00:19", 19 * 60),
        ] {
            let odt = <time::OffsetDateTime as AsValue>::parse(text)
                .unwrap_or_else(|e| panic!("Could not parse `{text}`: {e:#}"));
            assert_eq!(
                odt.offset().whole_seconds(),
                seconds,
                "Offset seconds were dropped parsing `{text}`"
            );
        }
    }

    #[test]
    fn uuid_from_varchar() {
        let u = uuid::Uuid::try_from_value(Value::Varchar(Some(
            "550e8400-e29b-41d4-a716-446655440000".into(),
        )))
        .unwrap();
        assert_eq!(u.to_string(), "550e8400-e29b-41d4-a716-446655440000");
    }

    #[test]
    fn time_from_interval() {
        let t = time::Time::try_from_value(Value::Interval(Some(
            Interval::from_hours(2) + Interval::from_mins(30),
        )))
        .unwrap();
        assert_eq!(t, time::Time::from_hms(2, 30, 0).unwrap());
    }

    #[test]
    fn date_from_timestamp() {
        let d = time::Date::from_calendar_date(2024, time::Month::January, 1).unwrap();
        let t = time::Time::MIDNIGHT;
        let ts = time::PrimitiveDateTime::new(d, t);
        let result = time::Date::try_from_value(Value::Timestamp(Some(ts))).unwrap();
        assert_eq!(result, d);

        let t2 = time::Time::from_hms(12, 0, 0).unwrap();
        let ts2 = time::PrimitiveDateTime::new(d, t2);
        assert!(time::Date::try_from_value(Value::Timestamp(Some(ts2))).is_err());
    }

    #[test]
    fn offset_datetime_from_timestamp() {
        let d = time::Date::from_calendar_date(2024, time::Month::January, 1).unwrap();
        let t = time::Time::from_hms(12, 0, 0).unwrap();
        let ts = time::PrimitiveDateTime::new(d, t);
        let odt = time::OffsetDateTime::try_from_value(Value::Timestamp(Some(ts))).unwrap();
        assert_eq!(odt.date(), d);
    }

    #[test]
    fn vec_and_list_conversions() {
        let v = vec![1_i32, 2, 3].as_value();
        let back: Vec<i32> = Vec::try_from_value(v).unwrap();
        assert_eq!(back, vec![1, 2, 3]);

        let v = Value::Json(Some(serde_json::json!([1, 2, 3])));
        let back: Vec<i32> = Vec::try_from_value(v).unwrap();
        assert_eq!(back, vec![1, 2, 3]);
    }

    #[test]
    fn array_conversions() {
        let v = [10_i32, 20, 30].as_value();
        let back: [i32; 3] = <[i32; 3]>::try_from_value(v).unwrap();
        assert_eq!(back, [10, 20, 30]);
    }

    #[test]
    fn hashmap_conversions() {
        let mut m = HashMap::new();
        m.insert("key".to_string(), 42_i32);
        let v = m.clone().as_value();
        let back: HashMap<String, i32> = HashMap::try_from_value(v).unwrap();
        assert_eq!(back, m);
    }

    #[test]
    fn option_and_wrapper_conversions() {
        assert_eq!(Some(42_i32).as_value(), Value::Int32(Some(42)));
        assert_eq!(None::<i32>.as_value(), Value::Int32(None));
        let back: Option<i32> = Option::try_from_value(Value::Int32(Some(42))).unwrap();
        assert_eq!(back, Some(42));
        let none: Option<i32> = Option::try_from_value(Value::Int32(None)).unwrap();
        assert_eq!(none, None);

        assert_eq!(Box::new(42_i32).as_value(), Value::Int32(Some(42)));
        let back: Box<i32> = Box::try_from_value(Value::Int32(Some(42))).unwrap();
        assert_eq!(*back, 42);

        assert_eq!(Arc::new(42_i32).as_value(), Value::Int32(Some(42)));

        assert_eq!(Rc::new(42_i32).as_value(), Value::Int32(Some(42)));
    }

    #[test]
    fn fixed_decimal_round_trip() {
        let fd: FixedDecimal<10, 2> = Decimal::new(1234, 2).into();
        let v = fd.as_value();
        let back: FixedDecimal<10, 2> = FixedDecimal::try_from_value(v).unwrap();
        assert_eq!(back.0, Decimal::new(1234, 2));
    }

    #[test]
    fn isize_usize_conversions() {
        assert_eq!(42_isize.as_value(), Value::Int64(Some(42)));
        let back: isize = isize::try_from_value(Value::Int64(Some(42))).unwrap();
        assert_eq!(back, 42);

        assert_eq!(42_usize.as_value(), Value::UInt64(Some(42)));
        let back: usize = usize::try_from_value(Value::UInt64(Some(42))).unwrap();
        assert_eq!(back, 42);

        // Unsigned sources wider than isize::MAX must not silently wrap around.
        // On 64-bit these only fit up to isize::MAX; on 32-bit even u32 values
        // above i32::MAX are out of range, so this asserts the checked path is
        // taken rather than an unchecked `as` cast.
        let over_isize = (isize::MAX as u128 + 1) as u64;
        assert!(isize::try_from_value(over_isize.as_value()).is_err());
        assert!(isize::try_from_value(u64::MAX.as_value()).is_err());
        assert_eq!(
            isize::try_from_value((isize::MAX as u64).as_value()).unwrap(),
            isize::MAX
        );
        // u16/u8 sources always fit into isize on supported targets.
        assert_eq!(isize::try_from_value(65535_u16.as_value()).unwrap(), 65535);
        assert_eq!(isize::try_from_value(255_u8.as_value()).unwrap(), 255);

        // usize is checked against u64 on 64-bit targets.
        assert_eq!(
            usize::try_from_value(u64::MAX.as_value()).unwrap(),
            usize::MAX
        );
        assert_eq!(usize::try_from_value(255_u8.as_value()).unwrap(), 255);
        assert_eq!(usize::try_from_value(65535_u16.as_value()).unwrap(), 65535);
    }

    #[test]
    fn array_from_varchar_chars() {
        let v = <[char; 3]>::try_from_value(Value::Varchar(Some("abc".into()))).unwrap();
        assert_eq!(v, ['a', 'b', 'c']);
        assert!(<[char; 2]>::try_from_value(Value::Varchar(Some("abc".into()))).is_err());
        let list = <[i32; 2]>::try_from_value(Value::List(
            Some(vec![Value::Int32(Some(1)), Value::Int32(Some(2))]),
            Box::new(Value::Int32(None)),
        ))
        .unwrap();
        assert_eq!(list, [1, 2]);
        let arr = <[i32; 2]>::try_from_value(Value::Array(
            Some(vec![Value::Int32(Some(1)), Value::Int32(Some(2))].into()),
            Box::new(Value::Int32(None)),
            2,
        ))
        .unwrap();
        assert_eq!(arr, [1, 2]);
        assert!(
            <[i32; 3]>::try_from_value(Value::List(
                Some(vec![Value::Int32(Some(1))]),
                Box::new(Value::Int32(None))
            ))
            .is_err()
        );
    }

    #[test]
    fn unknown_parse_paths() {
        assert_eq!(
            i32::try_from_value(Value::Unknown(Some("42".into()))).unwrap(),
            42
        );
        assert!(i32::try_from_value(Value::Unknown(Some("x".into()))).is_err());
        assert_eq!(
            bool::try_from_value(Value::Unknown(Some("true".into()))).unwrap(),
            true
        );
    }

    #[test]
    fn decimal_from_json_and_unknown() {
        let d = Decimal::try_from_value(Value::Json(Some(serde_json::json!(12.5)))).unwrap();
        assert_eq!(d, Decimal::new(125, 1));
        assert!(Decimal::try_from_value(Value::Json(Some(serde_json::json!("x")))).is_err());
        assert!(Decimal::try_from_value(Value::Boolean(Some(true))).is_err());

        assert_eq!(
            Decimal::try_from_value(Value::Unknown(Some("3.14".into()))).unwrap(),
            Decimal::new(314, 2)
        );
        assert_eq!(
            Decimal::try_from_value(Value::Varchar(Some("2.5".into()))).unwrap(),
            Decimal::new(25, 1)
        );
        assert_eq!(Decimal::parse("7.25").unwrap(), Decimal::new(725, 2));

        let fd = <FixedDecimal<10, 2> as AsValue>::parse("1.25").unwrap();
        assert_eq!(fd.0, Decimal::new(125, 2));
        assert_eq!(
            FixedDecimal::<10, 2>::as_empty_value(),
            Value::Decimal(None, 0, 0)
        );
    }

    #[test]
    fn decimal_from_float64_overflow() {
        assert!(Decimal::try_from_value(Value::Float64(Some(f64::NAN))).is_err());
        assert!(Decimal::try_from_value(Value::Float64(Some(f64::INFINITY))).is_err());
        assert_eq!(
            Decimal::try_from_value(Value::Float64(Some(2.0))).unwrap(),
            Decimal::new(2, 0)
        );
    }

    #[test]
    fn u128_and_i128_decimal_conversions() {
        assert_eq!(
            u128::try_from_value(Value::Decimal(Some(Decimal::new(7, 0)), 0, 0)).unwrap(),
            7
        );
        assert!(u128::try_from_value(Value::Decimal(Some(Decimal::new(15, 1)), 0, 0)).is_err());
        assert_eq!(
            i128::try_from_value(Value::Decimal(Some(Decimal::new(9, 0)), 0, 0)).unwrap(),
            9
        );
        assert!(isize::try_from_value(Value::Decimal(Some(Decimal::new(1, 1)), 0, 0)).is_err());
        assert_eq!(
            isize::try_from_value(Value::Decimal(Some(Decimal::new(11, 0)), 0, 0)).unwrap(),
            11
        );
        assert_eq!(
            usize::try_from_value(Value::Decimal(Some(Decimal::new(13, 0)), 0, 0)).unwrap(),
            13
        );
        assert!(u64::try_from_value(Value::Decimal(Some(Decimal::new(1, 1)), 0, 0)).is_err());
    }

    #[test]
    fn json_number_integer_boundaries() {
        assert!(i8::try_from_value(Value::Json(Some(serde_json::json!(1000)))).is_err());
        assert_eq!(
            i8::try_from_value(Value::Json(Some(serde_json::json!(-128)))).unwrap(),
            -128
        );
        assert_eq!(
            i16::try_from_value(Value::Json(Some(serde_json::json!("300")))).unwrap(),
            300
        );
    }

    #[test]
    fn blob_hex_decode_errors() {
        assert!(Box::<[u8]>::parse("not-hex").is_err());
    }

    #[test]
    fn map_from_json_object() {
        let json = serde_json::json!({"1": 10, "2": 20});
        let map: HashMap<i32, i32> = HashMap::try_from_value(Value::Json(Some(json))).unwrap();
        assert_eq!(map.get(&1), Some(&10));
        assert!(HashMap::<i32, i32>::try_from_value(Value::Int32(Some(1))).is_err());
    }

    #[test]
    fn json_value_conversion() {
        assert_eq!(
            serde_json::Value::try_from_value(Value::Json(None)).unwrap(),
            serde_json::Value::Null
        );
        assert_eq!(
            serde_json::Value::try_from_value(Value::Json(Some(serde_json::json!(1)))).unwrap(),
            serde_json::json!(1)
        );
        assert!(serde_json::Value::try_from_value(Value::Int32(Some(1))).is_err());
    }

    #[test]
    fn wrapper_cell_refcell_rwlock() {
        assert_eq!(Cell::new(5_i32).as_value(), Value::Int32(Some(5)));
        assert_eq!(RefCell::new(6_i32).as_value(), Value::Int32(Some(6)));
        assert_eq!(RwLock::new(7_i32).as_value(), Value::Int32(Some(7)));
        let back: Cell<i32> = Cell::try_from_value(Value::Int32(Some(8))).unwrap();
        assert_eq!(back.get(), 8);
        let back: RefCell<i32> = RefCell::try_from_value(Value::Int32(Some(9))).unwrap();
        assert_eq!(*back.borrow(), 9);
        let back: RwLock<i32> = RwLock::try_from_value(Value::Int32(Some(10))).unwrap();
        assert_eq!(*back.read().unwrap(), 10);
        assert_eq!(*RwLock::<i32>::parse("12").unwrap().read().unwrap(), 12);
    }

    #[test]
    fn static_str_and_cow_parse() {
        assert_eq!(<Cow<'static, str> as AsValue>::parse("hi").unwrap(), "hi");
        // Owned varchar cannot be assigned to &'static str.
        assert!(
            <&'static str>::try_from_value(Value::Varchar(Some(Cow::Owned("x".into())))).is_err()
        );
        assert_eq!(
            <&'static str>::try_from_value(Value::Varchar(Some(Cow::Borrowed("x")))).unwrap(),
            "x"
        );
    }

    #[test]
    fn value_to_tokens() {
        let tokens = |v: &Value| v.to_token_stream().to_string();
        assert_eq!(tokens(&Value::Null), ":: tank :: Value :: Null");
        assert!(tokens(&Value::Int32(Some(1))).contains("Int32"));
        assert!(
            tokens(&Value::Decimal(
                Some(rust_decimal::Decimal::new(1, 2)),
                10,
                2
            ))
            .contains("Decimal")
        );
        assert!(
            tokens(&Value::Array(None, Box::new(Value::Int32(None)), 3))
                .contains("Array (None , Box :: new")
        );
        assert!(tokens(&Value::List(None, Box::new(Value::Int32(None)))).contains("List"));
        assert!(
            tokens(&Value::Map(
                None,
                Box::new(Value::Varchar(None)),
                Box::new(Value::Int32(None))
            ))
            .contains("Map")
        );
        assert!(tokens(&Value::Json(None)).contains("Json"));
        assert!(
            tokens(&Value::Struct(
                None,
                vec![("a".to_string(), Value::Int32(None))],
                TableRef::new("t".into())
            ))
            .contains("Struct")
        );
        assert!(tokens(&Value::Unknown(None)).contains("Unknown"));
    }
}
