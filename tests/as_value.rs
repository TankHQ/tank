#[cfg(test)]
mod tests {
    use quote::ToTokens;
    use rust_decimal::Decimal;
    use std::{
        borrow::Cow,
        cell::{Cell, RefCell},
        collections::{BTreeMap, HashMap, LinkedList, VecDeque},
        num::*,
        rc::Rc,
        sync::{Arc, RwLock},
    };
    use tank::{AsValue, CollectionType, MapType, StructType, TableRef, Value, ValueOwned};
    use tank_core::{FixedDecimal, Interval};

    #[test]
    fn borrowed_integer_conversions_match_owned_conversions() {
        fn check<T: AsValue + std::fmt::Debug + PartialEq>(values: &[Value<'_>]) {
            for value in values {
                let owned = T::try_from_value(value.clone());
                let borrowed = T::try_from_value(value);
                match (owned, borrowed) {
                    (Ok(owned), Ok(borrowed)) => assert_eq!(owned, borrowed),
                    (Err(owned), Err(borrowed)) => {
                        assert_eq!(owned.to_string(), borrowed.to_string())
                    }
                    other => panic!("Borrowed/owned conversion mismatch: {other:?}"),
                }
            }
        }

        let values = [
            i8::MIN.as_value(),
            i16::MIN.as_value(),
            i32::MIN.as_value(),
            i64::MIN.as_value(),
            i128::MIN.as_value(),
            u8::MAX.as_value(),
            u16::MAX.as_value(),
            u32::MAX.as_value(),
            u64::MAX.as_value(),
            u128::MAX.as_value(),
            42_i32.as_value(),
            "123".as_value(),
            "invalid".as_value(),
            Value::Null,
            Value::Int32(None),
        ];
        check::<i8>(&values);
        check::<i16>(&values);
        check::<i32>(&values);
        check::<i64>(&values);
        check::<i128>(&values);
        check::<isize>(&values);
        check::<u8>(&values);
        check::<u16>(&values);
        check::<u32>(&values);
        check::<u64>(&values);
        check::<u128>(&values);
        check::<usize>(&values);
        check::<Option<i32>>(&values);
    }

    fn assert_borrowed_string(value: Value<'_>, expected: &str) {
        let Value::Varchar(Some(Cow::Borrowed(actual))) = value else {
            panic!("Expected borrowed varchar, got {value:?}");
        };
        assert_eq!(actual, expected);
        assert_eq!(actual.as_ptr(), expected.as_ptr());
    }

    #[test]
    fn nonstatic_strings_and_cows() {
        let text = String::from("nonstatic");
        assert_borrowed_string(text.as_str().as_value(), &text);
        assert_borrowed_string((&text).as_value(), &text);
        let cow = Cow::Borrowed(text.as_str());
        assert_borrowed_string(cow.clone().as_value(), &text);
        assert_borrowed_string((&cow).as_value(), &text);
        let owned_cow: Cow<'_, str> = Cow::Owned(text.clone());
        assert_borrowed_string((&owned_cow).as_value(), &owned_cow);

        fn identity<'v>(value: Value<'v>) -> Value<'v> {
            value.as_value()
        }
        assert_borrowed_string(identity(text.as_str().as_value()), &text);
        let decoded: Cow<'_, str> = Cow::try_from_value(&text).unwrap();
        assert!(matches!(decoded, Cow::Owned(_)));
        assert_eq!(decoded, text);
        let parsed: Cow<'_, str> = <Cow<'_, str> as AsValue>::parse(&text).unwrap();
        assert!(matches!(parsed, Cow::Owned(_)));
    }

    #[test]
    fn owned_string_buffers_move_through_conversions() {
        let text = String::from("move encoding and decoding without reallocating");
        let pointer = text.as_ptr();
        let value = text.as_value();
        let Value::Varchar(Some(Cow::Owned(ref text))) = value else {
            panic!("Expected owned varchar");
        };
        assert_eq!(text.as_ptr(), pointer);
        let decoded = String::try_from_value(value).unwrap();
        assert_eq!(decoded.as_ptr(), pointer);
        let decoded = String::try_from_value(decoded).unwrap();
        assert_eq!(decoded.as_ptr(), pointer);

        let value: Cow<'_, Value<'_>> = Cow::Owned(decoded.as_value());
        let decoded = String::try_from_value(value).unwrap();
        assert_eq!(decoded.as_ptr(), pointer);
        let value = decoded.as_value();
        let borrowed: Cow<'_, Value<'_>> = Cow::Borrowed(&value);
        let Value::Varchar(Some(ref text)) = value else {
            panic!("Expected varchar");
        };
        assert_borrowed_string(borrowed.as_value(), text);
        let decoded: ValueOwned = Value::try_from_value(&value).unwrap();
        assert!(matches!(decoded, Value::Varchar(Some(Cow::Owned(_)))));
        assert_eq!(decoded, value);
        assert_eq!(
            String::try_from_value(Cow::Borrowed(&value)).unwrap(),
            "move encoding and decoding without reallocating"
        );
    }

    #[test]
    fn nested_reference_encodings_borrow_payloads() {
        let text = String::from("nested");
        assert_borrowed_string((&&&text).as_value(), &text);
        assert_borrowed_string((&&text.as_str()).as_value(), &text);
        let optional = Some(text.clone());
        assert_borrowed_string((&optional).as_value(), optional.as_deref().unwrap());
        let boxed = Box::new(text.clone());
        assert_borrowed_string((&boxed).as_value(), &boxed);
        let rc = Rc::new(text.clone());
        assert_borrowed_string((&rc).as_value(), &rc);
        let arc = Arc::new(text.clone());
        assert_borrowed_string((&arc).as_value(), &arc);

        let values = vec![text.clone()];
        let Value::List(Some(encoded), _) = (&values).as_value() else {
            panic!("Expected list");
        };
        assert_borrowed_string(encoded.into_iter().next().unwrap(), &values[0]);
        let values = [text.clone()];
        let Value::Array(Some(encoded), ..) = (&values).as_value() else {
            panic!("Expected array");
        };
        assert_borrowed_string(encoded.into_vec().pop().unwrap(), &values[0]);
        let nested = vec![vec![&text]];
        assert_eq!(
            Vec::<Vec<String>>::try_from_value(&nested).unwrap(),
            vec![vec![text.clone()]]
        );
        assert_eq!(<[String; 1]>::try_from_value(&values).unwrap(), values);

        let map = HashMap::from([(text.clone(), text.clone())]);
        let Value::Map(Some(encoded), _) = (&map).as_value() else {
            panic!("Expected map");
        };
        let (key, value) = map.iter().next().unwrap();
        let (encoded_key, encoded_value) = encoded.into_iter().next().unwrap();
        assert_borrowed_string(encoded_key, key);
        assert_borrowed_string(encoded_value, value);
        let tree = BTreeMap::from([(text.clone(), text.clone())]);
        assert_eq!(
            BTreeMap::<String, String>::try_from_value(&tree).unwrap(),
            tree
        );
        let deque = VecDeque::from([text.clone()]);
        assert_eq!(VecDeque::<String>::try_from_value(&deque).unwrap(), deque);
        let list = LinkedList::from([text.clone()]);
        assert_eq!(LinkedList::<String>::try_from_value(&list).unwrap(), list);
        assert_eq!(Vec::<String>::try_from_value(&values[..]).unwrap(), values);
    }

    #[test]
    fn reference_wrappers_copy_or_own_guarded_data() {
        let cell = Cell::new(42);
        assert_eq!((&cell).as_value(), Value::Int32(Some(42)));
        let text = String::from("guarded");
        let reference_cell = Cell::new(text.as_str());
        assert_borrowed_string((&reference_cell).as_value(), &text);
        let cell = RefCell::new(text.as_str());
        let value = (&cell).as_value();
        assert!(matches!(value, Value::Varchar(Some(Cow::Owned(_)))));
        cell.replace("changed");
        assert_eq!(String::try_from_value(value).unwrap(), text);
        let lock = RwLock::new(text.as_str());
        let value = (&lock).as_value();
        assert!(matches!(value, Value::Varchar(Some(Cow::Owned(_)))));
        *lock.write().unwrap() = "changed";
        assert_eq!(String::try_from_value(value).unwrap(), text);
    }

    #[test]
    fn byte_containers_keep_list_and_blob_distinct() {
        let bytes = vec![1_u8, 2, 3];
        assert!(matches!((&bytes).as_value(), Value::List(..)));
        assert!(matches!(bytes.as_value(), Value::List(..)));
        let bytes = vec![1_u8, 2, 3].into_boxed_slice();
        let Value::Blob(Some(Cow::Borrowed(encoded))) = (&bytes).as_value() else {
            panic!("Expected borrowed blob");
        };
        assert_eq!(encoded.as_ptr(), bytes.as_ptr());
        assert!(matches!(bytes.as_value(), Value::Blob(Some(Cow::Owned(_)))));
    }

    #[test]
    fn owned_targets_decode_borrowed_sources() {
        let text = String::from("borrowed source");
        let decoded = String::try_from_value(&text).unwrap();
        assert_eq!(decoded, text);
        assert_ne!(decoded.as_ptr(), text.as_ptr());
        drop(text);
        let value = decoded.as_value();
        let decoded = String::try_from_value(&value).unwrap();
        assert_eq!(decoded, "borrowed source");
        let Value::Varchar(Some(text)) = &value else {
            panic!("Expected varchar");
        };
        assert_ne!(decoded.as_ptr(), text.as_ptr());
        drop(value);
        assert_eq!(decoded, "borrowed source");

        let number = 42_i32;
        assert_eq!(i32::try_from_value(&number).unwrap(), number);
    }

    #[test]
    fn decoding_reference_targets_returns_clear_errors() {
        fn check<T: AsValue>() {
            let error = T::try_from_value("temporary".to_string()).err().unwrap();
            assert_eq!(
                error.to_string(),
                format!(
                    "Decoding into {} is not implemented",
                    std::any::type_name::<T>()
                )
            );
        }
        check::<&str>();
        check::<&String>();
        check::<&Value<'_>>();
        check::<&i32>();
        check::<&&str>();
        check::<&Option<String>>();
        check::<&Vec<String>>();
        check::<&Box<[u8]>>();
        check::<&serde_json::Value>();
        check::<&Cow<'_, str>>();
        check::<&Cow<'_, Value<'_>>>();
        check::<&[String; 1]>();
        check::<&[String]>();
        check::<&Cell<i32>>();
        check::<&RefCell<String>>();
        check::<&RwLock<String>>();
    }

    #[test]
    fn encode_only_owned_types_use_default_decoder() {
        struct InputOnly(i32);

        impl AsValue for InputOnly {
            fn as_empty_value() -> ValueOwned {
                i32::as_empty_value()
            }

            fn as_value<'a>(self) -> Value<'a>
            where
                Self: 'a,
            {
                self.0.as_value()
            }
        }

        assert_eq!(i32::try_from_value(InputOnly(42)).unwrap(), 42);
        let error = InputOnly::try_from_value(42).err().unwrap();
        assert_eq!(
            error.to_string(),
            format!(
                "Decoding into {} is not implemented",
                std::any::type_name::<InputOnly>()
            )
        );
    }

    #[test]
    fn reborrow_recursively_preserves_container_payloads_and_metadata() {
        fn check(original: &Value<'_>, borrowed: &Value<'_>) {
            assert_eq!(original, borrowed);
            match (original, borrowed) {
                (Value::Varchar(Some(a)), Value::Varchar(Some(Cow::Borrowed(b))))
                | (Value::Unknown(Some(a)), Value::Unknown(Some(Cow::Borrowed(b)))) => {
                    assert_eq!(a.as_ptr(), b.as_ptr());
                }
                (Value::Blob(Some(a)), Value::Blob(Some(Cow::Borrowed(b)))) => {
                    assert_eq!(a.as_ptr(), b.as_ptr());
                }
                (Value::Varchar(Some(_)) | Value::Unknown(Some(_)) | Value::Blob(Some(_)), _) => {
                    panic!("Payload was not reborrowed");
                }
                (Value::Array(a, a_t), Value::Array(b, b_t)) => {
                    check(&a_t.element, &b_t.element);
                    if let (Some(a), Some(b)) = (a, b) {
                        for (a, b) in a.iter().zip(b.iter()) {
                            check(a, b);
                        }
                    }
                }
                (Value::List(a, a_t), Value::List(b, b_t)) => {
                    check(&a_t.element, &b_t.element);
                    if let (Some(a), Some(b)) = (a, b) {
                        for (a, b) in a.iter().zip(b.iter()) {
                            check(a, b);
                        }
                    }
                }
                (Value::Map(a, a_t), Value::Map(b, b_t)) => {
                    check(&a_t.key, &b_t.key);
                    check(&a_t.value, &b_t.value);
                    if let (Some(a), Some(b)) = (a, b) {
                        for (key, value) in a.iter() {
                            let (bk, bv) = b.get_key_value(key).unwrap();
                            check(key, bk);
                            check(value, bv);
                        }
                    }
                }
                (Value::Struct(a, a_t), Value::Struct(b, b_t)) => {
                    for ((_, a), (_, b)) in a_t.types.iter().zip(&b_t.types) {
                        check(a, b);
                    }
                    if let (Some(a), Some(b)) = (a, b) {
                        for ((_, a), (_, b)) in a.iter().zip(b) {
                            check(a, b);
                        }
                    }
                }
                _ => {}
            }
        }
        let text = String::from("metadata and payload");
        let inner = Value::List(
            Some(
                vec![
                    text.clone().as_value(),
                    Value::Blob(Some(Cow::Owned(vec![1, 2]))),
                ]
                .into(),
            ),
            Box::new(CollectionType {
                element: Value::Unknown(Some(Cow::Owned(text.clone()))),
                len: 0,
            }),
        );
        let map = Value::Map(
            Some(Box::new(HashMap::from([(
                text.clone().as_value(),
                inner.clone(),
            )]))),
            Box::new(MapType {
                key: text.clone().as_value(),
                value: inner.clone(),
            }),
        );
        let array = Value::Array(
            Some(vec![map.clone()].into()),
            Box::new(CollectionType {
                element: map,
                len: 1,
            }),
        );
        let value = Value::Struct(
            Some(vec![("field".into(), array.clone())].into()),
            Box::new(StructType {
                types: vec![("field".into(), array)],
                name: TableRef::new("custom_type".into()),
            }),
        );
        check(&value, &(&value).as_value());
        let cow: Cow<'_, Value<'_>> = Cow::Borrowed(&value);
        check(&value, &cow.as_value());
        let borrowed_value = text.as_str().as_value();
        check(&borrowed_value, &(&borrowed_value).as_value());
        for value in [
            Value::Null,
            Value::Int32(Some(42)),
            Value::Decimal(Some(Decimal::ONE), 10, 2),
            Value::Json(Some(serde_json::json!({"x": [1, 2]}))),
            Value::Array(
                None,
                Box::new(CollectionType {
                    element: inner.clone(),
                    len: 2,
                }),
            ),
            Value::List(
                None,
                Box::new(CollectionType {
                    element: inner.clone(),
                    len: 0,
                }),
            ),
            Value::Map(
                None,
                Box::new(MapType {
                    key: inner.clone(),
                    value: inner,
                }),
            ),
            Value::Varchar(None),
            Value::Blob(None),
            Value::Unknown(None),
        ] {
            check(&value, &(&value).as_value());
        }
    }

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
        assert_eq!(
            bool::try_from_value(Value::UInt128(Some(Box::new(1)))).unwrap(),
            true
        );
        assert_eq!(
            bool::try_from_value(Value::Int128(Some(Box::new(0)))).unwrap(),
            false
        );

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
            i128::try_from_value(Value::UInt128(Some(Box::new(999)))).unwrap(),
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

        assert!(i128::try_from_value(Value::UInt128(Some(Box::new(u128::MAX)))).is_err());
        assert_eq!(
            i128::try_from_value(Value::UInt128(Some(Box::new(i128::MAX as u128)))).unwrap(),
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
                    Value::Int128(Some(v), ..) => **v,
                    Value::UInt8(Some(v), ..) => *v as i128,
                    Value::UInt16(Some(v), ..) => *v as i128,
                    Value::UInt32(Some(v), ..) => *v as i128,
                    Value::UInt64(Some(v), ..) => *v as i128,
                    Value::UInt128(Some(v), ..) => {
                        if **v > i128::MAX as u128 {
                            i128::MAX
                        } else {
                            **v as i128
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
        assert_same!(i64, Value::Int128(Some(Box::new(i64::MAX as i128 + 1))));
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
            Some(vec![Value::Int32(Some(1)), Value::Int32(Some(2))].into()),
            Box::new(CollectionType {
                element: Value::Int32(None),
                len: 0,
            }),
        ))
        .unwrap();
        assert_eq!(list, [1, 2]);
        let arr = <[i32; 2]>::try_from_value(Value::Array(
            Some(vec![Value::Int32(Some(1)), Value::Int32(Some(2))].into()),
            Box::new(CollectionType {
                element: Value::Int32(None),
                len: 2,
            }),
        ))
        .unwrap();
        assert_eq!(arr, [1, 2]);
        assert!(
            <[i32; 3]>::try_from_value(Value::List(
                Some(vec![Value::Int32(Some(1))].into()),
                Box::new(CollectionType {
                    element: Value::Int32(None),
                    len: 0
                })
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
        // `&'static str` can never be decoded from a database value.
        // The payload would have to borrow from the row buffer, whose lifetime is not `'static`.
        assert!(
            <&'static str>::try_from_value(Value::Varchar(Some(Cow::Owned("x".into())))).is_err()
        );
        assert!(<&'static str>::try_from_value(Value::Varchar(Some(Cow::Borrowed("x")))).is_err());
    }

    #[test]
    fn value_to_tokens() {
        let tokens = |v: &ValueOwned| v.to_token_stream().to_string();
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
            tokens(&Value::Array(
                None,
                Box::new(CollectionType {
                    element: Value::Int32(None),
                    len: 3
                })
            ))
            .contains("Array (None , Box :: new")
        );
        assert!(
            tokens(&Value::List(
                None,
                Box::new(CollectionType {
                    element: Value::Int32(None),
                    len: 0
                })
            ))
            .contains("List")
        );
        assert!(
            tokens(&Value::Map(
                None,
                Box::new(MapType {
                    key: Value::Varchar(None),
                    value: Value::Int32(None)
                })
            ))
            .contains("Map")
        );
        assert!(tokens(&Value::Json(None)).contains("Json"));
        assert!(
            tokens(&Value::Struct(
                None,
                Box::new(StructType {
                    types: vec![("a".to_string(), Value::Int32(None))],
                    name: TableRef::new("t".into())
                })
            ))
            .contains("Struct")
        );
        assert!(tokens(&Value::Unknown(None)).contains("Unknown"));
    }
}
