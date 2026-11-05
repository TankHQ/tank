#[cfg(test)]
mod tests {
    use syn::parse_str;
    use tank::{CollectionType, MapType, TypeDecoded, Value, ValueOwned, decode_type};

    fn decode(src: &str) -> TypeDecoded {
        decode_type(&parse_str(src).unwrap())
    }

    fn value(src: &str) -> ValueOwned {
        decode(src).value
    }

    #[test]
    fn scalar_paths() {
        for (src, expected) in [
            ("bool", Value::Boolean(None)),
            ("i8", Value::Int8(None)),
            ("i16", Value::Int16(None)),
            ("i32", Value::Int32(None)),
            ("i64", Value::Int64(None)),
            ("i128", Value::Int128(None)),
            ("u8", Value::UInt8(None)),
            ("u16", Value::UInt16(None)),
            ("u32", Value::UInt32(None)),
            ("u64", Value::UInt64(None)),
            ("u128", Value::UInt128(None)),
            ("f32", Value::Float32(None)),
            ("f64", Value::Float64(None)),
            ("char", Value::Char(None)),
            ("str", Value::Varchar(None)),
            ("String", Value::Varchar(None)),
            ("std::string::String", Value::Varchar(None)),
        ] {
            assert_eq!(value(src), expected, "decoding `{src}`");
            assert!(!decode(src).nullable, "`{src}` should not be nullable");
        }

        // `isize`/`usize` map to the platform-width integer.
        assert!(matches!(
            value("isize"),
            Value::Int32(None) | Value::Int64(None)
        ));
        assert!(matches!(
            value("usize"),
            Value::UInt32(None) | Value::UInt64(None)
        ));
        assert!(matches!(
            value("std::num::NonZeroIsize"),
            Value::Int32(None) | Value::Int64(None)
        ));
        assert!(matches!(
            value("std::num::NonZeroUsize"),
            Value::UInt32(None) | Value::UInt64(None)
        ));
    }

    #[test]
    fn nonzero_paths() {
        for (src, expected) in [
            ("std::num::NonZeroI8", Value::Int8(None)),
            ("std::num::NonZeroI16", Value::Int16(None)),
            ("std::num::NonZeroI32", Value::Int32(None)),
            ("std::num::NonZeroI64", Value::Int64(None)),
            ("std::num::NonZeroI128", Value::Int128(None)),
            ("std::num::NonZeroU8", Value::UInt8(None)),
            ("std::num::NonZeroU16", Value::UInt16(None)),
            ("std::num::NonZeroU32", Value::UInt32(None)),
            ("std::num::NonZeroU64", Value::UInt64(None)),
            ("std::num::NonZeroU128", Value::UInt128(None)),
        ] {
            assert_eq!(value(src), expected, "decoding `{src}`");
        }
    }

    #[test]
    fn temporal_and_other_paths() {
        for (src, expected) in [
            ("rust_decimal::Decimal", Value::Decimal(None, 0, 0)),
            ("time::Time", Value::Time(None)),
            ("chrono::NaiveTime", Value::Time(None)),
            ("time::Date", Value::Date(None)),
            ("chrono::NaiveDate", Value::Date(None)),
            ("time::PrimitiveDateTime", Value::Timestamp(None)),
            ("time::UtcDateTime", Value::Timestamp(None)),
            ("chrono::NaiveDateTime", Value::Timestamp(None)),
            ("time::OffsetDateTime", Value::TimestampWithTimezone(None)),
            ("chrono::DateTime", Value::TimestampWithTimezone(None)),
            ("std::time::Duration", Value::Interval(None)),
            ("tank::Interval", Value::Interval(None)),
            ("uuid::Uuid", Value::Uuid(None)),
        ] {
            assert_eq!(value(src), expected, "decoding `{src}`");
        }
    }

    #[test]
    fn fixed_decimal_uses_generic_constants() {
        assert_eq!(
            value("tank::FixedDecimal<18, 4>"),
            Value::Decimal(None, 18, 4)
        );
        assert_eq!(
            value("tank::FixedDecimal< 38 , 30 >"),
            Value::Decimal(None, 38, 30)
        );
    }

    #[test]
    fn wrappers_propagate_nullability() {
        assert_eq!(value("Option<i32>"), Value::Int32(None));
        assert!(decode("Option<i32>").nullable);
        assert_eq!(value("std::option::Option<u8>"), Value::UInt8(None));
        assert!(decode("std::option::Option<u8>").nullable);

        // A non-Option wrapper of an Option keeps the inner nullability.
        assert_eq!(value("Box<Option<i32>>"), Value::Int32(None));
        assert!(decode("Box<Option<i32>>").nullable);
        assert_eq!(value("Box<i32>"), Value::Int32(None));
        assert!(!decode("Box<i32>").nullable);

        for wrapper in [
            "std::boxed::Box<i32>",
            "std::cell::Cell<i32>",
            "std::cell::RefCell<i32>",
            "std::rc::Rc<i32>",
            "std::sync::Arc<i32>",
            "std::sync::RwLock<i32>",
        ] {
            assert_eq!(value(wrapper), Value::Int32(None), "decoding `{wrapper}`");
            assert!(!decode(wrapper).nullable);
        }

        // `Cow`'s first type argument is `str`, not the lifetime.
        assert_eq!(
            value("std::borrow::Cow<'static, str>"),
            Value::Varchar(None)
        );
        assert!(!decode("std::borrow::Cow<'static, str>").nullable);
    }

    #[test]
    fn lists_and_maps() {
        for list in [
            "Vec<i32>",
            "std::vec::Vec<i32>",
            "std::collections::VecDeque<i32>",
            "std::collections::LinkedList<i32>",
        ] {
            assert_eq!(
                value(list),
                Value::List(
                    None,
                    Box::new(CollectionType {
                        element: Value::Int32(None),
                        len: 0,
                    })
                ),
                "decoding `{list}`"
            );
        }

        for map in [
            "std::collections::HashMap<String, i32>",
            "std::collections::BTreeMap<String, u8>",
        ] {
            let decoded = value(map);
            let Value::Map(_, ty) = &decoded else {
                panic!("expected a map for `{map}`, got {decoded:?}");
            };
            assert!(matches!(ty.key, Value::Varchar(..)));
            assert!(matches!(ty.value, Value::Int32(..) | Value::UInt8(..)));

            // Nested maps/lists exercise the recursive path.
            assert!(matches!(value(&format!("{map}")), Value::Map(..)));
        }
        assert_eq!(
            value("std::collections::HashMap<i32, Vec<u8>>"),
            Value::Map(
                None,
                Box::new(MapType {
                    key: Value::Int32(None),
                    value: Value::List(
                        None,
                        Box::new(CollectionType {
                            element: Value::UInt8(None),
                            len: 0,
                        })
                    ),
                })
            )
        );
    }

    #[test]
    fn arrays_and_slices() {
        assert_eq!(
            value("[i32; 3]"),
            Value::Array(
                None,
                Box::new(CollectionType {
                    element: Value::Int32(None),
                    len: 3,
                })
            )
        );
        // A slice of `u8` is a blob; any other slice is a list.
        assert_eq!(value("[u8]"), Value::Blob(None));
        assert_eq!(
            value("[i32]"),
            Value::List(
                None,
                Box::new(CollectionType {
                    element: Value::Int32(None),
                    len: 0,
                })
            )
        );
        // An array of nullable u8 is not a blob.
        assert!(matches!(value("[Option<u8>; 2]"), Value::Array(..)));
    }

    #[test]
    fn unknown_paths_fall_back_to_unknown() {
        assert_eq!(value("some::CustomType"), Value::Unknown(None));
        assert_eq!(value("MyStruct"), Value::Unknown(None));
    }
}

#[cfg(test)]
mod panic_tests {
    use syn::parse_str;
    use tank::decode_type;

    fn decode_panics(src: &str) {
        let ty = parse_str(src).unwrap();
        let result = std::panic::catch_unwind(|| decode_type(&ty));
        assert!(result.is_err(), "decoding `{src}` should panic");
    }

    #[test]
    fn malformed_generics_panic() {
        // `FixedDecimal` without the two const generics.
        decode_panics("tank::FixedDecimal");
        // A const generic that is not an integer literal.
        decode_panics("tank::FixedDecimal<A, B>");
        // A map with a non-type second argument.
        decode_panics("std::collections::HashMap<i32, 3>");
        // A wrapper whose first generic argument is a const/lifetime, not a type.
        decode_panics("std::vec::Vec<3>");
        // A wrapper with no generic arguments at all.
        decode_panics("std::vec::Vec");
        // A type that is neither a path, array nor slice.
        decode_panics("*const i32");
    }
}
