#[cfg(test)]
mod tests {
    use tank::RowOwned;

    // #region method-conversion
    use http::Method;
    use tank::{AsValue, Entity, Error, ErrorContext, Result, Value, ValueOwned};

    #[derive(Entity)]
    pub struct Request {
        #[tank(primary_key)]
        pub id: i64,
        pub target: String,
        #[tank(conversion_type = MethodWrap)]
        pub method: Method,
        pub begin_timestamp_ms: i64,
        pub end_timestamp_ms: Option<i64>,
    }

    #[derive(Entity)]
    pub struct RequestLimit {
        #[tank(primary_key)]
        pub id: i64,
        pub target_pattern: String,
        pub requests: u32,
        #[tank(conversion_type = MethodWrap::<Option<Method>>)]
        pub method: Option<Method>,
        pub interval_ms: Option<u32>,
    }

    pub struct MethodWrap<T = Method>(T);
    impl<T> AsValue for MethodWrap<T>
    where
        T: Into<Option<Method>>,
        Self: TryFrom<Option<Method>, Error: Into<Error>>,
    {
        fn as_empty_value() -> ValueOwned {
            Value::Varchar(None)
        }

        fn as_value<'a>(self) -> Value<'a>
        where
            Self: 'a,
        {
            self.0.into().map(|method| method.to_string()).as_value()
        }

        fn try_from_value(value: impl AsValue) -> Result<Self> {
            let method = Option::<String>::try_from_value(value)?
                .map(|text| {
                    text.parse::<Method>()
                        .with_context(|| format!("Could not parse HTTP method {text:?}"))
                })
                .transpose()?;
            Self::try_from(method).map_err(Into::into)
        }
    }

    impl TryFrom<Option<Method>> for MethodWrap {
        type Error = Error;

        fn try_from(value: Option<Method>) -> Result<Self> {
            value.context("HTTP method cannot be NULL").map(Self)
        }
    }

    impl<T> From<T> for MethodWrap<T> {
        fn from(value: T) -> Self {
            Self(value)
        }
    }

    impl From<MethodWrap> for Method {
        fn from(value: MethodWrap) -> Self {
            value.0
        }
    }

    impl From<MethodWrap<Option<Method>>> for Option<Method> {
        fn from(value: MethodWrap<Option<Method>>) -> Self {
            value.0
        }
    }
    // #endregion method-conversion

    #[test]
    fn method_round_trips() -> Result<()> {
        for text in ["GET", "POST", "CUSTOM"] {
            let method: Method = <MethodWrap as AsValue>::try_from_value(text)?.into();
            assert_eq!(method.as_str(), text);
            assert_eq!(MethodWrap::from(method).as_value(), text.as_value());

            let method: Option<Method> = MethodWrap::<Option<Method>>::try_from_value(text)?.into();
            assert_eq!(method.as_ref().map(Method::as_str), Some(text));
            assert_eq!(MethodWrap::from(method).as_value(), text.as_value());
        }
        assert_eq!(
            MethodWrap::from(None::<Method>).as_value(),
            Value::Varchar(None)
        );
        Ok(())
    }

    #[test]
    fn invalid_methods_and_nulls() -> Result<()> {
        for text in ["", "not a method", "GET\n"] {
            assert!(<MethodWrap as AsValue>::try_from_value(text).is_err());
            assert!(MethodWrap::<Option<Method>>::try_from_value(text).is_err());
        }
        for value in [Value::Null, Value::Varchar(None), Value::Int64(None)] {
            assert!(<MethodWrap as AsValue>::try_from_value(value.clone()).is_err());
            let method: Option<Method> =
                MethodWrap::<Option<Method>>::try_from_value(value)?.into();
            assert_eq!(method, None);
        }
        Ok(())
    }

    #[test]
    fn entity_encoding_and_nullability() -> Result<()> {
        let required = Request {
            id: 1,
            target: "/health".into(),
            method: Method::GET,
            begin_timestamp_ms: 100,
            end_timestamp_ms: Some(200),
        };
        let values = required.row_values();
        assert_eq!(values[2], "GET".as_value());
        assert_eq!(Request::from_row(required.row())?.row_values(), values);

        for method in [Some(Method::POST), None] {
            let expected = method.as_ref().map(Method::as_str).as_value();
            let optional = RequestLimit {
                id: 1,
                target_pattern: "/health".into(),
                requests: 5,
                method: method.clone(),
                interval_ms: Some(1000),
            };
            let values = optional.row_values();
            assert_eq!(values[3], expected);
            assert_eq!(RequestLimit::from_row(optional.row())?.row_values(), values);
        }
        assert_eq!(Request::columns()[2].value, Value::Varchar(None));
        assert!(!Request::columns()[2].nullable);
        assert_eq!(RequestLimit::columns()[3].value, Value::Varchar(None));
        assert!(RequestLimit::columns()[3].nullable);
        Ok(())
    }

    #[test]
    fn entity_decoding_ordered_and_reordered() -> Result<()> {
        for ordered in [true, false] {
            for value in [
                "GET".as_value(),
                "POST".as_value(),
                "CUSTOM".as_value(),
                "".as_value(),
                "not a method".as_value(),
                "GET\n".as_value(),
                Value::Null,
                Value::Varchar(None),
                Value::Int64(None),
            ] {
                let mut required = RowOwned::new(
                    Request::columns()
                        .iter()
                        .map(|column| column.name().to_string())
                        .collect(),
                    vec![
                        1_i64.as_value(),
                        "/health".as_value(),
                        value.clone(),
                        100_i64.as_value(),
                        Some(200_i64).as_value(),
                    ],
                );
                let mut optional = RowOwned::new(
                    RequestLimit::columns()
                        .iter()
                        .map(|column| column.name().to_string())
                        .collect(),
                    vec![
                        1_i64.as_value(),
                        "/health".as_value(),
                        5_u32.as_value(),
                        value.clone(),
                        Some(1000_u32).as_value(),
                    ],
                );
                if !ordered {
                    required.labels = required.labels.iter().cloned().rev().collect();
                    required.values.reverse();
                    optional.labels = optional.labels.iter().cloned().rev().collect();
                    optional.values.reverse();
                }
                let required = Request::from_row(required);
                let optional = RequestLimit::from_row(optional);
                match value {
                    Value::Varchar(Some(text))
                        if ["GET", "POST", "CUSTOM"].contains(&text.as_ref()) =>
                    {
                        assert_eq!(required?.method.as_str(), text.as_ref());
                        assert_eq!(
                            optional?.method.as_ref().map(Method::as_str),
                            Some(text.as_ref())
                        );
                    }
                    Value::Null | Value::Varchar(None) | Value::Int64(None) => {
                        assert!(required.is_err());
                        assert_eq!(optional?.method, None);
                    }
                    _ => {
                        assert!(required.is_err());
                        assert!(optional.is_err());
                    }
                }
            }
        }
        Ok(())
    }
}
