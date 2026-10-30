#[cfg(test)]
mod tests {
    use std::{borrow::Cow, sync::Arc};
    use tank::{Row, RowLabels, RowOwned, Value};

    #[test]
    fn owned_values_are_borrowed_and_ownership_moves_storage() {
        let names: RowLabels = ["name".to_owned()].into();
        let text = "sensitive payload".to_owned();
        let text_ptr = text.as_ptr();
        let row = RowOwned::new(names.clone(), vec![Value::Varchar(Some(Cow::Owned(text)))]);
        let values_ptr = row.values.as_ptr();
        assert!(matches!(row.value(0), Some(Cow::Borrowed(_))));
        assert!(matches!(row.get_column("name"), Some(Cow::Borrowed(_))));
        assert!(matches!(row.values().next(), Some(Cow::Borrowed(_))));
        assert!(matches!(
            row.iter().next(),
            Some(("name", Cow::Borrowed(_)))
        ));
        assert!(row.value(1).is_none());
        assert!(row.get_column("missing").is_none());
        assert!(!format!("{row:?}").contains("sensitive payload"));
        let owned = row.into_owned();
        assert!(Arc::ptr_eq(&names, &owned.names));
        assert_eq!(owned.values.as_ptr(), values_ptr);
        let Some(Cow::Borrowed(Value::Varchar(Some(text)))) = owned.value(0) else {
            panic!("Expected stored text")
        };
        assert_eq!(text.as_ptr(), text_ptr);
    }

    #[derive(Debug)]
    struct NativeRow<'a> {
        names: &'a [String],
        text: &'a str,
    }

    impl Row for NativeRow<'_> {
        fn names(&self) -> &[String] {
            self.names
        }

        fn len(&self) -> usize {
            1
        }

        fn value(&self, index: usize) -> Option<Cow<'_, Value<'_>>> {
            (index == 0).then(|| Cow::Owned(Value::Varchar(Some(Cow::Borrowed(self.text)))))
        }
    }

    #[test]
    fn native_views_and_references_materialize_borrowed_payloads() {
        let names: RowLabels = ["name".to_owned()].into();
        let text = "decoded".to_owned();
        let row = NativeRow {
            names: &names,
            text: &text,
        };
        assert!(matches!(row.value(0), Some(Cow::Owned(_))));
        assert_eq!(Row::names(&&row), &["name".to_owned()]);
        assert_eq!(Row::iter(&&row).count(), 1);
        let owned = Row::into_owned(&row);
        assert_eq!(Arc::strong_count(&names), 1);
        assert!(!Arc::ptr_eq(&names, &owned.names));
        drop(text);
        assert!(
            matches!(owned.value(0), Some(Cow::Borrowed(Value::Varchar(Some(text)))) if text == "decoded")
        );
    }

    #[derive(Debug)]
    struct GappedRow {
        names: [String; 3],
        gap: usize,
    }

    impl Row for GappedRow {
        fn names(&self) -> &[String] {
            &self.names
        }

        fn len(&self) -> usize {
            self.names.len()
        }

        fn value(&self, index: usize) -> Option<Cow<'_, Value<'_>>> {
            (index < self.len() && index != self.gap)
                .then_some(Cow::Owned(Value::Int32(Some(index as i32))))
        }
    }

    #[test]
    fn missing_cells_stop_iteration_and_materialization_without_shifting_columns() {
        for gap in [0, 1, 2] {
            let row = GappedRow {
                names: ["first".to_owned(), "second".to_owned(), "third".to_owned()],
                gap,
            };
            assert_eq!(row.values().count(), gap);
            assert_eq!(row.iter().count(), gap);
            let borrowed = (&row).into_owned();
            let consumed = row.into_owned();
            assert_eq!(borrowed.len(), gap);
            assert_eq!(consumed.len(), gap);
            assert_eq!(borrowed.values, consumed.values);
            assert!(consumed.value(gap).is_none());
            assert!(consumed.get_column("third").is_none());
            for index in 0..gap {
                assert_eq!(consumed.values[index], Value::Int32(Some(index as i32)));
            }
        }
    }
}
