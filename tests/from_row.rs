#[cfg(test)]
mod tests {
    use std::{
        borrow::Cow,
        cell::{Cell, RefCell},
        sync::{
            Arc, Weak,
            atomic::{AtomicUsize, Ordering},
        },
    };
    use tank::{AsValue, Entity, Result, Row, RowLabels, RowOwned, Value, ValueOwned};

    #[derive(Debug)]
    struct CountingSource {
        values: Vec<Option<ValueOwned>>,
        calls: Vec<AtomicUsize>,
    }

    impl CountingSource {
        fn new(values: Vec<Option<ValueOwned>>) -> Self {
            Self {
                calls: (0..values.len()).map(|_| AtomicUsize::new(0)).collect(),
                values,
            }
        }

        fn row(&self, names: &[&str]) -> CountingRow<'_> {
            CountingRow {
                source: self,
                names: names.iter().map(|name| (*name).to_owned()).collect(),
            }
        }

        fn counts(&self) -> Vec<usize> {
            self.calls
                .iter()
                .map(|n| n.load(Ordering::Relaxed))
                .collect()
        }
    }

    #[derive(Clone, Debug)]
    struct CountingRow<'a> {
        source: &'a CountingSource,
        names: RowLabels,
    }

    impl Row for CountingRow<'_> {
        fn names(&self) -> &[String] {
            &self.names
        }

        fn len(&self) -> usize {
            self.source.values.len()
        }

        fn value(&self, col: usize) -> Option<Cow<'_, Value<'_>>> {
            if let Some(calls) = self.source.calls.get(col) {
                calls.fetch_add(1, Ordering::Relaxed);
            }
            self.source
                .values
                .get(col)
                .cloned()
                .flatten()
                .map(Cow::Owned)
        }
    }

    #[derive(Clone, Debug, Default, PartialEq)]
    struct Text(String);

    thread_local! {
        static LABEL_COUNT: RefCell<Option<(Weak<[String]>, usize)>> = const { RefCell::new(None) };
        static HOOK_CALLS: Cell<usize> = const { Cell::new(0) };
    }

    impl From<String> for Text {
        fn from(value: String) -> Self {
            Self(value)
        }
    }

    impl From<Text> for String {
        fn from(value: Text) -> Self {
            value.0
        }
    }

    impl AsValue for Text {
        fn as_empty_value() -> ValueOwned {
            String::as_empty_value()
        }

        fn as_value<'a>(self) -> Value<'a>
        where
            Self: 'a,
        {
            self.0.as_value()
        }

        fn try_from_value(value: impl AsValue) -> Result<Self> {
            HOOK_CALLS.set(HOOK_CALLS.get() + 1);
            LABEL_COUNT.with_borrow(|expected| {
                if let Some((labels, count)) = expected {
                    assert_eq!(labels.strong_count(), *count);
                }
            });
            String::try_from_value(value).map(Self)
        }
    }

    #[derive(Debug, Entity, PartialEq)]
    struct Required {
        id: i32,
        #[tank(name = "db_text", conversion_type = Text)]
        text: String,
        direct: Text,
        #[tank(ignore)]
        ignored: usize,
    }

    #[derive(Debug, Entity, PartialEq)]
    struct WithDefault {
        id: i32,
        #[tank(name = "db_text", conversion_type = Text)]
        text: String,
        direct: Text,
        #[tank(ignore)]
        ignored: usize,
    }

    impl Default for WithDefault {
        fn default() -> Self {
            Self {
                id: 99,
                text: "fallback".into(),
                direct: Text("default direct".into()),
                ignored: 7,
            }
        }
    }

    #[test]
    fn native_row_view_decodes_fresh_values_without_materializing() {
        #[derive(Debug)]
        struct NativeRow {
            names: RowLabels,
            text: String,
            direct: ValueOwned,
            calls: [AtomicUsize; 3],
        }

        impl Row for NativeRow {
            fn names(&self) -> &[String] {
                &self.names
            }

            fn len(&self) -> usize {
                3
            }

            fn value(&self, index: usize) -> Option<Cow<'_, Value<'_>>> {
                self.calls.get(index)?.fetch_add(1, Ordering::Relaxed);
                Some(match index {
                    0 => Cow::Owned(Value::Int32(Some(42))),
                    1 => Cow::Owned(Value::Varchar(Some(Cow::Borrowed(&self.text)))),
                    2 => Cow::Borrowed(&self.direct),
                    _ => unreachable!(),
                })
            }
        }

        let (required, default) = {
            let row = NativeRow {
                names: ["id".into(), "db_text".into(), "direct".into()].into(),
                text: "converted".into(),
                direct: "direct".as_value(),
                calls: std::array::from_fn(|_| AtomicUsize::new(0)),
            };
            LABEL_COUNT.with_borrow_mut(|expected| {
                *expected = Some((Arc::downgrade(&row.names), 1));
            });
            let hooks_before = HOOK_CALLS.get();
            let required = Required::from_row(&row).unwrap();
            assert_eq!(Required::from_row(&row).unwrap(), required);
            let default = WithDefault::from_row(&row).unwrap();
            assert_eq!(HOOK_CALLS.get() - hooks_before, 6);
            assert_eq!(
                row.calls.map(|calls| calls.load(Ordering::Relaxed)),
                [3, 3, 3],
            );
            LABEL_COUNT.with_borrow_mut(|expected| *expected = None);
            (required, default)
        };
        assert_eq!(required.id, 42);
        assert_eq!(required.text, "converted");
        assert_eq!(required.direct, Text("direct".into()));
        assert_eq!(default.text, required.text);
        assert_eq!(default.direct, required.direct);
        assert_eq!(default.ignored, 7);
    }

    #[test]
    fn retained_entity_row_values_own_normalized_payloads() {
        #[derive(Debug, Entity)]
        struct Retained {
            text: ValueOwned,
        }

        let values = Retained {
            text: Value::Varchar(Some(Cow::Borrowed("borrowed payload"))),
        }
        .row_values();
        assert!(matches!(
            values.get(0),
            Some(Value::Varchar(Some(Cow::Owned(text)))) if text == "borrowed payload"
        ));
    }

    #[test]
    fn required_decodes_lazy_columns_once_in_row_order() {
        let source = CountingSource::new(vec![
            Some(Value::Null),
            Some("converted".as_value()),
            Some("direct".as_value()),
            Some(Value::Null),
            Some(42_i32.as_value()),
            None,
        ]);
        let result = Required::from_row(
            source.row(&["unknown", "db_text", "direct", "ignored", "id", "extra"]),
        )
        .unwrap();
        assert_eq!(
            result,
            Required {
                id: 42,
                text: "converted".into(),
                direct: Text("direct".into()),
                ignored: 0,
            }
        );
        assert_eq!(source.counts(), [1, 1, 1, 1, 1, 1]);
    }

    #[test]
    fn ordered_rows_preserve_conversion_hooks_for_lazy_and_owned_values() {
        let source = CountingSource::new(vec![
            Some(42_i32.as_value()),
            Some("converted".as_value()),
            Some("direct".as_value()),
        ]);
        let labels = ["id", "db_text", "direct"];
        let lazy = Required::from_row(source.row(&labels)).unwrap();
        assert_eq!(source.counts(), [1, 1, 1]);
        let materialized = Required::from_row(source.row(&labels).into_owned()).unwrap();
        assert_eq!(lazy, materialized);
        let default = WithDefault::from_row(source.row(&labels)).unwrap();
        assert_eq!(default.id, lazy.id);
        assert_eq!(default.text, lazy.text);
        assert_eq!(default.direct, lazy.direct);
        assert_eq!(default.ignored, 7);
    }

    #[test]
    fn borrowed_conversion_reuses_lazy_and_owned_rows_without_cloning_labels() {
        let source = CountingSource::new(vec![
            Some(42_i32.as_value()),
            Some("converted".as_value()),
            Some("direct".as_value()),
        ]);
        let row = source.row(&["id", "db_text", "direct"]);
        LABEL_COUNT.with_borrow_mut(|expected| {
            *expected = Some((Arc::downgrade(&row.names), Arc::strong_count(&row.names)));
        });
        let first = Required::from_row(&row).unwrap();
        assert_eq!(Required::from_row(&row).unwrap(), first);
        let default = WithDefault::from_row(&row).unwrap();
        assert_eq!(default.text, first.text);
        assert_eq!(default.direct, first.direct);
        assert_eq!(default.ignored, 7);
        assert_eq!(source.counts(), [3, 3, 3]);

        let owned = row.into_owned();
        LABEL_COUNT.with_borrow_mut(|expected| {
            *expected = Some((
                Arc::downgrade(&owned.names),
                Arc::strong_count(&owned.names),
            ));
        });
        assert_eq!(Required::from_row(&owned).unwrap(), first);
        assert_eq!(Required::from_row(&owned).unwrap(), first);
        assert_eq!(WithDefault::from_row(&owned).unwrap(), default);
        LABEL_COUNT.with_borrow_mut(|expected| *expected = None);
        assert_eq!(Required::from_row(owned).unwrap(), first);
    }

    #[test]
    fn borrowed_materialized_conversion_owns_output() {
        #[derive(Debug)]
        struct BorrowedRow<'a> {
            names: RowLabels,
            values: &'a [Value<'a>],
        }

        impl Row for BorrowedRow<'_> {
            fn names(&self) -> &[String] {
                &self.names
            }

            fn len(&self) -> usize {
                self.values.len()
            }

            fn value(&self, index: usize) -> Option<Cow<'_, Value<'_>>> {
                self.values.get(index).map(Cow::Borrowed)
            }
        }

        let result = {
            let text = String::from("converted");
            let direct = String::from("direct");
            let values = [
                42_i32.as_value(),
                Value::Varchar(Some(Cow::Borrowed(text.as_str()))),
                Value::Varchar(Some(Cow::Borrowed(direct.as_str()))),
            ];
            let row = BorrowedRow {
                names: ["id".into(), "db_text".into(), "direct".into()].into(),
                values: &values,
            };
            let result = Required::from_row(&row).unwrap();
            assert_eq!(Required::from_row(&row).unwrap(), result);
            result
        };
        assert_eq!(result.id, 42);
        assert_eq!(result.text, "converted");
        assert_eq!(result.direct, Text("direct".into()));
    }

    #[test]
    fn borrowed_and_consuming_paths_match_for_ordered_and_fallback_labels() {
        for labels in [
            vec!["id", "db_text", "direct"],
            vec!["direct", "id", "db_text"],
            vec!["id", "unknown", "db_text", "direct"],
            vec!["id", "db_text"],
            vec!["id", "db_text", "direct", "db_text", "id"],
            vec!["id", "direct", "db_text"],
        ] {
            let source = CountingSource::new(
                labels
                    .iter()
                    .enumerate()
                    .map(|(i, label)| {
                        Some(match *label {
                            "id" => (i as i32).as_value(),
                            "db_text" | "direct" => format!("value {i}").as_value(),
                            _ => Value::Null,
                        })
                    })
                    .collect(),
            );
            let hooks_per_row = labels
                .iter()
                .filter(|label| matches!(**label, "db_text" | "direct"))
                .count();
            let row = source.row(&labels);
            let hooks_before = HOOK_CALLS.get();
            let required = Required::from_row(row.clone()).map_err(|error| error.to_string());
            assert_eq!(
                Required::from_row(&row).map_err(|error| error.to_string()),
                required,
            );
            let default = WithDefault::from_row(row.clone()).unwrap();
            assert_eq!(WithDefault::from_row(&row).unwrap(), default);
            assert_eq!(source.counts(), vec![4; labels.len()]);
            assert_eq!(HOOK_CALLS.get() - hooks_before, 4 * hooks_per_row);

            let owned = row.into_owned();
            let hooks_before = HOOK_CALLS.get();
            assert_eq!(
                Required::from_row(&owned).map_err(|error| error.to_string()),
                required,
            );
            assert_eq!(WithDefault::from_row(&owned).unwrap(), default);
            assert_eq!(HOOK_CALLS.get() - hooks_before, 2 * hooks_per_row);
            assert_eq!(source.counts(), vec![5; labels.len()]);
        }
    }

    #[test]
    fn row_conversion_initializes_default_once_per_call() {
        thread_local! {
            static DEFAULT_CALLS: Cell<usize> = const { Cell::new(0) };
        }

        #[derive(Debug, Entity)]
        struct CountedDefault {
            id: i32,
        }

        impl Default for CountedDefault {
            fn default() -> Self {
                DEFAULT_CALLS.set(DEFAULT_CALLS.get() + 1);
                Self { id: 77 }
            }
        }

        for labels in [vec!["id"], vec!["unknown", "id"], vec![], vec!["id", "id"]] {
            let source = CountingSource::new(
                labels
                    .iter()
                    .enumerate()
                    .map(|(i, _)| Some((i as i32).as_value()))
                    .collect(),
            );
            let row = source.row(&labels);
            let before = DEFAULT_CALLS.get();
            let expected = labels
                .iter()
                .rposition(|label| *label == "id")
                .map_or(77, |i| i as i32);
            for call in 1..=2 {
                assert_eq!(CountedDefault::from_row(&row).unwrap().id, expected);
                assert_eq!(DEFAULT_CALLS.get(), before + call);
            }
            let other = source.row(&labels);
            assert_eq!(CountedDefault::from_row(other).unwrap().id, expected);
            assert_eq!(DEFAULT_CALLS.get(), before + 3);
        }
    }

    #[test]
    fn borrowed_and_consuming_conversion_preserve_error_order() {
        for labels in [["id", "db_text", "direct"], ["direct", "id", "db_text"]] {
            let source = CountingSource::new(
                labels
                    .iter()
                    .map(|label| {
                        Some(match *label {
                            "id" => "not an integer".as_value(),
                            _ => "text".as_value(),
                        })
                    })
                    .collect(),
            );
            let row = source.row(&labels);
            let hooks_before = HOOK_CALLS.get();
            assert_eq!(
                Required::from_row(row.clone()).unwrap_err().to_string(),
                Required::from_row(&row).unwrap_err().to_string(),
            );
            assert_eq!(
                WithDefault::from_row(row.clone()).unwrap_err().to_string(),
                WithDefault::from_row(&row).unwrap_err().to_string(),
            );
            let id_index = labels.iter().position(|label| *label == "id").unwrap();
            assert_eq!(HOOK_CALLS.get() - hooks_before, 4 * id_index);
            assert_eq!(
                source.counts(),
                (0..labels.len())
                    .map(|i| { if i <= id_index { 4 } else { 0 } })
                    .collect::<Vec<_>>()
            );
        }
    }

    #[test]
    fn duplicate_entity_names_keep_first_field_dispatch() {
        #[derive(Default, Debug, Entity)]
        struct DuplicateNames {
            #[tank(name = "value")]
            first: Text,
            #[tank(name = "value")]
            second: Text,
        }

        #[derive(Debug, Entity)]
        struct RequiredDuplicateNames {
            #[tank(name = "value")]
            first: Text,
            #[tank(name = "value")]
            second: Text,
        }

        let source = CountingSource::new(vec![Some("first".as_value()), Some("last".as_value())]);
        let row = source.row(&["value", "value"]);
        let hooks_before = HOOK_CALLS.get();
        let result = DuplicateNames::from_row(&row).unwrap();
        assert_eq!(result.first, Text("last".into()));
        assert_eq!(result.second, Text::default());
        let error = RequiredDuplicateNames::from_row(&row).unwrap_err();
        assert_eq!(
            error.to_string(),
            RequiredDuplicateNames::from_row(row.clone())
                .unwrap_err()
                .to_string(),
        );
        assert!(error.to_string().contains("Column `value` does not exist"));
        assert_eq!(source.counts(), [3, 3]);
        assert_eq!(HOOK_CALLS.get() - hooks_before, 6);
        let owned = row.into_owned();
        let result = DuplicateNames::from_row(&owned).unwrap();
        assert_eq!(result.first, Text("last".into()));
        assert_eq!(result.second, Text::default());
        assert_eq!(HOOK_CALLS.get() - hooks_before, 8);
    }

    #[test]
    fn default_decodes_lazy_columns_and_keeps_missing_defaults() {
        let source = CountingSource::new(vec![
            Some(Value::Null),
            Some("direct".as_value()),
            Some("converted".as_value()),
            None,
        ]);
        let result =
            WithDefault::from_row(&source.row(&["unknown", "direct", "db_text", "ignored"]))
                .unwrap();
        assert_eq!(
            result,
            WithDefault {
                text: "converted".into(),
                direct: Text("direct".into()),
                ..Default::default()
            }
        );
        assert_eq!(source.counts(), [1, 1, 1, 1]);
    }

    #[test]
    fn absent_unmapped_value_still_stops_decoding() {
        let source = CountingSource::new(vec![None, Some(42_i32.as_value())]);
        let labels = ["unknown", "id"];
        let error = Required::from_row(&source.row(&labels)).unwrap_err();
        assert!(error.to_string().contains("Column `id` does not exist"));
        assert_eq!(source.counts(), [1, 0]);
        assert_eq!(
            WithDefault::from_row(&source.row(&labels)).unwrap(),
            WithDefault::default()
        );
        assert_eq!(source.counts(), [2, 0]);
    }

    #[test]
    fn duplicate_labels_keep_the_last_value_with_and_without_default() {
        let source = CountingSource::new(vec![
            Some(1_i32.as_value()),
            Some("first".as_value()),
            Some("direct".as_value()),
            Some("last".as_value()),
            Some(2_i32.as_value()),
        ]);
        let labels = ["id", "db_text", "direct", "db_text", "id"];
        let required = Required::from_row(&source.row(&labels)).unwrap();
        assert_eq!(required.id, 2);
        assert_eq!(required.text, "last");
        assert_eq!(source.counts(), [1, 1, 1, 1, 1]);

        let default = WithDefault::from_row(&source.row(&labels)).unwrap();
        assert_eq!(default.id, 2);
        assert_eq!(default.text, "last");
        assert_eq!(source.counts(), [2, 2, 2, 2, 2]);
    }

    #[test]
    fn missing_required_column_still_errors() {
        let source = CountingSource::new(vec![Some(1_i32.as_value())]);
        let error = Required::from_row(&source.row(&["id"])).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("Column `db_text` does not exist")
        );
        assert_eq!(source.counts(), [1]);
    }

    #[test]
    fn absent_mapped_value_still_stops_decoding() {
        let source = CountingSource::new(vec![
            Some(1_i32.as_value()),
            None,
            Some("not decoded".as_value()),
        ]);
        let labels = ["id", "db_text", "direct"];
        let required = Required::from_row(source.row(&labels)).unwrap_err();
        assert!(
            required
                .to_string()
                .contains("Column `db_text` does not exist")
        );
        assert_eq!(source.counts(), [1, 1, 0]);

        let default = WithDefault::from_row(source.row(&labels)).unwrap();
        assert_eq!(
            default,
            WithDefault {
                id: 1,
                ..Default::default()
            }
        );
        assert_eq!(source.counts(), [2, 2, 0]);
    }

    #[test]
    fn ordered_rows_preserve_early_stop_parity_without_retrying_hooks() {
        let labels = ["id", "db_text", "direct"];
        let values = [
            1_i32.as_value(),
            "converted".as_value(),
            "direct".as_value(),
        ];
        for missing in 0..labels.len() {
            let source = CountingSource::new(
                values
                    .iter()
                    .enumerate()
                    .map(|(i, value)| (i != missing).then(|| value.clone()))
                    .collect(),
            );
            let row = source.row(&labels);
            let hooks_before = HOOK_CALLS.get();
            let error = Required::from_row(&row).unwrap_err().to_string();
            assert!(error.contains(&format!("Column `{}` does not exist", labels[missing])));
            assert_eq!(
                error,
                Required::from_row(row.clone()).unwrap_err().to_string(),
            );
            let expected = WithDefault::from_row(row.clone()).unwrap();
            assert_eq!(WithDefault::from_row(&row).unwrap(), expected);
            assert_eq!(expected.id, if missing == 0 { 99 } else { 1 });
            assert_eq!(
                expected.text,
                if missing < 2 { "fallback" } else { "converted" }
            );
            assert_eq!(expected.direct, Text("default direct".into()));
            assert_eq!(expected.ignored, 7);
            assert_eq!(
                HOOK_CALLS.get() - hooks_before,
                4 * missing.saturating_sub(1)
            );
            assert_eq!(
                source.counts(),
                (0..labels.len())
                    .map(|i| if i <= missing { 4 } else { 0 })
                    .collect::<Vec<_>>(),
            );

            // Keep the labels exact while truncating materialized values at the same column.
            let owned = RowOwned::new(
                labels.iter().map(|label| (*label).to_owned()).collect(),
                values[..missing].to_vec(),
            );
            let hooks_before = HOOK_CALLS.get();
            assert_eq!(Required::from_row(&owned).unwrap_err().to_string(), error);
            assert_eq!(WithDefault::from_row(&owned).unwrap(), expected);
            assert_eq!(
                HOOK_CALLS.get() - hooks_before,
                2 * missing.saturating_sub(1)
            );
        }
    }

    #[test]
    fn empty_entities_support_borrowed_ordered_and_fallback_rows() {
        #[derive(Debug, Entity)]
        struct Empty {}
        #[derive(Debug, Default, Entity)]
        struct EmptyDefault {}

        let empty = RowOwned::default();
        Empty::from_row(&empty).unwrap();
        EmptyDefault::from_row(&empty).unwrap();
        let source = CountingSource::new(vec![None, Some(Value::Null)]);
        let row = source.row(&["unknown", "extra"]);
        Empty::from_row(&row).unwrap();
        EmptyDefault::from_row(&row).unwrap();
        assert_eq!(source.counts(), [2, 0]);
    }

    #[test]
    fn short_materialized_row_keeps_defaults_or_reports_missing_column() {
        let row = RowOwned::new(
            ["id".into(), "db_text".into(), "direct".into()].into(),
            vec![1_i32.as_value()],
        );
        let error = Required::from_row(row.clone()).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("Column `db_text` does not exist")
        );
        assert_eq!(
            WithDefault::from_row(row).unwrap(),
            WithDefault {
                id: 1,
                ..Default::default()
            }
        );
    }

    #[test]
    fn null_value_is_converted_rather_than_treated_as_absent() {
        #[derive(Debug, Entity)]
        struct Nullable {
            text: Option<String>,
        }

        let source = CountingSource::new(vec![Some(Value::Varchar(None))]);
        assert_eq!(
            Nullable::from_row(source.row(&["text"])).unwrap().text,
            None
        );
        assert_eq!(source.counts(), [1]);
        assert_eq!(
            Nullable::from_row(&source.row(&["text"])).unwrap().text,
            None
        );
        assert_eq!(source.counts(), [2]);
    }
}
