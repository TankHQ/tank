#[cfg(test)]
mod tests {
    use tank::AsValue;

    #[test]
    fn chrono_offset_roundtrip() {
        let date_time = chrono::NaiveDate::from_ymd_opt(2025, 6, 15)
            .unwrap()
            .and_hms_nano_opt(14, 30, 0, 123_456_789)
            .unwrap();
        for offset_secs in [0, 5 * 3600 + 30 * 60, -(4 * 3600), 45 * 60] {
            let fixed = chrono::FixedOffset::east_opt(offset_secs).unwrap();
            let original: chrono::DateTime<chrono::FixedOffset> =
                chrono::DateTime::from_naive_utc_and_offset(date_time, fixed);
            let back = chrono::DateTime::<chrono::FixedOffset>::decode_value(original.as_value())
                .expect("Could not convert back to chrono::DateTime<FixedOffset>");
            assert_eq!(
                original.with_timezone(&chrono::Utc),
                back.with_timezone(&chrono::Utc),
                "DateTime<FixedOffset> round-trip changed the instant for offset {offset_secs}s"
            );
        }
    }

    #[test]
    fn chrono_naive_roundtrips() {
        let date = chrono::NaiveDate::from_ymd_opt(2025, 6, 15).unwrap();
        assert_eq!(
            chrono::NaiveDate::decode_value(date.as_value()).unwrap(),
            date
        );

        let time = chrono::NaiveTime::from_hms_nano_opt(14, 30, 0, 500).unwrap();
        assert_eq!(
            chrono::NaiveTime::decode_value(time.as_value()).unwrap(),
            time
        );

        let date_time = date.and_hms_nano_opt(14, 30, 0, 500).unwrap();
        assert_eq!(
            chrono::NaiveDateTime::decode_value(date_time.as_value()).unwrap(),
            date_time
        );

        let utc =
            chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(date_time, chrono::Utc);
        assert_eq!(
            chrono::DateTime::<chrono::Utc>::decode_value(utc.as_value()).unwrap(),
            utc
        );
    }

    #[test]
    fn chrono_range_overflow() {
        let far = chrono::DateTime::<chrono::Utc>::from_timestamp(253_402_300_800, 0)
            .expect("Should be a valid chrono timestamp");
        let value = far.as_value();
        assert!(
            matches!(value, tank::Value::TimestampWithTimezone(None)),
            "Out-of-range chrono::DateTime<Utc> should map to a null value, got {value:?}"
        );
    }
}
