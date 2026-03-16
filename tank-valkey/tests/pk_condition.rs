#[cfg(test)]
mod tests {
    use std::{borrow::Cow, str::FromStr};
    use tank::{Entity, Expression, Fragment, expr};
    use tank_valkey::{IsPKCondition, ValkeySqlWriter};
    use time::{Date, Month};
    use uuid::Uuid;

    #[tokio::test]
    pub async fn pk_condition_1() {
        #[derive(Entity)]
        #[tank(schema = "namespace", primary_key = (partition, name, id, day))]
        struct TableName {
            partition: isize,
            name: Cow<'static, str>,
            day: Date,
            id: Uuid,
            payload: Vec<u8>,
        }

        // value.primary_key_expr()
        {
            let value = TableName {
                partition: 14,
                name: "database".into(),
                day: Date::from_calendar_date(2026, Month::March, 15).unwrap(),
                id: Uuid::from_str("9f875744-eeb8-414a-9dc5-552f1643046d").unwrap(),
                payload: "database data".as_bytes().into(),
            };
            let mut is_pk_condition = IsPKCondition::new(
                format!("{}:{}", TableName::table().schema, TableName::table().name),
                TableName::primary_key_def(),
            );
            assert!(value.primary_key_expr().accept_visitor(
                &mut is_pk_condition,
                &ValkeySqlWriter::default(),
                &mut ValkeySqlWriter::make_context(Fragment::None),
                &mut Default::default(),
            ));
            assert_eq!(
                is_pk_condition.key,
                "namespace:table_name:partition:14:name:database:id:9f875744-eeb8-414a-9dc5-552f1643046d:day:2026-03-15"
            );
        }

        // name && (id && (partition && day)
        {
            let mut is_pk_condition = IsPKCondition::new("".into(), TableName::primary_key_def());
            let uuid = Uuid::from_str("00000000-0000-0000-0000-000000000000").unwrap();
            let day = Date::from_calendar_date(2026, Month::March, 10).unwrap();
            assert!( // This failes
                expr!(TableName::name == "the name" && (id == #uuid && (partition == 60 && day == #day)))
                    .accept_visitor(
                        &mut is_pk_condition,
                        &ValkeySqlWriter::default(),
                        &mut ValkeySqlWriter::make_context(Fragment::None),
                        &mut Default::default(),
                    )
            );
            assert_eq!(
                is_pk_condition.key,
                "partition:60:name:the name:id:00000000-0000-0000-0000-000000000000:day:2026-03-10"
            );
        }

        // name && (id && partition) && day
        {
            let mut is_pk_condition = IsPKCondition::new("".into(), TableName::primary_key_def());
            let uuid = Uuid::from_str("00000000-0000-0000-0000-000000000000").unwrap();
            let day = Date::from_calendar_date(2026, Month::March, 10).unwrap();
            assert!( // This failes
                expr!(TableName::name == "the name" && ((id == #uuid && partition == 60) && day == #day))
                    .accept_visitor(
                        &mut is_pk_condition,
                        &ValkeySqlWriter::default(),
                        &mut ValkeySqlWriter::make_context(Fragment::None),
                        &mut Default::default(),
                    )
            );
            assert_eq!(
                is_pk_condition.key,
                "partition:60:name:the name:id:00000000-0000-0000-0000-000000000000:day:2026-03-10"
            );
        }

        // (name && id) && (partition && day)
        {
            let mut is_pk_condition = IsPKCondition::new("".into(), TableName::primary_key_def());
            let uuid = Uuid::from_str("00000000-0000-0000-0000-000000000000").unwrap();
            let day = Date::from_calendar_date(2026, Month::March, 10).unwrap();
            assert!( // This failes
                expr!((TableName::name == "the name" && id == #uuid) && (partition == 60 && day == #day))
                    .accept_visitor(
                        &mut is_pk_condition,
                        &ValkeySqlWriter::default(),
                        &mut ValkeySqlWriter::make_context(Fragment::None),
                        &mut Default::default(),
                    )
            );
            assert_eq!(
                is_pk_condition.key,
                "partition:60:name:the name:id:00000000-0000-0000-0000-000000000000:day:2026-03-10"
            );
        }

        // ((name && id) && partition) && day
        {
            let mut is_pk_condition = IsPKCondition::new("".into(), TableName::primary_key_def());
            let uuid = Uuid::from_str("00000000-0000-0000-0000-000000000000").unwrap();
            let day = Date::from_calendar_date(2026, Month::March, 10).unwrap();
            assert!( // This failes
                expr!(((TableName::name == "the name" && id == #uuid) && partition == 60) && day == #day)
                    .accept_visitor(
                        &mut is_pk_condition,
                        &ValkeySqlWriter::default(),
                        &mut ValkeySqlWriter::make_context(Fragment::None),
                        &mut Default::default(),
                    )
            );
            assert_eq!(
                is_pk_condition.key,
                "partition:60:name:the name:id:00000000-0000-0000-0000-000000000000:day:2026-03-10"
            );
        }
    }

    #[tokio::test]
    pub async fn pk_condition_errors() {
        #[derive(Entity)]
        #[tank(schema = "auth", primary_key = (user_id, token_type))]
        struct UserSession {
            user_id: Uuid,
            token_type: Cow<'static, str>,
            expires_at: Date,
            ip_address: String,
        }

        let test_user_id = Uuid::from_str("12345678-1234-1234-1234-123456789abc").unwrap();
        let test_date = Date::from_calendar_date(2026, Month::April, 1).unwrap();

        {
            let mut is_pk_condition = IsPKCondition::new("".into(), UserSession::primary_key_def());
            let is_valid_pk = expr!(UserSession::user_id == #test_user_id).accept_visitor(
                &mut is_pk_condition,
                &ValkeySqlWriter::default(),
                &mut ValkeySqlWriter::make_context(Fragment::None),
                &mut Default::default(),
            );
            assert!(
                !is_valid_pk,
                "Condition should fail: missing token_type from PK"
            );
        }

        {
            let mut is_pk_condition = IsPKCondition::new("".into(), UserSession::primary_key_def());
            let is_valid_pk = expr!((UserSession::user_id == #test_user_id && token_type == "refresh_token") && expires_at == #test_date)
                .accept_visitor(
                    &mut is_pk_condition,
                    &ValkeySqlWriter::default(),
                    &mut ValkeySqlWriter::make_context(Fragment::None),
                    &mut Default::default(),
                );
            assert!(
                !is_valid_pk,
                "Condition should fail: expires_at is not part of the primary key"
            );
        }

        {
            let mut is_pk_condition = IsPKCondition::new("".into(), UserSession::primary_key_def());
            let is_valid_pk =
                expr!(UserSession::user_id == #test_user_id && expires_at == #test_date)
                    .accept_visitor(
                        &mut is_pk_condition,
                        &ValkeySqlWriter::default(),
                        &mut ValkeySqlWriter::make_context(Fragment::None),
                        &mut Default::default(),
                    );
            assert!(
                !is_valid_pk,
                "Condition should fail: misses token_type and includes expires_at"
            );
        }

        {
            let mut is_pk_condition = IsPKCondition::new("".into(), UserSession::primary_key_def());
            let is_valid_pk =
                expr!(UserSession::user_id == #test_user_id && token_type != "refresh_token")
                    .accept_visitor(
                        &mut is_pk_condition,
                        &ValkeySqlWriter::default(),
                        &mut ValkeySqlWriter::make_context(Fragment::None),
                        &mut Default::default(),
                    );
            assert!(
                !is_valid_pk,
                "Condition should fail: PK lookups require equality (==)"
            );
        }
    }
}
