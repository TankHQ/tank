#[cfg(test)]
mod tests {
    use std::{borrow::Cow, str::FromStr};
    use tank::{Entity, Expression, Fragment, GenericSqlWriter, expr};
    use tank_valkey::{IsPKCondition, ValkeySqlWriter};
    use time::{Date, Month};
    use uuid::Uuid;

    #[tokio::test]
    pub async fn pk_condition() {
        #[derive(Entity)]
        #[tank(schema = "namespace", primary_key = (partition, name, id, day))]
        struct TableName {
            partition: isize,
            name: Cow<'static, str>,
            day: Date,
            id: Uuid,
            payload: Vec<u8>,
        }
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
        // {
        //     let mut is_pk_condition = IsPKCondition::new(
        //         format!("{}:{}", TableName::table().schema, TableName::table().name),
        //         TableName::primary_key_def(),
        //     );
        //     assert!(expr!().accept_visitor(
        //         &mut is_pk_condition,
        //         &ValkeySqlWriter::default(),
        //         &mut ValkeySqlWriter::make_context(Fragment::None),
        //         &mut Default::default(),
        //     ));
        //     assert_eq!(
        //         is_pk_condition.key,
        //         "namespace:table_name:partition:14:name:database:id:9f875744-eeb8-414a-9dc5-552f1643046d:day:2026-03-15"
        //     );
        // }
    }
}
