#[cfg(test)]
mod tests {
    use indoc::indoc;
    use rust_decimal::Decimal;
    use std::{
        array,
        borrow::Cow,
        collections::{BTreeMap, HashMap},
    };
    use tank::{
        Action, DynQuery, DefaultValueType, Entity, GenericSqlWriter, Passive, PrimaryKeyType,
        QueryBuilder, SqlWriter, TableRef, Value, expr,
    };
    use time::macros::datetime;
    use uuid::Uuid;

    #[derive(Entity)]
    #[tank(schema = "trading.company", name = "trade_execution", primary_key = ("trade_id", "execution_time"))]
    pub struct Trade {
        #[tank(name = "trade_id")]
        pub trade: u64,
        #[tank(name = "order_id", default = "241d362d-797e-4769-b3f6-412440c8cf68", references = order(id), on_delete = set_default, on_update = restrict)]
        pub order: Uuid,
        /// Ticker symbol
        pub symbol: String,
        pub isin: [char; 12],
        pub price: tank::FixedDecimal<18, 4>,
        pub quantity: u32,
        pub execution_time: Passive<time::PrimitiveDateTime>,
        pub currency: Option<String>,
        pub is_internalized: bool,
        /// Exchange
        pub venue: Option<String>,
        pub child_trade_ids: Option<Vec<i64>>,
        pub metadata: Option<Box<[u8]>>,
        pub tags: Option<BTreeMap<String, String>>,
    }
    impl Trade {
        pub fn sample() -> Self {
            Self {
                trade: 46923,
                order: Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap(),
                symbol: "RIVN".to_string(),
                isin: array::from_fn(|i| "US76954A1034".chars().nth(i).unwrap()),
                price: Decimal::new(1226, 2).into(), // 12.26
                quantity: 500,
                execution_time: datetime!(2025-06-07 14:32:00).into(),
                currency: Some("USD".into()),
                is_internalized: true,
                venue: Some("NASDAQ".into()),
                child_trade_ids: vec![36209, 85320].into(),
                metadata: b"Metadata Bytes".to_vec().into_boxed_slice().into(),
                tags: BTreeMap::from_iter([
                    ("source".into(), "internal".into()),
                    ("strategy".into(), "scalping".into()),
                ])
                .into(),
            }
        }
    }
    const WRITER: GenericSqlWriter = GenericSqlWriter {};

    #[test]
    fn test_trade() {
        assert!(matches!(
            Trade::table(),
            TableRef {
                name: Cow::Borrowed("trade_execution"),
                schema: Cow::Borrowed("trading.company"),
                alias: Cow::Borrowed(""),
            }
        ));
        assert_eq!(
            Trade::primary_key_def()
                .iter()
                .map(|c| c.column_ref.name)
                .collect::<Vec<_>>(),
            ["trade_id", "execution_time"]
        );
        let columns = Trade::columns();
        assert_eq!(columns.len(), 13);
        assert_eq!(columns[0].column_ref.name, "trade_id");
        assert_eq!(columns[1].column_ref.name, "order_id");
        assert_eq!(columns[2].column_ref.name, "symbol");
        assert_eq!(columns[3].column_ref.name, "isin");
        assert_eq!(columns[4].column_ref.name, "price");
        assert_eq!(columns[5].column_ref.name, "quantity");
        assert_eq!(columns[6].column_ref.name, "execution_time");
        assert_eq!(columns[7].column_ref.name, "currency");
        assert_eq!(columns[8].column_ref.name, "is_internalized");
        assert_eq!(columns[9].column_ref.name, "venue");
        assert_eq!(columns[10].column_ref.name, "child_trade_ids");
        assert_eq!(columns[11].column_ref.name, "metadata");
        assert_eq!(columns[12].column_ref.name, "tags");
        assert_eq!(columns[0].column_ref.table, "trade_execution");
        assert_eq!(columns[1].column_ref.table, "trade_execution");
        assert_eq!(columns[2].column_ref.table, "trade_execution");
        assert_eq!(columns[3].column_ref.table, "trade_execution");
        assert_eq!(columns[4].column_ref.table, "trade_execution");
        assert_eq!(columns[5].column_ref.table, "trade_execution");
        assert_eq!(columns[6].column_ref.table, "trade_execution");
        assert_eq!(columns[7].column_ref.table, "trade_execution");
        assert_eq!(columns[8].column_ref.table, "trade_execution");
        assert_eq!(columns[9].column_ref.table, "trade_execution");
        assert_eq!(columns[10].column_ref.table, "trade_execution");
        assert_eq!(columns[11].column_ref.table, "trade_execution");
        assert_eq!(columns[12].column_ref.table, "trade_execution");
        assert_eq!(columns[0].column_ref.schema, "trading.company");
        assert_eq!(columns[1].column_ref.schema, "trading.company");
        assert_eq!(columns[2].column_ref.schema, "trading.company");
        assert_eq!(columns[3].column_ref.schema, "trading.company");
        assert_eq!(columns[4].column_ref.schema, "trading.company");
        assert_eq!(columns[5].column_ref.schema, "trading.company");
        assert_eq!(columns[6].column_ref.schema, "trading.company");
        assert_eq!(columns[7].column_ref.schema, "trading.company");
        assert_eq!(columns[8].column_ref.schema, "trading.company");
        assert_eq!(columns[9].column_ref.schema, "trading.company");
        assert_eq!(columns[10].column_ref.schema, "trading.company");
        assert_eq!(columns[11].column_ref.schema, "trading.company");
        assert_eq!(columns[12].column_ref.schema, "trading.company");
        assert!(matches!(columns[0].value, Value::UInt64(..)));
        assert!(matches!(columns[1].value, Value::Uuid(..)));
        assert!(matches!(columns[2].value, Value::Varchar(..)));
        assert!(matches!(
            columns[3].value,
            Value::Array(_, ref ty, 12) if matches!(**ty, Value::Char(..))
        ));
        assert!(matches!(columns[4].value, Value::Decimal(..)));
        assert!(matches!(columns[5].value, Value::UInt32(..)));
        assert!(matches!(columns[6].value, Value::Timestamp(..)));
        assert!(matches!(columns[7].value, Value::Varchar(..)));
        assert!(matches!(columns[8].value, Value::Boolean(..)));
        assert!(matches!(columns[9].value, Value::Varchar(..)));
        assert!(matches!(
            columns[10].value,
            Value::List(_, ref ty) if matches!(**ty, Value::Int64(..))
        ));
        assert!(matches!(columns[11].value, Value::Blob(..)));
        assert!(matches!(
            columns[12].value,
            Value::Map(_, ref k_ty, ref v_ty) if matches!(**k_ty, Value::Varchar(..)) && matches!(**v_ty, Value::Varchar(..))
        ));
        assert_eq!(columns[0].nullable, false);
        assert_eq!(columns[1].nullable, false);
        assert_eq!(columns[2].nullable, false);
        assert_eq!(columns[3].nullable, false);
        assert_eq!(columns[4].nullable, false);
        assert_eq!(columns[5].nullable, false);
        assert_eq!(columns[6].nullable, false);
        assert_eq!(columns[7].nullable, true);
        assert_eq!(columns[8].nullable, false);
        assert_eq!(columns[9].nullable, true);
        assert_eq!(columns[10].nullable, true);
        assert_eq!(columns[11].nullable, true);
        assert_eq!(columns[12].nullable, true);
        assert!(matches!(columns[0].default, DefaultValueType::None));
        assert!(matches!(
            columns[1].default,
            DefaultValueType::Value(Value::Varchar(Some(Cow::Borrowed(
                "241d362d-797e-4769-b3f6-412440c8cf68"
            )))),
        ));
        assert!(matches!(columns[2].default, DefaultValueType::None));
        assert!(matches!(columns[3].default, DefaultValueType::None));
        assert!(matches!(columns[4].default, DefaultValueType::None));
        assert!(matches!(columns[5].default, DefaultValueType::None));
        assert!(matches!(columns[6].default, DefaultValueType::None));
        assert!(matches!(columns[7].default, DefaultValueType::None));
        assert!(matches!(columns[8].default, DefaultValueType::None));
        assert!(matches!(columns[9].default, DefaultValueType::None));
        assert!(matches!(columns[10].default, DefaultValueType::None));
        assert!(matches!(columns[11].default, DefaultValueType::None));
        assert!(matches!(columns[12].default, DefaultValueType::None));
        assert_eq!(columns[0].primary_key, PrimaryKeyType::PartOfPrimaryKey);
        assert_eq!(columns[1].primary_key, PrimaryKeyType::None);
        assert_eq!(columns[2].primary_key, PrimaryKeyType::None);
        assert_eq!(columns[3].primary_key, PrimaryKeyType::None);
        assert_eq!(columns[4].primary_key, PrimaryKeyType::None);
        assert_eq!(columns[5].primary_key, PrimaryKeyType::None);
        assert_eq!(columns[6].primary_key, PrimaryKeyType::PartOfPrimaryKey);
        assert_eq!(columns[7].primary_key, PrimaryKeyType::None);
        assert_eq!(columns[8].primary_key, PrimaryKeyType::None);
        assert_eq!(columns[9].primary_key, PrimaryKeyType::None);
        assert_eq!(columns[10].primary_key, PrimaryKeyType::None);
        assert_eq!(columns[11].primary_key, PrimaryKeyType::None);
        assert_eq!(columns[12].primary_key, PrimaryKeyType::None);
        assert_eq!(columns[0].unique, false);
        assert_eq!(columns[1].unique, false);
        assert_eq!(columns[2].unique, false);
        assert_eq!(columns[3].unique, false);
        assert_eq!(columns[4].unique, false);
        assert_eq!(columns[5].unique, false);
        assert_eq!(columns[6].unique, false);
        assert_eq!(columns[7].unique, false);
        assert_eq!(columns[8].unique, false);
        assert_eq!(columns[9].unique, false);
        assert_eq!(columns[10].unique, false);
        assert_eq!(columns[11].unique, false);
        assert_eq!(columns[12].unique, false);
        assert_eq!(columns[0].on_delete, None);
        assert_eq!(columns[1].on_delete, Some(Action::SetDefault));
        assert_eq!(columns[2].on_delete, None);
        assert_eq!(columns[3].on_delete, None);
        assert_eq!(columns[4].on_delete, None);
        assert_eq!(columns[5].on_delete, None);
        assert_eq!(columns[6].on_delete, None);
        assert_eq!(columns[7].on_delete, None);
        assert_eq!(columns[8].on_delete, None);
        assert_eq!(columns[9].on_delete, None);
        assert_eq!(columns[10].on_delete, None);
        assert_eq!(columns[11].on_delete, None);
        assert_eq!(columns[12].on_delete, None);
        assert_eq!(columns[0].on_update, None);
        assert_eq!(columns[1].on_update, Some(Action::Restrict));
        assert_eq!(columns[2].on_update, None);
        assert_eq!(columns[3].on_update, None);
        assert_eq!(columns[4].on_update, None);
        assert_eq!(columns[5].on_update, None);
        assert_eq!(columns[6].on_update, None);
        assert_eq!(columns[7].on_update, None);
        assert_eq!(columns[8].on_update, None);
        assert_eq!(columns[9].on_update, None);
        assert_eq!(columns[10].on_update, None);
        assert_eq!(columns[11].on_update, None);
        assert_eq!(columns[12].on_update, None);
        assert_eq!(columns[0].passive, false);
        assert_eq!(columns[1].passive, false);
        assert_eq!(columns[2].passive, false);
        assert_eq!(columns[3].passive, false);
        assert_eq!(columns[4].passive, false);
        assert_eq!(columns[5].passive, false);
        assert_eq!(columns[6].passive, true);
        assert_eq!(columns[7].passive, false);
        assert_eq!(columns[8].passive, false);
        assert_eq!(columns[9].passive, false);
        assert_eq!(columns[10].passive, false);
        assert_eq!(columns[11].passive, false);
        assert_eq!(columns[12].passive, false);
    }

    #[test]
    fn test_trade_create_table() {
        let mut query = DynQuery::default();
        WRITER.write_create_table::<Trade>(&mut query, false);
        assert_eq!(
            query.as_str(),
            indoc! {r#"
                CREATE TABLE "trading.company"."trade_execution" (
                "trade_id" UBIGINT,
                "order_id" UUID NOT NULL DEFAULT '241d362d-797e-4769-b3f6-412440c8cf68',
                "symbol" VARCHAR NOT NULL,
                "isin" CHAR(1)[12] NOT NULL,
                "price" DECIMAL(18,4) NOT NULL,
                "quantity" UINTEGER NOT NULL,
                "execution_time" TIMESTAMP,
                "currency" VARCHAR,
                "is_internalized" BOOLEAN NOT NULL,
                "venue" VARCHAR,
                "child_trade_ids" BIGINT[],
                "metadata" BLOB,
                "tags" MAP(VARCHAR,VARCHAR),
                PRIMARY KEY ("trade_id", "execution_time"),
                FOREIGN KEY ("order_id") REFERENCES "order"("id") ON DELETE SET DEFAULT ON UPDATE RESTRICT);
                COMMENT ON COLUMN "trading.company"."trade_execution"."symbol" IS 'Ticker symbol';
                COMMENT ON COLUMN "trading.company"."trade_execution"."venue" IS 'Exchange';
            "#}
            .trim()
        );
    }

    #[test]
    fn test_trade_drop_table() {
        let mut query = DynQuery::default();
        WRITER.write_drop_table::<Trade>(&mut query, true);
        assert_eq!(
            query.as_str(),
            r#"DROP TABLE IF EXISTS "trading.company"."trade_execution";"#
        );
    }

    #[test]
    fn test_trade_select() {
        let mut query = DynQuery::default();
        WRITER.write_select(
            &mut query,
            &QueryBuilder::new()
                .select(Trade::columns())
                .from(Trade::table())
                .where_condition(expr!(Trade::quantity >= 100 && Trade::price > 1000)),
        );
        assert_eq!(
            query.as_str(),
            indoc! {r#"
                SELECT "trade_id", "order_id", "symbol", "isin", "price", "quantity", "execution_time", "currency", "is_internalized", "venue", "child_trade_ids", "metadata", "tags"
                FROM "trading.company"."trade_execution"
                WHERE "quantity" >= 100 AND "price" > 1000;
            "#}
            .trim()
        );
    }

    #[test]
    fn test_trade_insert() {
        let mut docs = HashMap::new();
        docs.insert("contract.pdf".to_string(), vec![1, 2, 3, 4]);
        let employee = Trade::sample();
        let mut query = DynQuery::default();
        WRITER.write_insert(&mut query, [&employee], false);
        assert!(
            // Last part of the query (the map) is removed becaus order of keys is not defined. Value stores a HashMap
            query.as_str().starts_with(indoc! {r#"
                INSERT INTO "trading.company"."trade_execution" ("trade_id", "order_id", "symbol", "isin", "price", "quantity", "execution_time", "currency", "is_internalized", "venue", "child_trade_ids", "metadata", "tags") VALUES
                (46923, '550e8400-e29b-41d4-a716-446655440000', 'RIVN', ['U','S','7','6','9','5','4','A','1','0','3','4'], 12.26, 500, '2025-06-07T14:32:00.0', 'USD', true, 'NASDAQ', [36209,85320], '\x4D\x65\x74\x61\x64\x61\x74\x61\x20\x42\x79\x74\x65\x73',
            "#}.trim())
    );
    }

    #[test]
    fn test_trade_delete() {
        let mut query = DynQuery::default();
        WRITER.write_delete::<Trade>(&mut query, expr!(Trade::trade == 68391));
        assert_eq!(
            query.as_str(),
            indoc! {r#"
                DELETE FROM "trading.company"."trade_execution"
                WHERE "trade_id" = 68391;
            "#}
            .trim()
        );
    }
}
