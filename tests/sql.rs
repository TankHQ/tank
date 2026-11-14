#[cfg(test)]
mod fixtures;

#[cfg(test)]
mod tests {
    use crate::fixtures::DRIVER;
    use indoc::indoc;
    use rust_decimal::Decimal;
    use std::{collections::BTreeMap, str::FromStr};
    use tank::{
        BinaryOp, BinaryOpType, ColumnDef, Context, DynQuery, Entity, Expression, Fragment,
        GenericSqlWriter, Operand, Sql, SqlCoreWriter, SqlExpressionWriter, SqlValueWriter, Value,
        expr,
    };
    use time::{Date, Month, OffsetDateTime, PrimitiveDateTime, Time, UtcOffset};
    use uuid::Uuid;

    const WRITER: GenericSqlWriter = GenericSqlWriter {};

    #[test]
    fn sql_simple_table() {
        #[derive(Default, Entity)]
        #[tank(name = "my_table")]
        struct Table {
            #[tank(name = "special_column")]
            _first_column: Option<String>,
            _second_column: Box<f64>,
            #[tank(primary_key)]
            _third_column: i32,
        }
        // CREATE TABLE
        {
            let query = Sql().create_table(Table::table()).build(&DRIVER);
            assert_eq!(
                query.as_str(),
                indoc! {r#"
                    CREATE TABLE "my_table" (
                    "special_column" TEXT,
                    "second_column" DOUBLE NOT NULL,
                    "third_column" INTEGER PRIMARY KEY);
                "#}
                .trim()
            );
        }
        // DROP TABLE IF EXISTS
        {
            let query = Sql().drop_table(Table::table()).if_exists().build(&DRIVER);
            assert_eq!(query.as_str(), r#"DROP TABLE IF EXISTS "my_table";"#);
        }
        // SELECT
        {
            let query = Sql()
                .select(Table::columns())
                .from(Table::table())
                .where_expr(expr!(
                    Table::_second_column < 100 && Table::_first_column == "OK"
                ))
                .build(&DRIVER);
            assert_eq!(
                query.as_str(),
                indoc! {r#"
                    SELECT "special_column", "second_column", "third_column"
                    FROM "my_table"
                    WHERE "second_column" < 100 AND "special_column" = 'OK';
                "#}
                .trim()
            );
        }
        // INSERT single
        {
            let table = Table::default();
            let query = Sql()
                .insert_into(Table::table())
                .values(&[table])
                .build(&DRIVER);
            assert_eq!(
                query.as_str(),
                indoc! {r#"
                    INSERT INTO "my_table" ("special_column", "second_column", "third_column") VALUES
                    (NULL, 0.0, 0);
                "#}.trim()
            );
        }
        // INSERT single with upsert
        {
            let table = Table {
                _first_column: Some("hello".into()),
                _second_column: 512.5.into(),
                _third_column: 478,
            };
            let query = Sql()
                .insert_into(Table::table())
                .values(&[table])
                .update()
                .build(&DRIVER);
            assert_eq!(
                query.as_str(),
                indoc! {r#"
                    INSERT INTO "my_table" ("special_column", "second_column", "third_column") VALUES
                    ('hello', 512.5, 478)
                    ON CONFLICT ("third_column") DO UPDATE SET
                    "special_column" = EXCLUDED."special_column",
                    "second_column" = EXCLUDED."second_column";
                "#}.trim()
            );
        }
    }

    #[test]
    fn sql_cart() {
        #[derive(Entity)]
        #[tank(name = "cart")]
        struct Cart {
            #[tank(primary_key)]
            id: Box<u32>,
            user_id: Uuid,
            created_at: PrimitiveDateTime,
            items: Vec<Uuid>,
            is_active: bool,
            total_price: Decimal,
        }
        // CREATE TABLE IF NOT EXISTS
        {
            let query = Sql()
                .create_table(Cart::table())
                .if_not_exists()
                .build(&DRIVER);
            assert_eq!(
                query.as_str(),
                indoc! {r#"
                    CREATE TABLE IF NOT EXISTS "cart" (
                    "id" UINTEGER PRIMARY KEY,
                    "user_id" UUID NOT NULL,
                    "created_at" TIMESTAMP NOT NULL,
                    "items" UUID[] NOT NULL,
                    "is_active" BOOLEAN NOT NULL,
                    "total_price" DECIMAL NOT NULL);
                "#}
                .trim()
            );
        }
        // DROP TABLE
        {
            let query = Sql().drop_table(Cart::table()).build(&DRIVER);
            assert_eq!(query.as_str(), r#"DROP TABLE "cart";"#);
        }
        // SELECT with LIMIT
        {
            let query = Sql()
                .select(Cart::columns())
                .from(Cart::table())
                .where_expr(expr!(Cart::is_active == true && Cart::total_price > 100))
                .limit(Some(1000))
                .build(&DRIVER);
            assert_eq!(
                query.as_str(),
                indoc! {r#"
                    SELECT "id", "user_id", "created_at", "items", "is_active", "total_price"
                    FROM "cart"
                    WHERE "is_active" = true AND "total_price" > 100
                    LIMIT 1000;
                "#}
                .trim()
            );
        }
        // INSERT single
        {
            let cart = Cart {
                id: Default::default(),
                user_id: Uuid::from_str("b0fa843f-6ae4-4a16-a13c-ddf5512f3bb2").unwrap(),
                created_at: PrimitiveDateTime::new(
                    Date::from_calendar_date(2025, Month::May, 31).unwrap(),
                    Time::from_hms(12, 30, 11).unwrap(),
                ),
                items: Default::default(),
                is_active: Default::default(),
                total_price: Default::default(),
            };
            let query = Sql()
                .insert_into(Cart::table())
                .values(&[cart])
                .build(&DRIVER);
            assert_eq!(
                query.as_str(),
                indoc! {r#"
                    INSERT INTO "cart" ("id", "user_id", "created_at", "items", "is_active", "total_price") VALUES
                    (0, 'b0fa843f-6ae4-4a16-a13c-ddf5512f3bb2', '2025-05-31 12:30:11.0', [], false, 0);
                "#}.trim()
            );
        }
        // INSERT single with upsert
        {
            let cart = Cart {
                id: Default::default(),
                user_id: Uuid::parse_str("22222222-2222-2222-2222-222222222222").unwrap(),
                created_at: PrimitiveDateTime::new(
                    Date::from_calendar_date(2020, Month::January, 19).unwrap(),
                    Time::from_hms(19, 26, 54).unwrap(),
                ),
                items: vec![
                    Uuid::from_str("30c68157-5c43-452d-8caa-300776260b3f").unwrap(),
                    Uuid::from_str("772ba17d-b3bd-4771-a34e-2926d4731b44").unwrap(),
                    Uuid::from_str("3d4e9cb1-021f-48ab-848e-6c97d0ad670d").unwrap(),
                ],
                is_active: true,
                total_price: Decimal::new(2599, 2), // 25.99
            };
            let query = Sql()
                .insert_into(Cart::table())
                .values(&[cart])
                .update()
                .build(&DRIVER);
            assert_eq!(
                query.as_str(),
                indoc! {r#"
                    INSERT INTO "cart" ("id", "user_id", "created_at", "items", "is_active", "total_price") VALUES
                    (0, '22222222-2222-2222-2222-222222222222', '2020-01-19 19:26:54.0', ['30c68157-5c43-452d-8caa-300776260b3f','772ba17d-b3bd-4771-a34e-2926d4731b44','3d4e9cb1-021f-48ab-848e-6c97d0ad670d'], true, 25.99)
                    ON CONFLICT ("id") DO UPDATE SET
                    "user_id" = EXCLUDED."user_id",
                    "created_at" = EXCLUDED."created_at",
                    "items" = EXCLUDED."items",
                    "is_active" = EXCLUDED."is_active",
                    "total_price" = EXCLUDED."total_price";
                "#}.trim()
            );
        }
    }

    #[test]
    fn timestamptz_negative_offset() {
        let ts = OffsetDateTime::new_in_offset(
            Date::from_calendar_date(2025, Month::June, 15).unwrap(),
            Time::from_hms(14, 30, 0).unwrap(),
            UtcOffset::from_hms(-5, -30, 0).unwrap(),
        );
        let mut out = Default::default();
        let mut ctx = Context::new(Fragment::SqlSelect, false);
        WRITER.write_value(&mut ctx, &mut out, &Value::TimestampWithTimezone(Some(ts)));
        let sql = out.as_str();
        assert!(
            sql.contains("-05:30"),
            "Expected -05:30 in timestamptz output, got: {sql}"
        );

        let ts = OffsetDateTime::new_in_offset(
            Date::from_calendar_date(2025, Month::June, 15).unwrap(),
            Time::from_hms(14, 30, 0).unwrap(),
            UtcOffset::from_hms(5, 45, 0).unwrap(),
        );
        let mut out = Default::default();
        WRITER.write_value(&mut ctx, &mut out, &Value::TimestampWithTimezone(Some(ts)));
        let sql = out.as_str();
        assert!(
            sql.contains("+05:45"),
            "Expected +05:45 in timestamptz output, got: {sql}"
        );

        let ts = OffsetDateTime::new_in_offset(
            Date::from_calendar_date(2025, Month::June, 15).unwrap(),
            Time::from_hms(14, 30, 0).unwrap(),
            UtcOffset::from_hms(0, -30, 0).unwrap(),
        );
        let mut out = Default::default();
        WRITER.write_value(&mut ctx, &mut out, &Value::TimestampWithTimezone(Some(ts)));
        let sql = out.as_str();
        assert!(
            sql.contains("-00:30"),
            "Expected -00:30 for sub-hour negative offset, got: {sql}"
        );
    }

    #[test]
    fn sql_writer_types_operators() {
        let mut ctx = Context::new(Fragment::SqlCreateTable, false);
        for (value, expected) in [
            (Value::Int128(None), "HUGEINT"),
            (Value::UInt8(None), "UTINYINT"),
            (Value::UInt128(None), "UHUGEINT"),
            (Value::TimestampWithTimezone(None), "TIMESTAMPTZ"),
            (Value::Json(None), "JSON"),
            (Value::Null, ""),
        ] {
            let mut out = DynQuery::default();
            WRITER.write_column_type(&mut ctx, &mut out, &value);
            assert_eq!(out.as_str(), expected);
        }

        let mut out = DynQuery::default();
        WRITER.write_column_overridden_type(
            &mut ctx,
            &mut out,
            &ColumnDef::default(),
            &BTreeMap::from([("", "CUSTOM")]),
        );
        assert_eq!(out.as_str(), "CUSTOM");

        for (value, expected) in [
            (Value::UInt8(Some(1)), "1"),
            (Value::UInt128(Some(Box::new(5))), "5"),
        ] {
            let mut out = DynQuery::default();
            WRITER.write_value(&mut Context::empty(), &mut out, &value);
            assert_eq!(out.as_str(), expected);
        }

        let tuple: [&dyn Expression; 2] = [&Operand::LitInt(1), &Operand::LitInt(2)];
        for (operand, expected) in [
            (Operand::LitTuple(&tuple), "(1,2)"),
            (Operand::Value(&Value::Int32(Some(9))), "9"),
        ] {
            let mut out = DynQuery::default();
            WRITER.write_operand(&mut ctx, &mut out, &operand);
            assert_eq!(out.as_str(), expected);
        }

        let arr = Value::Array(
            Some(vec![Value::Int32(Some(1)), Value::Int32(Some(2))].into()),
            Box::new(tank::CollectionType {
                element: Value::Int32(None),
                len: 2,
            }),
        );
        let mut out = DynQuery::default();
        WRITER.write_value(&mut Context::empty(), &mut out, &arr);
        assert_eq!(out.as_str(), "[1,2]");

        let list = Value::List(
            Some(vec![Value::Varchar(Some("a".into()))].into()),
            Box::new(tank::CollectionType {
                element: Value::Varchar(None),
                len: 0,
            }),
        );
        let mut out = DynQuery::default();
        WRITER.write_value(&mut Context::empty(), &mut out, &list);
        assert_eq!(out.as_str(), "[a]");

        let mut map = std::collections::HashMap::new();
        map.insert(Value::Varchar(Some("k".into())), Value::Int32(Some(1)));
        let map = Value::Map(
            Some(Box::new(map)),
            Box::new(tank::MapType {
                key: Value::Varchar(None),
                value: Value::Int32(None),
            }),
        );
        let mut out = DynQuery::default();
        WRITER.write_value(&mut Context::empty(), &mut out, &map);
        assert_eq!(out.as_str(), "{k:1}");

        let strukt = Value::Struct(
            Some(vec![("f".into(), Value::Int32(Some(1)))].into()),
            Box::new(tank::StructType {
                fields: vec![],
                name: tank::TableRef::new("t".into()),
            }),
        );
        let mut out = DynQuery::default();
        WRITER.write_value(&mut Context::empty(), &mut out, &strukt);
        assert_eq!(out.as_str(), "{f:1}");

        let args: &[&dyn Expression] = &[];
        let lit_list: [&dyn Expression; 2] = [&Operand::LitInt(1), &Operand::LitInt(2)];
        for (operand, expected) in [
            (Operand::Call("NOW", args), "NOW()"),
            (Operand::LitList(&lit_list), "[1,2]"),
            (Operand::LitIdent("ident"), "ident"),
        ] {
            let mut out = DynQuery::default();
            WRITER.write_operand(&mut Context::empty(), &mut out, &operand);
            assert_eq!(out.as_str(), expected);
        }

        // CAST goes through the dedicated cast fragment.
        let mut out = DynQuery::default();
        BinaryOp {
            op: BinaryOpType::Cast,
            lhs: Operand::LitInt(1),
            rhs: Operand::Type(Value::Int64(None)),
        }
        .write_query(
            &WRITER,
            &mut Context::new(Fragment::SqlSelect, false),
            &mut out,
        );
        assert_eq!(out.as_str(), "CAST(1 AS BIGINT)");

        for (op, expected) in [
            (BinaryOpType::Is, "a IS 'x'"),
            (BinaryOpType::Regexp, "a REGEXP 'x'"),
            (BinaryOpType::NotRegexp, "a NOT REGEXP 'x'"),
            (BinaryOpType::Glob, "a GLOB 'x'"),
            (BinaryOpType::ShiftLeft, "a << 'x'"),
        ] {
            let mut out = DynQuery::default();
            (BinaryOp {
                op,
                lhs: Operand::LitIdent("a"),
                rhs: Operand::LitStr("x"),
            })
            .write_query(
                &WRITER,
                &mut Context::new(Fragment::SqlSelect, false),
                &mut out,
            );
            assert_eq!(out.as_str(), expected);
        }
    }
}
