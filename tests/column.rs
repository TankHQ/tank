#[cfg(test)]
mod tests {
    use quote::ToTokens;
    use std::{borrow::Cow, collections::HashSet, sync::Arc};
    use tank::{
        Action, AsValue, ColumnDef, ColumnRef, Context, DefaultValueType, DynQuery, Entity,
        Expression, Fragment, GenericSqlWriter, IsAsterisk, OpPrecedence, PrimaryKeyType, Row,
        Value,
    };

    #[test]
    fn column_conversions() {
        #[derive(Entity)]
        #[tank(schema = "the_schema", name = "my_table")]
        struct Entity {
            #[tank(name = "solo_column")]
            col: i32,
        }

        let column: &ColumnDef = &Entity::columns()[0];
        assert_eq!(column.name(), "solo_column");
        assert_eq!(column.table(), "my_table");
        assert_eq!(column.schema(), "the_schema");

        let col_ref: &ColumnRef = column.into();
        assert_eq!(col_ref.name, "solo_column");
        assert_eq!(col_ref.table, "my_table");
        assert_eq!(col_ref.schema, "the_schema");

        let my_column = ColumnRef::new("my_column".into());
        assert_eq!(my_column.name, "my_column");
        assert_eq!(my_column.table, "");
        assert_eq!(my_column.schema, "");
    }

    #[test]
    fn column_ref_table() {
        let table = ColumnRef {
            name: "id".into(),
            table: "users".into(),
            schema: "public".into(),
        }
        .table();
        assert_eq!(table.name, "users");
        assert_eq!(table.schema, "public");
        assert_eq!(table.alias, "");
    }

    #[test]
    fn column_def_equality() {
        #[derive(Entity)]
        struct TestTable {
            id: i32,
            name: String,
        }
        let columns = TestTable::columns();
        assert_ne!(columns[0], columns[1]);
        assert_eq!(columns[0], columns[0]);
        let mut set = HashSet::new();
        set.insert(&columns[0]);
        set.insert(&columns[1]);
        assert_eq!(set.len(), 2);
        set.insert(&columns[0]);
        assert_eq!(set.len(), 2);
    }

    #[test]
    fn default_value_type() {
        #[derive(Entity)]
        struct Defaults {
            #[tank(default = 0)]
            a: i64,
            #[tank(default = 1.5)]
            b: f64,
            #[tank(default = 'x')]
            c: char,
        }
        let columns = Defaults::columns();
        assert!(matches!(
            columns[0].default,
            DefaultValueType::Value(Value::Int64(Some(0)))
        ));
        assert!(matches!(
            columns[1].default,
            DefaultValueType::Value(Value::Float64(Some(v))) if v == 1.5
        ));
        assert!(matches!(
            columns[2].default,
            DefaultValueType::Value(Value::Char(Some('x')))
        ));

        let writer = GenericSqlWriter::new();
        let default_value = DefaultValueType::None;
        assert!(!default_value.is_set());
        assert_eq!(default_value.precedence(&writer), 0);
        assert_eq!(format!("{default_value:?}"), "None");

        let value = DefaultValueType::Value(Value::Int64(Some(7)));
        assert!(value.is_set());
        assert_eq!(format!("{value:?}"), "Value(Int64(Some(7)))");
        let mut out = DynQuery::default();
        value.write_query(&writer, &mut Context::empty(), &mut out);
        assert_eq!(out.as_str(), "7");

        let expression = DefaultValueType::Expression(Arc::new(Value::Boolean(Some(true))));
        assert!(expression.is_set());
        assert_eq!(format!("{expression:?}"), "Expression(\"..\")");
        assert_eq!(expression.precedence(&writer), 0);
        let mut out = DynQuery::default();
        expression.write_query(&writer, &mut Context::empty(), &mut out);
        assert_eq!(out.as_str(), "true");

        assert!(matches!(
            DefaultValueType::from(true),
            DefaultValueType::Value(Value::Boolean(Some(true)))
        ));
        assert!(matches!(
            DefaultValueType::from("x"),
            DefaultValueType::Value(Value::Varchar(Some(Cow::Borrowed("x"))))
        ));
        assert!(matches!(
            DefaultValueType::from(1_i64),
            DefaultValueType::Value(Value::Int64(Some(1)))
        ));
        assert!(matches!(
            DefaultValueType::from(1.5_f64),
            DefaultValueType::Value(Value::Float64(Some(_)))
        ));
        assert!(matches!(
            DefaultValueType::from('z'),
            DefaultValueType::Value(Value::Char(Some('z')))
        ));
        assert!(matches!(
            DefaultValueType::from(Value::Null),
            DefaultValueType::Value(Value::Null)
        ));
    }

    #[test]
    fn column_accepts_visitor() {
        #[derive(Entity)]
        struct VTable {
            id: i32,
        }
        let column = &VTable::columns()[0];
        let mut out = DynQuery::default();
        let mut context = Context::empty();
        assert!(!column.accept_visitor(
            &mut IsAsterisk,
            &GenericSqlWriter::new(),
            &mut context,
            &mut out
        ));
        column.write_query(
            &GenericSqlWriter::new(),
            &mut Context::fragment(Fragment::SqlCreateTable),
            &mut out,
        );
        assert_eq!(out.as_str(), "\"id\"");
    }

    #[test]
    fn primary_key_action_tokens() {
        assert_eq!(
            PrimaryKeyType::PrimaryKey.to_token_stream().to_string(),
            ":: tank :: PrimaryKeyType :: PrimaryKey"
        );
        assert_eq!(
            PrimaryKeyType::PartOfPrimaryKey
                .to_token_stream()
                .to_string(),
            ":: tank :: PrimaryKeyType :: PartOfPrimaryKey"
        );
        assert_eq!(
            PrimaryKeyType::None.to_token_stream().to_string(),
            ":: tank :: PrimaryKeyType :: None"
        );
        assert_eq!(
            Action::NoAction.to_token_stream().to_string(),
            ":: tank :: Action :: NoAction"
        );
        assert_eq!(
            Action::Restrict.to_token_stream().to_string(),
            ":: tank :: Action :: Restrict"
        );
        assert_eq!(
            Action::Cascade.to_token_stream().to_string(),
            ":: tank :: Action :: Cascade"
        );
        assert_eq!(
            Action::SetNull.to_token_stream().to_string(),
            ":: tank :: Action :: SetNull"
        );
        assert_eq!(
            Action::SetDefault.to_token_stream().to_string(),
            ":: tank :: Action :: SetDefault"
        );
    }

    #[test]
    fn entity_row() {
        #[derive(Entity)]
        struct RTable {
            id: i32,
            name: String,
        }
        let entity = RTable {
            id: 7,
            name: "x".into(),
        };
        assert_eq!(entity.row_values().len(), 2);
        assert_eq!(entity.row_values().next(), Some(7.as_value()));
        let row = entity.row();
        assert_eq!(row.labels(), &["id", "name"]);
        assert_eq!(*row.values().nth(1).unwrap(), "x".as_value());
    }
}
